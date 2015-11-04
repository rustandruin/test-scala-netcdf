package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

import scala.sys.process._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Try}
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import java.nio.file.{Files, Paths}
import java.io.File

import ucar.nc2.{NetcdfFile, Dimension, Variable}
import ucar.ma2.DataType
import ucar.ma2.{Array => netcdfArray}

import breeze.linalg.{DenseVector, DenseMatrix}

object convertGribToParquet extends Logging {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("convertGribToParquet")
    val sc = new SparkContext(conf)
    sys.addShutdownHook({ sc.stop() })
    appMain(sc, args)
  }

  // first arg: file containing the grb2 files to work on, one per line
  // second arg: comma-separated list of variables to use
  // third arg: directory to place the output in
  def appMain(sc : SparkContext, args: Array[String]) {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val fnameRDD = sc.parallelize(sc.textFile(args(0)).collect())
    val results = fnameRDD.mapPartitionsWithIndex((index, fnames) => convertToParquet(fnames, args(1), index)).toDF
    results.saveAsParquetFile(args(2))
  }

  // given a group of filenames and the names of the variables to extract, extracts them 
  def convertToParquet( fnameGen: Iterator[String], fieldnames: String, index: Int) : Iterator[Tuple3[String, Array[Float], Array[Boolean]]] = {
    val fnames = fnameGen.toArray
    val results = ArrayBuffer[Tuple3[String, Array[Float], Array[Boolean]]]()

    logInfo("This partition contains files: " + fnames.mkString(","))

    val randGen = new Random(index)
    val tempfname = "%s.%s".format(randGen.nextInt(64), "nc")
    for(curfname <- fnames) {breakable{

      logInfo(s"Converting ${curfname} from GRIB2 to NetCDF, extracting desired variables")
      try {
        // convert from grib to netcdf, preserving the variables we care about 
        val result1 = s"ncl_convert2nc ${curfname} -v ${fieldnames}".!!
        logInfo(s"Conversion done")

        // stupid hacky way to get the name of the netcdf file resulting from the ncl_convert2nc command
        // it is located in the current directory
        val ncfname = ("""\w+""".r findFirstIn Paths.get(curfname).getFileName.toString).get + ".nc"
        Files.move(Paths.get(ncfname), Paths.get(tempfname))
      } catch {
        case t : Throwable => {
          logInfo(s"Error converting ${curfname}: " + t.toString)
          break
        }
      }

      // flatten all the variable we care about into one long vector and a mask of missing values
      logInfo("Extracting desired information from ${ncfname.getFileName.toString}")
      try {
        val (rowvector, indices) = vectorizeVariables(tempfname, fieldnames)
        results += Tuple3(curfname, rowvector, indices)
      } catch {
        case NonFatal(t) => logInfo(s"Error extracting variables from ${curfname}: " + t.toString) 
      }

      Files.delete(Paths.get(tempfname))
    }}
    results.toIterator
  }

  // Given a filename for a netcdf dataset, and a common-separated list of names of variables,
  // extracts those variables, flattens them into a long vector and drops missing values
  // returns this vector, and the flattened array of boolean masks indicating which values were dropped 
  def vectorizeVariables(fname: String, fieldnames: String) : Tuple2[Array[Float], Array[Boolean]] = {
    val fields = fieldnames.split(",") 
    val valueaccumulator = ArrayBuffer[Float]()
    val maskaccumulator = ArrayBuffer[Boolean]()

    val infileTry = Try(NetcdfFile.open(fname))
    if (!infileTry.isSuccess) {
      // record an error and fail this file
    }
    val fin = infileTry.get

    for (field <- fields) {
      val variable = fin.findVariable(field)
      val rank = variable.getRank
      val dimensions = variable.getDimensions
      val datatype = variable.getDataType.toString
      
      if (rank == 2) {
        val rows = variable.getDimension(0).getLength 
        val cols = variable.getDimension(1).getLength
        var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[Float]
        val vectorizedmatrix = variable.read.copyTo1DJavaArray.asInstanceOf[Array[Float]]
        val missingmask = vectorizedmatrix.map(_ == fillValue)
        val missingcount = missingmask.count(_ == true)
        valueaccumulator ++= vectorizedmatrix
        maskaccumulator ++= missingmask
        logInfo(s"Loaded $field, a 2D $datatype field with dimensions $dimensions, and $missingcount missing values")
      } 
      else if (rank == 3) {
        val depth = variable.getDimension(0).getLength
        val rows = variable.getDimension(1).getLength
        val cols = variable.getDimension(2).getLength
        var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[Float]
        val vectorizedtensor = variable.read.copyTo1DJavaArray.asInstanceOf[Array[Float]]
        val missingmask = vectorizedtensor.map(_ == fillValue)
        val missingcount = missingmask.count(_ == true)
        valueaccumulator ++= vectorizedtensor
        maskaccumulator ++= missingmask
        logInfo(s"Loaded $field, a 3D $datatype field with dimensions $dimensions, and $missingcount missing values")
      } 
      else {
        logInfo(s"don't know what this is: $field")
      }
    }

    fin.close
    (valueaccumulator.toArray, maskaccumulator.toArray)
  }
}

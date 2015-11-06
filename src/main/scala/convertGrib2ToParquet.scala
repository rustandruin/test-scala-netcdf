package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

import scala.sys.process._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import java.nio.file.{Files, Paths}
import java.io.{File, FileInputStream, FileOutputStream}
import java.util.concurrent.ThreadLocalRandom

import ucar.nc2.{NetcdfFile, Dimension, Variable}
import ucar.ma2.DataType
import ucar.ma2.{Array => netcdfArray}

import breeze.linalg.{DenseVector, DenseMatrix}

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils

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
    val tempfname = "%s.%s".format(ThreadLocalRandom.current.nextLong(Long.MaxValue), "nc")

    logInfo("This partition contains files: " + fnames.mkString(","))
    logInfo(s"Using temporary file name : $tempfname")

    // process non-tar files
    for(curfname <- fnames.filter(s => !s.endsWith(".tar"))) {
      val tempresult = getDataFromFile(curfname, fieldnames, tempfname) 
      if (tempresult.isDefined) { results += tempresult.get}
    }

    // process tar files
    for(curfname <- fnames.filter(s => s.endsWith(".tar"))){
      logInfo(s"Processing tar file: ${curfname}")
      val archive = new TarArchiveInputStream(new FileInputStream(curfname)) 
      var curentry = archive.getNextTarEntry
      val tempfname2 = "%s.%s".format(ThreadLocalRandom.current.nextLong(Long.MaxValue), "grb2")

      while ( curentry != null) {
        logInfo(s"Processing ${curentry.getName} in ${curfname}")

        try {
          val tempout = new FileOutputStream( new File(tempfname2))
          IOUtils.copy(archive, tempout)
          tempout.close()
          val tempresult = getDataFromFile(tempfname2, fieldnames, tempfname) 
          if (tempresult.isDefined) { results += tempresult.get}
        } catch {
          case e : Throwable => logInfo(s"Error in extracting and processing ${curentry.getName} from ${curfname}")
        } finally {
          // always delete the temporary file
          val tempfile = new File(tempfname2)
          if ( tempfile.exists() ) {
            Files.delete(tempfile.toPath)
          }
        }
        curentry = archive.getNextTarEntry
      }
    }

    results.toIterator
  }

  // given the name of an input grib file, a string consisting of comma-separated variable names, and a name for temporary file,
  // returns the name of the file, a vector containing the values of the desired variables, and a mask indicating which entries of
  // the desired variables are missing
  def getDataFromFile(inputfname: String, fieldnames: String, tempfname: String) : Option[Tuple3[String, Array[Float], Array[Boolean]]] = {

    var result : Option[Tuple3[String, Array[Float], Array[Boolean]]] = None

    logInfo(s"Converting ${inputfname} from GRIB2 to NetCDF, extracting desired variables")
    try {
      // convert from grib to netcdf, preserving the variables we care about 
      val result1 = s"ncl_convert2nc ${inputfname} -v ${fieldnames}".!!
      logInfo(s"Conversion done")

      // stupid hacky way to get the name of the netcdf file resulting from the ncl_convert2nc command
      // it is located in the current directory
      val ncfname = ("""\w+""".r findFirstIn Paths.get(inputfname).getFileName.toString).get + ".nc"
      Files.move(Paths.get(ncfname), Paths.get(tempfname))
    } catch {
      case t : Throwable => {
        logInfo(s"Error converting ${inputfname}: " + t.toString)
        break
      }
    }

    // flatten all the variable we care about into one long vector and a mask of missing values
    logInfo(s"Extracting desired information from ${inputfname}")
    try {
      val (rowvector, indices) = vectorizeVariables(tempfname, fieldnames)
      result = Some(inputfname, rowvector, indices)
    } catch {
      case NonFatal(t) => logInfo(s"Error extracting variables from ${inputfname}: " + t.toString) 
    }

    Files.delete(Paths.get(tempfname))
    logInfo(s"Deleted temporary file $tempfname")

    result
  }

  // Given a filename for a netcdf dataset, and a common-separated list of names of variables,
  // extracts those variables, flattens them into a long vector and drops missing values
  // returns this vector, and the flattened array of boolean masks indicating which values were missing 
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

    logInfo(s"Done loading variables")
    fin.close
    (valueaccumulator.toArray, maskaccumulator.toArray)
  }
}

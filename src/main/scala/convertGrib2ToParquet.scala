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
import scala.math.ceil

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

  // first arg: file containing the tars with grb2 files to work on, one per line
  // second arg: comma-separated list of variables to use
  // third arg: directory to place the output in
  def appMain(sc : SparkContext, args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val filelistfname = args(0)
    val variablenames = args(1)
    val outputdir = args(2)

    val fnames = sc.parallelize(sc.textFile(filelistfname).collect()).flatMap( 
      fname => 
        if (!fname.endsWith(".tar")) { 
          List((fname, 0)) 
        } else {
          val archive = new TarArchiveInputStream(new FileInputStream(fname))
          var numentries = 0
          while ( archive.getNextTarEntry != null) { numentries += 1}
          List.range(0, numentries).map(s => (fname, s))
        }
      ).collect()

    // ensure that the data extracted from the files in each partition can be held in memory on the
    // executors and the driver
    val maxfilesperpartition = 1
    val fnamesRDD = sc.parallelize(fnames, ceil(fnames.length.toFloat/maxfilesperpartition).toInt)

    val results = fnamesRDD.mapPartitionsWithIndex((index, fnames) => convertToParquet(fnames, variablenames, index)).toDF
    results.saveAsParquetFile(outputdir)
  }

  // given a group of filenames and the names of the variables to extract, extracts them 
  def convertToParquet( filepairsIter: Iterator[Tuple2[String, Int]], fieldnames: String, index: Int) : Iterator[Tuple2[String, Array[Float]]] = {
    val filepairs = filepairsIter.toArray
    val results = ArrayBuffer[Tuple2[String, Array[Float]]]()
    val tempfname = "%s.%s".format(ThreadLocalRandom.current.nextLong(Long.MaxValue), "nc")

    logInfo("This partition contains files: " + filepairs.map( pair => s"(${pair._1}, ${pair._2})").mkString(","))
    logInfo(s"Using temporary file name : $tempfname")

    // process non-tar files
    for(curpair <- filepairs.filter(pair => !(pair._1).endsWith(".tar"))) {
      val tempresult = getDataFromFile(curpair._1, fieldnames, tempfname) 
      if (tempresult.isDefined) { results += tempresult.get}
    }

    // process tar files
    for(curpair <- filepairs.filter(pair => (pair._1).endsWith(".tar"))){
      logInfo(s"Processing tar file: ${curpair._1}")
      val archive = new TarArchiveInputStream(new FileInputStream(curpair._1)) 
      var offset = 0
      while (offset < curpair._2) { offset += 1 }

      var curentry = archive.getNextTarEntry
      val tempfname2 = "%s.%s".format(ThreadLocalRandom.current.nextLong(Long.MaxValue), "grb2")

      logInfo(s"Processing ${curentry.getName} in ${curpair._1}")
      try {
        val tempout = new FileOutputStream( new File(tempfname2))
        IOUtils.copy(archive, tempout)
        tempout.close()
        val tempresult = getDataFromFile(tempfname2, fieldnames, tempfname) 
        if (tempresult.isDefined) { results += tempresult.get}
      } catch {
        case e : Throwable => logInfo(s"Error in extracting and processing ${curentry.getName} from ${curpair._1}")
      } finally {
        // always delete the temporary file
        val tempfile = new File(tempfname2)
        if ( tempfile.exists() ) {
          Files.delete(tempfile.toPath)
        }
      }
    }

    results.toIterator
  }

  // given the name of an input grib file, a string consisting of comma-separated variable names, and a name for temporary file,
  // returns the name of the file, a vector containing the values of the desired variables, and a mask indicating which entries of
  // the desired variables are missing
  def getDataFromFile(inputfname: String, fieldnames: String, tempfname: String) : Option[Tuple2[String, Array[Float]]] = {

    var result : Option[Tuple2[String, Array[Float]]] = None

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
      val rowvector = vectorizeVariables(tempfname, fieldnames)
      result = Some(inputfname, rowvector)
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
  def vectorizeVariables(fname: String, fieldnames: String) : Array[Float] = {
    val fields = fieldnames.split(",") 
    val valueaccumulator = ArrayBuffer[Float]()

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
        valueaccumulator ++= vectorizedmatrix
        logInfo(s"Loaded $field, a 2D $datatype field with dimensions $dimensions")
      } 
      else if (rank == 3) {
        val depth = variable.getDimension(0).getLength
        val rows = variable.getDimension(1).getLength
        val cols = variable.getDimension(2).getLength
        var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[Float]
        val vectorizedtensor = variable.read.copyTo1DJavaArray.asInstanceOf[Array[Float]]
        valueaccumulator ++= vectorizedtensor
        logInfo(s"Loaded $field, a 3D $datatype field with dimensions $dimensions")
      } 
      else {
        logInfo(s"don't know what this is: $field")
      }
    }

    logInfo(s"Done loading variables")
    fin.close
    valueaccumulator.toArray
  }
}

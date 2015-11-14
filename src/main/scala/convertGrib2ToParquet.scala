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

import org.apache.log4j.Logger

object convertGribToParquet {

  private val mylogger = Logger.getLogger("conversion")

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("convertGribToParquet")
    val sc = new SparkContext(conf)
    sys.addShutdownHook({ sc.stop() })
    appMain(sc, args)
  }

  def logInfo(message: String) {
    mylogger.info(message)
  }

  // first arg: file containing the tars with grb2 files to work on, one per line
  // second arg: comma-separated list of variables to use
  // third arg: directory to place the output in
  // fourth arg: number of files per partition
  def appMain(sc : SparkContext, args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val filelistfname = args(0)
    val variablenames = args(1)
    val outputdir = args(2)
    val numfilesperpartition = args(3).toInt

    logInfo(sc.textFile(filelistfname).collect().mkString(","))

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
    val fnamesRDD = sc.parallelize(fnames, ceil(fnames.length.toFloat/numfilesperpartition).toInt)

    logInfo(fnamesRDD.collect().map( pair => s"(${pair._1}, ${pair._2})").mkString(", "))

    val results = fnamesRDD.mapPartitionsWithIndex((index, fnames) => convertToParquet(fnames, variablenames, index))
    logInfo(results.collect.head._2.length.toString)
    results.toDF.saveAsParquetFile(outputdir)
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
      logInfo("processing a non-tar file")
      if (tempresult.isDefined) { results += Tuple2(curpair._1, tempresult.get)}
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
        if (tempresult.isDefined) { results += Tuple2(curentry.getName, tempresult.get)}
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

    logInfo(s"Results have length ${results.length}")
    results.toIterator
  }

  // given the name of an input grib file, a string consisting of comma-separated variable names, and a name for temporary file,
  // returns the name of the file, a vector containing the values of the desired variables, and a mask indicating which entries of
  // the desired variables are missing
  def getDataFromFile(inputfname: String, fieldnames: String, tempfname: String) : Option[Array[Float]] = {

    var result : Option[Array[Float]] = None
    var ncfname = ""

    logInfo(s"Converting ${inputfname} from GRIB2 to NetCDF, extracting desired variables")
    try {
      // convert from grib to netcdf, preserving the variables we care about 
      val result1 = s"ncl_convert2nc ${inputfname} -v ${fieldnames}".!!
      logInfo(s"Conversion done")

      // stupid hacky way to get the name of the netcdf file resulting from the ncl_convert2nc command
      // it is located in the current directory
      //ncfname = ("""\w+""".r findFirstIn Paths.get(inputfname).getFileName.toString).get + ".nc"
      ncfname = Paths.get((""".grb2$""".r replaceFirstIn (Paths.get(inputfname).getFileName.toString, ".nc"))).getFileName.toString
      Files.move(Paths.get(ncfname), Paths.get(tempfname))
    } catch {
      case t : Throwable => {
        logInfo(s"Error converting ${inputfname} (stored in $ncfname): " + t.toString)
	return result
      }
    }

    // flatten all the variable we care about into one long vector and a mask of missing values
    logInfo(s"Extracting desired information from ${inputfname}")
    try {
      val rowvector = vectorizeVariables(tempfname, fieldnames)
      result = Some(rowvector)
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
    val valueaccumulator = ArrayBuffer[Array[Float]]()

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
        valueaccumulator += vectorizedmatrix
        logInfo(s"Loaded $field, a 2D $datatype field with dimensions $dimensions")
      } 
      else if (rank == 3) {
        val depth = variable.getDimension(0).getLength
        val rows = variable.getDimension(1).getLength
        val cols = variable.getDimension(2).getLength
        var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[Float]
        val vectorizedtensor = variable.read.copyTo1DJavaArray.asInstanceOf[Array[Float]]
        valueaccumulator += vectorizedtensor
        logInfo(s"Loaded $field, a 3D $datatype field with dimensions $dimensions")
      } 
      else {
        logInfo(s"don't know what this is: $field")
      }
    }

    fin.close
    var result = valueaccumulator.toArray.flatten
    logInfo(s"returning an array of size ${4*result.length/math.pow(10, 9)} Gb")
    result
  }
}

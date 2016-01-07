// TODO:
// For complete generality and to avoid issues with missing data:
// 1. accept a list of variables
// 2. go over the data once to establish what grid points don't contain data for each of the variables and the sizes of each variable
// 3. group the variables into chunks of manageable size
// 4. convert each such group into an RDD and store to disk
// 5. join these RDDs and store back to disk
// Should keep track of missing data locations and ordering of the columns corresponding to different dates

package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import org.apache.spark.mllib.linalg.{DenseVector => SDV}

import scala.sys.process._
import scala.collection.mutable.{ArrayBuffer, WrappedArray}
import scala.util.Sorting.quickSort
import scala.collection.mutable.{Map => MutableMap}
import scala.util.Try
import scala.util.control.Breaks._
import scala.util.control.NonFatal
import scala.math.ceil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem => HFileSystem, Path => HPath}

import java.nio.file.{Files, Paths}
import java.io.{File, FileInputStream, FileOutputStream}
import java.util.concurrent.ThreadLocalRandom

import ucar.nc2.{NetcdfFile, Dimension, Variable}
import ucar.ma2.DataType
import ucar.ma2.{Array => netcdfArray}

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils

import com.databricks.spark.avro._

import org.apache.log4j.Logger

object transposeAvroToParquet {

  private val mylogger = Logger.getLogger("conversion")

  def main(args: Array[String]) = {
    val conf = new SparkConf()
       .setAppName("transposeAvroToParquet")
    val sc = new SparkContext(conf)
    sys.addShutdownHook({ sc.stop() })
    appMain(sc, args)
  }

  def logInfo(message: String) {
    mylogger.info(message)
    println(message)
  }

  // first arg: basefilename for the avro chunks
  // second arg: number of avro row chunk files
  // third arg: directory to place the output in
  // fourth arg: directory to store the names of the columns
  // fifth arg: number of columns to transpose at a time
  def appMain(sc : SparkContext, args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val baseavrofname = args(0)
    val numrowchunks = args(1).toInt
    val outputdir = args(2)
    val colnameFname = args(3)
    val transposechunksize = args(4).toInt

    // figure out the number of chunks we'll need to use to transpose A^T by transposing
    // transposechunksize columns at a time
    // recall the rows are stored as Tuple2[String, Array[Float]]

    val numcolsATranspose = sqlContext.read.avro(baseavrofname + "0").rdd.first.getAs[WrappedArray[Float]](1).length
    val numtransposechunks = ceil(numcolsATranspose.toFloat / transposechunksize).toInt
    logInfo(s"There are ${numcolsATranspose} columns in A^T, so at a transpose chunksize of ${transposechunksize}, we'll need to use ${numtransposechunks} chunks to transpose the matrix")

    // extract the row names so we can use them to order the rows of A^T, and create a lookup table that maps row names to row indices
    val rownames = ArrayBuffer.empty[String]
    for (rowChunkidx <- 0 until numrowchunks) {
      rownames ++= sqlContext.read.avro(baseavrofname + rowChunkidx.toInt).rdd.map(row => row.getAs[String](0)).collect
    }
    val sortedrownames = rownames.toArray
    quickSort(sortedrownames)

    val rowIndexLUT = MutableMap.empty[String, Int]
    for (rowidx <- 0 until sortedrownames.length) {
      rowIndexLUT += Tuple2(sortedrownames(rowidx), rowidx)
    }

    logInfo(s"There are ${sortedrownames.length} rows")
    sc.parallelize(sortedrownames).saveAsTextFile(colnameFname)

    /*

    // loop over the chunks of columns in A^T, transposing each chunk
    val nextColATranspose = 0
    for( transposechunkidx <- 0 until numtransposechunks) { 
      var startColIdxATranspose = nextColATranspose
      var endColIdxATranspose = { 
        if (transposechunkidx < (numtransposechunks - 1))
          startColIdxATranspose + transposechunksize - 1
        else
          numcolsATranspose - 1
      }
      nextColATranspose = endColIdxATranspose + 1


      // loop over the row chunks of A^T, extracting the relevant columns and concatenating them to get the column chunks from all of A^T
      // since Spark doesn't guarantee that the row ordering will be the same each time this loop is called, manually enforce it by ordering
      // the rows
      for( rowChunkIdx <- 0 until numrowchunks) {
        sqlContext.read.avro(baseavrofname + "0").rdd.

      }
    }
      nextColATranpose = 
  */
  }
}

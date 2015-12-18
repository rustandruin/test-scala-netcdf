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

import org.apache.log4j.Logger

object convertGribToParquet {

  private val mylogger = Logger.getLogger("conversion")
  private val tmpdir = new File("/mnt") // write temporary files here b/c the /tmp on spark-ec2's r3.8xlarge instances is too small to store the temp files

  def main(args: Array[String]) = {
    val conf = new SparkConf()
       .setAppName("convertGribToParquet")
       .setExecutorEnv("NCARG_ROOT", "/root/ncl")
       .setExecutorEnv("PATH", System.getenv("PATH") + ":/root/ncl/bin")
    val sc = new SparkContext(conf)
    sys.addShutdownHook({ sc.stop() })
    appMain(sc, args)
  }

  def logInfo(message: String) {
    mylogger.info(message)
    println(message)
  }

  // first arg: location of all the tars
  // second arg: file containing the tars with grb2 files to work on, one per line
  // third arg: comma-separated list of variables to use
  // fourth arg: directory to place the output in
  // fifth arg: number of files per partition
  def appMain(sc : SparkContext, args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val roothdfsdir = args(0)
    val filelistfname = args(1)
    val variablenames = args(2)
    val outputdir = args(3)
    val numfilesperpartition = args(4).toInt
    val divisionsize = 200000

    val filenamelist = sc.textFile(filelistfname).collect
    logInfo("Requested conversion of " + filenamelist.length.toString + " physical files")

    val filePairsFname = "allfilepairs"
    val fs = HFileSystem.get(sc.hadoopConfiguration)
    val fnames = if (fs.exists(new HPath(filePairsFname))) { 
      sc.textFile(filePairsFname).map(str => str.split(" ")).map(pair => (pair(0), pair(1).toInt)).collect 
    } else {
      val hdfsname = sc.hadoopConfiguration.get("fs.default.name")
      val filenames = sc.parallelize(filenamelist).flatMap( 
      fname =>  {
        val conf = new Configuration()
        conf.set("fs.default.name", hdfsname)
        val fs = HFileSystem.get(conf)

        if (!fname.endsWith(".tar")) { 
          List((fname, 0)) 
        } else {
          val archive = new TarArchiveInputStream(fs.open(new HPath(roothdfsdir + "/" + fname)))
          var numentries = 0
          while ( archive.getNextTarEntry != null) { numentries += 1}
          List.range(0, numentries).map(s => (fname, s))
        }
      }).collect.map( pair => (roothdfsdir + "/" + pair._1, pair._2))
      sc.parallelize(filenames).map(pair => s"${pair._1} ${pair._2}").saveAsTextFile(filePairsFname)
      filenames
    }

    // ensure that the data extracted from the files in each partition can be held in memory on the
    // executors and the driver, and there's enough disk space to convert them
    // writes A^T
    val chunks = fnames.grouped(609)
    val hdfsname = sc.hadoopConfiguration.get("fs.default.name")
    var numdivisions = 1
    for( chunk <- chunks) {
      val fnamesRDD = sc.parallelize(chunk, ceil(chunk.length.toFloat/numfilesperpartition).toInt)
      var results = fnamesRDD.mapPartitionsWithIndex((index, fnames) => extractData(hdfsname, fnames, variablenames, divisionsize, index)).cache
      if (numdivisions == 1)
        numdivisions = results.first._2.length
      for (idx <- 0 until numdivisions) {
        results.map( pair => (pair._1, pair._2(idx))).toDF.write.mode("append").parquet(outputdir + "Transpose" + idx)
      }
    }

    /*
    // take the transpose
    val tempdf = sqlContext.read.parquet(outputdir + "Transpose" + 0.toString)
    val numrows = tempdf.count // number of rows in A^T
    val rownames = tempdf.rdd.map(row => row(0).asInstanceOf[String]) // the names of the files corresponding to the rows of A^T
    rownames.saveAsTextFile(outputdir + "ColNames")
    var rowidx = 0 // the current row in A

    //for (idx <- 0 until numdivisions) {
    for (idx <- 0 until 275) {
      // reads a numrows-by-numcols chunk of the columns of A^T
      val chunkofcols = sqlContext.read.parquet(outputdir + "Transpose" + idx.toString).rdd.
                          map(row => row(1).asInstanceOf[WrappedArray[Float]].toArray).collect.toArray 
      val numcols = chunkofcols(0).length 

      // uses the fact that breeze stores matrices in column-major format to form the transpose of the chunk of columns of A^T
      val matrixChunkTranspose = new BDM[Float](numcols.toInt, numrows.toInt, chunkofcols.flatten)


      // uses the fact that breeze stores matrices in column-major format to extract the rows of the chunk of A
      val matrixChunkTransposeData =
      matrixChunkTranspose.t.copy.data.grouped(numrows.toInt).toArray.map(v => {
        rowidx = rowidx + 1
        val dv = v map {_.toDouble}
        (rowidx.toLong, new SDV(dv))
      })
      val matrixChunkTransposeRDD = sc.parallelize(matrixChunkTransposeData)
      matrixChunkTransposeRDD.toDF.write.mode("append").parquet(outputdir)
    }
    */


  }

  // given a group of filenames and the names of the variables to extract, extracts them 
    // PROCESSING OF NON-TAR FILES NOT IMPLEMENTED
  def extractData(hdfsname : String, filepairsIter: Iterator[Tuple2[String, Int]], fieldnames: String, divisionsize: Int, index: Int) : Iterator[Tuple2[String, Array[Array[Float]]]] = {
    val filepairs = filepairsIter.toArray
    val results = ArrayBuffer[Tuple2[String, Array[Array[Float]]]]()
    //val tempfname = "%s.%s".format(ThreadLocalRandom.current.nextLong(Long.MaxValue), "nc")
    val tempfile = File.createTempFile(ThreadLocalRandom.current.nextLong(Long.MaxValue).toString, ".nc", tmpdir)
    val tempfname = tempfile.getAbsolutePath()
    Files.delete(tempfile.toPath)

    logInfo("This partition contains files: " + filepairs.map( pair => s"(${pair._1}, ${pair._2})").mkString(","))
    logInfo(s"Using temporary file name : $tempfname")

    // process tar files
    val conf = new Configuration()
    conf.set("fs.default.name", hdfsname)
    val fs = HFileSystem.get(conf)

    for(curpair <- filepairs.filter(pair => (pair._1).endsWith(".tar"))){
      val archive = new TarArchiveInputStream(fs.open(new HPath(curpair._1))) 
      var offset = 0
      while (offset < curpair._2) { 
        archive.getNextTarEntry
        offset += 1 
      }

      val curentry = archive.getNextTarEntry
      val recordname = curentry.getName
      val tempfile2 = File.createTempFile(ThreadLocalRandom.current.nextLong(Long.MaxValue).toString, ".grb2", tmpdir)
      val tempfname2 = tempfile2.getAbsolutePath()
      Files.delete(tempfile2.toPath)

      logInfo(s"Processing ${recordname} in ${curpair._1}, entry ${curpair._2}")
      try {
        val tempout = new FileOutputStream( new File(tempfname2))
        IOUtils.copy(archive, tempout)
        tempout.close()
        val tempresult = getDataFromFile(tempfname2, fieldnames, tempfname) 
        if (tempresult.isDefined) { results += Tuple2(recordname, tempresult.get.grouped(divisionsize).toArray) }
      } catch {
        case e : Throwable => logInfo(s"Error in extracting and processing ${recordname} from ${curpair._1} : ${e.getMessage}")
      } finally {
        // always delete the temporary files
        val tempfile = new File(tempfname2)
        if ( tempfile.exists() ) {
          Files.delete(tempfile.toPath)
        }
      }
      archive.close()
      
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

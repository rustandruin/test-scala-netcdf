package org.apache.spark.mllib.linalg.distributed

// basic Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging

// Mllib support

// support my primitive logging routines
import java.util.Calendar
import java.text.SimpleDateFormat

// NetCDF support
import ucar.nc2.{NetcdfFile, NetcdfFileWriter}

// support writing data out using msgpack
import java.util.Arrays
import java.io.FileOutputStream
import org.msgpack.MessagePack

// misc Scala
import scala.util.Try

object netCDFTest extends Logging {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("testNetCDF")
    val sc = new SparkContext(conf)
    sys.addShutdownHook( {sc.stop() } )
    appMain(sc, args)
  }

  def appMain(sc: SparkContext, args: Array[String]) = {
    val inpath = args(0)
    val outpath  = args(1)

    // Load using NetCDF binding
    val infileTry = Try(NetcdfFile.open(inpath))
    if (!infileTry.isSuccess) { 
      logError("Couldn't open the input file " + inpath)
      sc.stop()
      System.exit(1)
    }
    val infile = infileTry.get
    
    // Total Cloud Cover
    var varName = "TCDC_P8_L234_GLL0_avg"
    var curVar = infile.findVariable(varName)

    report(varName + " " + curVar.getDescription())

    // Save back out using NetCDF binding
  }
  
  def report(message: String, verbose: Boolean = true) {
    if (verbose) logInfo(message);
  }

}


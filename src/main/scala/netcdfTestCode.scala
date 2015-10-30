package org.apache.spark.mllib.linalg.distributed

// basic Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

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

object netCDFTest {

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
      report("Couldn't open the input file " + inpath)
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
    if (verbose) {
      val now = Calendar.getInstance().getTime()
      val formatter = new SimpleDateFormat("H:m:s")
      println("STATUS REPORT (" + formatter.format(now) + "): " + message)
    }
  }

}


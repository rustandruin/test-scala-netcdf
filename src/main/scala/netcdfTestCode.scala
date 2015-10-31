package org.apache.spark.mllib.linalg.distributed

// basic Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging

// Breeze
import breeze.linalg.{DenseVector, DenseMatrix}

// Mllib support

// support my primitive logging routines
import java.util.Calendar
import java.text.SimpleDateFormat

// NetCDF support
import ucar.nc2.{NetcdfFile, NetcdfFileWriter}

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
    
    // How to error-handle this?
    val outfile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, outpath, null)

    // Example of loading a 2D dataset into breeze: Total Cloud Cover
    var varName = "TCDC_P8_L234_GLL0_avg"
    var results = load2D[Float](infile, varName)

    var varName = ""
    var results = load2D[Float](infile, varName)

    // Save back out using NetCDF binding
    outfile.close
  }
  
  // Note: NetCDF uses the row-major C convention for storing matrices: in the flattened vector, the index of the last axis changes fastest (e.g. see the help for numpy.reshape) 
  // while Breeze uses the column-major Fortran convention for storing matrices
  def load2D[T](fin: NetcdfFile, varName: String) : Tuple5[Int, Int, T, DenseMatrix[T], DenseMatrix[Boolean]] = {
    var variable = fin.findVariable(varName)
    logInfo("Loading " + varName + " " + variable.getFullName)
    logInfo("varName " + " has dimensions " + variable.getDimensions())
    var rows = variable.getDimension(0).getLength()
    var cols = variable.getDimension(1).getLength()
    var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[T]
    var breezeMat = (new DenseVector(variable.read.copyTo1DJavaArray.asInstanceOf[Array[T]])).toDenseMatrix.reshape(cols, rows).t
    var maskMat = breezeMat.copy.mapValues(_ == fillValue)
    return (rows, cols, fillValue, breezeMat, maskMat)
  }

  // returns a 3d matrix of dimensions [h, w, d] as a 2d matrix of size [d,
  // h*w] where the rows can be unfolded back to get an [h, w] matrix using the
  // row-major C convention
  def load3D[T](fin: NetcdfFile, varName: String) : Tuple6[Int, Int, Int, T, DenseMatrix[T], DenseMatrix[Boolean]] = {
    var variable = fin.findVariable(varName)
    logInfo("Loading " + varName + " " + variable.getFullName)
    logInfo("varName " + " has dimensions " + variable.getDimensions())
    var rows = variable.getDimension(0).getLength()
    var cols = variable.getDimension(1).getLength()
    var depth = variable.getDimension(2).getLength()
    var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[T]
    var breezeMat = (new DenseVector(variable.read.copyTo1DJavaArray.asInstanceOf[Array[T]])).toDenseMatrix.reshape(depth, cols*rows)
    var maskMat = breezeMat.copy.mapValue(_ == fillValue)
    return (rows, cols, depth, fillValue, breezeMat, maskMat)
  }

  // the climate data is stored as (depth, lat, lon) so using load3D would return a [lon, lat*depth] matrix; it's more 
  // useful to return a [depth, lat*lon] matrix, which this routine does
  def loadClimate3D[T](fin: NetcdfFile, varName: String) = {
    var orig = load3D(fin, varName)
    var mungedMat = orig._5
    var mungedMask = mungedMat.copy.mapValue(_ == fillValue)
    return (orig._1, orig._2, orig._3, orig._4, mungedMat, mungedMask)
  }

  def report(message: String, verbose: Boolean = true) {
    if (verbose) logInfo(message);
  }

}


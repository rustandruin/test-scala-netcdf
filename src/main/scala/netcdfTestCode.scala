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
import ucar.ma2.DataType
import ucar.ma2.{Array => netcdfArray}

// misc Scala
import scala.util.Try
import scala.reflect.ClassTag
import collection.JavaConversions._
import collection.mutable._

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
    
    // Example of loading a 2D dataset into breeze: Total Cloud Cover
    var varName = "TCDC_P8_L234_GLL0_avg"
    var results = load2D[Float](infile, varName)

    // Example of loading a 3D dataset into breeze: Temperature
    varName = "TMP_P0_L100_GLL0"
    var results2 = loadClimate3D[Float](infile, varName)
    var rows = results2._2
    var cols = results2._3
    var mat = results2._5(1,::).t.toDenseMatrix.reshape(cols, rows).t // extracts measurements at depth 1 as (lat, lon) matrix

    // Save back out using NetCDF binding

    // How to error-handle this?
    val outfile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, outpath, null)
    
    val depthDim = outfile.addDimension(null, "depth", results2._1)
    val latDim = outfile.addDimension(null, "lat", rows)
    val lonDim = outfile.addDimension(null, "lon", cols)
    val twodims = ArrayBuffer(depthDim, latDim, lonDim)
    val threedims = ArrayBuffer(latDim, lonDim)
    
    val cloudcover = outfile.addVariable(null, "cloudcover", DataType.FLOAT, twodims);
    val temperature = outfile.addVariable(null, "temperature", DataType.FLOAT, threedims);

    outfile.write(cloudcover, null, netcdfArray.factory(DataType.FLOAT, Array(rows, cols), results._4.t.data))
    outfile.create
    outfile.close
  }
  

  // Note: NetCDF uses the row-major C convention for storing matrices: in the flattened vector, the index of the last axis changes fastest (e.g. see the help for numpy.reshape) 
  // while Breeze uses the column-major Fortran convention for storing matrices
  def netCDFflatToBreeze2D[T](flatdata : Array[T], rows : Int, cols : Int ) : DenseMatrix[T] = {
    (new DenseVector(flatdata)).toDenseMatrix.reshape(cols, rows).t.copy
  }

  def load2D[T](fin: NetcdfFile, varName: String) : Tuple5[Int, Int, T, DenseMatrix[T], DenseMatrix[Boolean]] = {
    var variable = fin.findVariable(varName)
    logInfo("Loading " + varName + " " + variable.getFullName)
    logInfo("Loading " + varName + " " + variable.getDescription)
    logInfo(varName + " has dimensions " + variable.getDimensions)
    var rows = variable.getDimension(0).getLength
    var cols = variable.getDimension(1).getLength
    var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[T]
    var breezeMat = netCDFflatToBreeze2D(variable.read.copyTo1DJavaArray.asInstanceOf[Array[T]], rows, cols)
    var maskMat = breezeMat.copy.mapValues(_ == fillValue)
    (rows, cols, fillValue, breezeMat, maskMat)
  }

  // returns a 3d matrix of dimensions [h, w, d] as a 2d matrix of size [d,
  // h*w] where the rows can be unfolded back to get an [h, w] matrix using the
  // row-major C convention
  def load3D[T](fin: NetcdfFile, varName: String) : Tuple6[Int, Int, Int, T, DenseMatrix[T], DenseMatrix[Boolean]] = {
    var variable = fin.findVariable(varName)
    logInfo("Loading " + varName + " " + variable.getFullName)
    logInfo("Loading " + varName + " " + variable.getDescription)
    logInfo(varName + " has dimensions " + variable.getDimensions)
    var rows = variable.getDimension(0).getLength
    var cols = variable.getDimension(1).getLength
    var depth = variable.getDimension(2).getLength
    var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[T]
    var breezeMat = (new DenseVector(variable.read.copyTo1DJavaArray.asInstanceOf[Array[T]])).toDenseMatrix.reshape(depth, cols*rows)
    var maskMat = breezeMat.copy.mapValues(_ == fillValue)
    (rows, cols, depth, fillValue, breezeMat, maskMat)
  }

  // the climate data is stored as (depth, lat, lon) so using load3D would return a [lon, lat*depth] matrix; it's more 
  // useful to return a [depth, lat*lon] matrix, which this routine does
  def loadClimate3D[T:ClassTag](fin: NetcdfFile, varName: String) : Tuple6[Int, Int, Int, T, DenseMatrix[T], DenseMatrix[Boolean]]= {
    var variable = fin.findVariable(varName)
    logInfo("Loading " + varName + " " + variable.getFullName)
    logInfo("Loading " + varName + " " + variable.getDescription)
    logInfo(varName + " has dimensions " + variable.getDimensions)
    var depth = variable.getDimension(0).getLength
    var rows = variable.getDimension(1).getLength
    var cols = variable.getDimension(2).getLength
    var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[T]
    var rawdata = new Array[T](rows*cols*depth)
    for (level <- 0 until depth) {
      var origin = Array[Int](level, 0, 0)
      var shape = Array[Int](1, rows, cols)
      variable.read(origin, shape).copyTo1DJavaArray.asInstanceOf[Array[T]] copyToArray (rawdata, level*rows*cols)
    }
    var mungedMat = (new DenseVector(rawdata)).toDenseMatrix.reshape(cols*rows, depth).t.copy
    var mungedMask = mungedMat.copy.mapValues(_ == fillValue)
    (depth, rows, cols, fillValue, mungedMat, mungedMask)
  }

  def report(message: String, verbose: Boolean = true) {
    if (verbose) logInfo(message);
  }

}


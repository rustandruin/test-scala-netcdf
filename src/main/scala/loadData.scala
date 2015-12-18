package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.spark.sql.{SQLContext, Row => SQLRow}

import org.apache.spark.mllib.linalg.{DenseVector, DenseMatrix}

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}

import java.text.SimpleDateFormat

import scala.collection.mutable.{Map, WrappedArray}
import scala.reflect.runtime.universe._

import org.apache.log4j.Logger

object testMatMultiply {

  private val mylogger = Logger.getLogger("conversion")

  def main(args: Array[String]) = {
    val conf = new SparkConf()
       .setAppName("testMatMultiply")
    val sc = new SparkContext(conf)
    sys.addShutdownHook({ sc.stop() })
    appMain(sc, args)
  }

  def logInfo(message: String) {
    mylogger.info(message)
    println(message)
  }

  def rownameToDate(rowname: String) : Long = {
    (new SimpleDateFormat("'pgbh02.gdas.'yyyyMMddHH'.grb2'")).parse(rowname).getTime
  }

  // first arg: location of the parquet dataset
  def appMain(sc : SparkContext, args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //val datafname = args(0)
    val datafname = "hdfs:///user/root/CFSRAparquetTranspose"
    
    val rownames = sqlContext.read.parquet(datafname).select("_1").rdd.collect.map(r => r(0).asInstanceOf[String])
    val datenums = rownames.map(rowname => (rownameToDate(rowname), rowname))
    val sorteddates = datenums.sortWith( (pair1, pair2) => pair1._1 < pair2._1).map( t => t._2 )

    // saves the dates in increasing order just to check the sorting logic and lets one account for any missing rows when
    // interpreting the results
    val outfname = "hdfs:///user/root/sortedrownames"
    sc.parallelize(sorteddates).saveAsTextFile(outfname)
    /*

    // this lookup table maps the rowname to the row index in the matrix A (e.g. earliest date maps to 0, latest to last row)
    val indexLUT = Map[String, Long]()
    sorteddates.view.zipWithIndex foreach { case (rowname, index) => indexLUT += (rowname -> index) }

    // load the data into a row matrix
    val rows = sqlContext.read.parquet(datafname).rdd.map { row => {
      val v = row(1).asInstanceOf[WrappedArray[Float]]
      val vDouble = v.map( x => x.toDouble).toArray
      new IndexedRow(indexLUT(row(0).asInstanceOf[String]), new DenseVector(vDouble))
    }}

    val nrows : Long = 46752
    val ncols = 54843120
    //val A = new IndexedRowMatrix(rows, nrows, ncols)
    //A.rows.unpersist()

    //val x = new DenseMatrix(ncols, 1, BDV.rand(ncols).data)
    //A.multiply(x).rows.collect
    */
  }

}


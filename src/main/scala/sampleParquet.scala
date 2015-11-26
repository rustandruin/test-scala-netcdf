package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, Row}

import org.msgpack.annotation.Message
import org.msgpack.ScalaMessagePack

import scala.collection.mutable.ArrayBuffer

import java.io.FileOutputStream

object sampleParquet {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
       .setAppName("sampleParquet")
    val sc = new SparkContext(conf)
    sys.addShutdownHook({ sc.stop() })
    appMain(sc, args)
  }

  // first arg: name of the parquet file
  // second arg: name of the file (date) to dump to msgpack
  // third arg: name of the msgpack output file
  def appMain(sc : SparkContext, args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val parquetfname = args(0)
    val recordname = args(1)
    val outputfname = args(2)

    val measurements = sqlContext.read.parquet(parquetfname).rdd.filter( (row : Row) => row(0) == recordname).first.get(1).asInstanceOf[ArrayBuffer[Float]].toArray
    
    val serialized = ScalaMessagePack.write(measurements)
    var out = None : Option[FileOutputStream]
    try {
      out = Some(new FileOutputStream(outputfname))
      out.get.write(serialized)
    } finally {
      if (out.isDefined) out.get.close
    }

  }
}


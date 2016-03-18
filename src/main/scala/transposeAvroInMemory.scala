package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, Row}

import scala.math.{ceil, Ordering}
import scala.util.Sorting.quickSort
import scala.collection.mutable.{ArrayBuffer, WrappedArray}

import breeze.linalg.{DenseMatrix => BDM}

import com.databricks.spark.avro._

import org.apache.log4j.Logger

object transposeAvroInMemory {

  private val myLogger = Logger.getLogger("transposeAvro")

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("transposeAvroInMemory")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512m")
    conf.registerKryoClasses(Array(classOf[Tuple2[Int, Array[Float]]]))
    val sc = new SparkContext(conf)
    sys.addShutdownHook({sc. stop() })
    appMain(sc, args)
  }

  def logInfo(message: String) {
    myLogger.info(message)
    println(message)
  }

  /*
   first arg: basefilename for the avro row chunks of A^T
   second arg: number of avro row chunk files of A^T
   third arg: basefilename for where to place the final parquet A
   fourth arg: filename for where to store the column names for A
   fifth arg: number of column chunks to break each row into when transposing
   sixth arg: number of partitions to break rows of A^T over
   */
  def appMain(sc: SparkContext, args : Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val baseInputFname = args(0)
    val numAvroRowChunks = args(1).toInt
    val baseOutputFname = args(2)
    val colNamesOutputFname = args(3)
    val numSubChunks  = args(4).toInt
    val numPartitions = args(5).toInt

    // load all the rows of A^T into memory
    def getRowPair(r: Row) : Tuple2[String, Array[Float]] = {
      (r.getAs[String](0), r.getAs[WrappedArray[Float]](1).toArray)
    }
    var transposeRowsRDD = sqlContext.read.avro(baseInputFname + "0").rdd.map(
      r => getRowPair(r))
    for( rowChunkIdx <- 1 until numAvroRowChunks ) {
      transposeRowsRDD = transposeRowsRDD.union(sqlContext.read
        .avro(baseInputFname + rowChunkIdx.toString).rdd.map( r => getRowPair(r)))
    }
    transposeRowsRDD.cache()
    transposeRowsRDD.repartition(numPartitions)

    val rowNames = transposeRowsRDD.map(_._1).collect.toArray
    quickSort(rowNames)
    def lookupRowName( name: String) = {
        rowNames.indexWhere(_ == name)
    }

    // count number of columns in A^T
    val numCols = transposeRowsRDD.first._2.length

    // transpose the columns in chunks
    val colChunkIndices = (0 until numCols).grouped(ceil(numCols/numSubChunks).toInt).toArray
    for( colChunkIndex <- 0 until colChunkIndices.length) {
        val startIndex = colChunkIndices(colChunkIndex)(0)
        val endIndex = colChunkIndices(colChunkIndex).reverse(0)+1
        val columnChunkRDD = transposeRowsRDD.map( r => (lookupRowName(r._1), r._2.slice(startIndex, endIndex)))

        def explodeRow(r: Tuple2[Int, Array[Float]]) = {
            r._2.zipWithIndex.map{ case (value, colindex) => (colindex, (r._1, value)) }
        }
        def collapseArray( pairs : Iterable[Tuple2[Int, Float]]) = {
            pairs.toArray.sortBy(_._1).map(_._2)
        }
        val rowChunkRDD = columnChunkRDD.flatMap(explodeRow).groupByKey
                            .map( pair => (pair._1, collapseArray(pair._2)))
        rowChunkRDD.toDF.write.mode("append").parquet(baseOutputFname)
    }

    // Store the column names
    sc.parallelize(rowNames).saveAsTextFile(colNamesOutputFname)
  }
}

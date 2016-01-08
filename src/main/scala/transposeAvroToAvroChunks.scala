package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import scala.math.{ceil, Ordering}
import scala.util.Sorting.quickSort
import scala.collection.mutable.{ArrayBuffer, WrappedArray}

import breeze.linalg.{DenseMatrix => BDM}

import com.databricks.spark.avro._

import org.apache.log4j.Logger

object transposeAvroToAvroChunks {

  private val myLogger = Logger.getLogger("transposeAvro")

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("transposeAvroToAvroChunks")
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
   third arg: basefilename for where to place the avro column chunks of A in
   fourth arg: filename for where to store the column names for A
   fifth arg: number of column chunks to get from one row chunk
   */
  def appMain(sc: SparkContext, args : Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val baseInputFname = args(0)
    val numAvroRowChunks = args(1).toInt
    val baseOutputFname = args(2)
    val colNamesOutputFname = args(3)
    val numSubChunks  = args(4).toInt

    // the number of columns in A^T
    val numCols = sqlContext.read.avro(baseInputFname + "0").first
                  .getAs[WrappedArray[Float]](1).length

    // keep track of the current chunk of columns of A being computed
    // and all the rows in A^T / columns in A
    var chunkCounterA  = 0
    val allColNames = ArrayBuffer.empty[String]

    // iterate over the avro row chunks of A^T
    for( rowChunkIdx <- 0 until numAvroRowChunks ) {
      // get the names of all the rows in this chunk of A^T and sort them
      val rowNames = sqlContext.read.avro(baseInputFname + rowChunkIdx.toString)
                     .select("_1").map( row => row.getAs[String](0)).collect
      val numRowsInChunk = rowNames.length
      logInfo(s"Collected ${numRowsInChunk} row names")
      quickSort(rowNames)
      allColNames ++= rowNames
      val rowChunks = rowNames.grouped(
                     ceil(numRowsInChunk.toDouble/numSubChunks).toInt).toArray

      // for memory reasons, split this chunk of rows of A^T into multiple pieces
      // then transpose each piece into a column chunk of A
      for(subRowChunkIdx <- 0 until rowChunks.length) {
        val chunkOfATranspose = 
          sqlContext.read.avro(baseInputFname + rowChunkIdx.toString)
            .filter($"_1" isin(rowChunks(subRowChunkIdx):_*) ).rdd
            .map(row => 
                     Tuple2(row.getAs[String](0), 
                            row.getAs[WrappedArray[Float]](1).toArray))
            .collect
        logInfo(s"Collected the ${chunkCounterA} chunk of rows from A^T;" 
          + s"contains ${chunkOfATranspose.size} rows")
        quickSort(chunkOfATranspose)(Ordering.by(_._1))
        logInfo(s"Sorted the rows by increasing date")
        val sortedChunkRowNames = chunkOfATranspose.map(pair => pair._1)
        logInfo(s"Extracted the row names")

        // checks that the row ordering is correct a la 
        // http://blog.bruchez.name/2013/05/scala-array-comparison-without-phd.html
        assert( (sortedChunkRowNames : Seq[String]) ==
                (rowChunks(subRowChunkIdx) : Seq[String]) )
        logInfo(s"Asserted that the row ordering is correct")

        // contains concat(row1, ..., lastrow) of this chunk of A^T 
        val rowChunksOfATransposeData = 
          chunkOfATranspose.map(pair => pair._2).toArray.flatten
        logInfo(s"Flattened these rows of A^T into one array")

        // Breeze stores matrices in column-major format, 
        // so this implicitly transposes the chunk of A^T to get 
        // a column chunk of A, then returns the rows of this chunk
        // along with their row indices as (idx, floats)
        val colChunksOfAData = 
          new BDM(numCols, rowChunks(subRowChunkIdx).length, rowChunksOfATransposeData)
          .t.copy.data.grouped(rowChunks(subRowChunkIdx).length).toArray
          .zipWithIndex.map(pair => (pair._2, pair._1))
        logInfo(s"Transposed the rows of A^T to get columns of A and added row indices")

        sc.parallelize(colChunksOfAData).toDF.write
          .avro(baseOutputFname + chunkCounterA.toString)
        chunkCounterA = chunkCounterA + 1
      }
    }
    sc.parallelize(allColNames).saveAsTextFile(colNamesOutputFname)
  }

}







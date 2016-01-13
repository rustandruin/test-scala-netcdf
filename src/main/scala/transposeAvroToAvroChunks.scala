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
   third arg: basefilename for where to place the avro column chunks of A in
   fourth arg: basefilename for where to place the final parquet A
   fifth arg: filename for where to store the column names for A
   sixth arg: number of column chunks to get from one row chunk

   NB: when choosing parameters, consider both the JVM maximum array size
    and the driver memory limit
   */
  def appMain(sc: SparkContext, args : Array[String]) {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val baseInputFname = args(0)
    val numAvroRowChunks = args(1).toInt
    val tempOutputFname = args(2)
    val baseOutputFname = args(3)
    val colNamesOutputFname = args(4)
    val numSubChunks  = args(5).toInt

    // the number of columns in A^T
    val numCols = sqlContext.read.avro(baseInputFname + "0").first
                  .getAs[WrappedArray[Float]](1).length
    logInfo(s"There are ${numCols} columns in A^T")

    // keep track of the current chunk of columns of A being computed
    // and all the rows in A^T / columns in A
    var chunkCounterA  = 0
    val allColNames = ArrayBuffer.empty[String]

    // iterate over the avro row chunks of A^T, converting to column chunks of A^T
    for( rowChunkIdx <- 0 until numAvroRowChunks ) {
      // get the names of all the rows in this chunk of A^T and sort them
      val rowNames = sqlContext.read.avro(baseInputFname + rowChunkIdx.toString)
                     .select("_1").map( row => row.getAs[String](0)).collect
      val numRowsInChunk = rowNames.length
      logInfo(s"Collected ${numRowsInChunk} row names")
      quickSort(rowNames)

      // for memory reasons, split this chunk of rows of A^T into multiple pieces
      // then write the transpose of each piece into the column chunk of A
      allColNames ++= rowNames
      val rowChunks = rowNames.grouped(
                     ceil(numRowsInChunk.toDouble/numSubChunks).toInt).toArray

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

        val colChunkOfA = new Array[Array[Float]](rowChunks(subRowChunkIdx).length)
        var curColInChunkOfA = 0

        chunkOfATranspose.foreach( pair => {
            colChunkOfA(curColInChunkOfA) = pair._2
            curColInChunkOfA = curColInChunkOfA + 1
            if (curColInChunkOfA % 100 == 0)
              logInfo(s"wrote into column ${curColInChunkOfA} of this chunk of columns of A")
        })

        // write out this chunk of columns of A as rows along with their 
        // row indices as (idx, floats)

        val rowChunk = new Array[Tuple2[Int,Array[Float]]](numCols)
        val colIndices = (0 until colChunkOfA.length).toArray

        (0 until numCols).foreach( rowIdx => { 
          rowChunk(rowIdx) = Tuple2(rowIdx, colIndices.map(colChunkOfA(_)(rowIdx))) 
          if (rowIdx % 6000000 == 0)
            logInfo(s"Populated row $rowIdx of this chunk of columns of A")
        })

        logInfo(s"Materializing row chunk RDD")
        val parallelRowChunkRDD = sc.parallelize(rowChunk).cache
        parallelRowChunkRDD.count
        
        logInfo(s"Writing row chunk RDD")
        parallelRowChunkRDD.toDF.write
          .parquet(tempOutputFname + chunkCounterA.toString)
        chunkCounterA = chunkCounterA + 1
      }
    }

    // Store the column names
    sc.parallelize(allColNames).saveAsTextFile(colNamesOutputFname)
    
    /*
    // Now merge all the chunks of columns back together into the final parquet files
    val outputRowChunkSize = ceil(numCols.toDouble/numOutputRowChunks).toInt
    val rowChunks = (0 until numCols).grouped(outputRowChunkSize).toArray

    for( rowChunkIdx <- 0 until rowChunks.length) {
      var rowChunkRdd = new RDD[Tuple[
      for( colChunkIdx <- 0 until chunkCounterA) {
        filter.

      }
    }
    */

  }

}







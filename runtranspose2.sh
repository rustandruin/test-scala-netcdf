#!/usr/bin/env bash

# This driver transposes a short and fat matrix A^T into a tall and skinny matrix A
# It assumes the matrix A^T is given in Avro format as a collection of chunks of 
# (row-string-label, Array[Float]) and returns a single parquet file of rows in the format
# (rowidx, Array[Float]) and a text-file containing the col-string-labels obtained
# from the row-string-labels from A^T.
#
# ROWCHUNKSBASEFNAME is the base filename for the input chunks of rows from A^T, and
# NUMROWCHUNKFILES identifies how many chunks there are. So the code loads the files
# ROWCHUNKSBASEFNAME0 through ROWCHUNKSBASEFNAME(NUMROWCHUNKFILES-1) as input.
#
# Because the transposition operations are done on the driver which has limited 
# memory, we can't transpose one full input chunk of rows at a time. Instead, we break
# each input chunk into NUMSUBROWCHUNKS pieces and transpose each of these at a time.
# 
# These pieces are transposed and stored as Avro column matrices with baseoutput filename
# TEMPOUTPUTDIR. Finally, these pieces are read back in and concatenated in row chunks, and
# progressively appended to the final OUTPUTDIR. The column names are stored in COLNAMEDIR

# Note: NUMROWCHUNKFILES=47

JARFILE=$1
ROWCHUNKSBASEFNAME=hdfs:///user/root/CFSRAparquetTranspose/CFSRAparquetTranspose
NUMROWCHUNKFILES=2
NUMSUBROWCHUNKS=5 
# we need about 3 copies in memory in the driver at the same time, so choose accordingly
# 10 corresponds to transposing about 100 rows of A^T at once

CURDIR="$(cd "`dirname "$0"`"; pwd)"
DIR=/mnt2/climateLogs
LOGDIR=$DIR/eventLogs
LOGFILE=$DIR/fulltransposerun.log
TEMPDIR=hdfs:///user/root/tempCFSRAtransposition/tempCFSRAparquetColPiece
OUTPUTDIR=hdfs:///user/root/CFSRAparquet/CFSRAparquetmatrix
COLNAMEDIR=hdfs:///user/root/CFSRAparquet/CFSRAparquetColNames
mkdir -p $LOGDIR

# expects to be run on EC2 in standalone mode with 29-execs on r3.8xlarge instances
# note maxResultSize needs to be large because are returning large chunks of data (10GB is not sufficient, maybe 13GB might be?)

# 116, 8, 20G
# 174, 4, 20G # runs faster (maybe 2x) when collecting row names, and also faster when collecting the row vectors
# 203, 4, 10G # get java heap memory errors
# 203, 4, 20G # not sure this is any faster than 174, 4, 20G
# the results of drawing down one chunk of rows (500 rows) is about 103 GB, so set maxResultSize higher
spark-submit --verbose \
  --driver-memory 220G \
  --num-executors 203 \
  --executor-cores 4 \
  --executor-memory 20G \
  --driver-java-options "-Dlog4j.configuration=file://$CURDIR/log4j.properties -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode" \
  --conf "spark.driver.maxResultSize=120G" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeout=1200000 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --class org.apache.spark.mllib.linalg.distributed.transposeAvroToAvroChunks \
  --files log4j.properties \
  $JARFILE \
  $ROWCHUNKSBASEFNAME $NUMROWCHUNKFILES $TEMPDIR $OUTPUTDIR $COLNAMEDIR $NUMSUBROWCHUNKS \
  2>&1 | tee $LOGFILE

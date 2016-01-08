#!/usr/bin/env bash

JARFILE=$1
ROWCHUNKSBASEFNAME=hdfs:///user/root/CFSRAparquetTranspose/CFSRAparquetTranspose
NUMROWCHUNKFILES=47
NUMSUBROWCHUNKS=3 
# 3 corresponds to transposing about 333 rows of A^T at once

#DIR="$(cd "`dirname "$0"`"; pwd)"
DIR=/mnt2/climateLogs
LOGDIR=$DIR/eventLogs
LOGFILE=$DIR/fulltransposerun.log
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
  --driver-java-options '-Dlog4j.configuration=log4j.properties -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode' \
  --conf "spark.driver.maxResultSize=120G" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeout=1200000 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --jars $JARFILE \
  --class org.apache.spark.mllib.linalg.distributed.transposeAvroToAvroChunks \
  $JARFILE \
  $ROWCHUNKSBASEFNAME $NUMROWCHUNKFILES $OUTPUTDIR $COLNAMEDIR $NUMSUBROWCHUNKS \
  2>&1 | tee $LOGFILE

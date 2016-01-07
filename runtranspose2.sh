#!/usr/bin/env bash

JARFILE=$1
ROWCHUNKSBASEFNAME=hdfs:///user/root/CFSRAparquetTranspose/CFSRAparquetTranspose
NUMROWCHUNKFILES=47
NUMSUBROWCHUNKS=2

#DIR="$(cd "`dirname "$0"`"; pwd)"
DIR=/mnt2/climateLogs
LOGDIR=$DIR/eventLogs
LOGFILE=$DIR/fulltransposerun.log
OUTPUTDIR=hdfs:///user/root/CFSRAparquet/CFSRAparquetmatrix
COLNAMEDIR=hdfs:///user/root/CFSRAparquet/CFSRAparquetColNames
mkdir -p $LOGDIR

# expects to be run on EC2 in standalone mode with 29-execs on r3.8xlarge instances
# note maxResultSize needs to be large because are returning large chunks of data (10GB is not sufficient, maybe 13GB might be?)

spark-submit --verbose \
  --driver-memory 220G \
  --num-executors 116 \
  --executor-cores 8 \
  --executor-memory 20G \
  --driver-java-options '-Dlog4j.configuration=log4j.properties' \
  --conf "spark.driver.maxResultSize=20G" \
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

#!/usr/bin/env bash

JARFILE=$1
HDFSDIR=hdfs:///user/root/CFSRAparquetTranspose
ROWCHUNKSBASEFNAME=CFSRAparquetTranspose
NUMROWCHUNKFILES=47
TRANSPOSECHUNKSIZE=1

#DIR="$(cd "`dirname "$0"`"; pwd)"
DIR=/mnt2/climateLogs
LOGDIR=$DIR/eventLogs
LOGFILE=$DIR/fulltransposerun.log
OUTPUTDIR=hdfs:///user/root/CFSRAparquet
mkdir -p $LOGDIR

# expects to be run on EC2 in standalone mode with 29-execs on r3.8xlarge instances

spark-submit --verbose \
  --driver-memory 220G \
  --num-executors 116 \
  --executor-cores 8 \
  --executor-memory 20G \
  --driver-java-options '-Dlog4j.configuration=log4j.properties' \
  --conf "spark.driver.maxResultSize=10G" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --jars $JARFILE \
  --class org.apache.spark.mllib.linalg.distributed.transposeAvroChunks \
  $JARFILE \
  $HDFSDIR $ROWCHUNKSBASEFNAME $NUMROWCHUNKFILES $OUTPUTDIR $TRANSPOSECHUNKSIZE \
  2>&1 | tee $LOGFILE

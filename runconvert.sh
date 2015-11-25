#!/usr/bin/env bash

JARFILE=$1
VARNAMES=$2
HDFSDIR=hdfs:///user/root/CFSRArawtars
FILELISTFNAME=hdfs:///user/root/completefilelist 
NUMFILESPERPARTITION=3

DIR="$(cd "`dirname "$0"`"; pwd)"
LOGDIR="$DIR/eventLogs"
LOGFILE="fullrun.log"
OUTPUTDIR=hdfs:///user/root/CFSRAparquetTranspose

# expects to be run on EC2 in standalone mode with 29-execs on r3.8xlarge instances
# 

spark-submit --verbose \
  --driver-memory 220G \
  --num-executors 203 \
  --executor-cores 4 \
  --executor-memory 10G \
  --driver-java-options '-Dlog4j.configuration=log4j.properties' \
  --conf "spark.driver.maxResultSize=5G" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --jars $JARFILE \
  --class org.apache.spark.mllib.linalg.distributed.convertGribToParquet \
  $JARFILE \
  $HDFSDIR $FILELISTFNAME $VARNAMES $OUTPUTDIR $NUMFILESPERPARTITION \
  2>&1 | tee $LOGFILE

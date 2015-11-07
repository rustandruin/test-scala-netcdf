#!/usr/bin/env bash

JARFILE=$1
VARNAMES=$2
FILELISTFNAME="filenamelist"

DIR="$(cd "`dirname "$0"`"; pwd)"
LOGDIR="$DIR/eventLogs"
LOGFILE="testrun.log"
OUTPUTDIR="data"

spark-submit --verbose \
  --master yarn \
  --num-executors 1 \
  --driver-memory 5G \
  --executor-memory 5G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --jars $JARFILE \
  --class org.apache.spark.mllib.linalg.distributed.convertGribToParquet \
  $JARFILE \
  $FILELISTFNAME $VARNAMES $OUTPUTDIR \
  2>&1 | tee $LOGFILE




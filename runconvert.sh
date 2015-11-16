#!/usr/bin/env bash

JARFILE=$1
VARNAMES=$2
FILELISTFNAME="debugfilelist"
NUMFILESPERPARTITION=3

DIR="$(cd "`dirname "$0"`"; pwd)"
LOGDIR="$DIR/eventLogs"
LOGFILE="debugrun.log"
OUTPUTDIR="debugdata"

MASTER=$SPARKURL

# expects to be run on Edison with mppwidth=480 for production
#  --driver-memory 15G \
#  --num-executors 119 \
#  --executor-cores 4 \
#  --executor-memory 10G \

# debug settings on Edison with mppwidth=120
#  --driver-memory 15G \
#  --num-executors 29 \
#  --executor-cores 4 \
#  --executor-memory 10G \

spark-submit --verbose \
  --master $MASTER \
  --driver-memory 15G \
  --num-executors 119 \
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
  $FILELISTFNAME $VARNAMES $OUTPUTDIR $NUMFILESPERPARTITION \
  2>&1 | tee $LOGFILE

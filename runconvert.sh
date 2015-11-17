#!/usr/bin/env bash

JARFILE=$1
VARNAMES=$2
FILELISTFNAME="completefilelist"
NUMFILESPERPARTITION=3

DIR="$(cd "`dirname "$0"`"; pwd)"
LOGDIR="$DIR/eventLogs"
LOGFILE="fullrun.log"
OUTPUTDIR="CFSRAparquet"

MASTER=$SPARKURL

# expects to be run on Edison with mppwidth=288 for production
# (because converting 3 files at a time on 70 executors means can convert 210 files by assigning one partition per executors, and each executor takes 4 cores, so need 280 cores + 4 for the driver, and the closest multiple of 24 to that is 288) note 260 files with this setting gives an OOM error run in ccm_queue 
#  --driver-memory 60G \
#  --num-executors 70 \
#  --executor-cores 4 \
#  --executor-memory 10G \

# debug settings on Edison with mppwidth=480 (using chunks of 342 files at a time)
#  --driver-memory 60G \
#  --num-executors 114 \
#  --executor-cores 4 \
#  --executor-memory 10G \

spark-submit --verbose \
  --master $MASTER \
  --driver-memory 60G \
  --num-executors 50 \
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

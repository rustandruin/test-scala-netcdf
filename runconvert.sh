#!/usr/bin/env bash

# NB: the conversion spark event log files are > 2.6GB, and the spark console logs take up more than 1GB, so can't store these on the master's / directory
# instead, store in /mnt2/climateLogs/eventLogs and /mnt2/climateLogs/fullrun.log

JARFILE=$1
VARNAMES=$2
HDFSDIR=hdfs:///user/root/CFSRArawtars
FILELISTFNAME=hdfs:///user/root/completefilelist 
NUMFILESPERPARTITION=3

#DIR="$(cd "`dirname "$0"`"; pwd)"
DIR=/mnt2/climateLogs
LOGDIR=$DIR/eventLogs
LOGFILE=$DIR/fullrun.log
OUTPUTDIR=hdfs:///user/root/CFSRAparquet
mkdir -p $LOGDIR

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

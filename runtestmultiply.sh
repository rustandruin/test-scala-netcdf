#!/usr/bin/env bash
JARFILE=$1

DIR="$(cd "`dirname "$0"`"; pwd)"
LOGDIR="$DIR/eventLogs"
LOGFILE="testmultrun.log"

# expects to be run on EC2 in standalone mode with 29-execs on r3.8xlarge instances
# 7 executors per node => 203 executors

spark-submit --verbose \
  --driver-memory 220G \
  --num-executors 203 \
  --executor-cores 4 \
  --executor-memory 25G \
  --driver-java-options '-Dlog4j.configuration=log4j.properties' \
  --driver-java-options '-XX:MaxPermSize=512M' \
  --conf spark.storage.memoryFraction=0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf 'spark.executor.extraJavaOptions=-XX:MaxPermSize=512M' \
  --conf 'spark.kryoserializer.buffer.max=512M' \
  --conf 'spark.driver.maxResultSize=1G' \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeout=1200000 \
  --jars $JARFILE \
  --class org.apache.spark.mllib.linalg.distributed.testMatMultiply \
  $JARFILE \
  2>&1 | tee $LOGFILE


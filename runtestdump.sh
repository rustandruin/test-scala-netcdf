#!/usr/bin/env bash

JARFILE=$1
FILENAME=$2
PARQUETFNAME=hdfs:///user/root/CFSRAparquetTranspose
LOGFILE=samplingrun.log
OUTFILE=samples.msgpack

spark-submit --verbose \
 --driver-memory 220G \
 --num-executors 203 \
 --executor-cores 4 \
 --executor-memory 10G \
 --driver-java-options '-XX:MaxPermSize=512M' \
 --conf 'spark.executor.extraJavaOptions=-XX:MaxPermSize=512M' \
 --conf 'spark.kryoserializer.buffer.max=128M' \
 --conf 'spark.driver.maxResultSize=10G' \
 --jars $JARFILE \
 --class org.apache.spark.mllib.linalg.distributed.sampleParquet \
 $JARFILE \
 $PARQUETFNAME $FILENAME $OUTFILE \
 2>&1 | tee $LOGFILE

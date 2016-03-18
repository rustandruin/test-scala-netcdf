#!/usr/bin/env bash

# For testing out code on a smaller subset of the matrix A^T
#
# This driver transposes a short and fat matrix A^T into a tall and skinny matrix A
# It assumes the matrix A^T is given in Avro format as a collection of chunks of 
# (row-string-label, Array[Float]) and returns a single parquet file of rows in the format
# (rowidx, Array[Float]) and a text-file containing the col-string-labels obtained
# from the row-string-labels from A^T.
#
# ROWCHUNKSBASEFNAME is the base filename for the input chunks of rows from A^T, and
# NUMROWCHUNKFILES identifies how many chunks there are. So the code loads the files
# ROWCHUNKSBASEFNAME0 through ROWCHUNKSBASEFNAME(NUMROWCHUNKFILES-1) as input.
#
# The transposition works by loading the matrix A^T entirely into memory as one RDD,
# then going over chunks of its columns sequentially, transposing each chunk using
# explosion into (i,j,val) and groupBys, then writing those chunks out to the same parquet
# file in append mode
#

JARFILE=$1
ROWCHUNKSBASEFNAME=$SCRATCH/CFSRA/CFSRAparquetTranspose/CFSRAparquetTranspose
NUMROWCHUNKFILES=2
NUMSUBROWCHUNKS=5
# this controls the number of column chunks we break A^T into, so the size of
# the column chunk we deal with when transposing, so the memory pressure we put
# on the executors
NUMPARTITIONS=66
# this controls the number of partitions A^T is (row) partitioned over, so
# should be chosen based on the memory available on the executors and the size
# of A^T
# use 46752/R partitions at least, to ensure each executor is in use, but can
# subdivide to get more partitions for increased flexibility

CURDIR="$(cd "`dirname "$0"`"; pwd)"
DIR=$CURDIR
LOGDIR=$DIR/eventLogs
LOGFILE=$DIR/fulltransposerun.log
OUTPUTDIR=$DIR/CFSRA/CFSRAparquetmatrix
COLNAMEDIR=$DIR/CFSRA/CFSRAparquetColNames
mkdir -p $LOGDIR

# expects to be run on Cori

# The matrix is a 46752-by-54843120 dense double precision matrix, so 10.3 Gb over all, with one row taking up 220 Mb. 
# Cori has 128 Gb available per node. Let's say we allocate 120 Gb for Spark, then we have 
# spark.memory.fraction * 120 = .75*120 = 90 Gb for execution and storage on each node,
# spark.memory.storageFraction * 90 = .5*90 = 45 Gb of which is immune to eviction
# Let's say we constrain ourselves to require the rows of A^T and the current
# column chunk of A^T to be both stored in eviction-free memory, leaving the
# rest for the transpose operation and usual Spark overhead 
# Let R be the number of rows of A^T per executor, and C be the number of column chunks we're using, then
# 220 Mb * R * (1  + 1/C) < 45 Gb is the requirement
# Let's say that it's fine to take 220 Mb * R < 40 Gb. Take R = 180 rows per
# executor => need 259 executors to fit all about 47000 rows in memory

spark-submit --verbose \
  --master $SPARKURL \
  --driver-memory 120G \
  --num-executors 33 \
  --executor-cores 16 \
  --executor-memory 120G \
  --conf "spark.driver.maxResultSize=120G" \
  --driver-java-options "-Dlog4j.configuration=file://$CURDIR/log4j.properties -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode" \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeout=1200000 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --class org.apache.spark.mllib.linalg.distributed.transposeAvroInMemory \
  $JARFILE \
  $ROWCHUNKSBASEFNAME $NUMROWCHUNKFILES $OUTPUTDIR $COLNAMEDIR $NUMSUBROWCHUNKS $NUMPARTITIONS \
  2>&1 | tee $LOGFILE

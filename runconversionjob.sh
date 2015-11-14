#!/bin/bash -l
start-all.sh

waitforstartup.sh 90 200
if [ "$?" -ne "0" ]; then echo "ERROR: Spark didn't start correctly, giving up for now, please try again"; fi

sbt runConvert
stop-all.sh 

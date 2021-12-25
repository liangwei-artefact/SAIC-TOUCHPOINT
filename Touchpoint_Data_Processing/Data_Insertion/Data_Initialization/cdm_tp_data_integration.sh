#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')

brand=$5
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queue_name \
cdm_tp_data_integration.py $pt1 $pt2 $brand


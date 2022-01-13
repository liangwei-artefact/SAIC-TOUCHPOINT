#!/bin/bash

queuename=$1
touchpoint=$2
pt_month=$3

export PATH=/usr/bin/python:/usr/jdk64/jdk1.8.0_112/bin:/usr/jdk64/jdk1.8.0_112/jre/bin:/usr/jdk64/jdk1.8.0_112/bin:/usr/jdk64/jdk1.8.0_112/jre/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/jdk64/jdk1.8.0_112/bin:/usr/hdp/3.1.0.0-78/hadoop/bin:/usr/hdp/3.1.0.0-78/spark2/bin

spark-submit --master yarn  \
--deploy-mode client \
--driver-memory 10G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queuename \
attri_report_save_result.py $touchpoint $pt_month
#!/bin/bash
# /*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Attribution_report
#*程序: attri_report_load_data_pyspark.sh
#*功能: 从hive中读取计算触点转化所需的数据并存储成csv
#*开发人: Liangwei Chen
#*开发日: 2021-12-25
#*修改记录:
#*
#*********************************************************************/

pt=$1
cd $(dirname $(readlink -f $0))
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
echo "queuename"_${queuename}
spark-submit --master yarn \
--driver-memory 5G  \
--num-executors 8 \
--executor-cores 8 \
--executor-memory 20G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queuename \
attri_report_load_data.py $pt
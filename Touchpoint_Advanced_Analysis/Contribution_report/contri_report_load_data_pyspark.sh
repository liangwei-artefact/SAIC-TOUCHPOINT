#!/bin/bash
# /*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Contribution_Report
#*程序: contri_report_load_data.sh
#*功能: 从hive中读取计算触点转化所需的数据并存储成csv
#*开发人: Liangwei Chen
#*开发日: 2021-12-25
#*修改记录:
#*
#*********************************************************************/
export PATH=/usr/bin/python:/usr/jdk64/jdk1.8.0_112/bin:/usr/jdk64/jdk1.8.0_112/jre/bin:/usr/jdk64/jdk1.8.0_112/bin:/usr/jdk64/jdk1.8.0_112/jre/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/jdk64/jdk1.8.0_112/bin:/usr/hdp/3.1.0.0-78/hadoop/bin:/usr/hdp/3.1.0.0-78/spark2/bin
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
contri_report_load_data.py $pt
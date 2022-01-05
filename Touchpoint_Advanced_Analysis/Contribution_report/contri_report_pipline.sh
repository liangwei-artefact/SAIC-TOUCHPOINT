#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Contribution_Report
#*程序: contri_report_pipeline.sh
#*功能: 串联触点转化计算的脚本
#*开发人: Boyan & Xiaofeng
#*开发日: 2021-09-05
#*修改记录:  
#*    
#*********************************************************************/
export PATH=/usr/bin/python:/usr/jdk64/jdk1.8.0_112/bin:/usr/jdk64/jdk1.8.0_112/jre/bin:/usr/jdk64/jdk1.8.0_112/bin:/usr/jdk64/jdk1.8.0_112/jre/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/jdk64/jdk1.8.0_112/bin:/usr/hdp/3.1.0.0-78/hadoop/bin:/usr/hdp/3.1.0.0-78/spark2/bin
pt=$1
cd $(dirname $(readlink -f $0))
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

brands=('MG' 'RW')
sh contri_report_load_data_pyspark.sh $pt

for brand in "${brands[@]}"
do
    sh contri_report_compute_result.sh ${brand}
    sh contri_report_aggregation.sh ${brand}
done
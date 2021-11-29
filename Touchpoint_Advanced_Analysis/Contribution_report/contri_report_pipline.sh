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

pt=$3

queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  ../../config/config.ini`

brands=('MG' 'RW')
sh contri_report_load_data.sh $pt

for brand in "${brands[@]}"
do
    python contri_report_compute_result.py ${brand}
    sh contri_report_aggregation.sh ${brand}
done
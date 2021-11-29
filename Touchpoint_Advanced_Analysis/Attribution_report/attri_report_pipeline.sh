#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Attribution_Report
#*程序: attri_report_pipeline.sh
#*功能: 串联贡献度计算的脚本
#*开发人: Boyan XU
#*开发日: 2021-09-05
#*修改记录:  
#*    
#*********************************************************************/

pt=$3
pt_month=$(date -d "${pt}" +%Y%m)
cur_month_start=$(date -d "${pt_month}01" +%Y%m%d)

queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  ../../config/config.ini`

sh attri_report_load_data.sh $pt

echo "Start to generate attribution result......"

touchpoints=('instore' 'trial' 'deal')
for touchpoint in ${touchpoints[@]}
do
    python3 attri_report_model_running.py ${touchpoint} MG $cur_month_start
    python3 attri_report_model_running.py ${touchpoint} RW $cur_month_start
    spark-submit --master yarn  \
    --driver-memory 10G  \
    --num-executors 10 \
    --executor-cores 10 \
    --executor-memory 32G \
    --conf "spark.excutor.memoryOverhead=10G"  \
    --queue $queuename \
    attri_report_save_result.py $touchpoint $pt_month
done

sh attri_report_merge_result.sh $pt 'MG'
sh attri_report_merge_result.sh $pt 'RW'
echo "Merging attribution result......"

echo "Finish generating attribution result......"
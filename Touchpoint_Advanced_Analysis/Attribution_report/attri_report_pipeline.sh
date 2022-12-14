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

pt=$1
pt_month=$(date -d "${pt}" +%Y%m)
cur_month_start=$(date -d "${pt_month}01" +%Y%m%d)
cd $(dirname $(readlink -f $0))
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

sh attri_report_load_data_pyspark.sh $pt

echo "Start to generate attribution result......"

touchpoints=('instore' 'trial' 'deal')
for touchpoint in ${touchpoints[@]}
do
    /usr/bin/python3.6 attri_report_model_running.py ${touchpoint} MG $cur_month_start
    /usr/bin/python3.6 attri_report_model_running.py ${touchpoint} RW $cur_month_start
    sh attri_report_save_result.sh $queuename $touchpoint $pt_month
done

sh attri_report_merge_result.sh $pt 'MG'
sh attri_report_merge_result.sh $pt 'RW'
echo "Merging attribution result......"

echo "Finish generating attribution result......"
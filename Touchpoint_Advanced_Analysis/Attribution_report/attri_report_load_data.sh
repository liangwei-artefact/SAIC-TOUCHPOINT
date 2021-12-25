#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Attribution_Report
#*程序: attri_report_load_data.sh
#*功能: 从hive中读取计算贡献度所需的数据并存储成csv
#*开发人: Boyan & Xiaofeng
#*开发日: 2021-09-05
#*修改记录:  
#*    
#*********************************************************************/

pt=$1
pt_month=$(date -d "${pt}" +%Y%m)
cur_month_start=$(date -d "${pt_month}01" +%Y%m%d)
cur_month_end=$(date -d "${cur_month_start} +1 month -1 day" +%Y%m%d)
bf_month_start=$(date -d "${cur_month_start} -6 month" +%Y%m%d)
bf_month_end=$(date -d "${cur_month_end} -1 month -1 day" +%Y%m%d)
cd $(dirname $(readlink -f $0))
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

echo "pt:" $pt
echo "pt_month:" $pt_month
echo "cur_month_start:" $cur_month_start
echo "cur_month_end:" $cur_month_end
echo "bf_month_start:" $bf_month_start
echo "bf_month_end:" $bf_month_end

echo "Generate temporary table to store MG selected data......"
hive -hivevar queuename=queuename --hivevar cur_month_start=$cur_month_start --hivevar cur_month_end=$cur_month_end --hivevar bf_month_start=$bf_month_start -e "
set tez.queue.name=${queuename};
set mapreduce.map.memory.mb=4096;

DROP TABLE IF EXISTS marketing_modeling.tmp_mg_tp_analysis_base; 
CREATE TABLE IF NOT EXISTS marketing_modeling.tmp_mg_tp_analysis_base AS
SELECT
    b.mobile,
    b.action_time,
    b.touchpoint_id
FROM 
(
    SELECT a.mobile
    FROM marketing_modeling.dw_mg_tp_ts_all_i a
    LEFT JOIN dtwarehouse.cdm_dim_dealer_employee_info b
    ON a.mobile = b.mobile
    WHERE 
        pt >= ${cur_month_start} AND pt <= ${cur_month_end}
        AND b.mobile IS NULL
        GROUP BY a.mobile
) a
INNER JOIN
(
    SELECT *
    FROM marketing_modeling.dw_mg_tp_ts_all_i a
    WHERE pt >= ${bf_month_start} AND pt <= ${cur_month_end}
) b
ON a.mobile = b.mobile;
" 

echo "Generate temporary table to store RW selected data......"
hive -hivevar queuename=queuename --hivevar cur_month_start=$cur_month_start --hivevar cur_month_end=$cur_month_end --hivevar bf_month_start=$bf_month_start -e "
set tez.queue.name=${queuename};
set mapreduce.map.memory.mb=4096;

DROP TABLE IF EXISTS marketing_modeling.tmp_rw_tp_analysis_base; 
CREATE TABLE IF NOT EXISTS marketing_modeling.tmp_rw_tp_analysis_base AS
SELECT
    b.mobile,
    b.action_time,
    b.touchpoint_id
FROM 
(
    SELECT a.mobile
    FROM marketing_modeling.dw_rw_tp_ts_all_i a
    LEFT JOIN dtwarehouse.cdm_dim_dealer_employee_info b
    ON a.mobile = b.mobile
    WHERE 
        pt >= ${cur_month_start} AND pt <= ${cur_month_end}
        AND b.mobile IS NULL
        GROUP BY a.mobile
) a
INNER JOIN
(
    SELECT *
    FROM marketing_modeling.dw_rw_tp_ts_all_i a
    WHERE pt >= ${bf_month_start} AND pt <= ${cur_month_end}
) b
ON a.mobile = b.mobile;
" 

echo "Finish generating temporary tables......"

echo "Start to load data......"
hive -hivevar queuename=queuename -e "set tez.queue.name=${queuename}; SELECT * FROM marketing_modeling.tmp_mg_tp_analysis_base" > mg_tp_analysis_base.csv
hive -hivevar queuename=queuename -e "set tez.queue.name=${queuename}; SELECT * FROM marketing_modeling.tmp_rw_tp_analysis_base" > rw_tp_analysis_base.csv

hive -hivevar queuename=queuename -e "set tez.queue.name=${queuename}; SELECT touchpoint_id, touchpoint_name FROM marketing_modeling.cdm_touchpoints_id_system" > id_mapping.csv

echo "Successfully load data and save locally......"
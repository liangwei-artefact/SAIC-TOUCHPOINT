#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Data_Processing/Data_Increment/
#*程序: cdm_ts_completed_fail_insertion.sh
#*功能: 加工“战败激活”触点
#*开发人: Xiaofeng XU
#*开发日期: 2021-09-05
#*修改记录: 
#*          
#*********************************************************************/
pt=$3
bt=$(date -d "1 days ago $pt" +%Y%m%d )

hive --hivevar pt=$pt --hivevar bt=$bt -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_completed_oppor_fail_i PARTITION (pt,brand)
SELECT 
    a.phone AS mobile,
    action_time,
    '014004000000_tp' AS touchpoint_id,
    pt,
    'MG' AS brand
FROM 
(
    SELECT 
        phone,
        mg_cust_stage,
        TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(pt,'yyyyMMdd'))) AS action_time,
        pt
    FROM marketing_modeling.dw_cust_stage_ref_a
    WHERE mg_cust_stage = '战败'
    AND pt = '${pt}'
) a
LEFT JOIN
(
    SELECT 
        phone,
        mg_cust_stage
    FROM marketing_modeling.dw_cust_stage_ref_a
    WHERE mg_cust_stage = '战败'
    AND pt = '${bt}'
) b
ON a.phone = b.phone
WHERE b.phone IS NULL

UNION ALL

SELECT 
    a.phone AS mobile,
    action_time,
    '014004000000_rw' AS touchpoint_id,
    pt,
    'RW' AS brand
FROM 
(
    SELECT 
        phone,
        rw_cust_stage,
        TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(pt,'yyyyMMdd'))) AS action_time,
        pt
    FROM marketing_modeling.dw_cust_stage_ref_a
    WHERE rw_cust_stage = '战败'
    AND pt = '${pt}'
) a
LEFT JOIN
(
    SELECT 
        phone,
        rw_cust_stage
    FROM marketing_modeling.dw_cust_stage_ref_a
    WHERE rw_cust_stage = '战败'
    AND pt = '${bt}'
) b
ON a.phone = b.phone
WHERE b.phone IS NULL;"
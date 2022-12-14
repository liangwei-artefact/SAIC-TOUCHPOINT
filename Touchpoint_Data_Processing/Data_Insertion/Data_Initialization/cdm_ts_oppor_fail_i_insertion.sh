#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=malg;
set hive.exec.dynamic.partition=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT overwrite TABLE marketing_modeling.cdm_ts_oppor_fail_i partition (pt,brand)
SELECT
mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
FROM
(SELECT mobile AS mobile,
       create_time AS action_time,
       CASE
           WHEN audit_status = '90281002' AND brand_id = 121 THEN '014001000000_tp' -- 意向战败分配至其他跟进人员
           WHEN audit_status = '90281003' AND brand_id = 121 THEN '014002000000_tp' -- 驳回战败申请
           WHEN audit_status = '90281004' AND brand_id = 121 THEN '014003000000_tp' -- 同意战败申请

           WHEN audit_status = '90281002' AND brand_id = 101 THEN '014001000000_rw' -- 意向战败分配至其他跟进人员
           WHEN audit_status = '90281003' AND brand_id = 101 THEN '014002000000_rw' -- 驳回战败申请
           WHEN audit_status = '90281004' AND brand_id = 101 THEN '014003000000_rw' -- 同意战败申请
       END AS touchpoint_id,
       pt AS pt,
       CASE
           WHEN brand_id = 121 THEN 'MG'
           WHEN brand_id = 101 THEN 'RW'
       END AS brand
FROM 
  (
    select
    phone mobile,
    to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') create_time,
    detail['audit_status'] audit_status,
    detail['brand_id'] brand_id,
    pt 
    from cdp.cdm_cdp_customer_behavior_detail
    where
    type ='failed_tp_new'
    and pt = '${pt2}'
    and regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') <= '${pt2}' and regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') >= '${pt1}'
    and phone regexp '^[1][3-9][0-9]{9}$'
    AND to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') IS NOT NULL

  ) x

) t1
where 
    touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
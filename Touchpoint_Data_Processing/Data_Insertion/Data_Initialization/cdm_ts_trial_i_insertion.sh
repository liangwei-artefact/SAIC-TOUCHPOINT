#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Data_Processing/Data_Initialization/
#*程序: cdm_ts_trial_i_insertion.sh
#*功能: 加工小B端试驾类触点
#*开发人: Junhai Ma
#*开发日期: 2021-06-04
#*修改记录: 
#*          
#*********************************************************************/

pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
set hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;

INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_trial_i PARTITION(pt,brand)
SELECT
 mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
FROM
(
    SELECT 
        mobile,
        action_time,
        CASE 
        WHEN status = '14021001' AND trial_type = 'E' AND brand_id = 121 THEN '007002001000_tp' --预约试乘确认
        WHEN status = '14021001' AND brand_id = 121 THEN '007002002000_tp' --预约试驾确认
        WHEN status = '14021005' AND brand_id = 121 THEN '007003000000_tp' -- 完成试乘试驾
        WHEN status = '14021006' AND brand_id = 121 THEN '007004000000_tp' -- 评价试乘试驾
        WHEN status = '14021007' AND brand_id = 121 THEN '007005000000_tp' -- 取消试乘试驾
    	WHEN status = '14021001' AND trial_type = 'E' AND brand_id = 101 THEN '007002001000_rw' --预约试乘确认
        WHEN status = '14021001' AND brand_id = 101 THEN '007002002000_rw' --预约试驾确认
        WHEN status = '14021005' AND brand_id = 101 THEN '007003000000_rw' -- 完成试乘试驾
        WHEN status = '14021006' AND brand_id = 101 THEN '007004000000_rw' -- 评价试乘试驾
        WHEN status = '14021007' AND brand_id = 101 THEN '007005000000_rw' -- 取消试乘试驾
        END AS touchpoint_id,
    	date_format(action_time,'yyyyMMdd') AS pt,
    	CASE
    	  WHEN brand_id = 121 THEN 'MG'
    	  WHEN brand_id = 101 THEN 'RW'
    	END AS brand
    FROM
    (
        select
        phone mobile,
        to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') action_time,
        detail['status'] status,
        detail['trial_type'] trial_type,
        detail['brand_id'] brand_id
        from cdp.cdm_cdp_customer_behavior_detail where type='trial_tp_new'
        and pt between '${pt1}' and '${pt2}'
        AND regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') >= '${pt1}'
        AND regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') <= '${pt2}'
        and detail['status'] IN ('14021005','14021006')
        and  phone regexp '^[1][3-9][0-9]{9}$'
    ) a 
) trial_tp_new
"
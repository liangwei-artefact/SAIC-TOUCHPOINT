#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Data_Processing/Data_Increment/
#*程序: cdm_ts_trial_i_insertion.sh
#*功能: 加工小B端试驾类触点
#*开发人: Junhai Ma
#*开发日期: 2021-06-04
#*修改记录: 
#*     sh cdm_ts_trial_i_insertion.sh 0 0 20211202
#*********************************************************************/

pt=$3
hive --hivevar pt=$pt -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;

INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_trial_i PARTITION(pt,brand)
SELECT * FROM
(SELECT 
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
FROM (
        select
        phone mobile,
        to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') action_time,
        detail['status'] status,
        detail['trial_type'] trial_type,
        detail['brand_id'] brand_id
        from cdp.cdm_cdp_customer_behavior_detail where type='trial_tp_new'
        and pt = ${pt}
        AND regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') >= '${pt}'
        and detail['status'] IN ('14021001','14021005','14021006','14021007')
        and  phone regexp '^[1][3-9][0-9]{9}$'
    ) a
) trial_tp_new
"
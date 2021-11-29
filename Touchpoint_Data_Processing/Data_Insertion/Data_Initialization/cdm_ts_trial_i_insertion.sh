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

pt1=$3
pt2=$4
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 -e "
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
    SELECT 
        customer_id,
        status,
        create_time AS action_time,
		trial_type
    -- FROM dtwarehouse.ods_dlm_t_trial_receive
	FROM dtwarehouse.ods_dlm_t_trial_receive_tmp
	WHERE pt = ${pt2}
	AND regexp_replace(to_date(create_time), '-', '') >= '${pt1}'
    AND regexp_replace(to_date(create_time), '-', '') <= '${pt2}'
) a
LEFT JOIN (
    SELECT 
        id, mobile, brand_id
    FROM
    (SELECT id, mobile, dealer_id FROM dtwarehouse.ods_dlm_t_cust_base WHERE pt = ${pt2}) a
    LEFT JOIN (SELECT dlm_org_id, brand_id FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = ${pt2}) b
    ON a.dealer_id = b.dlm_org_id
) b
ON a.customer_id = b.id
WHERE status IN ('14021001','14021005','14021006','14021007')
AND b.mobile regexp '^[1][3-9][0-9]{9}$'
"
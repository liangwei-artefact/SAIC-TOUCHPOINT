#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false; 

INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_dlm_call_i PARTITION(pt,brand)
SELECT
mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
FROM
(
	SELECT phone AS mobile,
		   cast(from_unixtime(unix_timestamp(cast(action_time as string), 'yyyyMMddHHmmss')) as TIMESTAMP) as action_time,
		   CASE
			   WHEN call_type = 'Inbound' and brand_id = 121
					AND talk_length > 0
					AND talk_length < 10 THEN '009003001001_tp'
			   WHEN call_type = 'Inbound' and brand_id = 121
					AND talk_length >= 10
					AND talk_length < 30 THEN '009003001002_tp'
			   WHEN call_type = 'Inbound' and brand_id = 121
					AND talk_length >= 30
					AND talk_length < 60 THEN '009003001003_tp'
			   WHEN call_type = 'Inbound' and brand_id = 121
					AND talk_length > 60 THEN '009003001004_tp'
			   WHEN call_type = 'Inbound' and brand_id = 121
					AND talk_length = 0 THEN '009003002000_tp'
			   WHEN call_type = 'Inbound' and brand_id = 121
					AND talk_length is null THEN '009003002000_tp'
			   WHEN call_type = 'Outbound' and brand_id = 121
					AND talk_length > 0
					AND talk_length < 10 THEN '009003003001_tp'
			   WHEN call_type = 'Outbound' and brand_id = 121
					AND talk_length >= 10
					AND talk_length < 30 THEN '009003003002_tp'
			   WHEN call_type = 'Outbound' and brand_id = 121
					AND talk_length >= 30
					AND talk_length < 60 THEN '009003003003_tp'
			   WHEN call_type = 'Outbound' and brand_id = 121
					AND talk_length > 60 THEN '009003003004_tp'
			   WHEN call_type = 'Outbound' and brand_id = 121
					AND talk_length = 0 THEN '009003004000_tp'
			   WHEN call_type = 'Outbound' and brand_id = 121
					AND talk_length is null THEN '009003004000_tp'


			   WHEN call_type = 'Inbound' and brand_id = 101
					AND talk_length > 0
					AND talk_length < 10 THEN '009003001001_rw'
			   WHEN call_type = 'Inbound' and brand_id = 101
					AND talk_length >= 10
					AND talk_length < 30 THEN '009003001002_rw'
			   WHEN call_type = 'Inbound' and brand_id = 101
					AND talk_length >= 30
					AND talk_length < 60 THEN '009003001003_rw'
			   WHEN call_type = 'Inbound' and brand_id = 101
					AND talk_length > 60 THEN '009003001004_rw'
			   WHEN call_type = 'Inbound' and brand_id = 101
					AND talk_length = 0 THEN '009003002000_rw'
			   WHEN call_type = 'Inbound' and brand_id = 101
					AND talk_length is null THEN '009003002000_rw'

			   WHEN call_type = 'Outbound' and brand_id = 101
					AND talk_length > 0
					AND talk_length < 10 THEN '009003003001_rw'
			   WHEN call_type = 'Outbound' and brand_id = 101
					AND talk_length >= 10
					AND talk_length < 30 THEN '009003003002_rw'
			   WHEN call_type = 'Outbound' and brand_id = 101
					AND talk_length >= 30
					AND talk_length < 60 THEN '009003003003_rw'
			   WHEN call_type = 'Outbound' and brand_id = 101
					AND talk_length > 60 THEN '009003003004_rw'
			   WHEN call_type = 'Outbound' and brand_id = 101
					AND talk_length = 0 THEN '009003004000_rw'
			   WHEN call_type = 'Outbound' and brand_id = 101
					AND talk_length is null THEN '009003004000_rw'
		   END AS touchpoint_id,
		   CASE
			   WHEN brand_id = 121 THEN 'MG'
			   WHEN brand_id = 101 THEN 'RW'
		   END AS brand,
		   pt
	FROM
	(
		SELECT 
			phone,
			detail['begin_time'] as action_time,
			detail['call_type'] as call_type,
			detail['talk_length'] AS talk_length,
			pt,
			brand_id
		FROM 
			( 
				SELECT phone, detail, pt,
				detail['brand_id'] brand_id
				FROM cdp.cdm_cdp_customer_behavior_detail
				WHERE TYPE = 'dlm_call'
					AND pt >= ${pt1}
					AND pt <= ${pt2}
			) a
	) t
) t1
WHERE  
    mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
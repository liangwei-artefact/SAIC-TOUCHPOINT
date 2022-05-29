#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
echo $pt1""$pt2
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "set hive.exec.dynamic.partition.mode=nonstrict;
set tez.queue.name=${queue_name};
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_ai_call_i PARTITION(pt,brand)
SELECT
mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
FROM (
    SELECT phone AS mobile,
       detail['call_time'] AS action_time,
       CASE
		   WHEN detail['call_result'] not in ('接通后我方挂机','接通后客户挂机','正常挂机','主动挂机','接通') and detail['oppor_brand'] = '121' THEN '002005004000_tp'
           WHEN detail['talk_length'] > 0.0 and detail['talk_length'] < 10 and detail['oppor_brand'] = '121' THEN '002005003001_tp'
           WHEN detail['talk_length'] >= 10 and detail['talk_length'] < 30 and detail['oppor_brand'] = '121' THEN '002005003002_tp'
           WHEN detail['talk_length'] >= 30 and detail['talk_length'] < 60 and detail['oppor_brand'] = '121' THEN '002005003003_tp'
           WHEN detail['talk_length'] >= 60 and detail['oppor_brand'] = '121' THEN '002005003004_tp'
		   WHEN detail['oppor_brand'] = '121' THEN '002011004000_tp'
		   WHEN detail['call_result'] not in ('接通后我方挂机','接通后客户挂机','正常挂机','主动挂机','接通') and detail['oppor_brand'] = '101' THEN '002005004000_rw'
           WHEN detail['talk_length'] > 0.0 and detail['talk_length'] < 10 and detail['oppor_brand'] = '101' THEN '002005003001_rw'
           WHEN detail['talk_length'] >= 10 and detail['talk_length'] < 30 and detail['oppor_brand'] = '101' THEN '002005003002_rw'
           WHEN detail['talk_length'] >= 30 and detail['talk_length'] < 60 and detail['oppor_brand'] = '101' THEN '002005003003_rw'
           WHEN detail['talk_length'] >= 60 and detail['oppor_brand'] = '101' THEN '002005003004_rw'
		   WHEN detail['oppor_brand'] = '101' THEN '002005004000_rw'
       END AS touchpoint_id,
       pt,
       CASE
           WHEN detail['oppor_brand'] = '121' THEN 'MG'
           WHEN detail['oppor_brand'] = '101' THEN 'RW'
       END AS brand
    FROM cdp.cdm_cdp_customer_behavior_detail
    WHERE TYPE = 'ai_call'
    AND pt >= '${pt1}' AND pt <= '${pt2}'
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
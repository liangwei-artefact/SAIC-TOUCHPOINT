#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
set hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_maintenance_i PARTITION(pt,brand)
select
mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
from
(select 
    phone as mobile,
    detail['start_time'] as action_time,
    case
        when detail['repair_type'] like '%免费保养%' and detail['brand'] = 'MG' then '018001001000_tp'
        when detail['repair_type'] like '%保养%' and detail['brand'] = 'MG' then '018001002000_tp'
        when (detail['repair_type'] like '%检查%' or detail['repair_type'] like '%检测%') and detail['brand'] = 'MG' then '018001003000_tp'
        when detail['repair_type'] like '%事故%' and detail['brand'] = 'MG' then '018002001000_tp'
        when detail['repair_type'] like '%保修%' and detail['brand'] = 'MG'then '018002002000_tp'
        when (detail['repair_type'] like '%小修%' or detail['repair_type'] like '%维修%' or detail['repair_type'] like '%修理%') and detail['brand'] = 'MG' then '018002003000_tp'
        when detail['repair_type'] like '%检修%' and detail['brand'] = 'MG' then '018002004000_tp'
        when detail['brand'] = 'MG' then '018003001000_tp'

        when detail['repair_type'] like '%免费保养%' and detail['brand'] = 'ROEWE' then '018001001000_rw'
        when detail['repair_type'] like '%保养%' and detail['brand'] = 'ROEWE' then '018001002000_rw'
        when (detail['repair_type'] like '%检查%' or detail['repair_type'] like '%检测%') and detail['brand'] = 'ROEWE' then '018001003000_rw'
        when detail['repair_type'] like '%事故%' and detail['brand'] = 'ROEWE' then '018002001000_rw'
        when detail['repair_type'] like '%保修%' and detail['brand'] = 'ROEWE' then '018002002000_rw'
        when (detail['repair_type'] like '%小修%' or detail['repair_type'] like '%维修%' or detail['repair_type'] like '%修理%') and detail['brand'] = 'ROEWE' then '018002003000_rw'
        when detail['repair_type'] like '%检修%' and detail['brand'] = 'ROEWE' then '018002004000_rw'
        when detail['brand'] = 'ROEWE' then '018003001000_rw'
    end as touchpoint_id,
    case
      when detail['brand'] = 'MG' THEN 'MG'
      when detail['brand'] = 'ROEWE' THEN 'RW'
    end as brand,
    pt
from cdp.cdm_cdp_customer_behavior_detail
where type = 'maintenance' and pt >= '${pt1}' and pt <= '${pt2}'
) t1
where 
	mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"

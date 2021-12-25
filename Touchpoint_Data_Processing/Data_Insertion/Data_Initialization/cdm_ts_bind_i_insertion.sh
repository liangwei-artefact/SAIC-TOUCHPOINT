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
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_bind_i PARTITION(pt,brand)
select
mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
 from
(
	select 
		phone as mobile,
		to_utc_timestamp(detail['created_date'],'yyyy-MM-dd HH:mm:ss') as action_time,
		case
			when detail['bind_type'] = '0' and detail['brand_code'] = '2' then '019001000000_tp' -- 弱绑车
			when detail['bind_type'] = '1' and detail['brand_code'] = '2' then '019002000000_tp' -- 强绑车
			when detail['bind_type'] = '0' and detail['brand_code'] = '1' then '019001000000_rw' -- 弱绑车
			when detail['bind_type'] = '1' and detail['brand_code'] = '1' then '019002000000_rw' -- 强绑车
		end as touchpoint_id,
			pt,
		case
		  when detail['brand_code'] = '2' THEN 'MG'
		  when detail['brand_code'] = '1' THEN 'RW'
		end as brand
	from cdp.cdm_cdp_customer_behavior_detail
	where type = 'bind' and pt >= ${pt1} and pt <= ${pt2}
) t1
where
	mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"

pt=$3
hive --hivevar pt=$pt -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamici.partition=true;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
set hive.execution.engine=mr;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_bind_i PARTITION(pt,brand)
select * from
(
	select
		phone as mobile,
		detail['created_date'] as action_time,
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
  where type = 'bind' and pt = ${pt}
) t1
where
	mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"

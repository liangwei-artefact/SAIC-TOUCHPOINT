pt=$3
hive --hivevar pt=$pt -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_maintenance_i PARTITION(pt,brand)
select * from
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
    pt,
    case
      when detail['brand'] = 'MG' THEN 'MG'
      when detail['brand'] = 'ROEWE' THEN 'RW'
    end as brand
from cdp.cdm_cdp_customer_behavior_detail
where type = 'maintenance' and pt >= '${pt}'
) t1
where 
	mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"

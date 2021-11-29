#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Data_Processing/Data_Increment/
#*程序: cdm_ts_adhoc_app_activity_i_insertion.sh
#*功能: 临时性的APP线上活动触点
#*开发人: Xiaofeng XU
#*开发日期: 2021-07-04
#*修改记录: 
#*          
#*********************************************************************/

pt=$3
hive --hivevar pt=$pt -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table marketing_modeling.cdm_ts_adhoc_app_activity_i partition (pt, brand)
select 
    a.*
from
(
    select 
        cellphone as mobile,
        b.activity_name,
        b.activity_type,
        min(a.created_date) as action_time,
        '008002006005_tp' as touchpoint_id, -- APP其他活动参与
		regexp_replace(to_date(a.created_date), '-', '') as pt,
		case 
			when brand_code = 2 then 'MG'
			else NULL
		end as brand
    from
	(
		select * from dtwarehouse.ods_ccmpoint_points_record
		where pt = '${pt}'
		and regexp_replace(to_date(created_date), '-', '') >= ${pt}
		and status = 'SUCCESS'
		and action = 'INCREASE'
	) a
    join marketing_modeling.dw_adhoc_app_activity b
    on a.description = b.activity_name
    join
    (
    	select cellphone, uid
    	from
    	(
    		select
    		    cellphone, uid,
    			Row_Number() OVER (partition by uid ORDER BY regist_date) rank_num
    		from dtwarehouse.ods_ccm_member
    		where pt = '${pt}'
    		and cellphone is not NULL
    		and uid is not NULL
    	) c0
    	where rank_num = 1
    ) c
    on a.uid = c.uid
    group by cellphone, b.activity_name, b.activity_type,
		case
		   when brand_code = 2 then 'MG'
		   else NULL
		end
) a
where
	mobile regexp '^[1][3-9][0-9]{9}$'
	and action_time is not NULL
	and touchpoint_id is not NULL
"

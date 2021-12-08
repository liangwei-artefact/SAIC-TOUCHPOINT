#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Data_Processing/Data_Initialization/
#*程序: cdm_ts_adhoc_app_activity_i_insertion.sh
#*功能: 临时性的APP线上活动触点
#*开发人: Xiaofeng XU
#*开发日期: 2021-07-04
#*修改记录: 
#*          
#*********************************************************************/

pt1=$3
pt2=$4
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table marketing_modeling.cdm_ts_adhoc_app_activity_i partition (pt,brand)
select 
    a.*
from
(
    select 
        cellphone as mobile,
        r.activity_name,
        r.activity_type,
        min(r.created_date) as action_time,
        '008002006005_tp' as touchpoint_id, 
		regexp_replace(to_date(r.created_date), '-', '') as pt,
		case 
			when brand_code = 2 then 'MG'
			else NULL
		end as brand
    from 
  (
    select
    phone cellphone,
    to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') as created_date,
    detail['description'] activity_name,
    detail['action'] activity_type,
    detail['brand_code'] brand_code
    from cdp.cdm_cdp_customer_behavior_detail cccbd where type='score'
    and  pt = '${pt2}'
    and regexp_replace(to_date(detail['behavior_time']), '-', '') >= ${pt1}
    and regexp_replace(to_date(detail['behavior_time']), '-', '') <= ${pt2}
  ) r
  group by cellphone, r.activity_name, r.activity_type,
		case
		   when brand_code = 2 then 'MG'
		   else NULL
		end
) a
where
	mobile regexp '^[1][3-9][0-9]{9}$'
	and action_time is not NULL
	and touchpoint_id is not NULL
group by cellphone,
"

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
echo $3" "$4
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;
-- set hive.execution.engine=mr;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;

insert overwrite table marketing_modeling.cdm_ts_adhoc_app_activity_i partition (pt,brand)
select
mobile
,activity_name
,activity_type
,action_time
,touchpoint_id
,cast(pt as string) pt
,cast(brand as string) brand
from (
select
    a.mobile,
    a.activity_name,
    a.activity_type,
    a.action_time,
    a.touchpoint_id,
    regexp_replace(to_date(action_time), '-', '') as pt,
    brand
from
(
    select 
        cellphone as mobile,
        r.activity_name,
        r.activity_type,
        min(r.created_date) as action_time,
        '008002006005_tp' as touchpoint_id,
		case 
			when brand_code = 2 then 'MG'
			else NULL
		end as brand
    from 
  (
    select
    c.cellphone,
    b.activity_name,
    b.activity_type,
    c.created_date,
    c.brand_code
    from (
    select
    phone cellphone,
    to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') as created_date,
    detail['description'] activity_name,
    detail['action'] activity_type,
    detail['brand_code'] brand_code
    from cdp.cdm_cdp_customer_behavior_detail cccbd where type='score'
    and  pt = '${pt2}'
    and regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') >= ${pt1}
    and regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') <= ${pt2}
    ) c
    join marketing_modeling.cdm_adhoc_app_activity b -- 这个表是我们自己添加的
    on c.activity_name = b.ccm_points_description
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
	and touchpoint_id is not NULL) w
"

#!/bin/bash
#特别说明：一个程序只修改一张正式表
#/*********************************************************************
#*模块: cdp.cdm_cdp_customer_behavior_detail
#*程序: cdp.cdm_cdp_customer_behavior_detail
#*功能: cdp.cdm_cdp_customer_behavior_detail
#*开发人: 陈良伟
#*开发日:
#*
#*
#*********************************************************************/

pt=$(date -d "-0 day $3 " +%Y%m%d)
hive --hivevar pt=${pt} -e "
set tez.queue.name=${queuename};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.position.alias=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;
insert overwrite table cdp.cdm_cdp_customer_behavior_detail partition (type,pt)
select
case phone when '' then phone when null then phone else sha2(phone,256) end phone
,device_id
,union_id
,cookies
,detail
,cast(type as string) type
,cast(pt as string) pt
from p_cdp.cdm_cdp_customer_behavior_detail where pt>='20211201'"

if [ $? -ne 0 ]; then
    echo "failed"
  exit 1
else
    echo "succeed"
fi
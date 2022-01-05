#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Attribution_Report
#*程序: attri_report_merge_result.sh
#*功能: Merge ml attribution and mk attribution into a single table
#*开发人: Boyan
#*开发日: 2021-11-02
#*修改记录:
#*
#*********************************************************************/

pt=$1
brand=$2
pt_month=$(date -d "${pt}" +%Y%m)

queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
cd $(dirname $(readlink -f $0))
hive -hivevar queuename=$queuename --hivevar pt_month=$pt_month --hivevar brand=$brand -e "
set tez.queue.name=${queuename};
set mapreduce.map.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=2048;
set hive.exec.max.dynamic.partitions.pernode=1000;


INSERT OVERWRITE TABLE marketing_modeling.app_attribution_report PARTITION(pt, brand)
select a.touchpoint_id as touchpoint_id,
       a.touchpoint_name as touchpoint_name,
       a.instore_mk_attribution as instore_mk_attribution,
       a.trial_mk_attribution as trial_mk_attribution,
       a.deal_mk_attribution as deal_mk_attribution,
       b.instore_ml_attribution as instore_ml_attribution,
       b.trial_ml_attribution as trial_ml_attribution,
       b.deal_ml_attribution as deal_ml_attribution,
       a.pt as pt,
       a.brand as brand
from (
         select touchpoint_id,
                min(touchpoint_name)        as touchpoint_name,
                min(instore_mk_attribution) as instore_mk_attribution,
                min(trial_mk_attribution)   as trial_mk_attribution,
                min(deal_mk_attribution)    as deal_mk_attribution,
                pt,
                brand
         from (
                  select touchpoint_id,
                         touchpoint_name,
                         IF(attribution_tp = 'instore', attribution, null) as instore_mk_attribution,
                         IF(attribution_tp = 'trial', attribution, null)   as trial_mk_attribution,
                         IF(attribution_tp = 'deal', attribution, null)    as deal_mk_attribution,
                         brand,
                         pt
                  from marketing_modeling.app_mk_attribution_report
                  where pt = '${pt_month}'
                    and brand = '${brand}'
              ) m
         group by touchpoint_id, pt, brand
     ) a
         left join (
    select touchpoint_id,
           min(touchpoint_name)        as touchpoint_name,
           min(instore_ml_attribution) as instore_ml_attribution,
           min(trial_ml_attribution)   as trial_ml_attribution,
           min(deal_ml_attribution)    as deal_ml_attribution,
           pt,
           brand
    from (
             select touchpoint_id,
                    touchpoint_name,
                    IF(attribution_tp = 'instore', attribution, null) as instore_ml_attribution,
                    IF(attribution_tp = 'trial', attribution, null)   as trial_ml_attribution,
                    IF(attribution_tp = 'deal', attribution, null)    as deal_ml_attribution,
                    brand,
                    pt
             from marketing_modeling.app_ml_attribution_report
             where pt = '${pt_month}'
               and brand = '${brand}'
         ) m
    group by touchpoint_id, pt, brand
) b
                   on a.touchpoint_id = b.touchpoint_id
"
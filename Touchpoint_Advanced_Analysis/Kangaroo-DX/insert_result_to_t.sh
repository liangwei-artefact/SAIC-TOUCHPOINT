#!/bin/bash
hive -e "

insert overwrite table marketing_modeling.app_mk_attribution_report_t
select
*
from marketing_modeling.app_mk_attribution_report;


insert overwrite table marketing_modeling.app_ml_attribution_report_t
select
*
from marketing_modeling.app_ml_attribution_report;


insert overwrite table marketing_modeling.app_attribution_report_t
select
*
from marketing_modeling.app_attribution_report;


insert overwrite table marketing_modeling.app_fir_contact_conversion_report_monthly_a_t
SELECT
*
from marketing_modeling.app_fir_contact_conversion_report_monthly_a;

insert overwrite table marketing_modeling.app_fir_contact_conversion_report_weekly_a_t
select
 a.mac_code
,a.rfs_code
,a.area
,a.is_sec_net
,a.activity_name
,a.fir_contact_tp_id
,a.fir_contact_series
,a.fir_contact_week
,a.cust_vol
,a.instore_vol
,a.trial_vol
,a.consume_vol
,a.deliver_vol
,a.brand
,a.pt
from
(
SELECT
*
from marketing_modeling.app_fir_contact_conversion_report_weekly_a) a;


insert overwrite table marketing_modeling.app_touchpoints_profile_monthly_t
select
 a.mobile
,a.fir_contact_month
,a.fir_contact_date
,a.fir_contact_series
,a.mac_code
,a.rfs_code
,a.area
,a.is_sec_net
,a.activity_name
,a.fir_contact_tp_id
,b.touchpoint_level
,b.level_1_tp_id
,a.brand
,a.pt
from
(select
mobile
,fir_contact_month
,fir_contact_date
,fir_contact_series
,mac_code
,rfs_code
,area
,is_sec_net
,activity_name
,fir_contact_tp_id
,brand
,pt
from marketing_modeling.app_touchpoints_profile_monthly where pt>='202201') a
left join
(
select
touchpoint_id,
touchpoint_level,
level_1_tp_id,
brand
from marketing_modeling.cdm_touchpoints_id_system
) b on a.fir_contact_tp_id=b.touchpoint_id and a.brand=b.brand;

insert overwrite table marketing_modeling.app_touchpoints_profile_weekly_t
select
*
from marketing_modeling.app_touchpoints_profile_weekly where pt>='2022W1';

insert overwrite table marketing_modeling.app_tp_asset_report_a_t
select
distinct
touchpoint_level
,touchpoint_id
,fir_contact_month
,fir_contact_tp_id
,cast( int(fir_contact_series) as string) fir_contact_series
,mac_code
,rfs_code
,area
,tp_pv
,tp_uv
,instore_vol
,trial_vol
,consume_vol
,cust_vol
,exit_pv
,undeal_vol
,tp_coverage
,tp_avg_times
,exit_rate
,tp_instore_rate
,tp_trial_rate
,tp_deal_rate
,pt
,brand
from marketing_modeling.app_tp_asset_report_a
where touchpoint_id !='NaN'
;


insert overwrite table marketing_modeling.app_dim_tree_big_small_area_t
select
distinct
*
from marketing_modeling.app_dim_tree_big_small_area;

insert overwrite table marketing_modeling.app_dim_area_t
select
distinct
*
from marketing_modeling.app_dim_area where area is not null;

insert overwrite table marketing_modeling.app_dim_activity_name_t
select
distinct
*
from marketing_modeling.app_dim_activity_name where activity_name is not null and trim(activity_name)!='';

insert overwrite table marketing_modeling.app_dim_car_series_t
select
distinct
b.series_chinese_name fir_contact_series,
a.brand,
a.fir_contact_series fir_contact_series_id
from
(
select
fir_contact_series,
brand
from marketing_modeling.app_dim_car_series
) a
left join
(
select
series_id,
series_chinese_name
from dtwarehouse.cdm_dim_series ) b
on a.fir_contact_series=b.series_id
where b.series_chinese_name != 'null'
;


insert overwrite table marketing_modeling.app_month_map_week_t
select
distinct
*
from marketing_modeling.app_month_map_week where month_key >= '202201';

insert overwrite table marketing_modeling.app_undeal_report_a_t
select
a.mobile ,
a.fir_contact_month ,
a.fir_contact_tp_id ,
a.tp_id ,
a.fir_contact_series ,
a.mac_code ,
a.rfs_code ,
a.area ,
a.undeal_vol,
b.touchpoint_level,
b.level_1_tp_id,
a.pt,
a.brand
from
(
select
distinct
mobile ,
fir_contact_month ,
fir_contact_tp_id ,
tp_id ,
cast( int(fir_contact_series) as string) fir_contact_series ,
mac_code ,
rfs_code ,
area ,
undeal_vol ,
pt ,
brand
from marketing_modeling.app_undeal_report_a
) a
left join
(
select
touchpoint_id,
touchpoint_level,
level_1_tp_id,
brand
from marketing_modeling.cdm_touchpoints_id_system
) b on a.fir_contact_tp_id=b.touchpoint_id and a.brand=b.brand;

insert overwrite table marketing_modeling.app_other_fir_contact_tp
SELECT
fir_contact_fir_sour_brand,
fir_contact_sec_sour_brand,
brand
from marketing_modeling.cdm_customer_touchpoints_profile_a
where touchpoint_id in ('001012000000_rw','001012000000_tp')
and pt>='20220101'
group by
fir_contact_fir_sour_brand,
fir_contact_sec_sour_brand,
brand
"

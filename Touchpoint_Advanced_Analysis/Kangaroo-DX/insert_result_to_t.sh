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


-- exclude level_4
insert overwrite table marketing_modeling.app_fir_contact_conversion_report_monthly_a_t
select
a.*
from
(
SELECT
*
from marketing_modeling.app_fir_contact_conversion_report_monthly_a ) a
inner join
(
select
touchpoint_id,
touchpoint_level
from
marketing_modeling.cdm_touchpoints_id_system
where touchpoint_level = 4
) b
on a.fir_contact_tp_id = b.touchpoint_id;


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
from marketing_modeling.app_fir_contact_conversion_report_weekly_a) a
inner join
(
select
touchpoint_id,
touchpoint_level
from
marketing_modeling.cdm_touchpoints_id_system
where touchpoint_level = 4
)b
on a.fir_contact_tp_id = b.touchpoint_id;

insert overwrite table marketing_modeling.app_touchpoints_profile_monthly_t
select
*
from marketing_modeling.app_touchpoints_profile_monthly;

insert overwrite table marketing_modeling.app_touchpoints_profile_weekly_t
select
*
from marketing_modeling.app_touchpoints_profile_weekly;

insert overwrite table marketing_modeling.app_tp_asset_report_a_t
select
a.*
from
(
select
distinct
touchpoint_level
,touchpoint_id
,fir_contact_month
,fir_contact_tp_id
,fir_contact_series
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
where touchpoint_id !='NaN') a
inner join
(
select
touchpoint_id,
touchpoint_level
from
marketing_modeling.cdm_touchpoints_id_system
)b
on a.fir_contact_tp_id = b.touchpoint_id
;

insert overwrite table marketing_modeling.cdm_customer_touchpoints_profile_a_t
select
*
from marketing_modeling.cdm_customer_touchpoints_profile_a;



insert overwrite table marketing_modeling.app_dim_tree_big_small_area_t
select
distinct
*
from marketing_modeling.app_dim_tree_big_small_area;

insert overwrite table marketing_modeling.app_dim_area_t
select
distinct
*
from marketing_modeling.app_dim_area;

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
;


insert overwrite table marketing_modeling.app_month_map_week_t
select
distinct
*
from marketing_modeling.app_month_map_week;
"

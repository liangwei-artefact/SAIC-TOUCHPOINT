#!/bin/bash
#/*********************************************************************
#*模块: Touchpoint_Advanced_Analysis/Touchpoint_profile
#*程序: firsttouch_report_monthly.sh
#*功能: 0
#*开发人: Liangwei Chen
#*开发日期: 2022-01-07
#*修改记录:
#*
#*********************************************************************/

pt=$3
pt_month=$(date -d "${pt}" +%Y%m)
this_month_start=$(date -d "${pt_month}01" +%Y%m%d)
this_month_end=$(date -d "${this_month_start} +1 month -1 day" +%Y%m%d)
cd $(dirname $(readlink -f $0))

queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

hive -hivevar queuename=$queuename --hivevar pt=$pt --hivevar pt_month=$pt_month --hivevar this_month_start=$this_month_start --hivevar this_month_end=$this_month_end -e "
set tez.queue.name=${queuename};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.position.alias=true;
set mapreduce.map.memory.mb=4096;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;
set hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE marketing_modeling.cdm_customer_touchpoints_profile_a PARTITION (pt)
SELECT
rr.mobile,
rr.last_fir_contact_date_brand,
rr.mac_code,
rr.rfs_code,
rr.area,
rr.is_sec_net,
rr.activity_name,
rr.touchpoint_id,
rr.fir_contact_fir_sour_brand,
rr.fir_contact_sec_sour_brand,
rr.series_id as fir_contact_series_brand,
rr.brand,
regexp_replace(to_date(last_fir_contact_date_brand), '-', '') as pt
from
(
SELECT
r1.mobile,
r1.dealer_id,
r1.last_fir_contact_date_brand,
r1.fir_contact_fir_sour_brand,
r1.fir_contact_sec_sour_brand,
r1.activity_name,
r1.brand,
r1.mark,
r1.area,
r1.city_name,
r1.mac_code,
r1.rfs_code,
r1.is_sec_net,
r1.big_customer_flag,
r1.touchpoint_id,
r2.brand_id,
r2.series_id,
r2.behavior_time,
r2.pt,
row_number() over(partition by r1.mobile,r1.last_fir_contact_date_brand order by r2.behavior_time) num
from
(
SELECT
result.mobile,
result.dealer_id,
result.last_fir_contact_date_brand,
result.fir_contact_fir_sour_brand,
result.fir_contact_sec_sour_brand,
result.activity_name,
result.brand,
result.mark,
result.area,
result.city_name,
result.mac_code,
result.rfs_code,
result.is_sec_net,
result.big_customer_flag,
case when result.touchpoint_id is null and result.brand = 'MG' then '001010000000_tp'
when result.touchpoint_id is null and result.brand = 'RW' then '001012000000_rw'
else result.touchpoint_id end touchpoint_id
from
(
SELECT
df1.mobile,
df1.dealer_id,
df1.last_fir_contact_date_brand,
df1.fir_contact_fir_sour_brand,
df1.fir_contact_sec_sour_brand,
df1.activity_name,
df1.brand,
df1.mark,
df2.area,
df2.city_name,
df2.mac_code,
df2.rfs_code,
df2.is_sec_net,
df3.big_customer_flag,
case when df3.big_customer_flag is not null and df1.brand = 'MG' then '001011000000_tp'
when df1.dealer_id = '220000000398438' and df1.brand ='MG' then '001003000000_tp'
when df1.mark in ('order_vhcl','offline') and df1.brand = 'MG' then '001002000000_tp'
when df1.mark in ('order_vhcl','offline') and df1.brand = 'RW' then '001002000000_rw'
else df4.touchpoint_id end touchpoint_id
from
(
SELECT
fir_contact_brand_df.mobile,
fir_contact_brand_df.dealer_id,
fir_contact_brand_df.last_fir_contact_date_brand,
fir_contact_brand_df.fir_contact_fir_sour_brand,
fir_contact_brand_df.fir_contact_sec_sour_brand,
fir_contact_brand_df.activity_name,
fir_contact_brand_df.brand,
fir_contact_brand_df.mark
from
(
select
ra.mobile,
rb.dealer_id,
ra.last_fir_contact_date_brand,
ra.fir_contact_fir_sour_brand,
ra.fir_contact_sec_sour_brand,
ra.activity_name,
ra.brand,
'order_vhcl' AS mark
from
(
SELECT
    leads_id,
    mobile,
    create_time AS last_fir_contact_date_brand,
    businesstypecode,
    firstresourcename AS fir_contact_fir_sour_brand,
    secondresourcename AS fir_contact_sec_sour_brand,
    CASE
        WHEN chinese_name = '荣威' THEN 'RW'
        WHEN chinese_name = 'MG' THEN 'MG'
        ELSE ''
    END AS brand,
    activity_name
FROM dtwarehouse.cdm_offline_leads_first_contact_brand
WHERE
    regexp_replace(to_date(create_time), '-', '') >= ${this_month_start} AND
    regexp_replace(to_date(create_time), '-', '') <= ${this_month_end}
) ra
inner join
(
SELECT
	a.id AS leads_id,
	a.get_vel_phone AS mobile,
	a.dealer_id,
	CASE WHEN a.brand_id = '10000000000086' THEN 'RW' ELSE 'MG' END AS brand
FROM
(
	SELECT * FROM dtwarehouse.ods_dlm_t_deliver_vel
	WHERE
		pt = ${pt}
		AND dealer_id is not null
		AND create_user is not null
		AND regexp_replace(to_date(create_time), '-', '') >= ${this_month_start}
) a
LEFT JOIN
(
  SELECT * FROM dtwarehouse.ods_dlm_t_order_vhcl_relation
  WHERE
	pt = ${pt}
	AND oppor_id IS NOT NULL
) b
ON a.oppor_id = b.oppor_id
WHERE b.oppor_id IS NULL
) rb
on ra.leads_id =rb.leads_id and ra.mobile=rb.mobile and ra.brand=rb.brand
union
SELECT
rd_1.mobile,
re.dealer_id,
rd_1.last_fir_contact_date_brand,
rd_1.fir_contact_fir_sour_brand,
rd_1.fir_contact_sec_sour_brand,
rd_1.activity_name,
rd_1.brand,
'offline' AS mark
from
(select
rc.leads_id,
rc.mobile,
rc.last_fir_contact_date_brand,
rc.businesstypecode,
rc.fir_contact_fir_sour_brand,
rc.fir_contact_sec_sour_brand,
rc.brand,
rc.activity_name
from
(SELECT
    leads_id,
    mobile,
    create_time AS last_fir_contact_date_brand,
    businesstypecode,
    firstresourcename AS fir_contact_fir_sour_brand,
    secondresourcename AS fir_contact_sec_sour_brand,
    CASE
        WHEN chinese_name = '荣威' THEN 'RW'
        WHEN chinese_name = 'MG' THEN 'MG'
        ELSE ''
    END AS brand,
    activity_name
FROM dtwarehouse.cdm_offline_leads_first_contact_brand
WHERE
    regexp_replace(to_date(create_time), '-', '') >= ${this_month_start} AND
    regexp_replace(to_date(create_time), '-', '') <= ${this_month_end}
    and businesstypecode = '10000000'
) rc
left join
(
SELECT
	a.id AS leads_id,
	a.get_vel_phone AS mobile,
	a.dealer_id,
	CASE WHEN a.brand_id = '10000000000086' THEN 'RW' ELSE 'MG' END AS brand
FROM
(
	SELECT * FROM dtwarehouse.ods_dlm_t_deliver_vel
	WHERE
		pt = ${pt}
		AND dealer_id is not null
		AND create_user is not null
		AND regexp_replace(to_date(create_time), '-', '') >= ${this_month_start}
) a
LEFT JOIN
(
  SELECT * FROM dtwarehouse.ods_dlm_t_order_vhcl_relation
  WHERE
	pt = ${pt}
	AND oppor_id IS NOT NULL
) b
ON a.oppor_id = b.oppor_id
WHERE b.oppor_id IS NULL
) rd
on rc.leads_id =rd.leads_id
where rd.leads_id is null
) rd_1
left join
(
SELECT
id,
dealer_id
FROM dtwarehouse.ods_dlm_t_cust_base
WHERE pt = ${pt}) re
on re.id=rd_1.leads_id
union
SELECT
ri.mobile,
rh.dealerid as dealer_id,
ri.last_fir_contact_date_brand,
ri.fir_contact_fir_sour_brand,
ri.fir_contact_sec_sour_brand,
ri.activity_name,
ri.brand,
'online' AS mark
from
(
SELECT
    leads_id,
    mobile,
    create_time AS last_fir_contact_date_brand,
    businesstypecode,
    firstresourcename AS fir_contact_fir_sour_brand,
    secondresourcename AS fir_contact_sec_sour_brand,
    CASE
        WHEN chinese_name = '荣威' THEN 'RW'
        WHEN chinese_name = 'MG' THEN 'MG'
        ELSE ''
    END AS brand,
    activity_name
FROM dtwarehouse.cdm_offline_leads_first_contact_brand
WHERE
    regexp_replace(to_date(create_time), '-', '') >= ${this_month_start} AND
    regexp_replace(to_date(create_time), '-', '') <= ${this_month_end}
    and businesstypecode != '10000000'
) ri
left join
(
SELECT
id,
dealerid
FROM dtwarehouse.ods_leadspool_t_leads WHERE pt = ${pt}
) rh
on ri.leads_id=rh.id
) fir_contact_brand_df ) df1
-- inner join
left join
(
SELECT
    dlm_org_id,
    area,
    city_name,
	mac_code,
    rfs_code,
    CASE
        WHEN dealer_level = 'A' THEN '0' -- 是二网
        ELSE '1'
    END AS is_sec_net
FROM dtwarehouse.cdm_dim_dealer_info
) df2
on df1.dealer_id=df2.dlm_org_id
left join
(
SELECT
	get_vel_phone AS mobile,
	CASE WHEN brand_id = '10000000000086' THEN 'RW' ELSE 'MG' END AS brand,
	1 AS big_customer_flag
FROM dtwarehouse.ods_dlm_t_deliver_vel
WHERE
	deliver_vel_type = 2
	AND pt >= ${pt}
GROUP BY get_vel_phone, brand_id
) df3
on df3.mobile=df1.mobile and df3.brand=df1.brand
left join
(
SELECT
    fir_lead_source as fir_contact_fir_sour_brand,
    sec_lead_source as fir_contact_sec_sour_brand,
    touchpoint_id,
    brand
FROM marketing_modeling.cdm_lead_source_mapping
) df4
on df1.fir_contact_fir_sour_brand=df4.fir_contact_fir_sour_brand and df1.fir_contact_sec_sour_brand=df4.fir_contact_sec_sour_brand and df1.brand=df4.brand
) result ) r1
left join
(
SELECT
series_df.mobile,
series_df.brand_id,
series_df.series_id,
series_df.behavior_time,
series_df.pt
from
(
SELECT
	phone AS mobile,
	detail['brand_id'] brand_id,
	detail['series_id'] series_id,
	to_utc_timestamp(detail['create_time'],'yyyy-MM-dd HH:mm:ss')  behavior_time,
	pt
	FROM cdp.cdm_cdp_customer_behavior_detail
    WHERE
        detail['brand_id'] in (121, 101)
        and type = 'oppor'
		AND pt >= ${this_month_start}
		and phone is not null and trim(phone) !=''
union
SELECT
	phone AS mobile,
	detail['brand_id'] brand_id,
	detail['series_id'] series_id,
	to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')  behavior_time,
	pt
	FROM cdp.cdm_cdp_customer_behavior_detail
    WHERE
        detail['brand_id'] in (121, 101)
        and type = 'trial'
		AND pt >= ${this_month_start}
		and phone is not null and trim(phone) !=''

union
SELECT
	phone AS mobile,
	detail['brand_id'] brand_id,
	detail['series_id'] series_id,
	to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')  behavior_time,
	pt
	FROM cdp.cdm_cdp_customer_behavior_detail
    WHERE
		detail['brand_id'] in (121, 101)
		and type='consume'
		AND pt >= ${this_month_start}
		and phone is not null and trim(phone) !=''
union
SELECT
	phone AS mobile,
	detail['brand_id'] brand_id,
	detail['series_id'] series_id,
	to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')  behavior_time,
	pt
	FROM cdp.cdm_cdp_customer_behavior_detail
    WHERE
		detail['brand_id'] in (121, 101)
		and type='deliver'
		AND pt >= ${this_month_start}
		and phone is not null and trim(phone) !=''
) series_df
) r2
on r1.mobile=r2.mobile
where to_date(r1.last_fir_contact_date_brand) <= to_date(r2.behavior_time)  ) rr
where rr.num=1;


set tez.queue.name=${queuename};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.position.alias=true;
set mapreduce.map.memory.mb=4096;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;
set hive.exec.dynamic.partition=true;

INSERT overwrite TABLE marketing_modeling.app_touchpoints_profile_monthly PARTITION (pt)
SELECT
    mobile,
    fir_contact_month,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
    area,
    is_sec_net,
    activity_name,
    fir_contact_tp_id,
    brand,
    fir_contact_month AS pt
FROM (
    SELECT
        *,
        row_number() over (PARTITION BY mobile, fir_contact_month, brand ORDER BY online_flag DESC, fir_contact_date DESC) AS rank_num
    FROM (
            SELECT
            mobile,
            date_format(last_fir_contact_date_brand, 'yyyyMM') AS fir_contact_month,
            last_fir_contact_date_brand AS fir_contact_date,
            fir_contact_series_brand AS fir_contact_series,
            mac_code,
            rfs_code,
            area,
            is_sec_net,
            activity_name,
            touchpoint_id AS fir_contact_tp_id,
            fir_contact_fir_sour_brand AS fir_contact_fir_sour,
            fir_contact_sec_sour_brand AS fir_contact_sec_sour,
            CASE
                WHEN touchpoint_id NOT IN ('001002000000_tp','001002000000_rw') THEN 1
                ELSE 0
            END AS online_flag,
            brand
            FROM marketing_modeling.cdm_customer_touchpoints_profile_a
            WHERE
            pt >= ${this_month_start} AND pt <= ${this_month_end}
        ) raw_profile_df
) AS t
WHERE rank_num = 1;

set tez.queue.name=${queuename};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.groupby.position.alias=true;
set mapreduce.map.memory.mb=4096;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;
set hive.exec.dynamic.partition=true;

INSERT OVERWRITE TABLE marketing_modeling.app_touchpoints_profile_weekly PARTITION (pt)
 SELECT
    mobile,
    fir_contact_week,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
    area,
    is_sec_net,
    activity_name,
    fir_contact_tp_id,
    brand,
    regexp_replace(fir_contact_week, ' ', '') as pt
FROM (
    SELECT
        *,
        row_number() over (PARTITION BY mobile, fir_contact_week, brand ORDER BY online_flag DESC, fir_contact_date DESC) AS rank_num
    FROM (
    select
    a.mobile,
    trim(b.clndr_wk_desc) AS fir_contact_week,
    a.fir_contact_date,
    a.fir_contact_series,
    a.mac_code,
    a.rfs_code,
    a.area,
    a.is_sec_net,
    a.activity_name,
    a.fir_contact_tp_id,
    a.online_flag,
    a.brand
    from
    (
    SELECT
        mobile,
        last_fir_contact_date_brand AS fir_contact_date,
        fir_contact_series_brand AS fir_contact_series,
        mac_code,
        rfs_code,
        area,
        is_sec_net,
        activity_name,
        touchpoint_id AS fir_contact_tp_id,
        CASE
            WHEN touchpoint_id NOT IN ('001002000000_tp','001002000000_rw') THEN 1
            ELSE 0
        END AS online_flag,
        brand,
        pt
    FROM marketing_modeling.cdm_customer_touchpoints_profile_a
    WHERE
        pt >= ${this_month_start} AND pt <= ${this_month_end}
    ) a
    left join
    (
    SELECT
        day_key,
        concat(substr(clndr_wk_desc,3, 5), substr(clndr_wk_desc,9, 10)) as clndr_wk_desc
    FROM dtwarehouse.cdm_dim_calendar
    GROUP BY day_key, clndr_wk_desc
    ) b
    on a.pt=b.day_key
        )
    raw_profile_df
) AS t
WHERE rank_num = 1;


INSERT OVERWRITE TABLE marketing_modeling.app_dim_area
SELECT
	area,
	brand
FROM marketing_modeling.cdm_customer_touchpoints_profile_a
GROUP BY area,brand;


INSERT OVERWRITE TABLE marketing_modeling.app_dim_activity_name
SELECT
	activity_name,
	brand
FROM marketing_modeling.cdm_customer_touchpoints_profile_a
GROUP BY activity_name,brand;


INSERT OVERWRITE TABLE marketing_modeling.app_dim_car_series
select
distinct
a.fir_contact_series,
a.brand
from
(
select
fir_contact_series_brand fir_contact_series,
brand
from
marketing_modeling.cdm_customer_touchpoints_profile_a
) a
inner join
(
select
series_id,
case when  brand_id=101  then 'RW'
when brand_id=121 then 'MG'
else brand_id end brand_id
from dtwarehouse.cdm_dim_series where series_chinese_name not like '%飞凡汽车%') b
on a.fir_contact_series=b.series_id and a.brand = b.brand_id
order by a.fir_contact_series;


INSERT OVERWRITE TABLE marketing_modeling.app_dim_tree_big_small_area
SELECT * FROM
(SELECT
   rfs_code,
   mac_code,
   rfs_name,
   mac_name,
   CASE WHEN brand_id = 121 THEN 'MG'
   WHEN brand_id = 101 THEN 'RW'
   END AS brand
FROM dtwarehouse.cdm_dim_dealer_info where rfs_name not like '%RWR%') t
GROUP BY rfs_code,mac_code,rfs_name,mac_name, brand;

INSERT OVERWRITE TABLE marketing_modeling.app_month_map_week
select
a.month_key,
a.clndr_wk_desc clndr_wk_desc
from
(
SELECT
from_unixtime(unix_timestamp(cast(day_key as string),'yyyymmdd'),'yyyymm')  month_key,
trim(concat(substr(clndr_wk_desc,3, 5), substr(clndr_wk_desc,9, 10))) as clndr_wk_desc
FROM dtwarehouse.cdm_dim_calendar
GROUP BY from_unixtime(unix_timestamp(cast(day_key as string),'yyyymmdd'),'yyyymm'), clndr_wk_desc ORDER BY from_unixtime(unix_timestamp(cast(day_key as string),'yyyymmdd'),'yyyymm') desc
) a
inner join
(
select
distinct
fir_contact_week
from marketing_modeling.app_touchpoints_profile_weekly
)  b on a.clndr_wk_desc = b.fir_contact_week
 where substr(a.month_key,0,4)=substr(a.clndr_wk_desc,0,4)
 ;

INSERT OVERWRITE TABLE marketing_modeling.cdm_dim_dealer_employee_info
select
*
from dtwarehouse.cdm_dim_dealer_employee_info;

INSERT OVERWRITE TABLE marketing_modeling.cdm_dim_dealer_employee_info
select
*
from dtwarehouse.cdm_dim_dealer_employee_info;
"





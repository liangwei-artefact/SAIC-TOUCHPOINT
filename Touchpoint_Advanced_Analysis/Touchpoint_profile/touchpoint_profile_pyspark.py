#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/12/25 3:11 PM
# @Author  : Liangwei Chen
# @FileName: touchpoint_profile_pyspark.py
# @Software: PyCharm


import sys
import datetime
import numpy as np
import calendar

from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext, Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp, collect_set, row_number, udf, when



def get_n_month_start(stamp, n):
    year = stamp.year + (stamp.month + n) / 12
    month = (stamp.month + n) % 12
    bf_month_end_stamp = datetime.datetime(year=year, month=month, day=1)
    return bf_month_end_stamp


# 获得月初
def get_month_start(stamp):
    cur_month_start_stamp = get_n_month_start(stamp, 0)
    return cur_month_start_stamp


# 获取多个月之后的月末
def get_add_n_month_end(stamp, n):
    year = stamp.year + (stamp.month + n) / 12
    month = (stamp.month + n) % 12
    a, b = calendar.monthrange(year, month)
    af_month_end_stamp = datetime.datetime(year=year, month=month, day=b)
    return af_month_end_stamp


# 获取月末
def get_month_end(stamp):
    cur_month_end_stamp = get_add_n_month_end(stamp, 0)
    return cur_month_end_stamp
# pt转化
input_pt = sys.argv[1]
pt_stamp = datetime.datetime.strptime(input_pt, '%Y%m%d')
cur_month_start_stamp = pt_stamp.replace(day=1)
cur_month_end_stamp = get_month_end(pt_stamp)

pt = datetime.datetime.strftime(pt_stamp, "%Y%m%d")
pt_month = datetime.datetime.strftime(pt_stamp, "%Y%m")
this_month_start = datetime.datetime.strftime(cur_month_start_stamp, "%Y%m%d")
this_month_end = datetime.datetime.strftime(cur_month_end_stamp, "%Y%m%d")

spark_session = SparkSession.builder.enableHiveSupport().appName("Attribution_Model") \
    .config("spark.driver.memory", "10g") \
    .config("spark.pyspark.driver.python", "/usr/bin/python2.7") \
    .config("spark.pyspark.python", "/usr/bin/python2.7") \
    .config("spark.yarn.executor.memoryOverhead", "10G") \
	.config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
	.config("spark.sql.hive.convertMetastoreOrc","true")\
	.config("spark.sql.hive.caseSensitiveInferenceMode","NEVER_INFER")\
	.enableHiveSupport() \
	.getOrCreate()

hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
hc.setConf("hive.exec.dynamici.partition","true")
hc.setConf("hive.exec.orc.split.strategy","ETL")


fir_contact_brand_df_sql ='''
select
yy.mobile,
zz.dealerid as dealer_id,
yy.last_fir_contact_date_brand,
yy.fir_contact_fir_sour_brand,
yy.fir_contact_sec_sour_brand,
yy.activity_name,
yy.brand,
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
    regexp_replace(to_date(create_time), '-', '') >= '{0}' AND
    regexp_replace(to_date(create_time), '-', '') <= '{1}'
	and businesstypecode != "10000000"
) yy
left join
(
SELECT id, dealerid FROM dtwarehouse.ods_leadspool_t_leads WHERE pt = '{2}' ) zz
on yy.leads_id=zz.id

union all

select
xxx.mobile,
www.dealer_id,
xxx.last_fir_contact_date_brand,
xxx.fir_contact_fir_sour_brand,
xxx.fir_contact_sec_sour_brand,
xxx.activity_name,
xxx.brand,
'offline' AS mark
from
(
select
ww.leads_id,
ww.mobile,
ww.last_fir_contact_date_brand,
ww.businesstypecode,
ww.fir_contact_fir_sour_brand,
ww.fir_contact_sec_sour_brand,
ww.brand,
ww.activity_name
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
    regexp_replace(to_date(create_time), '-', '') >= '{0}' AND
    regexp_replace(to_date(create_time), '-', '') <= '{1}'
    and businesstypecode = '10000000'
) ww
LEFT JOIN
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
		pt = '{2}'
		AND dealer_id is not null
		AND create_user is not null
		AND regexp_replace(to_date(create_time), '-', '') >= '{0}'
) a
LEFT JOIN
(
  SELECT * FROM dtwarehouse.ods_dlm_t_order_vhcl_relation
  WHERE
	pt = '{2}'
	AND oppor_id IS NOT NULL
) b
ON a.oppor_id = b.oppor_id 
WHERE b.oppor_id IS NULL) xx
on ww.leads_id = xx.leads_id
where xx.leads_id is null
) xxx
left join
(
SELECT id, dealer_id FROM dtwarehouse.ods_dlm_t_cust_base WHERE pt = '{2}') www
on xxx.leads_id = www.id
union all

SELECT
w.mobile,
s.dealer_id,
w.last_fir_contact_date_brand,
w.fir_contact_fir_sour_brand,
w.fir_contact_sec_sour_brand,
w.activity_name,
w.brand,
'order_vhcl' AS mark
FROM 
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
    regexp_replace(to_date(create_time), '-', '') >= '{0}' AND
    regexp_replace(to_date(create_time), '-', '') <= '{1}'
) w
inner join (
SELECT
	a.id AS leads_id,
	a.get_vel_phone AS mobile,
	a.dealer_id,
	CASE WHEN a.brand_id = '10000000000086' THEN 'RW' ELSE 'MG' END AS brand
FROM
(
	SELECT * FROM dtwarehouse.ods_dlm_t_deliver_vel
	WHERE 
		pt = '{1}'
		AND dealer_id is not null
		AND create_user is not null
		AND regexp_replace(to_date(create_time), '-', '') >= '{0}'
) a
LEFT JOIN
(
  SELECT * FROM dtwarehouse.ods_dlm_t_order_vhcl_relation
  WHERE
	pt = '{1}'
	AND oppor_id IS NOT NULL
) b
ON a.oppor_id = b.oppor_id 
WHERE b.oppor_id IS NULL ) s
on w.leads_id = s.leads_id and w.mobile=s.mobile and w.brand=s.brand'''.format(this_month_start,this_month_end,pt)


with open('./fir_contact_brand_df_sql.sql', 'w') as f:
    f.write(fir_contact_brand_df_sql)
hc.sql(fir_contact_brand_df_sql).createOrReplaceTempView('fir_contact_brand_df')
hc.sql('select * from  fir_contact_brand_df limit 100').toPandas().to_csv('./fir_contact_brand_df.csv')

fir_contact_brand_df_v2_sql ='''
select
mobile
,dealer_id
,last_fir_contact_date_brand
,fir_contact_fir_sour_brand
,fir_contact_sec_sour_brand
,activity_name
,brand
,mark
,area
,city_name
,mac_code
,rfs_code
,is_sec_net
,case when touchpoint_id is null and brand = 'MG' then '001010000000_tp'
when touchpoint_id is null and brand = 'RW' then '001012000000_rw'
else touchpoint_id end touchpoint_id
from 
(
select
df1.*,
df2.area, 
df2.city_name,
df2.mac_code,
df2.rfs_code,
df2.is_sec_net,
case when big_customer_flag is not null and df1.brand = 'MG' then '001011000000_tp'
when df1.dealer_id = '220000000398438' and df1.brand ='MG' then '001003000000_tp'
when df1.mark in ('order_vhcl','offline') and df1.brand = 'MG' then '001002000000_tp'
when df1.mark in ('order_vhcl','offline') and df1.brand = 'RW' then '001002000000_rw'
else touchpoint_id end touchpoint_id
from 
fir_contact_brand_df df1
inner join(
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
FROM dtwarehouse.cdm_dim_dealer_info) df2
on df1.dealer_id = df2.dlm_org_id
left join
(
SELECT 
	get_vel_phone AS mobile,
	CASE WHEN brand_id = '10000000000086' THEN 'RW' ELSE 'MG' END AS brand,
	1 AS big_customer_flag
FROM dtwarehouse.ods_dlm_t_deliver_vel 
WHERE 
	deliver_vel_type = 2
	AND pt >= '{2}'
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
on df1.fir_contact_fir_sour_brand = df4.fir_contact_fir_sour_brand and df1.fir_contact_sec_sour_brand=df4.fir_contact_sec_sour_brand and df1.brand=df4.brand) result
'''.format(this_month_start,this_month_end,pt)


series_df_sql = '''
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
		AND pt >= '{0}'
		and phone is not null and trim(phone) !=''
union all
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
		AND pt >= '{0}'
		and phone is not null and trim(phone) !=''

union all
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
	AND pt >= '{0}'
	and phone is not null and trim(phone) !=''


union all
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
	AND pt >= '{0}'
	and phone is not null and trim(phone) !=''
'''.format(this_month_start,this_month_end,pt)


hc.sql('''
insert overwrite table marketing_modeling.cdm_customer_touchpoints_profile_a partition (pt)
select
distinct
cast(mobile as string) mobile,
cast(last_fir_contact_date_brand as string) last_fir_contact_date_brand,
cast(mac_code as string) mac_code,
cast(rfs_code as string) rfs_code,
cast(area as string) area,
cast(is_sec_net as string) is_sec_net,
cast(activity_name as string) activity_name,
cast(touchpoint_id as string) touchpoint_id,
cast(fir_contact_fir_sour_brand as string) fir_contact_fir_sour_brand,
cast(fir_contact_sec_sour_brand as string) fir_contact_sec_sour_brand,
cast(fir_contact_series_brand as string) fir_contact_series_brand,
cast(brand as string) brand,
cast(pt as String) pt
from
(
select
df1.mobile,
df1.last_fir_contact_date_brand,
df1.mac_code,
df1.rfs_code,
df1.area,
df1.is_sec_net,
df1.activity_name,
df1.touchpoint_id,
df1.fir_contact_fir_sour_brand,
df1.fir_contact_sec_sour_brand,
df2.series_id as fir_contact_series_brand,
df1.brand,
regexp_replace(to_date(last_fir_contact_date_brand), "-", "") as pt,
row_number() over(partition by df1.mobile,df1.last_fir_contact_date_brand order by df2.behavior_time asc) num
from
fir_contact_brand_df_v2 df1
left join
series_df df2
on df1.mobile = df2.mobile
where to_date(last_fir_contact_date_brand) <= to_date(behavior_time)) w
where num =1
''')


result = hc.sql('''
select
distinct
cast(mobile as string) mobile,
cast(last_fir_contact_date_brand as string) last_fir_contact_date_brand,
cast(mac_code as string) mac_code,
cast(rfs_code as string) rfs_code,
cast(area as string) area,
cast(is_sec_net as string) is_sec_net,
cast(activity_name as string) activity_name,
cast(touchpoint_id as string) touchpoint_id,
cast(fir_contact_fir_sour_brand as string) fir_contact_fir_sour_brand,
cast(fir_contact_sec_sour_brand as string) fir_contact_sec_sour_brand,
cast(fir_contact_series_brand as string) fir_contact_series_brand,
cast(brand as string) brand,
cast(pt as String) pt
from
(
select
df1.mobile,
df1.last_fir_contact_date_brand,
df1.mac_code,
df1.rfs_code,
df1.area,
df1.is_sec_net,
df1.activity_name,
df1.touchpoint_id,
df1.fir_contact_fir_sour_brand,
df1.fir_contact_sec_sour_brand,
df2.series_id as fir_contact_series_brand,
df1.brand,
regexp_replace(to_date(last_fir_contact_date_brand), "-", "") as pt,
row_number() over(partition by df1.mobile,df1.last_fir_contact_date_brand order by df2.behavior_time asc) num
from
fir_contact_brand_df_v2 df1
left join
series_df df2
on df1.mobile = df2.mobile
where to_date(last_fir_contact_date_brand) <= to_date(behavior_time)) w
where num =1 limit 100
''')
result.toPandas().to_csv('./result.csv')
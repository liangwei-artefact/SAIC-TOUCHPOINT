#!/usr/bin/env python
# coding: utf-8
# /*********************************************************************
# *模块: /Touchpoint_Advanced_Analysis/Touchpoint_profile
# *程序: touchpoint_profile.py
# *功能: 计算首触用户的属性
# *开发人: Boyan XU
# *开发日期: 2021-09-05
# *修改记录:
# *
# *********************************************************************/

import sys
import datetime
import numpy as np
import calendar

from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext, Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp, collect_set, row_number, udf, when

import sys
reload(sys)
sys.setdefaultencoding('utf-8')



spark_session = SparkSession.builder.enableHiveSupport().appName("Attribution_Model") \
    .config("spark.driver.memory", "10g") \
    .config("spark.pyspark.driver.python", "/usr/bin/python2.7") \
    .config("spark.pyspark.python", "/usr/bin/python2.7") \
    .config("spark.yarn.executor.memoryOverhead", "10G") \
	.config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
	.config("spark.sql.hive.convertMetastoreOrc","true")\
	.config("spark.sql.hive.caseSensitiveInferenceMode","NEVER_INFER")\
	.config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")\
	.enableHiveSupport() \
	.getOrCreate()

hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
hc.setConf("hive.exec.dynamici.partition","true")
hc.setConf("hive.exec.orc.split.strategy","ETL")

# ---【加载参数】---
pt = sys.argv[1]
now = datetime.datetime.strptime(pt,"%Y%m%d") 
this_month_start = datetime.datetime(now.year, now.month, 1).strftime('%Y%m%d')
this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).strftime('%Y%m%d')

print pt #20211129
print now
print this_month_start #20211101
print this_month_end #20211130

# ---【提取数据】---
## 获取首触用户
fir_contact_brand = hc.sql('''
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
    regexp_replace(to_date(create_time), '-', '') >= {0} AND
    regexp_replace(to_date(create_time), '-', '') <= {1}
'''.format(this_month_start, this_month_end))

## 提取经销商信息
dim_dealer_info_df = hc.sql('''
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
''')

## 提取大客户的手机号码和经销商ID
big_customer = hc.sql('''
SELECT 
	get_vel_phone AS mobile,
	CASE WHEN brand_id = '10000000000086' THEN 'RW' ELSE 'MG' END AS brand,
	1 AS big_customer_flag
FROM dtwarehouse.ods_dlm_t_deliver_vel 
WHERE 
	deliver_vel_type = 2
	AND pt >= {0}
GROUP BY get_vel_phone, brand_id
'''.format(pt))


# ---【获取 dealer_id】---
## 1 - 交现车
order_vhcl = hc.sql('''
SELECT
	a.id AS leads_id,
	a.get_vel_phone AS mobile,
	a.dealer_id,
	CASE WHEN a.brand_id = '10000000000086' THEN 'RW' ELSE 'MG' END AS brand
FROM
(
	SELECT * FROM dtwarehouse.ods_dlm_t_deliver_vel
	WHERE 
		pt = {0}
		AND dealer_id is not null
		AND create_user is not null
		AND regexp_replace(to_date(create_time), '-', '') >= {1}
) a
LEFT JOIN
(
  SELECT * FROM dtwarehouse.ods_dlm_t_order_vhcl_relation
  WHERE
	pt = {0}
	AND oppor_id IS NOT NULL
) b
ON a.oppor_id = b.oppor_id 
WHERE b.oppor_id IS NULL
'''.format(pt, this_month_start))

fir_contact_deliver = fir_contact_brand.join(order_vhcl, ['leads_id','mobile','brand'], 'inner')\
.selectExpr('mobile',
			'dealer_id',
			'last_fir_contact_date_brand',
			'fir_contact_fir_sour_brand',
			'fir_contact_sec_sour_brand',
			'activity_name',
			'brand',
			"'order_vhcl' AS mark")


## 2 - 其他线下首触
fir_contact_brand_offline_df = fir_contact_brand.alias('df').filter('businesstypecode = "10000000"').join(order_vhcl.selectExpr('leads_id'), ['leads_id'], 'leftanti')
cust_pool = hc.sql('SELECT id, dealer_id FROM dtwarehouse.ods_dlm_t_cust_base WHERE pt = {0}'.format(pt))
fir_contact_brand_offline_df = fir_contact_brand_offline_df.alias('df1').join(cust_pool.alias('df2'), col('df1.leads_id') == col('df2.id'), how='left')\
.selectExpr('mobile',
			'dealer_id',
			'last_fir_contact_date_brand',
			'fir_contact_fir_sour_brand',
			'fir_contact_sec_sour_brand',
			'activity_name',
			'brand',
			"'offline' AS mark")

## 3 - 线上首触
fir_contact_brand_online_df = fir_contact_brand.alias('df').filter('businesstypecode != "10000000"')
leads_pool = hc.sql('SELECT id, dealerid FROM dtwarehouse.ods_leadspool_t_leads WHERE pt = {0}'.format(pt))
fir_contact_brand_online_df = fir_contact_brand_online_df.alias('df1').join(leads_pool.alias('df2'), col('df1.leads_id') == col('df2.ID'), how='left')\
.selectExpr('mobile',
			'dealerid as dealer_id',
			'last_fir_contact_date_brand',
			'fir_contact_fir_sour_brand',
			'fir_contact_sec_sour_brand',
			'activity_name',
			'brand',
			"'online' AS mark")
			
## 首触合并：交现车 + 线下 + 线上
fir_contact_brand_df = fir_contact_brand_online_df.unionAll(fir_contact_brand_offline_df).unionAll(fir_contact_deliver)

# ---【加工线索来源】---
resourcename_to_tpid = hc.sql('''
SELECT 
    fir_lead_source as fir_contact_fir_sour_brand,
    sec_lead_source as fir_contact_sec_sour_brand, 
    touchpoint_id,
    brand
FROM marketing_modeling.cdm_lead_source_mapping
''')

fir_contact_brand_df = fir_contact_brand_df.alias('df1').join(dim_dealer_info_df.alias('df2'), [col('df1.dealer_id') == col('df2.dlm_org_id')])\
.join(big_customer.alias('df3'),['mobile', 'brand'], 'left')\
.join(resourcename_to_tpid.alias('df4'),['fir_contact_fir_sour_brand','fir_contact_sec_sour_brand','brand'], 'left')\
.withColumn('touchpoint_id',\
F.expr('''
case when big_customer_flag is not null and brand = 'MG' then '001011000000_tp'
when dealer_id = '220000000398438' and brand ='MG' then '001003000000_tp'
when mark in ('order_vhcl','offline') and brand = 'MG' then '001002000000_tp'
when mark in ('order_vhcl','offline') and brand = 'RW' then '001002000000_rw'
else touchpoint_id end'''))\
.withColumn('touchpoint_id',\
F.expr('''
case when touchpoint_id is null and brand = 'MG' then '001010000000_tp'
when touchpoint_id is null and brand = 'RW' then '001012000000_rw'
else touchpoint_id end'''))


# ---【加工首触车系】---
# oppor = hc.sql('''
#     SELECT
#         phone AS mobile, behavior_time, series_id, brand_id
#     FROM marketing_modeling.dw_oppor_behavior
#     WHERE
#         brand_id in (121, 101)
# 		AND pt >= {0}
# '''.format(this_month_start))

oppor = hc.sql('''
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
		AND pt >= {0}
		and phone is not null and trim(phone) !=''
'''.format(this_month_start))

trial = hc.sql('''
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
		AND pt >= {0}
		and phone is not null and trim(phone) !=''
'''.format(this_month_start))

consume = hc.sql('''
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
		AND pt >= {0}
		and phone is not null and trim(phone) !=''
'''.format(this_month_start))

deliver = hc.sql('''
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
		AND pt >= {0}
		and phone is not null and trim(phone) !=''
'''.format(this_month_start))

series_df = oppor.unionAll(trial).unionAll(consume).unionAll(deliver)

fir_contact_series_brand = fir_contact_brand_df.alias('df1') \
    .join(series_df.alias('df2'),'mobile','left') \
    .filter('to_date(last_fir_contact_date_brand) <= to_date(behavior_time)') \
    .withColumn("row_number", F.row_number().over(
    Window.partitionBy(["mobile", "last_fir_contact_date_brand"]).orderBy(col("behavior_time").asc()))) \
    .filter("row_number = 1")


# ---【结果落表】---
final_df = fir_contact_series_brand\
    .selectExpr('mobile',
                'last_fir_contact_date_brand',
                'mac_code',
                'rfs_code',
				'area',
                'is_sec_net',
                'activity_name',
                'touchpoint_id',
                'fir_contact_fir_sour_brand',
                'fir_contact_sec_sour_brand',
				'series_id as fir_contact_series_brand',
                'brand',
                'regexp_replace(to_date(last_fir_contact_date_brand), "-", "") as pt')

# 没有使用就去除
final_df.createOrReplaceTempView('final_df')
hc.sql('''
INSERT OVERWRITE TABLE marketing_modeling.cdm_customer_touchpoints_profile_a PARTITION (pt)
SELECT 
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
FROM final_df where pt is not null
''')

# ---【首触表去重】---
## 月度首触表去重
hc.sql('''
WITH raw_profile_df AS (

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
        pt >= {0} AND pt <= {1}
),

filtered_profile_df AS (

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
        brand
    FROM (
        SELECT
            *,
            row_number() over (PARTITION BY mobile, fir_contact_month, brand ORDER BY online_flag DESC, fir_contact_date DESC) AS rank_num
        FROM raw_profile_df
    ) AS t
    WHERE rank_num = 1

)
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
FROM filtered_profile_df
'''.format(this_month_start, this_month_end))


## 周度首触表去重
hc.sql('''
WITH calendar_df AS (

	SELECT
		day_key,
		concat(substr(clndr_wk_desc,3, 5), substr(clndr_wk_desc,9, 10)) as clndr_wk_desc
	FROM dtwarehouse.cdm_dim_calendar
	GROUP BY day_key, clndr_wk_desc

),

tmp_raw_profile_df AS (

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
        pt >= {0} AND pt <= {1} 
),

raw_profile_df AS (

    SELECT
        tmp_raw_profile_df.mobile,
        calendar_df.clndr_wk_desc AS fir_contact_week,
        tmp_raw_profile_df.fir_contact_date,
        tmp_raw_profile_df.fir_contact_series,
        tmp_raw_profile_df.mac_code,
        tmp_raw_profile_df.rfs_code,
        tmp_raw_profile_df.area,
        tmp_raw_profile_df.is_sec_net,
        tmp_raw_profile_df.activity_name,
        tmp_raw_profile_df.fir_contact_tp_id,
        tmp_raw_profile_df.online_flag,
        tmp_raw_profile_df.brand
    FROM tmp_raw_profile_df
    LEFT JOIN calendar_df 
	ON tmp_raw_profile_df.pt = calendar_df.day_key
),

filtered_profile_df AS (

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
        brand
    FROM (
        SELECT
            *,
            row_number() over (PARTITION BY mobile, fir_contact_week, brand ORDER BY online_flag DESC, fir_contact_date DESC) AS rank_num
        FROM raw_profile_df
    ) AS t
    WHERE rank_num = 1

)

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
FROM filtered_profile_df
'''.format(this_month_start,this_month_end))


## 省份维表
hc.sql('''
INSERT OVERWRITE TABLE marketing_modeling.app_dim_area
SELECT
	area,
	brand
FROM marketing_modeling.cdm_customer_touchpoints_profile_a
GROUP BY area,brand
''')

## 活动维表
hc.sql('''
INSERT OVERWRITE TABLE marketing_modeling.app_dim_activity_name
SELECT
	activity_name,
	brand
FROM marketing_modeling.cdm_customer_touchpoints_profile_a
GROUP BY activity_name,brand
''')

## 首触车系维表
hc.sql('''
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
order by a.fir_contact_series
''')


## 大区小区维表
hc.sql('''
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
GROUP BY rfs_code,mac_code,rfs_name,mac_name, brand
''')


# 整理再拼一张时间维度表， select + union a
hc.sql('''
INSERT OVERWRITE TABLE marketing_modeling.app_month_map_week
select
a.month_key,
a.clndr_wk_desc
from 
(
SELECT
from_unixtime(unix_timestamp(cast(day_key as string),'yyyymmdd'),'yyyymm')  month_key,
concat(substr(clndr_wk_desc,3, 5), substr(clndr_wk_desc,9, 10)) as clndr_wk_desc
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
''')
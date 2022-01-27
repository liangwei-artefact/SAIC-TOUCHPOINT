#!/usr/bin/env python
# coding: utf-8



import numpy as np
import datetime
import sys
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp,  collect_set
 
spark_session = SparkSession.builder.enableHiveSupport().appName("attribution_data_processing") \
    .config("spark.driver.memory","30g") \
    .config("spark.yarn.executor.memoryOverhead","20G") \
    .config("spark.sql.broadcastTimeout", "3600")\
    .config("spark.driver.maxResultSize", "6g")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
    .config("hive.exec.dynamic.partition", True)\
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")\
            .config("spark.default.parallelism", 200) \
    .getOrCreate()

import numpy as np
import sys
hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")


pt = sys.argv[1]

# 最后一次首触日期
#brand
fir_contact_brand = hc.sql('SELECT * FROM dtwarehouse.cdm_leads_first_contact_brand')
fir_contact_brand_df = fir_contact_brand.alias('df').filter('chinese_name = "MG"')\
.withColumn("row_number", F.row_number().over(Window.partitionBy("mobile").orderBy(col("create_time").desc()))).filter("row_number = 1")

# fetch leads_pool to get area and city
# leads_pool = hc.sql('''
# SELECT
#     id, area, city_name
# FROM
# (SELECT id, dealerid FROM dtwarehouse.ods_leadspool_t_leads WHERE pt = {0}) a
# LEFT JOIN (SELECT dlm_org_id, area, city_name FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = {0}) b
# ON a.dealerid = b.dlm_org_id
# '''.format(pt))


leads_pool = hc.sql("""
select
phone mobile,
to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') create_time,
detail['firstresourcename'] firstresourcename,
detail['dealer_id'] id,
detail['secondresourcename'] secondresourcename,
detail['businesstypecode'] businesstypecode
from cdp.cdm_cdp_customer_behavior_detail
	WHERE 
		TYPE = 'leads_pool' 
		AND pt = {0} 
""".format(pt))

fir_contact_brand_df = fir_contact_brand_df.alias('df').join(leads_pool.alias('df1'),col('df.leads_id') == col('df1.id'), how='left')

fir_contact_brand_df = fir_contact_brand_df.selectExpr('mobile','create_time as last_fir_contact_date_brand', 
                                                       'firstresourcename as fir_contact_fir_sour_brand','secondresourcename as fir_contact_sec_sour_brand',
                                                       'area as fir_contact_province_brand','city_name as fir_contact_city_brand')

# area
fir_contact_area = hc.sql('SELECT * FROM dtwarehouse.cdm_leads_first_contact_area')
fir_contact_area_df = fir_contact_area.alias('df').filter('chinese_name = "MG"')\
.withColumn("row_number", F.row_number().over(Window.partitionBy("mobile").orderBy(col("create_time").desc()))).filter("row_number = 1")\
.selectExpr('mobile', 'create_time as last_fir_contact_date_area', 'firstresourcename as fir_contact_fir_sour_area',\
            'secondresourcename as fir_contact_sec_sour_area','area as fir_contact_province_area ','city_name as fir_contact_city_area')

# Get dealer's province, city
dealer_geo_df = hc.sql('SELECT DISTINCT dealer_code, area, city_name FROM dtwarehouse.ods_rdp_v_sales_region_dealer')

# oppor series
# oppor = hc.sql('SELECT * FROM marketing_modeling.dw_oppor_behavior').filter('brand_id = 121')
oppor = hc.sql('SELECT * FROM marketing_modeling.dw_oppor_behavior').filter('brand_id = 121')


oppor = oppor.alias('df').join(dealer_geo_df.alias('df1'),
                  col('df.dealer_code') == col('df1.dealer_code'), how='left')

oppor_series_brand = fir_contact_brand_df.alias('df1').join(oppor.alias('df2'),
                                                            [col('df1.mobile') == col('df2.phone')])\
.filter('last_fir_contact_date_brand <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('oppor_series_brand'))

oppor_series_area = fir_contact_area_df.alias('df1').join(oppor.alias('df2'),
                                                          [col('df1.mobile') == col('df2.phone'),
                                                           col('df1.fir_contact_province_area') == col('df2.area'),
                                                           col('df1.fir_contact_city_area') == col('df2.city_name')]
                                                         )\
.filter('last_fir_contact_date_area <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('oppor_series_area'))


# trial series
trial = hc.sql('SELECT * FROM marketing_modeling.dw_trial_behavior').filter('brand_id = 121')

trial = trial.alias('df').join(dealer_geo_df.alias('df1'),
                  col('df.dealer_code') == col('df1.dealer_code'), how='left')

trial_series_brand = fir_contact_brand_df.alias('df1').join(trial.alias('df2'),
                                                            [col('df1.mobile') == col('df2.phone')])\
.filter('last_fir_contact_date_brand <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('trail_series_brand'))

trial_series_area = fir_contact_area_df.alias('df1').join(trial.alias('df2'),
                                                          [col('df1.mobile') == col('df2.phone'),
                                                           col('df1.fir_contact_province_area') == col('df2.area'),
                                                           col('df1.fir_contact_city_area') == col('df2.city_name')]
                                                         )\
.filter('last_fir_contact_date_area <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('trial_series_area'))

# [to be updated] consume series  brand
consume = hc.sql('''
    SELECT  mobile AS phone, create_time AS behavior_time, dealer_code, series_id 
    FROM dtwarehouse.cdm_cust_sales
    WHERE num=1 AND mobile IS NOT NULL
''').filter('brand_id = 121')

consume = hc.sql('''
    SELECT  mobile AS phone, create_time AS behavior_time, dealer_code, series_id 
    FROM dtwarehouse.cdm_cust_sales
    WHERE num=1 AND mobile IS NOT NULL
''').filter('brand_id = 121')

consume = consume.alias('df').join(dealer_geo_df.alias('df1'),
                  col('df.dealer_code') == col('df1.dealer_code'), how='left')

consume_series_brand = fir_contact_brand_df.alias('df1').join(consume.alias('df2'),
                                                              [col('df1.mobile') == col('df2.phone')])\
.filter('last_fir_contact_date_brand <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('consume_series_brand'))

# [to be updated] consume series area
consume_series_area = fir_contact_area_df.alias('df1').join(consume.alias('df2'),
                                                            [col('df1.mobile') == col('df2.phone'),
                                                             col('df1.fir_contact_province_area') == col('df2.area'),
                                                             col('df1.fir_contact_city_area') == col('df2.city_name')]
                                                           )\
.filter('last_fir_contact_date_area <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('consume_series_area'))

# [to be updated]  deliver series
deliver = hc.sql('''
    SELECT  mobile AS phone, deliver_date AS behavior_time, dealer_code, series_id 
    FROM dtwarehouse.cdm_cust_sales_delivery
    WHERE num=1 AND mobile IS NOT NULL
''').filter('brand_id = 121')
deliver = deliver.alias('df').join(dealer_geo_df.alias('df1'),
                  col('df.dealer_code') == col('df1.dealer_code'), how='left')

deliver_series_brand = fir_contact_brand_df.alias('df1').join(deliver.alias('df2'),
                                                              [col('df1.mobile') == col('df2.phone')])\
.filter('last_fir_contact_date_brand <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('deliver_series_brand'))

deliver_series_area = fir_contact_area_df.alias('df1').join(deliver.alias('df2'),
                                                            [col('df1.mobile') == col('df2.phone'),
                                                             col('df1.fir_contact_province_area') == col('df2.area'),
                                                             col('df1.fir_contact_city_area') == col('df2.city_name')]
                                                            )\
.filter('last_fir_contact_date_area <= behavior_time')\
.groupby('mobile').agg(collect_set('series_id').alias('deliver_series_area'))

# 首触车系
series_df = oppor.select('phone','series_id','behavior_time', 'area', 'city_name')\
.unionAll(trial.select('phone','series_id','behavior_time', 'area', 'city_name'))\
.unionAll(consume.select('phone','series_id','behavior_time', 'area', 'city_name'))\
.unionAll(deliver.select('phone','series_id','behavior_time', 'area', 'city_name'))

fir_contact_series_brand = fir_contact_brand_df.alias('df1').join(series_df.alias('df2'),
                                                                  [col('df1.mobile') == col('df2.phone')])\
.filter('last_fir_contact_date_brand <= behavior_time')\
.withColumn("row_number", F.row_number().over(Window.partitionBy("mobile").orderBy(col("behavior_time").asc()))).filter("row_number = 1")\
.selectExpr('mobile','series_id as fir_contact_series_brand')

fir_contact_series_area = fir_contact_area_df.alias('df1').join(series_df.alias('df2'),
                                                                [col('df1.mobile') == col('df2.phone'),
                                                                 col('df1.fir_contact_province_area') == col('df2.area'),
                                                                 col('df1.fir_contact_city_area') == col('df2.city_name')]
                                                               )\
.filter('last_fir_contact_date_area <= behavior_time')\
.withColumn("row_number", F.row_number().over(Window.partitionBy("phone").orderBy(col("behavior_time").asc()))).filter("row_number = 1")\
.selectExpr('mobile','series_id as fir_contact_series_area')

# user_pool
#user_pool_df = hc.sql('select phone as mobile from marketing_modeling.tmp_smpv_user_pool_2y').filter('brand_id=121')
#user_pool_df = user_pool_df.union(fir_contact_brand_df.select('mobile'))\
#                           .union(fir_contact_area_df.select('mobile'))\
#                           .dropDuplicates()

user_pool_df = fir_contact_brand_df.select("mobile").unionAll(fir_contact_area_df.select("mobile")).dropDuplicates()

df_col = [fir_contact_brand_df, fir_contact_area_df, fir_contact_series_brand, fir_contact_series_area,
          oppor_series_brand, oppor_series_area,trial_series_brand, trial_series_area, 
          consume_series_brand, consume_series_area, deliver_series_brand, deliver_series_area]

for df in df_col:
    user_pool_df = user_pool_df.join(df,'mobile','left')

final_df = user_pool_df.select('mobile','last_fir_contact_date_brand','fir_contact_fir_sour_brand','fir_contact_sec_sour_brand',
                               'fir_contact_province_brand','fir_contact_city_brand','last_fir_contact_date_area',
                               'fir_contact_fir_sour_area','fir_contact_sec_sour_area','fir_contact_province_area',
                               'fir_contact_city_area','fir_contact_series_brand','fir_contact_series_area',
                               'oppor_series_brand','oppor_series_area','trail_series_brand',
                               'trial_series_area','consume_series_brand','consume_series_area','deliver_series_brand',
                               'deliver_series_area').withColumn('pt', F.lit(pt))

final_df.createOrReplaceTempView('final_df')
hc.sql('insert overwrite table marketing_modeling.cdm_customer_touchpoints_profile_a PARTITION (pt) select * from final_df')


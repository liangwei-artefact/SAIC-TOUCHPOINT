#!/usr/bin/python
# -*- coding: utf-8 -*-
# /*********************************************************************
# *模块: /Touchpoint_Data_Processing/Data_Insertion/Data_Initialization
# *程序: cdm_tp_data_integration.py
# *功能: 把各触点小竖表数据抽取到大竖表 
# *开发人: Xiaofeng XU
# *开发日: 2021-06-22
# *修改记录:
# *********************************************************************/

import numpy as np
import datetime
import sys
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp
  
spark_session = SparkSession.builder.enableHiveSupport().appName("attribution_data_processing") \
    .config("spark.driver.memory","30g") \
    .config("spark.yarn.executor.memoryOverhead","20G") \
    .config("spark.sql.broadcastTimeout", "3600")\
    .config("spark.driver.maxResultSize", "6g")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
    .config("hive.exec.dynamic.partition", True) \
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
    .config("spark.default.parallelism", 200) \
    .getOrCreate()

hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")

# 定义需要从小竖表抽取哪个品牌的数据，取值为'MG'或'RW'

pt1 = sys.argv[1]
pt2 = sys.argv[2]
brand = sys.argv[3]
print(pt1)
print(pt2)
print(brand)

brand_suffix_mapping = {'MG':'tp','RW':'rw'}
suffix = brand_suffix_mapping[brand]

brand_id_mapping = {'MG':121,'RW':101}

# 抽取自加工小竖表
df_col = ['cdm_ts_ccm_activity_i',
          'cdm_ts_online_activity_i',
          'cdm_ts_leads_i',
          'cdm_ts_register_i',
          'cdm_ts_wechat_i',
          'cdm_ts_scrm_i',
          'cdm_ts_sis_call_i',
          'cdm_ts_ai_call_i',
          'cdm_ts_sms_i',
          'cdm_ts_trial_i',
          'cdm_ts_followup_i',
          'cdm_ts_dlm_call_i',
          'cdm_ts_order_i',
          'cdm_ts_oppor_fail_i',
          'cdm_ts_activity_i',
          'cdm_ts_community_i',
          'cdm_ts_online_action_i',
          'cdm_ts_oppor_fail_activation_i',
          'cdm_ts_completed_oppor_fail_i',
          'cdm_ts_app_activity_i',
          'cdm_ts_adhoc_app_activity_i',
          'cdm_ts_bind_i',
		  'cdm_ts_maintenance_i'
         ]

for df in df_col:
    print('processing: ',df)
    source = '_'.join(df.split('_')[2:-1])
    final_df = hc.sql('select mobile, action_time, touchpoint_id, brand, pt, "{0}" as source from marketing_modeling.{1} where pt between "{2}" and "{3}" and brand = "{4}"'.format(source,df,pt1,pt2,brand))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.cdm_{0}_tp_ts_all_i PARTITION (source,pt) select * from final_df'.format(brand.lower()))


cdm_oppor_behavior = hc.sql('''
select
phone,
to_utc_timestamp(detail['create_time'],'yyyy-MM-dd HH:mm:ss') behavior_time,
detail['brand_id'] brand_id,pt
from cdp.cdm_cdp_customer_behavior_detail
where pt between '${0}' and '${1}' and type='oppor'
'''.format(pt1,pt2))
cdm_oppor_behavior.createOrReplaceTempView('cdm_oppor_behavior')

cdm_cards_behavior = hc.sql('''
select
phone,
to_utc_timestamp(detail['create_time'],'yyyy-MM-dd HH:mm:ss') behavior_time,
detail['brand_id'] brand_id,pt
from cdp.cdm_cdp_customer_behavior_detail
where pt between '${0}' and '${1}' and type='construction_cards'
'''.format(pt1,pt2))
cdm_cards_behavior.createOrReplaceTempView('cdm_cards_behavior')

cdm_instore_behavior = hc.sql('''
select
phone,
to_utc_timestamp(detail['create_time'],'yyyy-MM-dd HH:mm:ss') behavior_time,
detail['brand_id'] brand_id,pt
from cdp.cdm_cdp_customer_behavior_detail
where pt between '${0}' and '${1}' and type='instore'
'''.format(pt1,pt2))
cdm_instore_behavior.createOrReplaceTempView('cdm_instore_behavior')


# 抽取CDP行为表
beha_df = {
    'cdm_oppor_behavior':'004000000000_{0}'.format(suffix),
    'cdm_cards_behavior':'005000000000_{0}'.format(suffix),
    'cdm_instore_behavior':'006000000000_{0}'.format(suffix)
          }
		  
for k in beha_df.keys():
    print('processing: ',k)
    source = k.split('_')[1]
    final_df = hc.sql(
    '''
        select 
            phone as mobile, cast(behavior_time as timestamp) as action_time,
            "{0}" as touchpoint_id, "{1}" as brand, pt, "{2}" as source
            from {3}
            where brand_id = {4}       
    '''.format(beha_df[k],brand,source,k,brand_id_mapping[brand]))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.cdm_{0}_tp_ts_all_i PARTITION (source,pt) select * from final_df'.format(brand.lower()))
    
    
# 抽取线索首触记录
fir_contact = {'cdm_offline_leads_first_contact_brand':'001001001000_{0}'.format(suffix),
               'cdm_offline_leads_first_contact_area':'001001002000_{0}'.format(suffix)}
    
for k in fir_contact.keys():
    source = '_'.join(k.split('_')[-3:])
    print('processing: ',source)
    final_df = hc.sql(
    '''
    select 
        mobile, cast(create_time as timestamp) as action_time, 
        "{0}" as touchpoint_id, chinese_name as brand,
        regexp_replace(to_date(create_time), '-', '') as pt,
        "{1}" as source from dtwarehouse.{2}
        where regexp_replace(to_date(create_time), '-', '') between "{3}" and "{4}"
        and chinese_name = "{5}"
    '''.format(fir_contact[k],source,k,pt1,pt2,brand))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.cdm_{0}_tp_ts_all_i PARTITION (source,pt) select * from final_df'.format(brand.lower()))
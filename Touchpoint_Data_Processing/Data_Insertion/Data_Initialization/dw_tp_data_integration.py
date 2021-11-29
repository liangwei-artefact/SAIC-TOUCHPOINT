#!/usr/bin/python
# -*- coding: utf-8 -*-
# /*********************************************************************
# *模块: /Touchpoint_Data_Processing/Data_Insertion/Data_Initialization
# *程序: dw_tp_data_integration.py
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
    .config("hive.exec.dynamic.partition", True)\
            .config("spark.default.parallelism", 200) \
    .getOrCreate()

hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")

# 定义需要从小竖表抽取哪个品牌的数据，取值为'MG'或'RW'
brand = sys.argv[1]

brand_suffix_mapping = {'MG':'tp','RW':'rw'}
suffix = brand_suffix_mapping[brand]

brand_id_mapping = {'MG':121,'RW':101}

# 抽取自加工小竖表
df_col = ['dw_ts_ccm_activity_i',
          'dw_ts_online_activity_i',
          'dw_ts_leads_i',
          'dw_ts_register_i',
          'dw_ts_wechat_i',
          'dw_ts_scrm_i',
          'dw_ts_sis_call_i',
          'dw_ts_ai_call_i',
          'dw_ts_sms_i',
          'dw_ts_trial_i',
          'dw_ts_followup_i',
          'dw_ts_dlm_call_i',
          'dw_ts_order_i',
          'dw_ts_oppor_fail_i',
          'dw_ts_activity_i',
          'dw_ts_community_i',
          'dw_ts_online_action_i',
          'dw_ts_oppor_fail_activation_i',
          'dw_ts_completed_oppor_fail_i',
          'dw_ts_app_activity_i',
          'dw_ts_adhoc_app_activity_i',
          'dw_ts_bind_i',
		  'dw_ts_maintenance_i'
         ]

for df in df_col:
    print('processing: ',df)
    source = '_'.join(df.split('_')[2:-1])
    final_df = hc.sql('select mobile, action_time, touchpoint_id, brand, pt, "{0}" as source from marketing_modeling.{1} where pt >= "20190601" and brand = "{2}"'.format(source,df,brand))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.dw_{0}_tp_ts_all_i PARTITION (source,pt) select * from final_df'.format(brand.lower()))


# 抽取CDP行为表
beha_df = {
    'dw_oppor_behavior':'004000000000_{0}'.format(suffix),
    'dw_cards_behavior':'005000000000_{0}'.format(suffix),
    'dw_instore_behavior ':'006000000000_{0}'.format(suffix)
          }
		  
for k in beha_df.keys():
    print('processing: ',k)
    source = k.split('_')[1]
    final_df = hc.sql(
    '''
        select 
            phone as mobile, cast(behavior_time as timestamp) as action_time,
            "{0}" as touchpoint_id, "{1}" as brand, pt, "{2}" as source
            from marketing_modeling.{3}
            where pt >= "20190601"
            and brand_id = {4}       
    '''.format(beha_df[k],brand,source,k,brand_id_mapping[brand]))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.dw_{0}_tp_ts_all_i PARTITION (source,pt) select * from final_df'.format(brand.lower()))
    
    
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
        where regexp_replace(to_date(create_time), '-', '') >= "20190601" 
        and chinese_name = "{3}"
    '''.format(fir_contact[k],source,k,brand))
    final_df.createOrReplaceTempView('final_df')
	hc.sql('insert overwrite table marketing_modeling.dw_{0}_tp_ts_all_i PARTITION (source,pt) select * from final_df'.format(brand.lower()))
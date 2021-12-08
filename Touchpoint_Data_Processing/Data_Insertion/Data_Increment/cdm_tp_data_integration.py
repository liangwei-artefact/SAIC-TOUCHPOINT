#!/usr/bin/env python
# coding: utf-8

# 非上线版本

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

pt = sys.arg[1]

df_col = ['cdm_ts_ccm_activity_i','cdm_ts_online_activity_i','cdm_ts_leads_i','cdm_ts_register_i','cdm_ts_wechat_i','cdm_ts_scrm_i',
 'cdm_ts_sis_call_i','cdm_ts_ai_call_i','cdm_ts_sms_i','cdm_ts_trial_i','cdm_ts_followup_i','cdm_ts_dlm_call_i','cdm_ts_order_i',
 'cdm_ts_oppor_fail_i','cdm_ts_activity_i','cdm_ts_community_i','cdm_ts_online_action_i']

for df in df_col:
    print('processing: ',df)
    source = '_'.join(df.split('_')[2:-1])
    final_df = hc.sql("""
    select
        mobile, action_time, touchpoint_id, brand, pt, "{0}" as source
    from marketing_modeling.{1}
    where
        pt >= "{2}"
        and brand ="MG"
    """.format(source,df, pt))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.dw_mg_tp_ts_all_i PARTITION (source,pt) select * from final_df')

# insert from CDP behavior
beha_df = {'dw_oppor_behavior':'004000000000_tp',
'dw_cards_behavior':'005000000000_tp',
'dw_instore_behavior ':'006000000000_tp'}

for k in beha_df.keys():
    print('processing: ',k)
    source = k.split('_')[1]
    final_df = hc.sql(
    '''
        select
            phone as mobile, cast(behavior_time as timestamp) as action_time,
            "{0}" as touchpoint_id, "MG" as brand, pt, "{1}" as source
        from marketing_modeling.{2}
        where
            pt >= "{3}"
            and brand_id = 121
    '''.format(beha_df[k],source,k, pt))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.cdm_mg_tp_ts_all_i PARTITION (source,pt) select * from final_df')


# 线索首触-品牌
fir_contact = {'cdm_leads_first_contact_brand':'001001001000_tp',
            'cdm_leads_first_contact_area':'001001002000_tp'}

for k in fir_contact.keys():
    source = '_'.join(k.split('_')[-3:])
    print('processing: ',source)
    final_df = hc.sql(
    '''
    select
        mobile, cast(create_time as timestamp) as action_time,
        "{0}" as touchpoint_id, chinese_name as brand,
        regexp_replace(to_date(create_time), '-', '') as pt,
        "{1}" as source
    from dtwarehouse.{2}
    where
        regexp_replace(to_date(create_time), '-', '') >= "{3}"
        and chinese_name = 'MG'
    '''.format(fir_contact[k],source,k, pt))
    final_df.createOrReplaceTempView('final_df')
    hc.sql('insert overwrite table marketing_modeling.cdm_mg_tp_ts_all_i PARTITION (source,pt) select * from final_df')

#!/usr/bin/env python
# coding: utf-8

#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Contribution_Report
#*程序: contri_report_compute_result.py
#*功能: 计算触点转化
#*开发人: Boyan XU & Xiaofeng XU
#*开发日: 2021-09-05
#*修改记录:  
#*    
#*********************************************************************/

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pandas as pd
import numpy as np
import datetime

from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType, StructType, StructField
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp, collect_set, row_number, udf

spark_session = SparkSession.builder.enableHiveSupport().appName("Attribution_Model")\
                                                        .config("spark.driver.memory","10g")\
                                                        .config("spark.pyspark.driver.python","/usr/bin/python2.7")\
                                                        .config("spark.pyspark.python","/usr/bin/python2.7")\
                                                        .config("spark.yarn.executor.memoryOverhead","4G") \
                                                        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") \
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
    .config("spark.sql.execution.arrow.enabled", "true")\
    .getOrCreate()
                                                        
hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")


# ---【加载参数】---
## 需要进行计算的品牌，取值可以是"MG"或"RW"
brand = sys.argv[1]


# ---【读取数据】---
##读取首触表
names = ['mobile', 'fir_contact_month', 'fir_contact_date', 'fir_contact_series', 'mac_code', 'rfs_code', 'area', 'fir_contact_tp_id']
filtered_profile_df = pd.read_csv('{0}_filtered_profile_df.csv'.format(brand), sep='\t', names=names, encoding="utf-8", parse_dates=[2])
filtered_profile_df['mobile'] = filtered_profile_df['mobile'].astype('string')
filtered_profile_df['fir_contact_date'] = pd.to_datetime(filtered_profile_df['fir_contact_date'])
## 去掉经销商销售号码
mobile_to_remove_list = pd.read_csv('mobile_to_remove.csv', names = ['mobile']).mobile.unique()
mobile_to_remove_list = [str(i) for i in mobile_to_remove_list]
filtered_profile_df = filtered_profile_df[filtered_profile_df.mobile.isin(mobile_to_remove_list) == False]

## 读取触点大竖表
all_touchpoint_df = pd.read_csv('{0}_all_touchpoint_df.csv'.format(brand), sep='\t', names=['mobile','action_time','tp_id'], parse_dates=[1])
all_touchpoint_df['mobile'] = all_touchpoint_df['mobile'].astype('string').str.replace('\.0', '')

## 关联首触表和触点大竖表，获取首触用户的触点行为
tp_profile_df = filtered_profile_df.merge(all_touchpoint_df, on='mobile', how='left')
## 只考虑其首触日算起6个月内的行为
tp_profile_df = tp_profile_df[tp_profile_df['fir_contact_date'].dt.normalize() <= tp_profile_df['action_time'].dt.normalize()]
tp_profile_df = tp_profile_df[tp_profile_df['fir_contact_date'].dt.normalize() + pd.DateOffset(months=6) >= tp_profile_df['action_time'].dt.normalize()]
tp_profile_df['mobile'] = tp_profile_df['mobile'].astype('int64')

#OK 存在数据

# ---【指标计算】---

## 到店/试驾/成交触点
instore_tp_id    = '006000000000_tp' if brand == 'MG' else '006000000000_rw'
trial_df_tp_id_1 = '007003000000_tp' if brand == 'MG' else '007003000000_rw'
trial_df_tp_id_2 = '007004000000_tp' if brand == 'MG' else '007004000000_rw'
consume_tp_id_1  = '011001000000_tp' if brand == 'MG' else '011001000000_rw'
consume_tp_id_2  = '011002000000_tp' if brand == 'MG' else '011002000000_rw'


## 1) 计算首触总人数
agged_profile_df = filtered_profile_df.groupby(['fir_contact_month','fir_contact_series','mac_code','rfs_code','area','fir_contact_tp_id'])[['mobile']].count().reset_index()
agged_profile_df = agged_profile_df.rename(columns={'mobile':'cust_vol'})

## 2) 计算未成交 flag
## 计算最后成交时间
order_df = all_touchpoint_df.query("tp_id in ('{0}', '{1}')".format(consume_tp_id_1, consume_tp_id_2))\
                              .groupby(by='mobile')['action_time'].max().reset_index()\
                              .rename(columns={'action_time':'last_order_time'})\
                              .assign(action_month=lambda x: x.last_order_time.dt.strftime('%Y%m') )
order_df['mobile'] = order_df['mobile'].astype('int')

## 只保留未成交用户
order_tp_profile_df = tp_profile_df.merge(order_df, on='mobile', how='left')
order_tp_profile_df = order_tp_profile_df[order_tp_profile_df['last_order_time'].isna() | (order_tp_profile_df['last_order_time'].dt.normalize() <= order_tp_profile_df['fir_contact_date'].dt.normalize())]
# 找到未成交用户的最后一个触点
ranked_order_tp_profile_df = order_tp_profile_df.assign(rank_num=order_tp_profile_df.groupby(['mobile', 'fir_contact_tp_id', 'fir_contact_date'])['action_time']\
                                                        .rank(method='first', ascending = False))
flagged_order_tp_profile_df = ranked_order_tp_profile_df
flagged_order_tp_profile_df['undeal_flag'] = 1
flagged_order_tp_profile_df['exit_flag'] = np.where(flagged_order_tp_profile_df['rank_num'] == 1, 1, 0)

## 3) 计算到店/试驾/成交 flag
instore_df = all_touchpoint_df.query("tp_id == '{0}'".format(instore_tp_id))\
                              .groupby(by='mobile')['action_time'].max().reset_index()\
                              .rename(columns={'action_time':'last_instore_time'})
             
trial_df = all_touchpoint_df.query("tp_id in ('{0}', '{1}')".format(trial_df_tp_id_1, trial_df_tp_id_2))\
                              .groupby(by='mobile')['action_time'].max().reset_index()\
                              .rename(columns={'action_time':'last_trial_time'})

consume_df = all_touchpoint_df.query("tp_id in ('{0}', '{1}')".format(consume_tp_id_1, consume_tp_id_2))\
                              .groupby(by='mobile')['action_time'].max().reset_index()\
                              .rename(columns={'action_time':'last_consume_time'})

instore_df['mobile'] = instore_df['mobile'].astype('int')
trial_df['mobile'] = trial_df['mobile'].astype('int')
consume_df['mobile'] = consume_df['mobile'].astype('int')

indicators_df = tp_profile_df.merge(instore_df, on='mobile', how='left')\
                             .merge(trial_df, on='mobile', how='left')\
                             .merge(consume_df, on='mobile', how='left')
indicators_df = indicators_df.assign(instore_flag=np.where(indicators_df['fir_contact_date'].dt.normalize() <= indicators_df['last_instore_time'].dt.normalize(), 1, 0))\
                             .assign(trial_flag=np.where(indicators_df['fir_contact_date'].dt.normalize() <= indicators_df['last_trial_time'].dt.normalize(), 1, 0))\
                             .assign(consume_flag=np.where(indicators_df['fir_contact_date'].dt.normalize() <= indicators_df['last_consume_time'].dt.normalize(), 1, 0))\
                             .drop(columns=['last_instore_time', 'last_trial_time', 'last_consume_time'])

## 计算中间表
touchpoint_df = pd.read_csv('touchpoint_df.csv', sep='\t', names=['tp_id','level_1_tp_id','level_2_tp_id','level_3_tp_id','level_4_tp_id'])

final_df = indicators_df.merge(flagged_order_tp_profile_df, on=['mobile','fir_contact_month','fir_contact_date','fir_contact_series','mac_code','rfs_code','area','fir_contact_tp_id','tp_id','action_time'], how='left')\
                        .merge(touchpoint_df, on='tp_id', how='left')
final_df['undeal_flag'] = final_df['undeal_flag'].fillna(0).astype('int64')
final_df['exit_flag'] = final_df['exit_flag'].fillna(0).astype('int64')
final_df['fir_contact_date'] = final_df['fir_contact_date'].astype('string')
final_df['action_time'] = final_df['action_time'].astype('string')
final_df['fir_contact_month']=final_df['fir_contact_month'].astype('string')
final_df['fir_contact_series']=final_df['fir_contact_series'].astype('string')
final_df = final_df.drop(columns=['last_order_time', 'action_month', 'rank_num'])

## 计算未成交人数
undeal_df = final_df[['mobile','fir_contact_month','fir_contact_tp_id','fir_contact_series','mac_code','rfs_code','area','undeal_flag']]\
            .drop_duplicates()\
            .groupby(['fir_contact_month','fir_contact_tp_id','fir_contact_series','mac_code','rfs_code','area'])['undeal_flag'].count().reset_index()\
            .rename(columns={'undeal_flag':'undeal_vol'})


# ---【存入 Hive】---
## 首触总人数
spark_agged_profile_df = hc.createDataFrame(agged_profile_df)
spark_agged_profile_df.createOrReplaceTempView('spark_agged_profile_df')
hc.sql('''
DROP TABLE IF EXISTS marketing_modeling.app_{0}_tmp_agged_profile
'''.format(brand))
hc.sql('''
CREATE TABLE IF NOT EXISTS marketing_modeling.app_{0}_tmp_agged_profile AS
SELECT
    fir_contact_month,
    fir_contact_tp_id,
    fir_contact_series,
    mac_code,
    rfs_code,
	area,
    cust_vol
FROM spark_agged_profile_df
'''.format(brand))
 
##  指标中间表
df_schema = StructType([StructField("mobile", StringType()),
                        StructField("fir_contact_month", StringType()),
                        StructField("fir_contact_date", StringType()),
                        StructField("fir_contact_series", StringType()),
                        StructField("mac_code", StringType()),
                        StructField("rfs_code", StringType()),
                        StructField("area", StringType()),
                        StructField("fir_contact_tp_id", StringType()),
                        StructField("action_time", StringType()),
                        StructField("tp_id", StringType()),
                        StructField("instore_flag", IntegerType()),
                        StructField("trial_flag", IntegerType()),
                        StructField("consume_flag", IntegerType()),
                        StructField("undeal_flag", IntegerType()),
                        StructField("exit_flag", IntegerType()),
                        StructField("level_1_tp_id", StringType()),
                        StructField("level_2_tp_id", StringType()),
                        StructField("level_3_tp_id", StringType()),
                        StructField("level_4_tp_id", StringType())])

spark_final_df = hc.createDataFrame(final_df, schema=df_schema) 
spark_final_df.createOrReplaceTempView('spark_final_df')
hc.sql('''
DROP TABLE IF EXISTS marketing_modeling.app_{0}_tmp_tp_asset_report_a
'''.format(brand))
hc.sql('''
CREATE TABLE IF NOT EXISTS marketing_modeling.app_{0}_tmp_tp_asset_report_a AS
SELECT 
    mobile,
    fir_contact_month,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
    area,
    fir_contact_tp_id,
    tp_id,
    level_1_tp_id,
    level_2_tp_id,
    level_3_tp_id,
    level_4_tp_id,
    action_time,
    instore_flag,
    trial_flag,
    consume_flag,
    exit_flag,
    undeal_flag
FROM spark_final_df
'''.format(brand))

## 未成交人数
df_schema = StructType([StructField("fir_contact_month", StringType()),
                        StructField("fir_contact_tp_id", StringType()),
                        StructField("fir_contact_series", StringType()),
                        StructField("mac_code", StringType()),
                        StructField("rfs_code", StringType()),
                        StructField("area", StringType()),
                        StructField("undeal_vol", IntegerType())])

spark_tmp_df = hc.createDataFrame(undeal_df, schema=df_schema) 
spark_tmp_df.createOrReplaceTempView('spark_tmp_df')
hc.sql('''
DROP TABLE IF EXISTS marketing_modeling.app_{0}_tmp_undeal_report_a
'''.format(brand))
hc.sql('''
CREATE TABLE IF NOT EXISTS marketing_modeling.app_{0}_tmp_undeal_report_a AS
SELECT
    fir_contact_month,
    fir_contact_tp_id,
    fir_contact_series,
    mac_code,
    rfs_code,
    area,
    undeal_vol
FROM spark_tmp_df
'''.format(brand))
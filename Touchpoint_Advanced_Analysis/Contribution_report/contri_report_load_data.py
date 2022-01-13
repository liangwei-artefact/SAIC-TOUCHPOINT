#!/usr/bin/env python
# coding: utf-8
# @Author  : Liangwei Chen

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pandas as pd
import numpy as np
import datetime
import calendar

from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext, Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType, StructType, \
    StructField
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp, collect_set, row_number, udf


# load agrs time
# 获取月末
def get_month_end(stamp):
    year = stamp.year
    month = stamp.month
    a, b = calendar.monthrange(year, month)
    cur_month_end_stamp = datetime.datetime(year=year, month=month, day=b)
    return cur_month_end_stamp


# 获取多个月之后的月末
def get_add_n_month_end(stamp, n):
    year = stamp.year + (stamp.month + n) / 12
    if ((stamp.month + n) % 12 == 0):
        month = stamp.month
    else:
        month = (stamp.month + n) % 12
    a, b = calendar.monthrange(year, month)
    af_month_end_stamp = datetime.datetime(year=year, month=month, day=b)
    return af_month_end_stamp


input_pt = sys.argv[1]
pt_stamp = datetime.datetime.strptime(input_pt, '%Y%m%d')
cur_month_start_stamp = pt_stamp.replace(day=1)
cur_month_end_stamp = get_month_end(pt_stamp)
af_month_end_stamp = get_add_n_month_end(cur_month_end_stamp, 6)

pt = datetime.datetime.strftime(pt_stamp, "%Y%m%d")
pt_month = datetime.datetime.strftime(pt_stamp, "%Y%m")
cur_month_start = datetime.datetime.strftime(cur_month_start_stamp, "%Y%m%d")
cur_month_end = datetime.datetime.strftime(cur_month_end_stamp, "%Y%m%d")
af_month_end = datetime.datetime.strftime(af_month_end_stamp, "%Y%m%d")
# print pt
# print pt_month
# print cur_month_start
# print cur_month_end
# print af_month_end

# pyspark 配置
spark_session = SparkSession.builder.enableHiveSupport().appName("Attribution_Model") \
    .config("spark.driver.memory", "10g") \
    .config("spark.pyspark.driver.python", "/usr/bin/python2.7") \
    .config("spark.pyspark.python", "/usr/bin/python2.7") \
    .config("spark.yarn.executor.memoryOverhead", "4G") \
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") \
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()
hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
hc.setConf("metastore.catalog.default","hive")

mobile_to_remove_df = hc.sql('''
select
mobile
from marketing_modeling.cdm_dim_dealer_employee_info 
where mobile regexp '^[1][3-9][0-9]{9}$' 
group by mobile
''')
mobile_to_remove_df.toPandas().to_csv('./mobile_to_remove.csv', header=None, index=False, sep='\t')

MG_filtered_profile_df = hc.sql('''
select
    mobile,
    fir_contact_month,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
	area,
    fir_contact_tp_id
from marketing_modeling.app_touchpoints_profile_monthly
where pt = {0} and brand = 'MG'
'''.format(pt_month))
MG_filtered_profile_df.toPandas().to_csv('./MG_filtered_profile_df.csv', header=None, index=False, sep='\t')

RW_filtered_profile_df = hc.sql('''
select
    mobile,
    fir_contact_month,
    fir_contact_date,
    fir_contact_series,
    mac_code,
    rfs_code,
	area,
    fir_contact_tp_id
from marketing_modeling.app_touchpoints_profile_monthly
where pt = {0} and brand = 'RW'
'''.format(pt_month))
RW_filtered_profile_df.toPandas().to_csv('./RW_filtered_profile_df.csv', header=None, index=False, sep='\t')

MG_all_touchpoint_df = hc.sql('''
select
    mobile,
    action_time,
    touchpoint_id as tp_id
from marketing_modeling.cdm_mg_tp_ts_all_i
where pt >= {0} and pt <= {1} and brand = 'MG'
'''.format(cur_month_start, af_month_end))
MG_all_touchpoint_df.toPandas().to_csv('./MG_all_touchpoint_df.csv', header=None, index=False, sep='\t')

RW_all_touchpoint_df = hc.sql('''
select
    mobile,
    action_time,
    touchpoint_id as tp_id
from marketing_modeling.cdm_rw_tp_ts_all_i
where pt >= {0} and pt <= {1} and brand = 'RW'
'''.format(cur_month_start, af_month_end))
RW_all_touchpoint_df.toPandas().to_csv('./RW_all_touchpoint_df.csv', header=None, index=False, sep='\t')


cdm_touchpoints_id_system = hc.sql("""
select
    touchpoint_id as tp_id,
    level_1_tp_id,
    level_2_tp_id,
    level_3_tp_id,
    level_4_tp_id
from marketing_modeling.cdm_touchpoints_id_system""")
cdm_touchpoints_id_system.toPandas().to_csv('./touchpoint_df.csv', header=None, index=False, sep='\t')

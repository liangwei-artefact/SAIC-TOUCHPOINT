#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/12/25 1:43 PM
# @Author  : Liangwei Chen
# @FileName: attri_report_load_data.py
# @Software: PyCharm


import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pandas as pd

import datetime
import calendar

from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext, Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType, StructType, \
    StructField
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp, collect_set, row_number, udf


# load agrs time

# 获得多个月之后的月初
def get_n_month_start(stamp, n):
    year = stamp.year + (stamp.month + n) / 12
    if ((stamp.month + n) % 12 == 0):
        month = stamp.month
    else:
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
    if ((stamp.month + n) % 12 == 0):
        month = stamp.month
    else:
        month = (stamp.month + n) % 12
    a, b = calendar.monthrange(year, month)
    af_month_end_stamp = datetime.datetime(year=year, month=month, day=b)
    return af_month_end_stamp


# 获取月末
def get_month_end(stamp):
    cur_month_end_stamp = get_add_n_month_end(stamp, 0)
    return cur_month_end_stamp


# bf_month_start=$(date -d "${cur_month_start} -6 month" +%Y%m%d)
# bf_month_end=$(date -d "${cur_month_end} -1 month -1 day" +%Y%m%d)

input_pt = sys.argv[1]
pt_stamp = datetime.datetime.strptime(input_pt, '%Y%m%d')
cur_month_start_stamp = pt_stamp.replace(day=1)
cur_month_end_stamp = get_month_end(pt_stamp)
bf_month_start_stamp = get_n_month_start(pt_stamp, -6)
bf_month_end_stamp = get_add_n_month_end(pt_stamp, -1)

pt = datetime.datetime.strftime(pt_stamp, "%Y%m%d")
pt_month = datetime.datetime.strftime(pt_stamp, "%Y%m")
cur_month_start = datetime.datetime.strftime(cur_month_start_stamp, "%Y%m%d")
cur_month_end = datetime.datetime.strftime(cur_month_end_stamp, "%Y%m%d")
bf_month_start = datetime.datetime.strftime(bf_month_start_stamp, "%Y%m%d")
bf_month_end = datetime.datetime.strftime(bf_month_end_stamp, "%Y%m%d")

# print pt
# print pt_month
# print cur_month_start
# print cur_month_end
# print bf_month_start
# print bf_month_end

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


mg_tp_analysis_base = hc.sql('''
SELECT
    b.mobile,
    b.action_time,
    b.touchpoint_id
FROM 
(
    SELECT a.mobile
    FROM marketing_modeling.cdm_mg_tp_ts_all_i a
    LEFT JOIN marketing_modeling.cdm_dim_dealer_employee_info b
    ON a.mobile = b.mobile
    WHERE 
        pt >= {0} AND pt <= {1}
        AND b.mobile IS NULL
        GROUP BY a.mobile
) a
INNER JOIN
(
    SELECT *
    FROM marketing_modeling.cdm_mg_tp_ts_all_i a
    WHERE pt >= {2} AND pt <= {3}
) b
ON a.mobile = b.mobile
'''.format(cur_month_start, cur_month_end, bf_month_start, cur_month_end))
mg_tp_analysis_base.toPandas().to_csv('./mg_tp_analysis_base.csv', header=None, index=False, sep='\t')


rw_tp_analysis_base = hc.sql('''
SELECT
    b.mobile,
    b.action_time,
    b.touchpoint_id
FROM 
(
    SELECT a.mobile
    FROM marketing_modeling.cdm_rw_tp_ts_all_i a
    LEFT JOIN marketing_modeling.cdm_dim_dealer_employee_info b
    ON a.mobile = b.mobile
    WHERE 
        pt >= {0} AND pt <= {1}
        AND b.mobile IS NULL
        GROUP BY a.mobile
) a
INNER JOIN
(
    SELECT *
    FROM marketing_modeling.cdm_rw_tp_ts_all_i a
    WHERE pt >= {2} AND pt <= {3}
) b
ON a.mobile = b.mobile
'''.format(cur_month_start, cur_month_end, bf_month_start, cur_month_end))
rw_tp_analysis_base.toPandas().to_csv('./rw_tp_analysis_base.csv', header=None, index=False, sep='\t')


id_mapping = hc.sql('''
SELECT touchpoint_id, touchpoint_name FROM marketing_modeling.cdm_touchpoints_id_system
''')
id_mapping.toPandas().to_csv('./id_mapping.csv', header=None, index=False, sep='\t')
#!/usr/bin/python
# -*- coding: utf-8 -*-
# /*********************************************************************
# *模块: /Toichpoing_Advanced_Analysis/Attribution_report/
# *程序: attri_reprot_save_result.py
# *功能: 将马尔可夫贡献度 & 机器学习贡献度从本地存储到Hive
# *开发人: Boyan XU & Xiaofeng XU
# *开发日: 2021-09-11
# *修改记录:
#
# *********************************************************************/

from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp, collect_set, row_number, udf, expr, sum
import datetime
import pandas as pd
import sys
import logging

spark_session = SparkSession.builder.enableHiveSupport().appName("Attriution_Model") \
    .config("spark.driver.memory","10g") \
    .config("spark.driver.maxResultSize", "4096m")\
    .config("spark.pyspark.driver.python","/usr/bin/python2.7")\
    .config("spark.pyspark.python","/usr/bin/python2.7") \
    .config("spark.yarn.executor.memoryOverhead","4G") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spark.kryoserializer.buffer.max", "128m")\
    .getOrCreate()
hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")


# ---【设置日志配置】---
logger = logging.getLogger("Attribution_Model_Score_Saving")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("Logs/attribution_model.log")
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)


# ---【加载参数】---
# 归因触点，如：instore, trial, deal
attribution_tp = sys.argv[1]
# 归因月份，如：202105
pt = sys.argv[2]


# 保存马尔可夫贡献度结果
mg_mk_result = pd.read_csv('mg_mk_attribution_report.csv', encoding='utf-8')
rw_mk_result = pd.read_csv('rw_mk_attribution_report.csv', encoding='utf-8')
mk_result = pd.concat([mg_mk_result, rw_mk_result])

mk_result_df = hc.createDataFrame(mk_result)
mk_result_df.createOrReplaceTempView("mk_df")

hc.sql("""
INSERT OVERWRITE TABLE marketing_modeling.app_mk_attribution_report PARTITION(attribution_tp, pt)
SELECT
    touchpoint_id,
    touchpoint_name,
    share_in_result AS attribution,
	brand,
	'{0}' AS attribution_tp,
    '{1}' AS pt
FROM mk_df
""".format(attribution_tp, pt))
logger.info("{0} MK model result save successfully!".format(sys.argv[1]))

# 保存机器学习贡献度结果
mg_ml_result = pd.read_csv('mg_ml_attribution_report.csv', encoding='utf-8')
rw_ml_result = pd.read_csv('rw_ml_attribution_report.csv', encoding='utf-8')
ml_result = pd.concat([mg_ml_result, rw_ml_result])

ml_result_df = hc.createDataFrame(ml_result)
ml_result_df.createOrReplaceTempView("ml_df")

hc.sql("""
INSERT OVERWRITE TABLE marketing_modeling.app_ml_attribution_report PARTITION(attribution_tp, pt)
SELECT
    touchpoint_id,
    touchpoint_name,
    feature_importance AS attribution,
	brand,
	'{0}' AS attribution_tp,
    '{1}' AS pt
FROM ml_df
""".format(attribution_tp, pt))
logger.info("{0} ML model result save successfully!".format(sys.argv[1]))


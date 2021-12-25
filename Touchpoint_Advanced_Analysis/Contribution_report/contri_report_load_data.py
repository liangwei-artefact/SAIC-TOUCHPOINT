#!/usr/bin/env python
# coding: utf-8

import sys
import pandas as pd
import numpy as np
import datetime

from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext, Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType, StructType, \
    StructField
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp, collect_set, row_number, udf

spark_session = SparkSession.builder.enableHiveSupport().appName("Attribution_Model") \
    .config("spark.driver.memory", "10g") \
    .config("spark.pyspark.driver.python", "/usr/bin/python2.7") \
    .config("spark.pyspark.python", "/usr/bin/python2.7") \
    .config("spark.yarn.executor.memoryOverhead", "4G") \
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") \
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

cdm_touchpoints_id_system = hc.sql("""
select
    touchpoint_id as tp_id,
    level_1_tp_id,
    level_2_tp_id,
    level_3_tp_id,
    level_4_tp_id
from marketing_modeling.cdm_touchpoints_id_system""")

cdm_touchpoints_id_system.toPandas().to_csv('cdm_touchpoints_id_system.csv')

#!/usr/bin/env python
# coding: utf-8

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

pt       = sys.argv[1]
run_date = sys.argv[2]

# 盲订: 010002000000_tp
# 小订: 010001000000_tp
booking_df = hc.sql('''
    SELECT 
        buyer_tel AS mobile,
        order_date AS action_time,
        CASE 
           WHEN order_type = 1 AND brand_id='121' THEN '010002000000_tp'
           WHEN order_type = 3 AND brand_id='121' THEN '010001000000_tp' 
           WHEN order_type = 1 AND brand_id='101' THEN '010002000000_rw'
           WHEN order_type = 3 AND brand_id='101' THEN '010001000000_rw' 
        END AS touchpoint_id,
        date_format(order_date,'yyyyMMdd') as pt,
        CASE WHEN brand_id = '101' THEN 'RW' 
             WHEN brand_id = '121 'THEN 'MG' 
             ELSE NULL
        END AS brand
    FROM 
    (
        select
        to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss') order_date,
        detail['order_type'] order_type,
        detail['brand_id'] brand_id,
        phone buyer_tel
        from cdp.cdm_cdp_customer_behavior_detail
        WHERE pt = {0}
        AND regexp_replace(to_date(to_utc_timestamp(detail['behavior_time'],'yyyy-MM-dd HH:mm:ss')), '-', '') >= {0}
        and type = 'small_blind_order'
    ) a    
    WHERE order_type in (1,3)
'''.format(pt))

# 大订/交车: 011000000000_tp	
# 大订
consume_df = hc.sql("""
    SELECT * FROM marketing_modeling.dw_consume_behavior
    WHERE pt >= {0}
""".format(pt))
# 交车
deliver_df = hc.sql('''
    SELECT * FROM marketing_modeling.dw_deliver_behavior
    WHERE pt >= {0}
'''.format(pt))

consume_df = consume_df.withColumn('brand', F.expr('case when brand_id = 121 then "MG" when brand_id = 101 then "RW" end'))\
                        .withColumn('touchpoint_id',F.expr('case when brand_id = 121 then "011001000000_tp" when brand_id = 101 then "011001000000_rw" end'))

deliver_df = deliver_df.withColumn('brand', F.expr('case when brand_id = 121 then "MG" when brand_id = 101 then "RW" end'))\
                        .withColumn('touchpoint_id',F.expr('case when brand_id = 121 then "011002000000_tp" when brand_id = 101 then "011002000000_rw" end'))\
                        .withColumn('action_time', F.expr("from_unixtime(unix_timestamp(behavior_time ,'yyyy-MM-dd'), 'yyyy-MM-dd hh:mm:ss')"))

# Save Result
order_df = consume_df.selectExpr('phone as mobile','behavior_time as action_time','touchpoint_id','pt','brand')\
                    .unionAll(deliver_df.selectExpr('phone as mobile','action_time','touchpoint_id','pt','brand'))
final_df = order_df.unionAll(booking_df)
final_df = final_df.filter((col('mobile').rlike("^[1][3-9][0-9]{9}$")) & \
(col('action_time').isNotNull()) & (col('touchpoint_id').isNotNull()) & (col('brand').isNotNull()))

final_df.createOrReplaceTempView('final_df')
hc.sql('insert overwrite table marketing_modeling.cdm_ts_order_i PARTITION (pt,brand) select * from final_df')


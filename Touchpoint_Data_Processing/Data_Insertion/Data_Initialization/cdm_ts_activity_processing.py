#!/usr/bin/env python
# coding: utf-8
#/*********************************************************************
#*模块: XX
#*程序: XX
#*功能: XX
#*开发人: Xiaofeng XU
#*开发日: 2021-08-22
#*修改记录:

#*********************************************************************/

import datetime
import sys
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp


def activity_type_mapping(activity_name,mode):
    activity_map = {
        u'试驾会': [u'试乘', u'试驾'],
        u'外展类': [u'定展', u'商圈', u'外展'],
        u'车展': [u'车展', u'联展'],
        u'客户关爱活动': [u'答谢会',u'讲堂', u'观影活动', u'自驾游', u'节油赛', u'客户关怀'],
        u'团购促销类': [u'团购', u'抢购会',u'品鉴会',u'安心购',u'狂欢节',u'购车节',u'购车季',u'大促',u'秒杀',u'抄底',u'钜惠',
                 u'低价',u'特惠',u'补贴',u'盛惠',u'特卖',u'让利',u'抢订'],
        u'保客活动': [u'保客', u'车主派对',u'老车主'],
        u'展厅活动类': [u'展厅', u'店头'],
        u'异业合作类': [u'异业合作'],
        u'新车上市类': [u'新车上市', u'上市发布'],
        u'线下其他活动': [u'线下其他'],
        u'线下广告集客类': [u'集客', u'户外', u'平面', u'墙体广告', u'影院投放', u'楼宇广告'],
        u'活动直播': [u'直播', u'短视频'],
        u'线上其他活动': [u'电台', u'网络', u'微信', u'朋友圈', u'APP投放', u'电视']}

    activity_level = {u'团购促销类': [u'封闭式大型团购会', u'区域团购会', u'二网团购',u'区域团购',u'团购会'],
                      u'外展类': [u'带车外展', u'商圈', u'中、小型商超定展（室内）', u'中、小型商超定展（室外）',u'车展及商超定展'],
                      u'客户关爱活动': [u'客户关爱活动（答谢会，爱车讲堂，观影活动等）',u'区域自驾游',
                                 u'区域自驾游、节油赛（时间证明：隔日活动可附房费增值税发票，当日活动仍使用当日报纸或彩票）'],
                      u'展厅活动类':[u'展厅中、小型活动', u'店头活动', u'展厅大型活动',u'展厅活动'],
                      u'异业合作类':[u'异业合作'],
                      u'新车上市类': [u'新车上市', u'媒体新车上市会（商超）', u'媒体新车上市会（封闭）'],
                      u'线下广告集客类': [u'低成本集客', u'户外', u'平面', u'墙体广告', u'影院投放', u'楼宇广告'],
                      u'试驾会': [u'试乘试驾', u'大型试乘试驾会活动'],
                      u'车展': [u'D级车展、多品牌联展', u'车展', u'C级车展', u'A、B级车展'],
                      u'线下其他活动':[u'线下其他'],
                      u'活动直播':[u'线上直播', u'直播及短视频活动',u'展厅线上直播',u'新媒体活动（快手、抖音短视频相关）'],
                      u'线上其他活动':[u'网络',u'电台',u'微信营销（朋友圈联动、加好友比赛等）',u'微信相关（朋友圈联动、加好友比赛等）',
                               u'APP投放',u'线上其他',u'电视',u'线上其它'],
                      u'保客活动':[u'保客活动',u'车主活动']} 
    try:
        if mode == 1:
            for type_ in activity_map.keys():
                if any(kw in activity_name for kw in activity_map[type_]):
                    return type_
                else:
                    continue
        elif mode == 2:
            for type_ in activity_level.keys():
                if any(kw in activity_name for kw in activity_level[type_]):
                    return type_
                else:
                    continue
    except:
        return ''
    
spark_session = SparkSession.builder.enableHiveSupport().appName("attribution_data_processing") \
    .config("spark.driver.memory","30g") \
    .config("spark.yarn.executor.memoryOverhead","20G") \
    .config("spark.sql.broadcastTimeout", "3600")\
    .config("spark.driver.maxResultSize", "6g")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
    .config("hive.exec.dynamic.partition", True)\
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")\
    .config("spark.default.parallelism", 200)\
    .getOrCreate()

hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")
hc.udf.register("activity_type_mapping",activity_type_mapping,StringType())

activity_df = hc.sql('''
SELECT
    mobile,
    activity_name,
    CASE WHEN saic_type_group IN ('线下广告集客','车展') THEN saic_type
    WHEN tmp_activity_type <> '' THEN tmp_activity_type 
    WHEN saic_type_group <> '' THEN saic_type_group
    ELSE '其他活动参与' END AS activity_type,
    action_time,
    brand,
    pt
FROM
(
    SELECT 
        mobile,
        activity_name,
        activity_type_mapping(activity_name,1) AS tmp_activity_type,
        saic_type,
        activity_type_mapping(saic_type,2) AS saic_type_group,
        act_time AS action_time,
        brand,
        date_format(act_time,'yyyyMMdd') as pt
    FROM marketing_modeling.tmp_dw_ts_activity_i
) a
''')

mg_tp_id = hc.sql('''
SELECT touchpoint_id, touchpoint_name FROM marketing_modeling.cdm_touchpoints_id_system
WHERE (touchpoint_id LIKE '008%000_tp' AND touchpoint_level = 3 AND touchpoint_id NOT IN ('008001009000_tp'))
OR touchpoint_id = '012001000000_tp'
OR touchpoint_id = '008003000000_tp'
OR (touchpoint_id LIKE '008001009%_tp' AND touchpoint_level = 4)
OR (touchpoint_id LIKE '008001007%_tp' AND touchpoint_level = 4)
''')

rw_tp_id = hc.sql('''
SELECT touchpoint_id, touchpoint_name FROM marketing_modeling.cdm_touchpoints_id_system
    WHERE (touchpoint_id LIKE '008%000_rw' AND touchpoint_level = 3 AND touchpoint_id NOT IN ('008001009000_rw'))
    OR touchpoint_id = '012001000000_rw'
    OR touchpoint_id = '008003000000_rw'
    OR (touchpoint_id LIKE '008001009%_rw' AND touchpoint_level = 4)
    OR (touchpoint_id LIKE '008001007%_rw' AND touchpoint_level = 4)
''')

mg_ctivity_df = activity_df.filter('brand = "MG"').alias('df1').join(mg_tp_id.alias('df2'),col('df1.activity_type') == col('df2.touchpoint_name'), how='left')
rw_activity_df = activity_df.filter('brand = "RW"').alias('df1').join(rw_tp_id.alias('df2'),col('df1.activity_type') == col('df2.touchpoint_name'), how='left')

# Save result
final_df = mg_ctivity_df.unionAll(rw_activity_df)
final_df = final_df.select('mobile','activity_name','action_time','touchpoint_id','pt','brand')\
.filter((col('mobile').rlike("^[1][3-9][0-9]{9}$"))\
        & (col('action_time').isNotNull())\
        & (col('touchpoint_id').isNotNull())\
        & (col('brand').isNotNull()))
		
final_df.createOrReplaceTempView('final_df')
hc.sql('insert overwrite table marketing_modeling.cdm_ts_activity_i PARTITION (pt,brand) select * from final_df')







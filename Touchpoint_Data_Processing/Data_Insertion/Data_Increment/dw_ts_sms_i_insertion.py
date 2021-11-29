#!/usr/bin/env python
# coding: utf-8

import sys
import numpy as np
import yaml
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType
from pyspark.sql.functions import col, count, countDistinct, lit, to_timestamp
import datetime

def sms_type_mapping(sms_name):
    sms_map = {
        u'活动': [u'活动',u'直播',u'试驾',u'众筹',u'购车节',u'33亿活动',u'体验日',u'音乐节',u'盲盒',u'合伙人',u'共创',
                u'小魔盒',u'中奖',u'电竞',u'足球赛',u'车展',u'财神',u'花路',u'盲订',u'游园地图',u'三八节',u'邀您一起下江南',
                u'55节',u'618攻略',u'品鉴会'],
        u'推介': [u'推广',u'推介',u'销售支持',u'mg6 pro在线秀',u'邀约'],
        u'挽回': [u'战败',u'挽回',u'激活'],
        u'调研': [u'问卷',u'调研',u'调查'],
        u'潜客': [u'潜客',u'线索澄清',u'培育中心',u'享道合作',u'目标用户短信营销',u'华为屏营销',u'孵化',u'山河令',
                u'社交裂变',u'线索培育',u'留资'],
        u'服务': [u'提车攻略',u'核销',u'中签',u'通知',u'购车攻略'],
        u'保客': [u'车主',u'关怀',u'绑车引导',u'答谢',u'保客'],
        u'增换购': [u'置换',u'增换购'],
        u'促活': [u'促活',u'论坛上线引流',u'mglive抢票',u'学习强国站内信',u'一周精彩回顾',u'站内信',u'宠粉',
                u'最佳涂装邀你评选']}

    for type_ in [u'活动',u'推介',u'挽回',u'调研',u'潜客',u'服务',u'保客',u'增换购',u'促活']:
        if any(kw in sms_name for kw in sms_map[type_]):
            return type_
        else:
            continue

spark_session = SparkSession.builder.enableHiveSupport().appName("attribution_data_processing")\
        .config("spark.driver.memory","30g") \
        .config("spark.yarn.executor.memoryOverhead","20G") \
        .config("spark.sql.broadcastTimeout", "3600")\
        .config("spark.driver.maxResultSize", "6g")\
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .config("hive.exec.dynamic.partition", True)\
        .config("hive.exec.max.dynamic.partitions",2048)\
        .config("hive.exec.max.dynamic.partitions.pernode",1000)\
        .config("spark.default.parallelism", 200) \
        .getOrCreate()
        
hc = HiveContext(spark_session.sparkContext)
hc.udf.register("sms_type_mapping",sms_type_mapping,StringType())

# pt1 - 数据开始日期，pt2 - 数据结束日期
pt = sys.argv[1]

# 处理老MA系统数据
## 收到短信
sms_df_old_received = hc.sql('''
SELECT
    mobile,
    CASE 
        WHEN sms_type = '活动' AND brand = 'MG' THEN '002011005001_tp'
        WHEN sms_type = '服务' AND brand = 'MG' THEN '002011005002_tp'
        WHEN sms_type = '潜客' AND brand = 'MG' THEN '002011005003_tp'
        WHEN sms_type = '推介' AND brand = 'MG' THEN '002011005004_tp'
        WHEN sms_type = '挽回' AND brand = 'MG' THEN '002011005005_tp'
        WHEN sms_type = '增换购' AND brand = 'MG' THEN '002011005006_tp'
        WHEN sms_type = '调研' AND brand = 'MG' THEN '002011005008_tp'
        WHEN sms_type = '保客' AND brand = 'MG' THEN '002011005009_tp'
        WHEN sms_type = '促活' AND brand = 'MG' THEN '002011005010_tp'
        WHEN brand = 'MG' THEN '002011005007_tp'
        WHEN sms_type = '活动' AND brand = 'RW' THEN '002005005001_rw'
        WHEN sms_type = '服务' AND brand = 'RW' THEN '002005005002_rw'
        WHEN sms_type = '潜客' AND brand = 'RW' THEN '002005005003_rw'
        WHEN sms_type = '推介' AND brand = 'RW' THEN '002005005004_rw'
        WHEN sms_type = '挽回' AND brand = 'RW' THEN '002005005005_rw'
        WHEN sms_type = '增换购' AND brand = 'RW' THEN '002005005006_rw'
        WHEN sms_type = '调研' AND brand = 'RW' THEN '002005005008_rw'
        WHEN sms_type = '保客' AND brand = 'RW' THEN '002005005009_rw'
        WHEN sms_type = '促活' AND brand = 'RW' THEN '002005005010_rw'
        WHEN brand = 'RW' THEN '002005005007_rw' 
    END AS touchpoint_id,
    sms_type,
    sms_name,
    action_time,
    pt,
    brand
FROM
(
    SELECT 
        phone as mobile,
        detail['content_title'] as sms_name,
        sms_type_mapping(detail['content_title']) as sms_type,
        cast(from_unixtime(unix_timestamp(cast(detail['timestamp'] as string), 'yyyyMMddHHmmss')) as TIMESTAMP) as action_time,
        case   
            WHEN detail['zz1_cho9_mia'] = 121 THEN 'MG'
            WHEN detail['zz1_cho9_mia'] = 101 THEN 'RW'
        ELSE NULL END AS brand,
        pt
    FROM cdp.cdm_cdp_customer_behavior_detail
    WHERE 
        TYPE = 'ma_message'
        AND detail['content_title'] not like '%test%'
        AND detail['content_title'] not like '%测试%'
        AND pt >= {0}
) a
where brand is not null
'''.format(pt))

# 点击短信短链
sms_df_old_click = hc.sql('''
SELECT
    mobile,
    CASE 
        WHEN sms_type = '活动' AND brand = 'MG' THEN '002011005001_tp'
        WHEN sms_type = '服务' AND brand = 'MG' THEN '002011005002_tp'
        WHEN sms_type = '潜客' AND brand = 'MG' THEN '002011005003_tp'
        WHEN sms_type = '推介' AND brand = 'MG' THEN '002011005004_tp'
        WHEN sms_type = '挽回' AND brand = 'MG' THEN '002011005005_tp'
        WHEN sms_type = '增换购' AND brand = 'MG' THEN '002011005006_tp'
        WHEN sms_type = '调研' AND brand = 'MG' THEN '002011005008_tp'
        WHEN sms_type = '保客' AND brand = 'MG' THEN '002011005009_tp'
        WHEN sms_type = '促活' AND brand = 'MG' THEN '002011005010_tp'
        WHEN brand = 'MG' THEN '002011005007_tp'
        WHEN sms_type = '活动' AND brand = 'RW' THEN '002005005001_rw'
        WHEN sms_type = '服务' AND brand = 'RW' THEN '002005005002_rw'
        WHEN sms_type = '潜客' AND brand = 'RW' THEN '002005005003_rw'
        WHEN sms_type = '推介' AND brand = 'RW' THEN '002005005004_rw'
        WHEN sms_type = '挽回' AND brand = 'RW' THEN '002005005005_rw'
        WHEN sms_type = '增换购' AND brand = 'RW' THEN '002005005006_rw'
        WHEN sms_type = '调研' AND brand = 'RW' THEN '002005005008_rw'
        WHEN sms_type = '保客' AND brand = 'RW' THEN '002005005009_rw'
        WHEN sms_type = '促活' AND brand = 'RW' THEN '002005005010_rw'
        WHEN brand = 'RW' THEN '002005005007_rw' 
    END AS touchpoint_id,
    sms_type,
    sms_name,
    action_time,
    pt,
    brand
FROM
(
    SELECT 
    phone as mobile,
    detail['content_title'] as sms_name,
    sms_type_mapping(detail['content_title']) as sms_type,
    cast(from_unixtime(unix_timestamp(cast(detail['timestamp'] as string), 'yyyyMMddHHmmss')) as TIMESTAMP) as action_time,
    CASE 
        WHEN detail['zz1_cho9_mia'] = 121 THEN 'MG'
        WHEN detail['zz1_cho9_mia'] = 101 THEN 'RW'
    ELSE NULL END AS brand,
    pt
    FROM 
        cdp.cdm_cdp_customer_behavior_detail
    WHERE 
        TYPE = 'ma_message'
        AND detail['zh5_timestamp'] IS NOT NULL
        AND detail['content_title'] not like '%test%'
        AND detail['content_title'] not like '%测试%'
        AND pt >= {0}
) a
where brand is not null
'''.format(pt))

# 处理新MA数据
sms_df_new_received = hc.sql('''
SELECT
    mobile,
    CASE 
        WHEN sms_type = '活动' AND brand = 'MG' THEN '002011005001_tp'
        WHEN sms_type = '服务' AND brand = 'MG' THEN '002011005002_tp'
        WHEN sms_type = '潜客' AND brand = 'MG' THEN '002011005003_tp'
        WHEN sms_type = '推介' AND brand = 'MG' THEN '002011005004_tp'
        WHEN sms_type = '挽回' AND brand = 'MG' THEN '002011005005_tp'
        WHEN sms_type = '增换购' AND brand = 'MG' THEN '002011005006_tp'
        WHEN sms_type = '调研' AND brand = 'MG' THEN '002011005008_tp'
        WHEN sms_type = '保客' AND brand = 'MG' THEN '002011005009_tp'
        WHEN sms_type = '促活' AND brand = 'MG' THEN '002011005010_tp'
        WHEN brand = 'MG' THEN '002011005007_tp'
        WHEN sms_type = '活动' AND brand = 'RW' THEN '002005005001_rw'
        WHEN sms_type = '服务' AND brand = 'RW' THEN '002005005002_rw'
        WHEN sms_type = '潜客' AND brand = 'RW' THEN '002005005003_rw'
        WHEN sms_type = '推介' AND brand = 'RW' THEN '002005005004_rw'
        WHEN sms_type = '挽回' AND brand = 'RW' THEN '002005005005_rw'
        WHEN sms_type = '增换购' AND brand = 'RW' THEN '002005005006_rw'
        WHEN sms_type = '调研' AND brand = 'RW' THEN '002005005008_rw'
        WHEN sms_type = '保客' AND brand = 'RW' THEN '002005005009_rw'
        WHEN sms_type = '促活' AND brand = 'RW' THEN '002005005010_rw'
        WHEN brand = 'RW' THEN '002005005007_rw' 
    END AS touchpoint_id,
    sms_type,
    sms_name,
    action_time,
    pt,
    brand
FROM
(
    select 
        mobile_phone as mobile,
        attr4 as sms_name,
        sms_type_mapping(attr4) as sms_type,
        inserttime as action_time,
        case 
            when attr4 like '%荣威%' then 'RW'
            when attr4 like '%龚俊%' then 'RW'
            when attr4 like '%合伙人%' then 'RW'
            when attr4 like '%华南区销售支持%' then 'RW'
        else 'MG' end as brand,
        regexp_replace(to_date(inserttime), '-', '') as pt
    from 
    (
        select 
            attr4, contact_identity_id, inserttime 
        from dtwarehouse.ods_ma_doris_db_1_event
        where 
            pt = {0}
            and ((event='sms_huawei__push_sms' and attr1='发送请求成功。') or (event='app_sms__app_saic_message' and attr1='成功'))
            and regexp_replace(to_date(inserttime), '-', '') >= '{0}'
            and attr4 not like '%test%'
            and attr4 not like '%测试%'
    ) a
    LEFT JOIN
    (
        select 
            id, mobile_phone 
        from dtwarehouse.ods_ma_doris_db_1_contact_identity 
        where 
            pt = {0}
        group by id, mobile_phone
    ) b
    on a.contact_identity_id = b.id 
    where mobile_phone is not null
) a
where brand is not null
'''.format(pt))

final_df = sms_df_old_received.unionAll(sms_df_old_click).unionAll(sms_df_new_received)
final_df.createOrReplaceTempView('final_df')
hc.sql('insert overwrite table marketing_modeling.dw_ts_sms_i PARTITION (pt,brand) select * from final_df')
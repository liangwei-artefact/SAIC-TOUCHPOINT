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

pt = sys.argv[1]

def mg_lead_source_id_mapping(lead_source):
    # MG的线索来源匹配逻辑字典
    lead_source_dic = {
        u'厂方导入_官网': '001003001001_tp',
        u'第一方触点_名爵官网试乘试驾': '001003001002_tp',
        u'厂方二手车平台_名爵官网移动端二手车': '001003001003_tp',
        u'第一方触点_官网一键发信': '001003001004_tp',
        u'厂方二手车平台_名爵官网PC端二手车': '001003001005_tp',
        u'厂方二手车平台_名爵APP二手车': '001003002001_tp',
        u'名爵APP_名爵试乘试驾': '001003002002_tp',
        u'第一方触点_官方APP试乘试驾留资': '001003002003_tp',
        u'名爵APP_名爵小额定': '001003002004_tp',
        u'第一方触点_小额定单未支付': '001003002005_tp',
        u'名爵APP_定金定单未支付': '001003002005_tp',
        u'俱乐部_MG俱乐部': '001003002006_tp',
        u'第一方触点_支付宝小程序': '001011001001_tp',
        u'第一方触点_快应用': '001011001002_tp',
        u'第一方触点_快手小程序': '001011001003_tp',
        u'第一方触点_抖音小程序': '001011001004_tp',
        u'第一方触点_今日头条小程序': '001011001005_tp',
        u'第一方触点_益普索益起答': '001004001001_tp',
        u'厂方导入_地区性网站': '001004001002_tp',
        u'厂方导入_合作伙伴': '001004001003_tp',
        u'活动_KBU活动集客-顾问': '001004001004_tp',
        u'活动_KBU活动集客-小蜜蜂': '001004001005_tp',
        u'活动_KBU活动集客-大屏': '001004001006_tp',
        u'大数据平台_车享家': '001004002001_tp',
        u'大数据平台_工商银行': '001004002002_tp',
        u'大数据平台_平安租赁': '001004002003_tp',
        u'大数据平台_环球车享': '001004002004_tp',
        u'大数据平台_浦发银行': '001004002005_tp',
        u'大数据平台_金融保客': '001004002006_tp',
        u'厂方二手车平台_手动创建二手车': '001004003001_tp',
        u'第一方触点_车展互动活动': '001004003002_tp',
        u'第一方触点_总部短信营销': '001004003003_tp',
        u'厂方导入_员工推荐购车': '001004003004_tp',
        u'经销商网销主动开拓_基盘客户(网销)': '001004003005_tp',
        u'经销商网销主动开拓_官方分配': '001004003006_tp',
        u'经销商网销主动开拓_网销外展': '001004003007_tp',
        u'经销商网销主动开拓_网销线上外拓': '001004003008_tp',
        u'厂方的网销其他平台_新电商渠道': '001004003009_tp',
        u'经销商网销主动开拓_新渠道': '001004003010_tp',
        u'投放_微信朋友圈': '001005001001_tp',
        u'投放_抖音': '001005001002_tp',
        u'投放_今日头条': '001005001003_tp',
        u'广告投放_字节跳动效果通': '001005001004_tp',
        u'投放_第三方电商渠道': '001005001005_tp',
        u'大数据平台_冷线索': '001006001001_tp',
        u'大数据平台_社交裂变': '001006001002_tp',
        u'大数据平台_精准线索': '001006001003_tp',
        u'智能调配_精准线索': '001006001003_tp',
        u'大数据平台_增换购': '001006001004_tp',
        u'大数据平台_金线索': '001006002005_tp',
        u'大数据平台_活动报名': '001006002001_tp',
        u'大数据平台_一键询价': '001006002002_tp',
        u'大数据平台_一键留资': '001006002003_tp',
        u'大数据平台_销售顾问与我联系': '001006002004_tp',
        u'经销商网销主动开拓_太平洋汽车网': '001007001001_tp',
        u'经销商网销主动开拓_易车网': '001007001002_tp',
        u'经销商网销主动开拓_易车': '001007001002_tp',
        u'经销商网销主动开拓_汽车之家': '001007001003_tp',
        u'经销商网销主动开拓_懂车帝': '001007001004_tp',
        u'经销商网销主动开拓_社交渠道': '001007002001_tp',
        u'厂方的网销其他平台_社交渠道': '001007002001_tp',
        u'经销商网销主动开拓_论坛': '001007002002_tp',
        u'经销商网销主动开拓_QQ': '001007002003_tp',
        u'经销商网销主动开拓_天猫': '001007002004_tp',
        u'厂方的网销其他平台_天猫': '001007002004_tp',
        u'经销商网销主动开拓_腾讯': '001007002005_tp',
        u'经销商网销主动开拓_合作资源1': '001007002006_tp',
        u'经销商网销主动开拓_合作资源3': '001007002007_tp',
        u'经销商网销主动开拓_社交营销': '001007002008_tp',
        u'展厅主动开拓_车展': '001007002009_tp',
        u'经销商网销主动开拓_合作资源2': '001007002010_tp',
        u'网站平台接口_汽车之家': '001008001001_tp',
        u'投放_汽车之家投放': '001008001002_tp',
        u'网站平台接口_懂车帝': '001008002001_tp',
        u'投放_懂车帝广告投放': '001008002002_tp',
        u'网站平台接口_易车网': '001008003001_tp',
        u'投放_易车广告投放': '001008003002_tp',
        u'网站平台接口_太平洋': '001008004001_tp',
        u'网站平台接口_途虎网': '001008005007_tp',
        u'网站平台接口_17汽车网': '001008005001_tp',
        u'网站平台接口_电商': '001008005002_tp',
        u'网站平台接口_卡盟粒子': '001008005003_tp',
        u'网站平台接口_好车网': '001008005004_tp',
        u'网站平台接口_爱卡汽车': '001008005005_tp',
        u'网站平台接口_腾讯卖车宝': '001008005006_tp',
        u'网站平台接口_腾讯': '001008005006_tp',
        u'投放_58汽车': '001008006001_tp',
        u'投放_网上车市': '001008006002_tp',
        u'网站平台接口_其他': '001008007000_tp',
        u'SIS接口分配_SIS其它': '001009001001_tp',
        u'SIS接口分配_SIS战败客户': '001009001002_tp',
        u'SIS接口分配_C/N级客户': '001009001003_tp',
        u'SIS接口分配_爱卡': '001009001004_tp'
    }
    
    try:
        return lead_source_id_mapping[lead_source]
    except:
        return '001010000000_tp' # Unknown lead source

def rw_lead_source_id_mapping(lead_source):
	# 荣威的mapping逻辑
    lead_source_dic = {
        u'厂方二手车平台_荣威官网PC端二手车': '001003001001_rw',
        u'厂方二手车平台_荣威官网移动端二手车': '001003001002_rw',
        u'厂方导入_官网': '001003001003_rw',
        u'第一方触点_荣威官网试乘试驾': '001003001004_rw',
        u'第一方触点_荣威官网小额定': '001003001005_rw',
        u'第一方触点_荣威APP': '001003002001_rw',
        u'厂方二手车平台_荣威APP二手车': '001003002002_rw',
        u'第一方触点_官方APP试乘试驾留资': '001003002003_rw',
        u'荣威APP_荣威试乘试驾': '001003002003_rw',
        u'荣威APP_定金定单未支付': '001003002004_rw',
        u'荣威APP_荣威小额定': '001003002005_rw',
        u'第一方触点_小额定单未支付': '001003002006_rw',
        u'第一方触点_百度小程序': '001004001001_rw',
        u'第一方触点_今日头条小程序': '001004001002_rw',
        u'第一方触点_小程序': '001004001003_rw',
        u'官方认证二手车_二手车小程序': '001004001004_rw',
        u'第一方触点_抖音小程序': '001004001005_rw',
        u'第一方触点_支付宝小程序': '001004001006_rw',
        u'第一方触点_快手小程序': '001004001007_rw',
        u'活动_A类活动': '001005001001_rw',
        u'第一方触点_车展互动活动': '001005001002_rw',
        u'大数据平台_金线索': '001005002001_rw',
        u'大数据平台_车享家': '001005002002_rw',
        u'大数据平台_浦发银行': '001005002003_rw',
        u'大数据平台_邮储银行': '001005002004_rw',
        u'大数据平台_金融保客': '001005002005_rw',
        u'厂方导入_合作伙伴': '001005002006_rw',
        u'厂方二手车平台_手动创建二手车': '001005003001_rw',
        u'厂方导入_员工推荐购车': '001005003002_rw',
        u'厂方导入_厂方系统数据': '001005003003_rw',
        u'大数据平台_社交裂变': '001005003004_rw',
        u'活动_KBU活动集客-大屏': '001005003005_rw',
        u'活动_KBU活动集客-小蜜蜂': '001005003006_rw',
        u'活动_KBU活动集客-顾问': '001005003007_rw',
        u'投放_百度广告投放': '001006001001_rw',
        u'投放_快手效果通': '001006001002_rw',
        u'投放_携程': '001006001003_rw',
        u'投放_新浪财经': '001006001004_rw',
        u'投放_腾讯新闻': '001006001005_rw',
        u'投放_小红书': '001006001006_rw',
        u'投放_效果通': '001006001007_rw',
        u'投放_微信朋友圈': '001006001008_rw',
        u'投放_网易': '001006001009_rw',
        u'投放_抖音': '001006001010_rw',
        u'投放_新浪': '001006001011_rw',
        u'投放_一点资讯': '001006001012_rw',
        u'投放_广点通': '001006001013_rw',
        u'投放_今日头条': '001006001014_rw',
        u'投放_第三方电商渠道': '001006001015_rw',
        u'投放_车来了': '001006001016_rw',
        u'投放_知乎': '001006001017_rw',
        u'投放_OTT智能电视': '001006001018_rw',
        u'投放_去哪儿投放': '001006001019_rw',
        u'投放_妈妈帮': '001006001020_rw',
        u'投放_有数儿汽车': '001006001021_rw',
        u'投放_虎扑投放': '001006001022_rw',
        u'投放_云和点动': '001006001023_rw',
        u'投放_搜狐投放': '001006001024_rw',
        u'经销商网销主动开拓_官方分配': '',
        u'经销商网销主动开拓_新渠道': '001007001001_rw',
        u'经销商网销主动开拓_百度有驾线下建卡': '001007001002_rw',
        u'经销商网销主动开拓_社交营销': '001007001003_rw',
        u'经销商网销主动开拓_QQ': '001007001004_rw',
        u'经销商网销主动开拓_网销线上外拓': '001007001005_rw',
        u'经销商网销主动开拓_合作资源1': '001007002001_rw',
        u'经销商网销主动开拓_基盘客户(网销)': '001007002002_rw',
        u'三方电商_Tmall渠道': '001011001001_rw',
        u'电商_天猫': '001011001002_rw',
        u'三方电商_苏宁渠道': '001011002000_rw',
        u'网站平台接口_汽车之家': '001008001001_rw',
        u'投放_汽车之家投放': '001008001002_rw',
        u'网站平台接口_懂车帝': '001008002001_rw',
        u'投放_懂车帝广告投放': '001008002002_rw',
        u'网站平台接口_易车网': '001008003001_rw',
        u'投放_易车广告投放': '001008003002_rw',
        u'投放_易车效益达': '001008003003_rw',
        u'网站平台接口_车智推平台': '001008004001_rw',
        u'网站平台接口_卡盟粒子': '001008004002_rw',
        u'网站平台接口_车智推': '001008004003_rw',
        u'网站平台接口_好车网': '001008004004_rw',
        u'网站平台接口_太平洋': '001008004005_rw',
        u'网站平台接口_爱卡汽车': '001008004006_rw',
        u'网站平台接口_17汽车网': '001008004007_rw',
        u'网站平台接口_电商': '001008004008_rw',
        u'投放_网上车市': '001008005001_rw',
        u'投放_爱卡汽车': '001008005002_rw',
        u'投放_58汽车': '001008005003_rw',
        u'投放_太平洋汽车网': '001008005004_rw',
        u'SIS接口分配_SIS其它': '001009001001_rw',
        u'SIS接口分配_R客户转卡': '001009001002_rw',
        u'R-APP_定金定单未支付': '001010001001_rw',
        u'R汽车厂方导入_R汽车厂方线索': '001010001002_rw',
        u'第一方触点_R汽车车展互动': '001010001003_rw',
        u'第一方触点_R汽车门店活动定向拉新': '001010001004_rw',
        u'第一方触点_北京RSPACE集客小程序': '001010001005_rw',
        u'俱乐部_MG俱乐部': '001010002001_rw',
        u'厂方二手车平台_名爵车主俱乐部二手车': '001010002002_rw',
        u'名爵APP_名爵小额定': '001010002003_rw',
    }
    try:
        return rw_lead_source_id_mapping[lead_source]
    except:
        return '001012000000_rw' # other crosschannel lead source

#'经销商网销主动开拓_': '001007003000_tp',
#'展厅主动开拓_NULL': '001007003000_tp',
#'NULL_厂方的网销其他平台': '001007003000_tp',
#'线索池渠道未知':001010000000_tp

spark_session = SparkSession.builder.enableHiveSupport().appName("attribution_data_processing") \
    .config("spark.driver.memory","30g") \
    .config("spark.yarn.executor.memoryOverhead","20G") \
    .config("spark.sql.broadcastTimeout", "3600")\
    .config("spark.driver.maxResultSize", "6g")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
    .config("hive.exec.dynamic.partition", True)\
    .config("hive.exec.max.dynamic.partitions",2048)\
    .config("hive.exec.max.dynamic.partitions.pernode",1000)\
    .config("spark.default.parallelism", 200)\
    .getOrCreate()
hc = HiveContext(spark_session.sparkContext)
hc.udf.register("mg_lead_source_id_mapping", mg_lead_source_id_mapping, StringType())
hc.udf.register("rw_lead_source_id_mapping", rw_lead_source_id_mapping, StringType())

'''
--- Exception ---
MG:
'经销商网销主动开拓_': '001007003000_tp',
'展厅主动开拓_NULL': '001007003000_tp',
'NULL_厂方的网销其他平台': '001007003000_tp',
'线索池渠道未知':'001010000000_tp'
'''
lead_df = hc.sql('''
    SELECT * FROM
    (
        SELECT 
            phone AS mobile,
            detail['behavior_time'] AS action_time,
            CASE 
                WHEN detail['brand_id'] = 121 AND (detail['first_resource_name'] IS NULL OR length(detail['first_resource_name']) = 0) 
                AND (detail['second_resource_name'] IS NULL OR length(detail['second_resource_name']) = 0) THEN '001010000000_tp'
                WHEN detail['brand_id'] = 121 AND (detail['first_resource_name'] IN ('经销商网销主动开拓','展厅主动开拓'))
                AND (detail['second_resource_name'] IS NULL OR length(detail['second_resource_name']) = 0) THEN '001007003000_tp' 
                WHEN detail['brand_id'] = 121 AND (detail['first_resource_name'] IS NULL OR length(detail['first_resource_name']) = 0)
                AND (detail['second_resource_name'] = '厂方的网销其他平台') THEN '001004003008_tp'
                WHEN detail['brand_id'] = 121 THEN mg_lead_source_id_mapping(CONCAT(CONCAT(detail['first_resource_name'],'_'), detail['second_resource_name']))

                WHEN detail['brand_id'] = 101 THEN rw_lead_source_id_mapping(CONCAT(CONCAT(detail['first_resource_name'],'_'), detail['second_resource_name']))
            END AS touchpoint_id,
            pt,
            CASE
                WHEN detail['brand_id'] = 121 THEN 'MG'
                WHEN detail['brand_id'] = 101 THEN 'RW'
            ELSE NULL END AS brand
        FROM cdp.cdm_cdp_customer_behavior_detail
        WHERE 
            TYPE ='leads'
      AND pt >= {0}
    ) t1
    WHERE
        mobile regexp '^[1][3-9][0-9]{{9}}$'
        AND action_time IS NOT NULL
        AND touchpoint_id IS NOT NULL
        AND brand IS NOT NULL
'''.format(pt))

# SAVE RESULT
lead_df.createOrReplaceTempView('lead_df')
hc.sql('insert overwrite table marketing_modeling.dw_ts_leads_i PARTITION (pt,brand) select * from lead_df')
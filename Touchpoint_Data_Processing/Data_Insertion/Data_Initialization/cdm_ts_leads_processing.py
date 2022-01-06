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

pt1 = sys.argv[1]
pt2 = sys.argv[2]


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
        return lead_source_dic[lead_source]
    except:
        return '001010000000_tp' # Unknown lead source

def rw_lead_source_id_mapping(lead_source):
	# 荣威的mapping逻辑
    lead_source_dic = {
        u'第一方触点_车展互动活动': '001003001001_rw',
        u'第一方触点_A级车展集客': '001003001002_rw',
        u'第一方触点_B级车展集客': '001003001003_rw',
        u'第一方触点_C级车展集客': '001003001004_rw',
        u'活动_A类活动': '001003001005_rw',
        u'活动_车展集客-大屏': '001003001006_rw',
        u'活动_车展集客-小蜜蜂': '001003001007_rw',
        u'活动_车展集客-顾问': '001003001008_rw',
        u'广告投放_澎湃投放': '001004001001_rw',
        u'广告投放_一点资讯': '001004001002_rw',
        u'广告投放_悠易互通投放': '001004001003_rw',
        u'广告投放_携程': '001004001004_rw',
        u'广告投放_ZAKER投放': '001004001005_rw',
        u'广告投放_第三方电商渠道': '001004001006_rw',
        u'广告投放_新浪': '001004001007_rw',
        u'广告投放_字节跳动效果通': '001004001008_rw',
        u'广告投放_知乎': '001004001009_rw',
        u'广告投放_快手效果通': '001004001010_rw',
        u'广告投放_搜狐投放': '001004001011_rw',
        u'广告投放_易车广告投放': '001004001012_rw',
        u'广告投放_汽车之家投放': '001004001013_rw',
        u'广告投放_飞猪投放': '001004001014_rw',
        u'广告投放_今日头条': '001004001015_rw',
        u'广告投放_广点通': '001004001016_rw',
        u'广告投放_微信朋友圈': '001004001017_rw',
        u'广告投放_百度广告投放': '001004001018_rw',
        u'广告投放_驾考宝典': '001004001019_rw',
        u'广告投放_太平洋汽车网': '001004001020_rw',
        u'广告投放_懂车帝广告投放': '001004001021_rw',
        u'广告投放_云和点动': '001004001022_rw',
        u'广告投放_易车效益达': '001004001023_rw',
        u'广告投放_爱卡汽车': '001004001024_rw',
        u'广告投放_OTT智能电视': '001004001025_rw',
        u'广告投放_网上车市': '001004001026_rw',
        u'广告投放_抖音': '001004001027_rw',
        u'广告投放_有数儿汽车': '001004001028_rw',
        u'广告投放_网易': '001004001029_rw',
        u'投放_效果通': '001004001030_rw',
        u'投放_抖音': '001004001031_rw',
        u'投放_今日头条': '001004001032_rw',
        u'投放_微信朋友圈': '001004001033_rw',
        u'投放_懂车帝广告投放': '001004001034_rw',
        u'投放_易车广告投放': '001004001035_rw',
        u'投放_第三方电商渠道': '001004001036_rw',
        u'投放_小红书': '001004001037_rw',
        u'投放_新浪': '001004001038_rw',
        u'投放_知乎': '001004001039_rw',
        u'投放_社交营销(试驾邀请)': '001004001040_rw',
        u'投放_腾讯新闻': '001004001041_rw',
        u'投放_太平洋汽车网': '001004001042_rw',
        u'投放_一点资讯': '001004001043_rw',
        u'投放_网易': '001004001044_rw',
        u'投放_爱卡汽车': '001004001045_rw',
        u'投放_车来了': '001004001046_rw',
        u'投放_天眼车': '001004001047_rw',
        u'投放_华为': '001004001048_rw',
        u'投放_汽车之家投放': '001004001049_rw',
        u'投放_新浪财经': '001004001050_rw',
        u'投放_广点通': '001004001051_rw',
        u'投放_百度广告投放': '001004001052_rw',
        u'投放_快手效果通': '001004001053_rw',
        u'投放_网上车市': '001004001054_rw',
        u'投放_58汽车': '001004001055_rw',
        u'投放_车轮': '001004001056_rw',
        u'投放_驾考宝典': '001004001057_rw',
        u'投放_携程': '001004001058_rw',
        u'投放_易车效益达': '001004001059_rw',
        u'投放_去哪儿投放': '001004001060_rw',
        u'投放_凇银泽': '001004001061_rw',
        u'投放_搜狐投放': '001004001062_rw',
        u'投放_虎扑投放': '001004001063_rw',
        u'投放_云和点动': '001004001064_rw',
        u'投放_有数儿汽车': '001004001065_rw',
        u'投放_妈妈帮': '001004001066_rw',
        u'投放_OTT智能电视': '001004001067_rw',
        u'投放_keep投放': '001004001068_rw',
        u'投放_团车网': '001004001069_rw',
        u'投放_元兵': '001004001070_rw',
        u'投放_ZAKER投放': '001004001071_rw',
        u'投放_澎湃投放': '001004001072_rw',
        u'投放_悠易互通投放': '001004001073_rw',
        u'投放_飞猪投放': '001004001074_rw',
        u'享道出行_ONE试乘试驾': '001004001075_rw',
        u'SIS接口分配_SIS其他': '001004002001_rw',
        u'SIS接口分配_SIS其它': '001004002002_rw',
        u'厂方自媒体_快手官号': '001004002003_rw',
        u'厂方自媒体_小红书官号': '001004002004_rw',
        u'厂方自媒体_哔哩哔哩官号': '001004002005_rw',
        u'第一方触点_小程序': '001004002006_rw',
        u'第一方触点_荣威官网试乘试驾': '001004002007_rw',
        u'第一方触点_官方APP试乘试驾留资': '001004002008_rw',
        u'第一方触点_百度小程序': '001004002009_rw',
        u'第一方触点_支付宝小程序': '001004002010_rw',
        u'第一方触点_今日头条小程序': '001004002011_rw',
        u'第一方触点_抖音小程序': '001004002012_rw',
        u'第一方触点_快手小程序': '001004002013_rw',
        u'第一方触点_快应用': '001004002014_rw',
        u'第一方触点_神兽助威': '001004002015_rw',
        u'荣威APP_荣威试乘试驾': '001004002016_rw',
        u'荣威APP_试乘试驾': '001004002017_rw',
        u'网站平台接口_网站': '001005001001_rw',
        u'网站平台接口_其他': '001005001002_rw',
        u'网站平台接口_搜狐': '001005001003_rw',
        u'网站平台接口_易车网': '001005001004_rw',
        u'网站平台接口_汽车之家': '001005001005_rw',
        u'网站平台接口_太平洋': '001005001006_rw',
        u'网站平台接口_爱卡汽车': '001005001007_rw',
        u'网站平台接口_懂车帝': '001005001008_rw',
        u'网站平台接口_17汽车网': '001005001009_rw',
        u'网站平台接口_途虎网': '001005001010_rw',
        u'网站平台接口_头条': '001005001011_rw',
        u'网站平台接口_腾讯': '001005001012_rw',
        u'网站平台接口_好车网': '001005001013_rw',
        u'网站平台接口_卡盟粒子': '001005001014_rw',
        u'网站平台接口_腾讯车讯达': '001005001015_rw',
        u'网站平台接口_车智推': '001005001016_rw',
        u'网站平台接口_车智推平台': '001005001017_rw',
        u'网站平台接口_腾讯卖车宝': '001005001018_rw',
        u'网站平台接口_云和互动': '001005001019_rw',
        u'网站平台接口_易车线索提升': '001005001020_rw',
        u'网站平台接口_腾讯一键五联': '001005001021_rw',
        u'活动_KBU活动集客-大屏': '001005002001_rw',
        u'活动_KBU活动集客-小蜜蜂': '001005002002_rw',
        u'活动_KBU活动集客-顾问': '001005002003_rw',
        u'经销商新媒体_抖音平台': '001005002004_rw',
        u'上报平台_直播异地线索': '001005002005_rw',
        u'上报平台_商圈店转卖': '001005002006_rw',
        u'上报平台_异地客户': '001005002007_rw',
        u'经销商网销主动开拓_QQ': '001005002008_rw',
        u'经销商网销主动开拓_论坛': '001005002009_rw',
        u'经销商网销主动开拓_基盘客户(网销)': '001005002010_rw',
        u'经销商网销主动开拓_合作资源1': '001005002011_rw',
        u'经销商网销主动开拓_合作资源2': '001005002012_rw',
        u'经销商网销主动开拓_线下活动-非凡驾道': '001005002013_rw',
        u'经销商网销主动开拓_官方分配': '001005002014_rw',
        u'经销商网销主动开拓_腾讯': '001005002015_rw',
        u'经销商网销主动开拓_天猫': '001005002016_rw',
        u'经销商网销主动开拓_新渠道': '001005002017_rw',
        u'经销商网销主动开拓_网销线上外拓': '001005002018_rw',
        u'经销商网销主动开拓_网销外展': '001005002019_rw',
        u'经销商网销主动开拓_百度有驾线下建卡': '001005002020_rw',
        u'经销商网销主动开拓_社交营销': '001005002021_rw',
        u'经销商网销主动开拓_合作资源3': '001005002022_rw',
        u'经销商网销主动开拓_其他网站': '001005002023_rw',
        u'经销商网销主动开拓_外拓外展': '001005002024_rw',
        u'经销商网销主动开拓_百度推广': '001005002025_rw',
        u'经销商网销主动开拓_搜狐': '001005002026_rw',
        u'经销商网销主动开拓_易车网': '001005002027_rw',
        u'经销商网销主动开拓_社交渠道': '001005002028_rw',
        u'经销商网销主动开拓_首次到店': '001005002029_rw',
        u'经销商网销主动开拓_外拓': '001005002030_rw',
        u'经销商网销主动开拓_百度': '001005002031_rw',
        u'经销商网销主动开拓_网销外拓': '001005002032_rw',
        u'经销商网销主动开拓_新浪微博': '001005002033_rw',
        u'经销商网销主动开拓_老客户转介绍': '001005002034_rw',
        u'经销商网销主动开拓_17汽车网': '001005002035_rw',
        u'经销商网销主动开拓_到店': '001005002036_rw',
        u'经销商网销主动开拓_搜狐汽车': '001005002037_rw',
        u'经销商网销主动开拓_爱卡汽车': '001005002038_rw',
        u'经销商网销主动开拓_爱卡汽车网': '001005002039_rw',
        u'经销商网销主动开拓_王媛媛': '001005002040_rw',
        u'经销商网销主动开拓_社交营销(试驾邀请)': '001005002041_rw',
        u'经销商网销主动开拓_DCC电话网络销售': '001005002042_rw',
        u'经销商网销主动开拓_懂车帝': '001005002043_rw',
        u'经销商网销主动开拓_汽车之家': '001005002044_rw',
        u'经销商网销主动开拓_汽车之家400电话': '001005002045_rw',
        u'经销商网销主动开拓_网站': '001005002046_rw',
        u'经销商网销主动开拓_自然进店': '001005002047_rw',
        u'经销商网销主动开拓_车展': '001005002048_rw',
        u'经销商网销主动开拓_其它': '001005002049_rw',
        u'经销商网销主动开拓_厂家小篷车': '001005002050_rw',
        u'经销商网销主动开拓_SIS': '001005002051_rw',
        u'经销商网销主动开拓_北仑大港汽车': '001005002052_rw',
        u'经销商网销主动开拓_新浪汽车网': '001005002053_rw',
        u'经销商网销主动开拓_太平洋汽车网': '001005002054_rw',
        u'经销商网销主动开拓_易车网400电话': '001005002055_rw',
        u'经销商网销主动开拓_王秀丽': '001005002056_rw',
        u'经销商网销主动开拓_2020海西车展': '001005002057_rw',
        u'经销商网销主动开拓_SIS其它': '001005002058_rw',
        u'经销商网销主动开拓_主动开拓': '001005002059_rw',
        u'经销商网销主动开拓_公众平台': '001005002060_rw',
        u'经销商网销主动开拓_淘宝天猫': '001005002061_rw',
        u'经销商网销主动开拓_驾校学员': '001005002062_rw',
        u'经销商网销主动开拓_720水灾': '001005002063_rw',
        u'经销商网销主动开拓_太平洋': '001005002064_rw',
        u'经销商网销主动开拓_其他': '001005002065_rw',
        u'经销商网销主动开拓_展厅': '001005002066_rw',
        u'经销商网销主动开拓_微信': '001005002067_rw',
        u'经销商网销主动开拓_今日头条': '001005002068_rw',
        u'经销商网销主动开拓_夏鑫': '001005002069_rw',
        u'经销商网销主动开拓_厂方分配': '001005002070_rw',
        u'经销商网销主动开拓_展厅接待客户': '001005002071_rw',
        u'经销商网销主动开拓_太平洋汽车': '001005002072_rw',
        u'经销商网销主动开拓_车主转介绍': '001005002073_rw',
        u'经销商网销主动开拓_231321': '001005002074_rw',
        u'经销商网销主动开拓_10.31-11.4国际车展': '001005002075_rw',
        u'经销商网销主动开拓_网上车市': '001005002076_rw',
        u'经销商网销主动开拓_公司周边外拓': '001005002077_rw',
        u'经销商网销主动开拓_车讯网': '001005002078_rw',
        u'经销商网销主动开拓_萧山车网': '001005002079_rw',
        u'经销商网销主动开拓_黄明': '001005002080_rw',
        u'经销商网销主动开拓_上海汽车': '001005002081_rw',
        u'经销商网销主动开拓_商超展示': '001005002082_rw',
        u'经销商网销主动开拓_好买车': '001005002083_rw',
        u'经销商网销主动开拓_车展客户': '001005002084_rw',
        u'展厅主动开拓_展厅活动': '001005002085_rw',
        u'展厅主动开拓_线下活动-非凡驾道': '001005002086_rw',
        u'展厅主动开拓_扫一扫集客活动': '001005002087_rw',
        u'展厅主动开拓_上海车展': '001005002088_rw',
        u'展厅主动开拓_底成本集客活动': '001005002089_rw',
        u'展厅主动开拓_二级网点': '001005002090_rw',
        u'展厅主动开拓_活动到店': '001005002091_rw',
        u'展厅主动开拓_社交营销': '001005002092_rw',
        u'展厅主动开拓_厂方支持大篷车及BC级车展': '001005002093_rw',
        u'展厅主动开拓_厂方支持小篷车及商圈活动': '001005002094_rw',
        u'展厅主动开拓_试乘试驾活动': '001005002095_rw',
        u'展厅主动开拓_车展': '001005002096_rw',
        u'展厅主动开拓_走商': '001005002097_rw',
        u'展厅主动开拓_直邮': '001005002098_rw',
        u'展厅主动开拓_外展': '001005002099_rw',
        u'展厅主动开拓_基盘客户': '001005002100_rw',
        u'展厅主动开拓_展厅线上外拓': '001005002101_rw',
        u'展厅主动开拓_展厅外展': '001005002102_rw',
        u'展厅主动开拓_基盘客户(网销)': '001005002103_rw',
        u'展厅主动开拓_抖音平台': '001005002104_rw',
        u'展厅主动开拓_社交渠道': '001005002105_rw',
        u'展厅主动开拓_低成本集客活动': '001005002106_rw',
        u'展厅主动开拓_网销线上外拓': '001005002107_rw',
        u'展厅主动开拓_合作资源1': '001005002108_rw',
        u'展厅主动开拓_QQ': '001005002109_rw',
        u'展厅主动开拓_新媒体线上外拓': '001005002110_rw',
        u'展厅主动开拓_网销外展': '001005002111_rw',
        u'展厅主动开拓_朋友圈广告': '001005002112_rw',
        u'展厅主动开拓_新渠道': '001005002113_rw',
        u'展厅主动开拓_汽车之家': '001005002114_rw',
        u'展厅自然客流(网销建卡)_QQ': '001005003001_rw',
        u'展厅自然客流(网销建卡)_论坛': '001005003002_rw',
        u'展厅自然客流(网销建卡)_合作资源1': '001005003003_rw',
        u'展厅自然客流(网销建卡)_合作资源2': '001005003004_rw',
        u'展厅自然客流(网销建卡)_网站': '001005003005_rw',
        u'展厅自然客流_来电': '001005003006_rw',
        u'展厅自然客流_来店': '001005003007_rw',
        u'第一方触点_荣威官网小额定': '001006001001_rw',
        u'第一方触点_名爵官网小额定': '001006001002_rw',
        u'第一方触点_小额定单未支付': '001006001003_rw',
        u'荣威APP_荣威小额定': '001006001004_rw',
        u'荣威APP_定金定单未支付': '001006001005_rw',
        u'荣威APP_定金定单': '001006001006_rw',
        u'三方电商_Tmall渠道': '001006001007_rw',
        u'三方电商_苏宁渠道': '001006001008_rw',
        u'三方电商_拼多多渠道': '001006001009_rw',
        u'三方电商_Tmall试驾': '001006001010_rw',
        u'三方电商_苏宁试驾': '001006001011_rw',
        u'网站平台接口_电商': '001006001012_rw',
        u'厂方二手车平台_荣威车主俱乐部二手车': '001006002001_rw',
        u'厂方二手车平台_荣威官网PC端二手车': '001006002002_rw',
        u'厂方二手车平台_荣威官网移动端二手车': '001006002003_rw',
        u'厂方二手车平台_荣威APP二手车': '001006002004_rw',
        u'厂方二手车平台_手动创建二手车': '001006002005_rw',
        u'大数据平台_增换购': '001006002006_rw',
        u'大数据平台_社交裂变': '001006002007_rw',
        u'第一方触点_裂变': '001006002008_rw',
        u'第一方触点_荣威APP': '001006002009_rw',
        u'第一方触点_总部短信营销': '001006002010_rw',
        u'第一方触点_益普索益起答': '001006002011_rw',
        u'第一方触点_荣威H5活动': '001006002012_rw',
        u'官方认证二手车_二手车小程序': '001006002013_rw',
        u'官方认证二手车_二手车官网': '001006002014_rw',
        u'官方认证二手车_经销商线下创建二手车': '001006002015_rw',
        u'集团内部_上汽车有惠': '001006002016_rw',
        u'大数据平台_金线索': '001006003001_rw',
        u'大数据平台_冷线索': '001006003002_rw',
        u'大数据平台_精准线索': '001006003003_rw',
        u'智能调配_精准线索': '001006003003_rw',
        u'智能调配_ 精准线索': '001006003003_rw'
    }
    try:
        return lead_source_dic[lead_source]
    except:
        return '001012000000_rw' # other crosschannel lead source


spark_session = SparkSession.builder.enableHiveSupport().appName("attribution_data_processing") \
    .config("spark.driver.memory","30g") \
    .config("spark.yarn.executor.memoryOverhead","20G") \
    .config("spark.sql.broadcastTimeout", "3600")\
    .config("spark.driver.maxResultSize", "6g")\
    .config("hive.exec.dynamic.partition.mode", "nonstrict")\
    .config("hive.exec.dynamic.partition", True)\
    .config("hive.exec.max.dynamic.partitions",2048)\
    .config("hive.exec.max.dynamic.partitions.pernode",1000)\
    .config("spark.default.parallelism", 200) \
    .config("mapreduce.input.fileinputformat.input.dir.recursive", "true") \
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
                when detail['brand_id'] = 121 AND detail['businesstypecode'] = '10000000' 
                    then (case when detail['deale_id'] = '220000000398438' and detail['dealer_code'] = 'SQ666B' then '001003000000_tp' else '001002000000_tp' end)
                WHEN detail['brand_id'] = 121 AND (detail['firstresourcename'] IS NULL OR length(detail['firstresourcename']) = 0) 
                AND (detail['second_resource_name'] IS NULL OR length(detail['secondresourcename']) = 0) THEN '001010000000_tp'
                WHEN detail['brand_id'] = 121 AND (detail['firstresourcename'] IN ('经销商网销主动开拓','展厅主动开拓'))
                AND (detail['secondresourcename'] IS NULL OR length(detail['second_resource_name']) = 0) THEN '001007003000_tp' 
                WHEN detail['brand_id'] = 121 AND (detail['firstresourcename'] IS NULL OR length(detail['firstresourcename']) = 0)
                AND (detail['secondresourcename'] = '厂方的网销其他平台') THEN '001004003008_tp'
                WHEN detail['brand_id'] = 121 THEN mg_lead_source_id_mapping(CONCAT(CONCAT(detail['firstresourcename'],'_'), detail['secondresourcename']))
                WHEN detail['brand_id'] = 101 THEN rw_lead_source_id_mapping(CONCAT(CONCAT(detail['firstresourcename'],'_'), detail['secondresourcename']))
            END AS touchpoint_id,
            CASE
                WHEN detail['brand_id'] = 121 THEN 'MG'
                WHEN detail['brand_id'] = 101 THEN 'RW'
            ELSE NULL END AS brand,
            pt
        FROM cdp.cdm_cdp_customer_behavior_detail
        WHERE 
            TYPE ='leads_pool'
            AND pt >= {0} AND pt <= {1}
    ) t1
    WHERE
        mobile regexp '^[1][3-9][0-9]{{9}}$'
        AND action_time IS NOT NULL
        AND touchpoint_id IS NOT NULL
        AND brand IS NOT NULL
'''.format(pt1, pt2))

# SAVE RESULT
lead_df.createOrReplaceTempView('lead_df')
hc.sql('insert overwrite table marketing_modeling.cdm_ts_leads_i PARTITION (pt,brand) select * from lead_df')
#!/bin/bash
#/*********************************************************************
#*模块: /Config
#*程序: cdm_adhoc_app_activity.sh
#*功能: 更新hive中的线索来源mapping表
#*开发人: Xiaofeng XU
#*开发日: 2021-09-12
#*修改记录:
#
#*********************************************************************/
cd $(dirname $(readlink -f $0))
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`

hive --hivevar queuename=${queuename} -e "
set tez.queue.name=${queuename};
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.cdm_adhoc_app_activity;
CREATE  TABLE IF NOT EXISTS marketing_modeling.cdm_adhoc_app_activity (
    ccm_points_description string,
    activity_name string ,
    activity_type string
)
row format delimited
fields terminated by ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/marketing_modeling.db/cdm_adhoc_app_activity'
"
## 拷贝维表csv，生成临时csv
#cp cdm_adhoc_app_activity.csv cdm_adhoc_app_activity_tmp.csv
## 删除临时csv第一行
#sed -i '1d' cdm_adhoc_app_activity_tmp.csv
## 删除hdfs之前的维表
#hadoop fs -rm '/tmp/cdm_adhoc_app_activity_tmp.csv'
## 将最新维表放入hdfs
#hadoop fs -put cdm_adhoc_app_activity_tmp.csv /tmp
## 载入数据
#hive -e "load data inpath '/tmp/cdm_adhoc_app_activity_tmp.csv' overwrite into table marketing_modeling.cdm_adhoc_app_activity"
## 删除临时csv
#rm -rf cdm_lead_source_mapping_tmp.csv


# 拷贝维表csv，生成临时csv
#cp cdm_adhoc_app_activity.csv cdm_adhoc_app_activity_tmp.csv
# 删除临时csv第一行
#sed -i '1d' cdm_adhoc_app_activity.csv
# 删除hdfs之前的维表
hadoop fs -rm '/tmp/cdm_adhoc_app_activity.csv'
# 将最新维表放入hdfs
hadoop fs -put cdm_adhoc_app_activity.csv /tmp
# 载入数据
hive -e "load data inpath '/tmp/cdm_adhoc_app_activity.csv' overwrite into table marketing_modeling.cdm_adhoc_app_activity"
# 删除临时csv
#rm -rf cdm_adhoc_app_activity.csv

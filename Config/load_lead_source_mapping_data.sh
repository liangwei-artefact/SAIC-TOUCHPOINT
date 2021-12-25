#!/bin/bash
#/*********************************************************************
#*模块: /Config
#*程序: tp_info_init.sh
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

DROP TABLE IF EXISTS marketing_modeling.cdm_lead_source_mapping;
CREATE TABLE IF NOT EXISTS marketing_modeling.cdm_lead_source_mapping (
    sec_level_sour string COMMENT '触点二级行为',
    third_level_sour string COMMENT '触点三级行为',
    sour_detail string COMMENT '触点行为细节',
    fir_lead_source string COMMENT '一级线索来源',
    sec_lead_source string COMMENT '二级线索来源', 
    touchpoint_id string COMMENT '触点ID',
    brand string COMMENT '品牌，MG或RW'
)
row format delimited
fields terminated by '|'
STORED AS TEXTFILE
-- LOCATION '/warehouse/tablespace/managed/hive/marketing_modeling.db/cdm/cdm_lead_source_mapping'
"
# 拷贝维表csv，生成临时csv
#cp cdm_lead_source_mapping.csv cdm_lead_source_mapping_tmp.csv
# 删除临时csv第一行
#sed -i '1d' cdm_lead_source_mapping_tmp.csv
# 删除hdfs之前的维表
hadoop fs -rm '/tmp/cdm_lead_source_mapping.csv'
# 将最新维表放入hdfs
hadoop fs -put cdm_lead_source_mapping.csv /tmp
# 载入数据
hive -e "load data inpath '/tmp/cdm_lead_source_mapping.csv' overwrite into table marketing_modeling.cdm_lead_source_mapping"
# 删除临时csv
#rm -rf cdm_lead_source_mapping_tmp.csv

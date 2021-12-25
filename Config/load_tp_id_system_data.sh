#!/bin/bash
#/*********************************************************************
#*模块: /Config
#*程序: load_tp_id_system_data.sh
#*功能: 更新hive中的触点码表
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

DROP TABLE IF EXISTS marketing_modeling.cdm_touchpoints_id_system;
CREATE TABLE IF NOT EXISTS marketing_modeling.cdm_touchpoints_id_system (
    touchpoint_id string COMMENT '触点ID，取值为12位数字加后缀，12位数字每4位代表一级触点，MG后缀为_tp，RW后缀为_rw',
    touchpoint_name string COMMENT '触点名称',
    touchpoint_level string COMMENT '触点层级，取值为1，2，3，4',
    level_1_tp_id string COMMENT '该触点对应的一级触点名称',
    level_2_tp_id string COMMENT '该触点对应的二级触点名称', 
    level_3_tp_id string COMMENT '该触点对应的三级触点名称',
    level_4_tp_id string COMMENT '该触点对应的四级触点名称',
    brand string COMMENT '品牌，MG或RW'
)
row format delimited
fields terminated by '|'
STORED AS TEXTFILE
-- LOCATION '/warehouse/tablespace/managed/hive/marketing_modeling.db/cdm/cdm_touchpoints_id_system'
"
# 拷贝维表csv，生成临时csv
#cp cdm_touchpoints_id_system.csv cdm_touchpoints_id_system_tmp.csv
# 删除临时csv第一行
#sed -i '1d' cdm_touchpoints_id_system_tmp.csv
# 删除hdfs之前的维表
hadoop fs -rm '/tmp/cdm_touchpoints_id_system.csv'
# 将最新维表放入hdfs
hadoop fs -put cdm_touchpoints_id_system.csv /tmp
# 载入数据
hive -e "load data inpath '/tmp/cdm_touchpoints_id_system.csv' into table marketing_modeling.cdm_touchpoints_id_system"
# 删除临时csv
#rm -rf cdm_touchpoints_id_system_tmp.csv

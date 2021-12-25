#!/bin/bash
#/*********************************************************************
#*程序名  : cdm_tp_ts_all_i_creation.sh
#* 建立中间大表
#*********************************************************************/


hive -e "
DROP TABLE IF EXISTS marketing_modeling.cdm_mg_tp_ts_all_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_mg_tp_ts_all_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码',
brand STRING COMMENT 'MG/RW'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  source string COMMENT 'touchpoint data source'
)
STORED AS PARQUET;

DROP TABLE IF EXISTS marketing_modeling.cdm_rw_tp_ts_all_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_rw_tp_ts_all_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码',
brand STRING COMMENT 'MG/RW'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  source string COMMENT 'touchpoint data source'
)
STORED AS PARQUET;
"
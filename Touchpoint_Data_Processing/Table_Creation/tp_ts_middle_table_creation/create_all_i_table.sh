#!/bin/bash
#/*********************************************************************
#*程序名  : create_all_table.sh
#* 建立全部脚本
#*********************************************************************/

hive  --hivevar queue_name=${queue_name} -e"
set tez.queue.name=${queue_name};
-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_completed_oppor_fail_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_completed_oppor_fail_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS ORC;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_oppor_fail_activation_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_oppor_fail_activation_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS ORC;


-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_activity_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_activity_i (
mobile STRING COMMENT '电话号码',
activity_name STRING COMMENT '活动名字',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_adhoc_app_activity_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_adhoc_app_activity_i (
mobile STRING COMMENT '电话号码',
activity_name STRING COMMENT '活动名字',
activity_type STRING COMMENT '活动类型',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_ai_call_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_ai_call_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_app_activity_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_app_activity_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_bind_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_bind_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_ccm_activity_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码',
description STRING COMMENT '积分行为具体描述'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;



-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_community_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_community_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;


-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_dlm_call_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_dlm_call_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_followup_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_followup_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;


DROP TABLE IF EXISTS marketing_modeling.cdm_ts_leads_i;
CREATE  TABLE IF NOT EXISTS marketing_modeling.cdm_ts_leads_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;



-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_maintenance_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_maintenance_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_online_action_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_online_action_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_online_activity_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_online_activity_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_oppor_fail_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_oppor_fail_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_order_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_order_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_register_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_register_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;


-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_scrm_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_scrm_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;


-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_sis_call_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_sis_call_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;


--- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_sms_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_sms_i (
mobile STRING COMMENT '电话号码',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码',
sms_type STRING COMMENT '短信类型',
sms_name STRING COMMENT '短信名字',
action_time TIMESTAMP COMMENT '行为发生时间'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_trial_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_trial_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;

-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_wechat_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_wechat_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;


-- DROP TABLE IF EXISTS marketing_modeling.cdm_ts_app_activity_i;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_ts_app_activity_i (
mobile STRING COMMENT '电话号码',
action_time TIMESTAMP COMMENT '行为发生时间',
touchpoint_id STRING COMMENT '触点编号，每三位代表一级触点，四级共12位，XXX(001)XXX(001)XXX(001)XXX(001)，从1开始编码'
)
PARTITIONED BY (
  pt string COMMENT 'date used by partition, format: yyyymmdd',
  brand STRING COMMENT 'MG/RW'
)
STORED AS PARQUET;
"


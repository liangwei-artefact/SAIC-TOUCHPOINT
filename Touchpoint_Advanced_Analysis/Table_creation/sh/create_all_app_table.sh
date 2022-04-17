#!/bin/bash

hive -e "

DROP TABLE IF EXISTS marketing_modeling.app_fir_contact_conversion_report_weekly_a;
CREATE  TABLE IF NOT EXISTS marketing_modeling.app_fir_contact_conversion_report_weekly_a (
  mac_code STRING COMMENT '大区代码',
  rfs_code STRING COMMENT '小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  fir_contact_tp_id STRING COMMENT '线索来源触点ID',
  fir_contact_series STRING COMMENT '首触车系',
  fir_contact_week STRING COMMENT '首触所属周',
  cust_vol BIGINT COMMENT '总人数，按电话号码去重',
  instore_vol BIGINT COMMENT '到店人数，按电话号码去重',
  trial_vol BIGINT COMMENT '试驾人数，按电话号码去重',
  consume_vol BIGINT COMMENT '订单人数(含交现车)，按电话号码去重',
  deliver_vol BIGINT COMMENT '交车人数，按电话号码去重',
  brand STRING COMMENT '品牌'
) PARTITIONED BY (pt STRING COMMENT '分区键，yyyyww 格式的日期') STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_fir_contact_conversion_report_weekly_a'
"

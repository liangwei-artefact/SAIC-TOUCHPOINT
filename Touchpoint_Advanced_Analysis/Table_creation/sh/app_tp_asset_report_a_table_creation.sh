#!/bin/bash
cd $(dirname $(readlink -f $0))
hive -e "
DROP TABLE IF EXISTS marketing_modeling.app_tp_asset_report_a;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_tp_asset_report_a (
	  touchpoint_level INT COMMENT '触点等级',
	  touchpoint_id STRING COMMENT '触点码值',
	  fir_contact_month STRING COMMENT '首触月份',
	  fir_contact_tp_id STRING COMMENT '线索来源触点',
	  fir_contact_series STRING COMMENT '首触车系',
	  mac_code STRING COMMENT '大区代码',
	  rfs_code STRING COMMENT '小区代码',
	  area STRING COMMENT '省份',
	  tp_pv BIGINT COMMENT '触点覆盖人次',
	  tp_uv BIGINT COMMENT '触点覆盖独立用户数',
	  instore_vol BIGINT COMMENT '到店人数',
	  trial_vol BIGINT COMMENT '试驾人数',
	  consume_vol BIGINT COMMENT '成交人数（含订单和交车）',
	  cust_vol BIGINT COMMENT '首触用户数',
	  exit_pv BIGINT COMMENT '未成交用户中的停止人数，即未成交用户且最后一个触点是该触点的人数',
	  undeal_vol BIGINT COMMENT '未成交用户人数',
	  tp_coverage DOUBLE COMMENT '触点覆盖度',
	  tp_avg_times DOUBLE COMMENT '触点平均互动次数',
	  exit_rate DOUBLE COMMENT '触点停止率',
	  tp_instore_rate DOUBLE COMMENT '触点到店转化率',
	  tp_trial_rate DOUBLE COMMENT '触点试驾转化率',
	  tp_deal_rate DOUBLE COMMENT '触点成交转化率'
) PARTITIONED BY (
  pt STRING COMMENT '分区键，yyyymmdd格式的日期，数据生成日期',
  brand STRING COMMENT '分区键，品牌，MG/RW') STORED AS ORC
  ;"

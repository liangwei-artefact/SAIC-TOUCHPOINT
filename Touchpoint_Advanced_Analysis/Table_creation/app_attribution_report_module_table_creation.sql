DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_mk_attribution_report;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_mk_attribution_report (
  touchpoint_id STRING COMMENT '触点id',
  touchpoint_name STRING COMMENT '触点名称',
  attribution STRING COMMENT '马尔可夫贡献度结果',
  brand STRING COMMENT '品牌, MG or RW'
) PARTITIONED BY (
  attribution_tp STRING COMMENT '归因触点',
  pt STRING COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限'
) STORED AS ORC;


DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_ml_attribution_report;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_ml_attribution_report (
  touchpoint_id STRING COMMENT '触点id',
  touchpoint_name STRING COMMENT '触点名称',
  attribution STRING COMMENT '机器学习贡献度',
  brand STRING COMMENT '品牌, MG or RW'
) PARTITIONED BY (
  attribution_tp STRING COMMENT '归因触点',
  pt STRING COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限'
) STORED AS ORC;


DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_ml_attribution_report;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_ml_attribution_report (
  touchpoint_id STRING COMMENT '触点id',
  touchpoint_name STRING COMMENT '触点名称',
  attribution STRING COMMENT '机器学习贡献度',
  brand STRING COMMENT '品牌, MG or RW'
) PARTITIONED BY (
  attribution_tp STRING COMMENT '归因触点',
  pt STRING COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限'
) STORED AS ORC;


DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_attribution_report;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_attribution_report (
    touchpoint_id STRING COMMENT '触点id',
    touchpoint_name STRING COMMENT '触点名称',
    instore_mk_attribution STRING,
    trial_mk_attribution STRING,
    deal_mk_attribution STRING,
    instore_ml_attribution STRING,
    trial_ml_attribution STRING,
    deal_ml_attribution STRING
) PARTITIONED BY (
    pt STRING COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限',
    brand STRING
) STORED AS ORC;


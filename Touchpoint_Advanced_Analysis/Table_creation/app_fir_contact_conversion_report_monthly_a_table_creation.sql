DROP TABLE IF EXISTS marketing_modeling.app_fir_contact_conversion_report_monthly_a;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_fir_contact_conversion_report_monthly_a (
  mac_code STRING COMMENT '大区代码',
  rfs_code STRING COMMENT '小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  fir_contact_tp_id STRING COMMENT '线索来源触点ID',
  fir_contact_series STRING COMMENT '首触车系',
  fir_contact_month STRING COMMENT '首触所属月份',
  cust_vol BIGINT COMMENT '总人数，按电话号码去重',
  instore_vol BIGINT COMMENT '到店人数，按电话号码去重',
  trial_vol BIGINT COMMENT '试驾人数，按电话号码去重',
  consume_vol BIGINT COMMENT '订单人数(含交现车)，按电话号码去重',
  deliver_vol BIGINT COMMENT '交车人数，按电话号码去重',
  brand STRING COMMENT '品牌'
) PARTITIONED BY (pt STRING COMMENT '分区键，yyyymm 格式的日期') STORED AS ORC;
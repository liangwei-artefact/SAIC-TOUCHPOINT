DROP TABLE IF EXISTS marketing_modeling.cdm_customer_touchpoints_profile_a;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_customer_touchpoints_profile_a (
  mobile STRING COMMENT '电话号码',
  last_fir_contact_date_brand STRING COMMENT '首触时间',
  mac_code STRING COMMENT '首触大区代码',
  rfs_code STRING COMMENT '首触小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  touchpoint_id STRING COMMENT '触点id',
  fir_contact_fir_sour_brand STRING COMMENT '一级线索来源',
  fir_contact_sec_sour_brand STRING COMMENT '二级线索来源',
  fir_contact_series_brand STRING COMMENT '首触车系',
  brand STRING COMMENT '品牌, MG or RW'
) PARTITIONED BY (
  pt STRING COMMENT 'date used by partition, format: yyyymmdd'
) STORED AS ORC;
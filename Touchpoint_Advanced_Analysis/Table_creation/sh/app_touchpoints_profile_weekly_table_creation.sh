#!/bin/bash
cd $(dirname $(readlink -f $0))
hive -e "
DROP TABLE IF EXISTS marketing_modeling.app_touchpoints_profile_weekly;
CREATE TABLE IF NOT EXISTS marketing_modeling.app_touchpoints_profile_weekly (
  mobile STRING COMMENT '电话号码',
  fir_contact_week STRING COMMENT '首触周',
  fir_contact_date STRING COMMENT '首触时间',
  fir_contact_series STRING COMMENT '首触车系',
  mac_code STRING COMMENT '首触大区',
  rfs_code STRING COMMENT '首触小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  fir_contact_tp_id STRING COMMENT '首触触点id',
  brand STRING COMMENT '品牌'
) PARTITIONED BY (
  pt STRING COMMENT 'week used by partition, format: yyyyww'
) STORED AS ORC;
"
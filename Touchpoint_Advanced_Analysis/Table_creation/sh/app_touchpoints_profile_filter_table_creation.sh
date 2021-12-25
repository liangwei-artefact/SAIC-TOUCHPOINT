#!/bin/bash
cd $(dirname $(readlink -f $0))
hive -e "
DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_dim_area;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_area (
  area STRING COMMENT '省份',
  brand STRING COMMENT '品牌, MG or RW'
) STORED AS ORC;

DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_dim_activity_name;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_activity_name (
  activity_name STRING COMMENT '活动名称',
  brand STRING COMMENT '品牌, MG or RW'
) STORED AS ORC;


DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_dim_car_series;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_car_series (
  fir_contact_series STRING COMMENT '首触车系',
  brand STRING COMMENT '品牌, MG or RW'
) STORED AS ORC;


DROP EXTERNAL TABLE IF EXISTS marketing_modeling.app_dim_tree_big_small_area;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_tree_big_small_area (
  rfs_code STRING COMMENT '小区代码',
  mac_code STRING COMMENT '大区代码',
  rfs_name STRING COMMENT '小区中文名',
  mac_name STRING COMMENT '大区中文名',
  brand STRING COMMENT '品牌 RW,MG'
) STORED AS ORC;
"
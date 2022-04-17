#!/bin/bash
hive -e "
DROP TABLE IF EXISTS marketing_modeling.app_undeal_report_a_t;
create EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_undeal_report_a_t (
mobile string,
fir_contact_month STRING,
fir_contact_tp_id STRING,
tp_id string,
fir_contact_series STRING,
mac_code STRING,
rfs_code STRING,
area STRING,
undeal_vol int,
touchpoint_level string,
level_1_tp_id string,
pt STRING COMMENT '分区键，yyyymmdd格式的日期，数据生成日期',
brand STRING COMMENT '分区键，品牌，MG/RW'
)
STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_undeal_report_a_t';



"


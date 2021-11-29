USE mdasdb

DROP TABLE IF EXISTS cdm_customer_touchpoints_profile_a;
CREATE EXTERNAL TABLE IF NOT EXISTS cdm_customer_touchpoints_profile_a (
  `mobile` varchar(255) DEFAULT '' COMMENT '电话号码',
  `last_fir_contact_date_brand` varchar(255) DEFAULT '' COMMENT '首触时间',
  `mac_code` varchar(255) DEFAULT '' COMMENT '首触大区代码',
  `rfs_code` varchar(255) DEFAULT '' COMMENT '首触小区代码',
  `area` varchar(255) DEFAULT '' COMMENT '首触省份',
  `is_sec_net` varchar(255) DEFAULT '' COMMENT '是否二网 1:是 0：否',
  `activity_name` varchar(255) DEFAULT '' COMMENT '首触活动名称',
  `touchpoint_id` varchar(255) DEFAULT '' COMMENT '触点id',
  `fir_contact_fir_sour_brand` varchar(255) DEFAULT '' COMMENT '一级线索来源',
  `fir_contact_sec_sour_brand` varchar(255) DEFAULT '' COMMENT '二级线索来源',
  `fir_contact_series_brand` varchar(255) DEFAULT '' COMMENT '首触车系',
  `brand` varchar(255) DEFAULT '' COMMENT '品牌, MG or RW',
  `pt` varchar(255) DEFAULT '' COMMENT 'date used by partition, format: yyyymmdd'
)


DROP TABLE IF EXISTS app_touchpoints_profile_monthly;
CREATE TABLE IF NOT EXISTS app_touchpoints_profile_monthly (
  `mobile` varchar(255) DEFAULT '' COMMENT '电话号码',
  `fir_contact_month` varchar(255) DEFAULT '' COMMENT '首触月份',
  `fir_contact_date` varchar(255) DEFAULT '' COMMENT '首触时间',
  `fir_contact_series` varchar(255) DEFAULT '' COMMENT '首触车系',
  `mac_code` varchar(255) DEFAULT '' COMMENT '首触大区',
  `rfs_code` varchar(255) DEFAULT '' COMMENT '首触小区代码',
  `area` varchar(255) DEFAULT '' COMMENT '首触省份',
  `is_sec_net` varchar(255) DEFAULT '' COMMENT '是否二网 1:是 0：否',
  `activity_name` varchar(255) DEFAULT '' COMMENT '首触活动名称',
  `fir_contact_tp_id` varchar(255) DEFAULT '' COMMENT '首触触点id',
  `brand` varchar(255) DEFAULT '' COMMENT '品牌',
  `pt` varchar(255) DEFAULT '' COMMENT 'month used by partition, format: yyyymm'
)


DROP TABLE IF EXISTS app_touchpoints_profile_weekly;
CREATE TABLE IF NOT EXISTS app_touchpoints_profile_weekly (
  `mobile` varchar(255) DEFAULT '' COMMENT '电话号码',
  `fir_contact_week` varchar(255) DEFAULT '' COMMENT '首触周',
  `fir_contact_date` varchar(255) DEFAULT '' COMMENT '首触时间',
  `fir_contact_series` varchar(255) DEFAULT '' COMMENT '首触车系',
  `mac_code` varchar(255) DEFAULT '' COMMENT '首触大区',
  `rfs_code` varchar(255) DEFAULT '' COMMENT '首触小区代码',
  `area` varchar(255) DEFAULT '' COMMENT '首触省份',
  `is_sec_net` varchar(255) DEFAULT '' COMMENT '是否二网 1:是 0：否',
  `activity_name` varchar(255) DEFAULT '' COMMENT '首触活动名称',
  `fir_contact_tp_id` varchar(255) DEFAULT '' COMMENT '首触触点id',
  `brand` varchar(255) DEFAULT '' COMMENT '品牌',
  `pt` varchar(255) DEFAULT '' COMMENT 'week used by partition, format: yyyyww'
)


DROP TABLE IF EXISTS app_fir_contact_conversion_report_monthly_a;
CREATE EXTERNAL TABLE IF NOT EXISTS app_fir_contact_conversion_report_monthly_a (
  `mac_code` varchar(255) DEFAULT '' COMMENT '大区代码',
  `rfs_code` varchar(255) DEFAULT '' COMMENT '小区代码',
  `area` varchar(255) DEFAULT '' COMMENT '首触省份',
  `is_sec_net` varchar(255) DEFAULT '' COMMENT '是否二网 1:是 0：否',
  `activity_name` varchar(255) DEFAULT '' COMMENT '首触活动名称',
  `fir_contact_tp_id` varchar(255) DEFAULT '' COMMENT '线索来源触点ID',
  `fir_contact_series` varchar(255) DEFAULT '' COMMENT '首触车系',
  `fir_contact_month` varchar(255) DEFAULT '' COMMENT '首触所属月份',
  `cust_vol` bigint(20) COMMENT '总人数，按电话号码去重',
  `instore_vol` bigint(20) COMMENT '到店人数，按电话号码去重',
  `trial_vol` bigint(20) COMMENT '试驾人数，按电话号码去重',
  `consume_vol` bigint(20) COMMENT '订单人数(含交现车)，按电话号码去重',
  `deliver_vol` bigint(20) COMMENT '交车人数，按电话号码去重',
  `instore_rate` decimal(10, 4) COMMENT '到店率',
  `trial_rate` decimal(10, 4) COMMENT '试驾率',
  `consume_vs_instore_rate` decimal(10, 4) COMMENT '到店转化率',
  `consume_rate` decimal(10, 4) COMMENT '综合转化率',
  `consume_vs_deliver_rate` decimal(10, 4) COMMENT '订交比',
  `deliver_rate` decimal(10, 4) COMMENT '综合交车率',
  `brand` varchar(255) DEFAULT '' COMMENT '品牌',
  `pt` varchar(255) DEFAULT '' COMMENT '分区键，yyyymm 格式的日期'
)


DROP TABLE IF EXISTS app_fir_contact_conversion_report_weekly_a;
CREATE EXTERNAL TABLE IF NOT EXISTS app_fir_contact_conversion_report_weekly_a (
  `mac_code` varchar(255) DEFAULT '' COMMENT '大区代码',
  `rfs_code` varchar(255) DEFAULT '' COMMENT '小区代码',
  `area` varchar(255) DEFAULT '' COMMENT '首触省份',
  `is_sec_net` varchar(255) DEFAULT '' COMMENT '是否二网 1:是 0：否',
  `activity_name` varchar(255) DEFAULT '' COMMENT '首触活动名称',
  `fir_contact_tp_id` varchar(255) DEFAULT '' COMMENT '线索来源触点ID',
  `fir_contact_series` varchar(255) DEFAULT '' COMMENT '首触车系',
  `fir_contact_week` varchar(255) DEFAULT '' COMMENT '首触所属周',
  `cust_vol` bigint(20) COMMENT '总人数，按电话号码去重',
  `instore_vol` bigint(20) COMMENT '到店人数，按电话号码去重',
  `trial_vol` bigint(20) COMMENT '试驾人数，按电话号码去重',
  `consume_vol` bigint(20) COMMENT '订单人数(含交现车)，按电话号码去重',
  `deliver_vol` bigint(20) COMMENT '交车人数，按电话号码去重',
  `instore_rate` decimal(10, 4) COMMENT '到店率',
  `trial_rate` decimal(10, 4) COMMENT '试驾率',
  `consume_vs_instore_rate` decimal(10, 4) COMMENT '到店转化率',
  `consume_rate` decimal(10, 4) COMMENT '综合转化率',
  `consume_vs_deliver_rate` decimal(10, 4) COMMENT '订交比',
  `deliver_rate` decimal(10, 4) COMMENT '综合交车率',
  `brand` varchar(255) DEFAULT '' COMMENT '品牌',
  `pt` varchar(255) DEFAULT '' COMMENT '分区键，yyyyww 格式的日期'
)


DROP TABLE IF EXISTS app_tp_asset_report_a;
CREATE EXTERNAL TABLE IF NOT EXISTS app_tp_asset_report_a (
  `touchpoint_level` bigint(20) COMMENT '触点等级',
  `touchpoint_id` varchar(255) DEFAULT '' COMMENT '触点码值',
  `fir_contact_month` varchar(255) DEFAULT '' COMMENT '首触月份',
  `fir_contact_tp_id` varchar(255) DEFAULT '' COMMENT '线索来源触点',
  `fir_contact_series` varchar(255) DEFAULT '' COMMENT '首触车系',
  `mac_code` varchar(255) DEFAULT '' COMMENT '大区代码',
  `rfs_code` varchar(255) DEFAULT '' COMMENT '小区代码',
  `area` varchar(255) DEFAULT '' COMMENT '省份',
  `tp_pv` bigint(20) COMMENT '触点覆盖人次',
  `tp_uv` bigint(20) COMMENT '触点覆盖独立用户数',
  `instore_vol` bigint(20) COMMENT '到店人数',
  `trial_vol` bigint(20) COMMENT '试驾人数',
  `consume_vol` bigint(20) COMMENT '成交人数（含订单和交车）',
  `cust_vol` bigint(20) COMMENT '首触用户数',
  `exit_pv` bigint(20) COMMENT '未成交用户中的停止人数，即未成交用户且最后一个触点是该触点的人数',
  `undeal_vol` bigint(20) COMMENT '未成交用户人数',
  `tp_coverage` decimal(10, 4) COMMENT '触点覆盖度',
  `tp_avg_times` decimal(10, 4) COMMENT '触点平均互动次数',
  `exit_rate` decimal(10, 4) COMMENT '触点停止率',
  `tp_instore_rate` decimal(10, 4) COMMENT '触点到店转化率',
  `tp_trial_rate` decimal(10, 4) COMMENT '触点试驾转化率',
  `tp_deal_rate` decimal(10, 4) COMMENT '触点成交转化率',
  `pt` varchar(255) DEFAULT '' COMMENT '分区键，yyyymmdd格式的日期，数据生成日期',
  `brand` varchar(255) DEFAULT '' COMMENT '分区键，品牌，MG/RW'
)


DROP EXTERNAL TABLE IF EXISTS app_mk_attribution_report;
CREATE EXTERNAL TABLE IF NOT EXISTS app_mk_attribution_report (
  `touchpoint_id` varchar(255) DEFAULT '' COMMENT '触点id',
  `touchpoint_name` varchar(255) DEFAULT '' COMMENT '触点名称',
  `attribution` varchar(255) DEFAULT '' COMMENT '马尔可夫贡献度结果',
  `brand` varchar(255) DEFAULT '' COMMENT '品牌, MG or RW',
  `attribution_tp` varchar(255) DEFAULT '' COMMENT '归因触点',
  `pt` varchar(255) DEFAULT '' COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限'
)


DROP EXTERNAL TABLE IF EXISTS app_ml_attribution_report;
CREATE EXTERNAL TABLE IF NOT EXISTS app_ml_attribution_report (
  `touchpoint_id` varchar(255) DEFAULT '' COMMENT '触点id',
  `touchpoint_name` varchar(255) DEFAULT '' COMMENT '触点名称',
  `attribution` varchar(255) DEFAULT '' COMMENT '机器学习贡献度',
  `brand` varchar(255) DEFAULT '' COMMENT '品牌, MG or RW',
  `attribution_tp` varchar(255) DEFAULT '' COMMENT '归因触点',
  `pt` varchar(255) DEFAULT '' COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限'
)


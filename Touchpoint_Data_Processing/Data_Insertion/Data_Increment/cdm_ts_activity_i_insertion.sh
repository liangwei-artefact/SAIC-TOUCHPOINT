pt=$3
pt_date=$(date -d "-0 day $pt " +'%Y-%m-%d')
queue_name=marketing_modeling


hive --hivevar pt=$pt --hivevar pt_data=$pt_date -e "
set hive.execution.engine=mr;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;

DROP TABLE IF EXISTS marketing_modeling.tmp_dw_ts_activity_i;
CREATE table IF NOT EXISTS marketing_modeling.tmp_dw_ts_activity_i 
AS
SELECT 
    b.mobile as mobile,
    a.attend_time as act_time,
    c.saic_name as activity_name,
    c.saic_activity_codename as saic_type,
    CASE WHEN b.brand_id = 101 THEN 'RW' ELSE 'MG' END AS brand
FROM 
(
    SELECT cust_id, attend_time, activity_id 
    FROM dtwarehouse.ods_dlm_t_cust_activity 
    WHERE pt = ${pt}
    AND attend_time = '${pt_date}'
) a
LEFT JOIN
(
    SELECT 
        id, mobile, brand_id
    FROM
    (SELECT id, mobile, dealer_id FROM dtwarehouse.ods_dlm_t_cust_base WHERE pt = ${pt}) a
    LEFT JOIN (SELECT dlm_org_id, brand_id FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = ${pt}) b
    ON a.dealer_id = b.dlm_org_id
)b
ON a.cust_id = b.id
LEFT JOIN
(SELECT * FROM dtwarehouse.ods_activity_saic_activity WHERE pt = ${pt}) c
ON a.activity_id = c.saic_activityid
WHERE b.mobile IS NOT NULL AND a.attend_time IS NOT NULL
"

spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queue_name \
dw_ts_activity_processing.py
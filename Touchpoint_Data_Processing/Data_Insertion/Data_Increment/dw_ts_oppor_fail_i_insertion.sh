pt=$3

hive --hivevar pt=$pt -e "set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT overwrite TABLE marketing_modeling.dw_ts_oppor_fail_i partition (pt,brand)
SELECT * FROM
(SELECT b.mobile AS mobile,
       a.create_time AS action_time,
       CASE
           WHEN audit_status = '90281002' AND b.brand_id = 121 THEN '014001000000_tp' -- 意向战败分配至其他跟进人员
           WHEN audit_status = '90281003' AND b.brand_id = 121 THEN '014002000000_tp' -- 驳回战败申请
           WHEN audit_status = '90281004' AND b.brand_id = 121 THEN '014003000000_tp' -- 同意战败申请

           WHEN audit_status = '90281002' AND b.brand_id = 101 THEN '014001000000_rw' -- 意向战败分配至其他跟进人员
           WHEN audit_status = '90281003' AND b.brand_id = 101 THEN '014002000000_rw' -- 驳回战败申请
           WHEN audit_status = '90281004' AND b.brand_id = 101 THEN '014003000000_rw' -- 同意战败申请
       END AS touchpoint_id,
       a.pt AS pt,
       CASE
           WHEN b.brand_id = 121 THEN 'MG'
           WHEN b.brand_id = 101 THEN 'RW'
       END AS brand
FROM 
(
    SELECT cust_id,
            create_time,
            audit_status,
            regexp_replace(to_date(create_time), '-', '') as pt
    FROM dtwarehouse.ods_dlm_t_oppor_fail
    where 
        pt = '${pt}' -- 取全量数据
        and regexp_replace(to_date(create_time), '-', '') = '${pt}'
) a
JOIN
(
    SELECT 
        id,
        mobile,
        brand_id
    FROM
    (
        SELECT id,
             mobile,
             dealer_id
        FROM dtwarehouse.ods_dlm_t_cust_base
        WHERE 
            pt = '${pt}' -- 生产环境
    ) a1
    LEFT JOIN
    (
        SELECT dlm_org_id,
            brand_id
        FROM dtwarehouse.ods_rdp_v_sales_region_dealer
        WHERE 
            -- pt = '20210531'
            pt = '${pt}' -- 生产环境
    ) b1
    ON a1.dealer_id = b1.dlm_org_id
) b 
ON a.cust_id = b.id
) t1
WHERE
    mobile regexp '^[1][3-9][0-9]{9}$'
    AND action_time IS NOT NULL
    AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"
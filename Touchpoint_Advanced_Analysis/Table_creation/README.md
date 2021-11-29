## 触点能效看板数据开发建表语句

### 1. 首触用户属性
```python
├──cdm_customer_touchpoints_profile_a_table_creation.sql # 未去重的首触用户属性
├──app_touchpoints_profile_monthly_table_creation.sql  # 月度去重的首触用户属性
├──app_touchpoints_profile_weekly_table_creation.sql  # 周度去重的首触用户属性
```

### 2. 首触线索转化报表 & 首触线索转化趋势
```python
├──app_fir_contact_conversion_report_monthly_table_creation.sql # 月度首触报表
├──app_fir_contact_conversion_report_weekly_table_creation.sql  # 周度首触报表
├──app_touchpoints_profile_filter_table_creation.sql            # 省份、活动、车系维度表
```

### 3. 触点活跃及转化
```python
├──app_tp_asset_report_a_table_creation.sql # 触点资产报表
```

### 4. 触点贡献 
```python
├──app_attribution_report_module_table_creation.sql # MK贡献度和ML贡献度结果表
```

### 5. 报表 mysql schema 
```python
├──touchpoint_attribution_table_schema.sql
```
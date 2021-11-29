## 贡献度计算模块

### 1. 模块结构

```python
├──Logs  # 日志文件路径 
├──attri_report_load_data  # 加载数据到本地存成csv
├──attri_report_data_processing  # 数据处理模块，被attri_report_model_running调用
├──attri_report_model_running  # Markov Chain贡献度和机器学习贡献度计算（要求python3.7）
├──attri_report_save_result  # Pyspark将结果存入Hive (Python 2.7)
├──attri_report_pipeline  # 贡献度计算模型任务串联，袋鼠实际调用的脚本
├──removal_tp.yaml  # 维护需要从贡献度计算中去掉的触点
├──requirements.txt  # 任务所需python包
```
- 生产上的pyspark只支持python2.7，贡献度计算的相关包只支持python3.7
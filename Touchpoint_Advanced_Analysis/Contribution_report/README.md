## 计算触点活跃及转化

### 1. 模块结构

```python
├──contri_report_load_data.sh  # 把计算所需的数据拉到本地存成csv
├──contri_report_compute_result.py  # 计算是否经过关键节点的flag标记，生成中间表
├──contri_report_aggregation.sh  # 对上一步产生的中间结果进行聚合计算生成结果表
├──contri_report_pipeline.sh   # 运行脚本，将上述三个脚本串联起来
```
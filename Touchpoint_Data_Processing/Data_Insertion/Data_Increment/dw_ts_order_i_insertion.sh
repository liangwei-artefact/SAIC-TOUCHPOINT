pt=$3
run_date=$(date -d "-0 day $pt " +'%Y-%m-%d')
queue_name=marketing_modeling


spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queue_name \
dw_ts_order_processing.py $pt $run_date

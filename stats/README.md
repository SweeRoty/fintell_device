### 1. Count active devices each month and save the results(stats) to HDFS
e.g. `nohup spark-submit count_active_devices.py --query_month 202005 &`

### 2. Prepare device properties for monthly-active androids
e.g. `nohup spark-submit prepare_android_prop.py --query_month 202005 &`

### 3. Calculate properties stats (must be executed after step 2)
e.g. `nohup spark-submit cal_android_stats.py.py --query_month 202005 &`

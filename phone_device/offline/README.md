## Phone-Device bipartite

### 1. Extract valid phones with their device(imei) count stats with optional printing arguments
`nohup spark-submit extractPhones.py --query_month 202101 &`

### 2. Generate samples under different modes
`nohup spark-submit extractSamples.py --query_month 202101 --mode test &`

### 3. Extract bias features under different modes
`nohup spark-submit extractFeatures.py --query_month 202101 --mode test &`

### 4. Preprocess the input data
`nohup spark-submit preprocess.py --query_month 202101 --pca -- mode test &`

### 5. Model training, evaluation or inference
`nohup /opt/spark3/bin/spark-submit glm_spark.py --query_month 202101 --mode test &`

#### (optional) Weighted aggregation of the phone-device bipartite
`nohup spark-submit aggregate.py --query_month 202101 --mode test --print_part1 --print_part2_org --print_part2_new &`

### 6. Construct the bipartite
`nohup spark-submit build_bipartite.py --query_month 202101 --mode test &`

### 7. Output the FID
`nohup spark-submit bipartite_lpa.py --query_month 202101 --mode test &`
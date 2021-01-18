### Phone-Device bipartite

#### 1. Extract valid phones with their device(imei) count stats with optional printing arguments
`nohup spark-submit extract_phone_with_stats.py --query_month 201911 &`

#### 2. Generate samples or bias features for the training, evaluation or inference of the mapping model
`nohup spark-submit processSeq.py --query_month 201911 --kind sample --mode train &`

#### 3. Preprocess the input data for the training, evaluation or inference of the mapping model
`nohup spark-submit preprocess.py --query_month 201911 --pca -- mode train --prefix all`

P.S.: the *prefix* argument is optional which specifies whether you have sampled the data in Step 2

#### 4. Model training, evaluation or inference
`nohup spark-submit glm_spark.py --query_month 201911 --prefix all --mode train --save_model`

#### 5. Network aggregation

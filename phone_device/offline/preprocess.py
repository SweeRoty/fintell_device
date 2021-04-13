# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
import argparse

from pyspark import SparkConf
from pyspark.ml.feature import PCA, PCAModel
from pyspark.ml.feature import StandardScaler, StandardScalerModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType

def transform2row(t):
	return Row(phone=t[0], imei_count=t[1])

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Preprocess_Data_before_Modeling') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--print_y_dist', action='store_true', default=False)
	parser.add_argument('--pca', action='store_true', default=False)
	parser.add_argument('--mode', type=str, choices=['train', 'eval', 'test'])
	args = parser.parse_args()
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month+month_end

	print('====> Start computation')
	samples = spark.read.csv('/user/ronghui_safe/hgy/nid/samples/{}_{}'.format(args.query_month, args.mode), header=True)
	samples = samples.withColumn('key', F.concat_ws('_', F.col('phone_salt'), F.col('imei'), F.col('itime'), F.col('source')))
	if args.mode != 'test':
		samples = samples.withColumn('duration', F.round(F.col('duration')/F.lit(24*3600), scale=2))
	samples = samples.withColumn('itime', F.col('itime').cast(IntegerType()))
	samples = samples.withColumn('min_itime', F.col('min_itime').cast(IntegerType()))
	samples = samples.withColumn('aging', F.round((F.col('itime')-F.col('min_itime'))/F.lit(24*3600), scale=2))
	for col in ['itime', 'min_itime']:
		samples = samples.drop(col)
	if args.print_y_dist:
		quantiles = samples.approxQuantile('duration', [1.0, 0.99, 0.9, 0.75, 0.5, 0.25, 0.1, 0.01, 0.0], 0.002)
		for i, percentile in enumerate([1.0, 0.99, 0.9, 0.75, 0.5, 0.25, 0.1, 0.01, 0.0]):
			 print('----> Quantile for {} is {}'.format(percentile, quantiles[i]))
	features = spark.read.csv('/user/ronghui_safe/hgy/nid/features/bias_{}_{}'.format(args.query_month, args.mode), header=True, inferSchema=True)
	features = features.withColumn('key', F.concat_ws('_', F.col('phone_salt'), F.col('imei'), F.col('itime'), F.col('source')))
	for col in ['phone_salt', 'imei', 'itime', 'source']:
		features = features.drop(col)
	if args.pca:
		feature_cols = [col for col in features.columns if 'in' in col]
		assembler = VectorAssembler(inputCols=feature_cols, outputCol='feature_vec')
		features = assembler.transform(features).select('key', 'feature_vec')
		scaler_model = None
		if args.mode == 'train':
			scaler = StandardScaler(inputCol='feature_vec', outputCol='scaled_feature_vec', withStd=False, withMean=True)
			scaler_model = scaler.fit(features)
			scaler_model.save('/user/ronghui_safe/hgy/nid/edw/scaler_pca_model_v2')
		else:
			scaler_model = StandardScalerModel.load('/user/ronghui_safe/hgy/nid/edw/scaler_pca_model_v2')
		features = scaler_model.transform(features).select('key', 'scaled_feature_vec')
		pca_model = None
		if args.mode == 'train':
			pca = PCA(k=len(feature_cols), inputCol='scaled_feature_vec', outputCol='components')
			pca_model = pca.fit(features)
			print('----> Explained variance is {}'.format(pca_model.explainedVariance.toArray().tolist()))
			pca_model.save('/user/ronghui_safe/hgy/nid/edw/pca_model_v2')
		else:
			pca_model = PCAModel.load('/user/ronghui_safe/hgy/nid/edw/pca_model_v2')
			print('----> Explained variance is {}'.format(pca_model.explainedVariance.toArray().tolist()))
		features = pca_model.transform(features).select('key', 'components').rdd.map(lambda row: Row(key=row['key'], PC1=round(row['components'][0], 4), PC2=round(row['components'][1], 4), PC3=round(row['components'][2], 4), PC4=round(row['components'][3], 4))).toDF()
	samples = samples.join(features, on='key', how='inner')
	samples.repartition(50).write.csv('/user/ronghui_safe/hgy/nid/datasets/{}_{}'.format(args.query_month, args.mode), header=True)
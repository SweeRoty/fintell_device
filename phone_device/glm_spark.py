# -*- coding: utf-8 -*-

from calendar import monthrange
#from ConfigParser import RawConfigParser
import argparse
import configparser

from pyspark import SparkConf
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, OneHotEncoderModel
from pyspark.ml.feature import PolynomialExpansion, VectorAssembler
from pyspark.ml.feature import StandardScaler, StandardScalerModel
from pyspark.ml.feature import StringIndexer, StringIndexerModel
from pyspark.ml.regression import GeneralizedLinearRegression, GeneralizedLinearRegressionModel
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import TrainValidationSplit, TrainValidationSplitModel
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType

def transform2row(t):
	return Row(phone=t[0], imei_count=t[1])

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	#localConf = RawConfigParser()
	localConf = configparser.ConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Train_or_Test_GLM') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--prefix', type=str, choices=['sampled', 'all'], default='sampled')
	parser.add_argument('--mode', type=str, choices=['train', 'test'], default='train')
	parser.add_argument('--save_model', action='store_true', default=False)
	args = parser.parse_args()
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month+month_end

	print('====> Start computation')
	dataset = spark.read.csv('/user/ronghui_safe/hgy/nid/datasets/{}_{}'.format(args.prefix, args.query_month), header=True, inferSchema=True)
	#if args.mode == 'train':
	dataset = dataset.withColumn('duration', F.when(F.col('duration') == 0, 1e-6).otherwise(F.col('duration')))
	dataset = dataset.withColumn('duration', F.log(F.lit(1e-6))/F.col('duration'))
	dataset = dataset.withColumn('duration', F.exp(F.col('duration')))
	stringIndex_model = None
	if args.mode == 'train':
		stringIndexer = StringIndexer(inputCol='source', outputCol='source_index')
		stringIndex_model = stringIndexer.fit(dataset)
		stringIndex_model.save('/user/ronghui_safe/hgy/nid/models/stringIndex_model')
	else:
		stringIndex_model = StringIndexerModel.load('/user/ronghui_safe/hgy/nid/models/stringIndex_model')
	dataset = stringIndex_model.transform(dataset)
	encoder_model = None
	if args.mode == 'train':
		encoder = OneHotEncoder(inputCol='source_index', outputCol='source_vec')
		encoder_model = encoder.fit(dataset)
		encoder_model.save('/user/ronghui_safe/hgy/nid/models/oneHotEncoder_model')
	else:
		encoder_model = OneHotEncoderModel.load('/user/ronghui_safe/hgy/nid/models/oneHotEncoder_model')
	dataset = encoder_model.transform(dataset)
	feature_cols = ['source_vec', 'aging', 'PC1', 'PC2']
	assembler = VectorAssembler(inputCols=feature_cols, outputCol='feature_vec')
	dataset = assembler.transform(dataset)
	scaler_model = None
	if args.mode == 'train':
		scaler = StandardScaler(inputCol='feature_vec', outputCol='scaled_feature_vec', withStd=True, withMean=True)
		scaler_model = scaler.fit(dataset)
		scaler_model.save('/user/ronghui_safe/hgy/nid/models/standardScaler_model')
	else:
		scaler_model = StandardScalerModel.load('/user/ronghui_safe/hgy/nid/models/standardScaler_model')
	dataset = scaler_model.transform(dataset)
	polyExpansion = PolynomialExpansion(degree=2, inputCol='scaled_feature_vec', outputCol='polyFeatures')
	dataset = polyExpansion.transform(dataset)
	dataset = dataset.select(F.col('duration'), F.col('polyFeatures'), F.col('key')).cache()
	glr = None
	if args.mode == 'train':
		glr = GeneralizedLinearRegression(labelCol='duration', featuresCol='polyFeatures', family='Binomial', linkPredictionCol='link_pred')
		paramGrid = ParamGridBuilder() \
					.addGrid(glr.link, ['logit']) \
					.addGrid(glr.regParam, [1e-5]) \
					.build()
		tvs = TrainValidationSplit(estimator=glr, \
									estimatorParamMaps=paramGrid, \
									evaluator=RegressionEvaluator(metricName='r2', labelCol='duration'), \
									trainRatio=0.7)
		tvs_model = tvs.fit(dataset)
		print('----> {}'.format(tvs_model.validationMetrics))
		if args.save_model:
			tvs_model.write().save('/user/ronghui_safe/hgy/nid/models/glm_binomial_model')
	else:
		#glr_model = GeneralizedLinearRegressionModel.load('/user/ronghui_safe/hgy/nid/models/glm_binomial_model')
		glr_model = TrainValidationSplitModel.load('/user/ronghui_safe/hgy/nid/models/glm_binomial_model')
		dataset = glr_model.transform(dataset).select(F.col('duration'), F.col('prediction'), F.col('key')).cache()
		evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='duration', metricName='mae')
		print('----> The performance on the whole dataset is {}'.format(round(evaluator.evaluate(dataset), 4)))
		dataset.drop('duration').repartition(50).write.csv('/user/ronghui_safe/hgy/nid/weights/{}_{}'.format(args.prefix, args.query_month), header=True)
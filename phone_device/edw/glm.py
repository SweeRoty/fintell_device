# -*- coding: utf-8 -*-

from calendar import monthrange
import argparse
import configparser

from pyspark import SparkConf
from pyspark.ml.feature import OneHotEncoderModel
from pyspark.ml.feature import PolynomialExpansion, VectorAssembler
from pyspark.ml.feature import StandardScalerModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.tuning import TrainValidationSplitModel
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType

def getDataset(spark, query_month)
	sql = """
		select
			*
		from
			tmp.step4_dataset
		where
			data_date = '{0}'
	""".format(query_month)
	print(sql)
	dataset = spark.sql(sql)
	return dataset

def transform2row(t):
	return Row(phone=t[0], imei_count=t[1])

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = configparser.ConfigParser()
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___GLM_Inference') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	args = parser.parse_args()

	print('====> Start computation')
	dataset = getDataset(spark, args.query_month)
	dataset = dataset.withColumn('source', F.split(F.col('key'), '_').getItem(3))
	dataset = dataset.withColumn('source', F.when(F.col('source') == '__HIVE_DEFAULT_PARTITION__', 'null').otherwise(F.col('source')))
	dataset = dataset.withColumn('source', F.when(F.col('source') == 'cm_mail', 'null').otherwise(F.col('source')))
	stringIndex_model = StringIndexerModel.load('/user/ronghui_safe/hgy/nid/models/stringIndex_model_v2')
	dataset = stringIndex_model.transform(dataset)
	encoder_model = OneHotEncoderModel.load('/user/ronghui_safe/hgy/nid/models/oneHotEncoder_model_v2')
	dataset = encoder_model.transform(dataset)
	feature_cols = ['source_vec', 'aging', 'PC1', 'PC2', 'PC3', 'PC4']
	assembler = VectorAssembler(inputCols=feature_cols, outputCol='feature_vec')
	dataset = assembler.transform(dataset)
	scaler_model = StandardScalerModel.load('/user/ronghui_safe/hgy/nid/models/standardScaler_model_v2')
	dataset = scaler_model.transform(dataset)
	polyExpansion = PolynomialExpansion(degree=2, inputCol='scaled_feature_vec', outputCol='polyFeatures')
	dataset = polyExpansion.transform(dataset)
	dataset = dataset.select(F.col('polyFeatures'), F.col('key'))
	glr_model = TrainValidationSplitModel.load('/user/ronghui_safe/hgy/nid/models/glm_binomial_model_v2')
	dataset = glr_model.transform(dataset).select(F.col('prediction'), F.col('key'))
	dataset = dataset.select(['key', 'prediction'])
	dataset = dataset.registerTempTable('temp')
	### table name below should be changed
	spark.sql('''INSERT OVERWRITE TABLE tmp.step5_weight PARTITION (data_date = '{0}') SELECT * FROM temp'''.format(args.query_month)).collect()
# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
import argparse

from pyspark import SparkConf
from pyspark.ml.feature import PCAModel, StandardScalerModel, VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType

def getSamples(spark, query_month):
	sql = """
		select
			*
		from
			tmp.step2_sample
		where
			data_date = '{0}'
	""".format(query_month)
	print(sql)
	samples = spark.sql(sql)
	return samples

def getFeatures(spark, query_month):
	sql = """
		select
			*
		from
			tmp.step3_feature
		where
			data_date = '{0}'
	""".format(query_month)
	print(sql)
	features = spark.sql(sql)
	return features

def transform2row(t):
	return Row(phone=t[0], imei_count=t[1])

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('./config')
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
	args = parser.parse_args()

	print('====> Start computation')
	samples = getSamples(spark, args.query_month)
	samples = samples.withColumn('key', F.concat_ws('_', F.col('phone_salt'), F.col('imei'), F.col('itime'), F.col('source')))
	samples = samples.withColumn('itime', F.col('itime').cast(IntegerType()))
	samples = samples.withColumn('min_itime', F.col('min_itime').cast(IntegerType()))
	samples = samples.withColumn('aging', F.round((F.col('itime')-F.col('min_itime'))/F.lit(24*3600), scale=2))
	samples = samples.drop('itime').drop('min_itime')
		
	features = getFeatures(spark, args.query_month)
	features = features.withColumn('key', F.concat_ws('_', F.col('phone_salt'), F.col('imei'), F.col('itime'), F.col('source')))
	for col in ['phone_salt', 'imei', 'itime', 'source']:
		features = features.drop(col)
	
	feature_cols = [col for col in features.columns if 'in' in col]
	assembler = VectorAssembler(inputCols=feature_cols, outputCol='feature_vec')
	features = assembler.transform(features).select('key', 'feature_vec')
	scaler_model = StandardScalerModel.load('/user/ronghui_safe/hgy/nid/models/scaler_pca_model_v2')
	features = scaler_model.transform(features).select('key', 'scaled_feature_vec')
	pca_model = PCAModel.load('/user/ronghui_safe/hgy/nid/models/pca_model_v2')
	features = pca_model.transform(features).select('key', 'components').rdd.map(lambda row: Row(key=row['key'], PC1=round(row['components'][0], 4), PC2=round(row['components'][1], 4), PC3=round(row['components'][2], 4), PC4=round(row['components'][3], 4))).toDF()
	
	samples = samples.join(features, on='key', how='inner')
	samples = samples.select(['key', 'aging', 'PC1', 'PC2', 'PC3', 'PC4'])
	samples = samples.registerTempTable('temp')
	spark.sql('''INSERT OVERWRITE TABLE tmp4.step4_dataset PARTITION (data_date = '{0}') SELECT * FROM temp'''.format(args.query_month)).collect()
# -*- coding: utf-8 -*-

from __future__ import division
from calendar import monthrange
from datetime import datetime, timedelta
import argparse
import configparser
import time

from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	#localConf = RawConfigParser()
	localConf = configparser.ConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Build_the_Bipartite') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--mode', type=str, choices=['train', 'eval', 'test'])
	parser.add_argument('--print_pred_perc', action='store_true', default=False)
	parser.add_argument('--print_weight_perc', action='store_true', default=False)
	args = parser.parse_args()

	print('====> Start computation')
	month_end = monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1]
	obs_date = args.query_month+str(month_end)
	obs_time = time.mktime(datetime.strptime('{} 23:59:59'.format(obs_date), '%Y%m%d %H:%M:%S').timetuple())

	weights = spark.read.csv('/user/ronghui_safe/hgy/nid/weights/{}_{}'.format(args.query_month, args.mode), header=True, inferSchema=True)
	weights = weights.withColumn('phone_salt', F.split(F.col('key'), '_').getItem(0))
	weights = weights.withColumn('imei', F.split(F.col('key'), '_').getItem(1))
	weights = weights.withColumn('itime', F.split(F.col('key'), '_').getItem(2))
	weights = weights.withColumn('itime', F.col('itime').cast(IntegerType()))
	weights = weights.drop('key')
	weights = weights.withColumn('duration', F.lit(obs_time)-F.col('itime'))
	weights = weights.withColumn('duration', F.round(F.col('duration')/F.lit(24*3600), scale=2))
	weights = weights.withColumn('weight', F.pow(F.col('prediction'), F.col('duration')))

	if args.print_pred_perc:
		quantiles = weights.approxQuantile('weight', [1.0, 0.99, 0.75, 0.5, 0.25, 0.01, 0.0], 0.001)
		for i, percentile in enumerate([1.0, 0.99, 0.75, 0.5, 0.25, 0.01, 0.0]):
			print('----> Quantile for {} is {}'.format(percentile, quantiles[i]))

	weights = weights.groupby(['phone_salt', 'imei']).agg(F.sum('weight').alias('edge_weight'))
	if args.print_weight_perc:
		quantiles = weights.approxQuantile('edge_weight', [1.0, 0.99, 0.75, 0.5, 0.25, 0.01, 0.0], 0.001)
		for i, percentile in enumerate([1.0, 0.99, 0.75, 0.5, 0.25, 0.01, 0.0]):
			print('----> Quantile for {} is {}'.format(percentile, quantiles[i]))

	weights.select('phone_salt', 'imei', 'edge_weight').repartition(1).write.csv('/user/ronghui_safe/hgy/nid/graph/edge_weight_{}_{}'.format(args.query_month, args.mode), header=True)
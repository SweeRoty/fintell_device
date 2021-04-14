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

def getWeights(spark, query_month):
	sql = """
		select
			*
		from
			tmp.step5_weight ### table name should be changed
		where
			data_date <= '{0}'
	""".format(query_month)
	print(sql)
	weights = spark.sql(sql)
	return weights

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = configparser.ConfigParser()
	localConf.optionxform = str
	localConf.read('./config')
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
	args = parser.parse_args()

	print('====> Start computation')
	month_end = monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1]
	obs_date = args.query_month + str(month_end)
	obs_time = time.mktime(datetime.strptime('{} 23:59:59'.format(obs_date), '%Y%m%d %H:%M:%S').timetuple())

	weights = getWeights(spark, args.query_month)
	weights = weights.withColumn('phone_salt', F.split(F.col('key'), '_').getItem(0))
	weights = weights.withColumn('imei', F.split(F.col('key'), '_').getItem(1))
	weights = weights.withColumn('itime', F.split(F.col('key'), '_').getItem(2))
	weights = weights.withColumn('itime', F.col('itime').cast(IntegerType()))
	weights = weights.withColumn('duration', F.lit(obs_time)-F.col('itime'))
	weights = weights.withColumn('duration', F.round(F.col('duration')/F.lit(24*3600), scale=2))
	weights = weights.withColumn('weight', F.pow(F.col('prediction'), F.col('duration')))
	weights = weights.groupby(['phone_salt', 'imei']).agg(F.sum('weight').alias('edge_weight'))
	weights = weights.select(['phone_salt', 'imei', 'edge_weight'])
	weights = weights.registerTempTable('temp')
	spark.sql('''INSERT OVERWRITE TABLE tmp.step6_edge PARTITION (data_date = '{0}') SELECT * FROM temp'''.format(args.query_month)).collect()
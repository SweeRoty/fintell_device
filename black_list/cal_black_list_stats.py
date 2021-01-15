# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

def retrieveRecords(spark, query_date):
	sql = """
		select
			value,
			level,
			type,
			reasoncode,
			sourcecode,
			creator
		from
			ronghui_mart.risk_score_info
		where
			data_date = '{0}'
			and scene = 's9'
			and status = 1
	""".format(query_date)
	print(sql)
	records = spark.sql(sql)
	return records

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Black_List___Cal_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_date', type=str)
	args = parser.parse_args()
	#last_day = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])
	#query_date = args.query_month+last_day

	print('====> Start calculation')
	records = retrieveRecords(spark, args.query_date)
	level_stats = records.where(F.length('value') == 32).groupBy(['level']).agg(F.countDistinct('value').alias('level_count'))
	type_stats = records.where(F.length('value') == 32).groupBy(['type']).agg(F.countDistinct('value').alias('type_count'))
	creator_stats = records.where(F.length('value') == 32).groupBy(['creator']).agg(F.countDistinct('value').alias('creator_count'))
	#records.repartition(50).write.csv('/user/ronghui_safe/rlab/stats_report/device_stats/device_prop_{0}'.format(args.query_month), header=True)
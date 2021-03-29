# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from datetime import datetime
import argparse
import time

from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window

def getAndroidPairs(spark, data_date):
	global start_time, end_time
	sql = """
		select
			distinct phone_salt,
			imei,
			itime,
			case when source is null then 'null' else source end source
		from
			edw.jiguang_phone_encrypt
		where
			data_date <= '{0}'
			and phone_salt is not null
			and imei is not null
			and itime between {1} and {2}
	""".format(data_date, start_time, end_time)
	print(sql)
	pairs = spark.sql(sql)
	return pairs

def getValidPhones(spark, query_month):
	sql = """
		select
			phone_salt,
			imei_count,
			min_itime
		from
			tmp.step1_phone
		where
			data_date = '{0}'
	""".format(query_month)
	print(sql)
	phones = spark.sql(sql)
	return phones

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___Generate_Samples') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	args = parser.parse_args()
	month_end = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:6]))[1])
	data_date = args.query_month + month_end
	start_time = time.mktime(datetime.strptime('{}01 00:00:00'.format(query_month), '%Y%m%d %H:%M:%S').timetuple())
	end_time = time.mktime(datetime.strptime('{} 23:59:59'.format(data_date), '%Y%m%d %H:%M:%S').timetuple())

	print('====> Start computation')
	pairs = getAndroidPairs(spark, data_date)
	phones = getValidPhones(spark, args.query_month)
	phones = phones.where(F.col('imei_count').between(1, 20)).drop('imei_count')
	pairs = pairs.join(phones, on='phone_salt', how='inner')
	samples = samples.select(['phone_salt', 'imei', 'itime', 'source', 'min_itime'])
	samples = samples.registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE tmp.step2_sample PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(args.query_month)).collect()
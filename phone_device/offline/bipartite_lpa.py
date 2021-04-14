# -*- coding:utf-8 -*-

from __future__ import division
import argparse
import configparser
import numpy as np

from pyspark import SparkConf
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F

def render(t):
	global flag
	comp_ids = [e[0] for e in t[1]]
	probs = [e[1] for e in t[1]]
	partition = sum(probs)
	probs = [prob/partition for prob in probs] if partition > 0 else [1.0/len(probs)]*len(probs)
	the_id = str(np.random.choice(comp_ids, size=1, replace=False, p=probs)[0])
	if flag:
		return Row(imei=t[0], phone_salt=the_id)
	else:
		return Row(phone_salt=t[0], imei=the_id)

def maximize(t):
	global flag
	comp_ids = [e[0] for e in t[1]]
	probs = [e[1] for e in t[1]]
	index = np.argmax(probs)
	the_id = comp_ids[index]
	if flag:
		return Row(imei=t[0], phone_salt=the_id)
	else:
		return Row(phone_salt=t[0], imei=the_id)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = configparser.ConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_ID_Project___LPA_on_Bipartite') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str, help='The format should be YYYYmm')
	parser.add_argument('--mode', type=str, choices=['train', 'eval', 'test'])
	parser.add_argument('--iter', type=int, default=5)
	args = parser.parse_args()

	edges = spark.read.csv('/user/ronghui_safe/hgy/nid/graph/edge_weight_{}_{}'.format(args.query_month, args.mode), header=True).repartition(100)
	phones = edges.select('phone_salt').distinct()
	phones = phones.withColumn('comp_id', F.col('phone_salt'))
	#print('The initial count of ID is {}'.format(phones.where(F.col('phone_salt') == F.col('comp_id')).count()))
	devices = edges.select('imei').distinct()
	edges = edges.withColumn('weight', F.col('edge_weight').cast(DoubleType())).drop('edge_weight')
	flag = True
	for i in range(args.iter):
		device_part = edges.rdd.map(lambda row: (row['imei'], (row['phone_salt'], row['weight']))).groupByKey().map(render).toDF()
		devices = device_part.join(phones, on='phone_salt', how='inner').drop('phone_salt')
		flag = False
		phone_part = edges.rdd.map(lambda row: (row['phone_salt'], (row['imei'], row['weight']))).groupByKey().map(render).toDF()
		phones = phone_part.join(devices, on='imei', how='inner').drop('imei')
		flag = True
		#print('The count of ID after iteration {} is {}'.format(i, phones.where(F.col('phone_salt') == F.col('comp_id')).count()))
	#print('The final count of ID after iteration {} is {}'.format(args.iter, phones.where(F.col('phone_salt') == F.col('comp_id')).count()))
	devices.repartition(1).write.csv('/user/ronghui_safe/hgy/nid/graph/device_comp_{}_{}'.format(args.query_month, args.mode), header=True)
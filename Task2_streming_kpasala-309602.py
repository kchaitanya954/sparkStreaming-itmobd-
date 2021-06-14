#!/usr/bin/env python
# coding: utf-8

# In[13]:


#psss.py
#%%

import os
if "HADOOP_CONF_DIR" in os.environ:
   del os.environ["HADOOP_CONF_DIR"] 

#%%

"HADOOP_CONF_DIR" in os.environ

#%%

import socket
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, length, when, col, split, size, from_json, window, sum, to_json, from_json, struct
from pyspark.sql.types import BooleanType, IntegerType, TimestampType, DoubleType, LongType, StringType, ArrayType, FloatType, StructType, StructField
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
from jinja2 import Environment, FileSystemLoader

# In[7]:



# setting constants
APP_NAME = "kpasala-309602_streaming"
NORMALIZED_APP_NAME = APP_NAME.replace('/', '_').replace(':', '_')

APPS_TMP_DIR = os.path.join(os.getcwd(), "tmp")
APPS_CONF_DIR = os.path.join(os.getcwd(), "conf")
APPS_LOGS_DIR = os.path.join(os.getcwd(), "logs")
LOG4J_PROP_FILE = os.path.join(APPS_CONF_DIR, "pyspark-log4j-{}.properties".format(NORMALIZED_APP_NAME))
LOG_FILE = os.path.join(APPS_LOGS_DIR, 'pyspark-{}.log'.format(NORMALIZED_APP_NAME))
EXTRA_JAVA_OPTIONS = "-Dlog4j.configuration=file://{} -Dspark.hadoop.dfs.replication=1 -Dhttps.protocols=TLSv1.0,TLSv1.1,TLSv1.2,TLSv1.3"    .format(LOG4J_PROP_FILE)

LOCAL_IP = socket.gethostbyname(socket.gethostname())

# preparing configuration files from templates
for directory in [APPS_CONF_DIR, APPS_LOGS_DIR, APPS_TMP_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

env = Environment(loader=FileSystemLoader('/opt'))
template = env.get_template("pyspark_log4j.properties.template")
template    .stream(logfile=LOG_FILE)    .dump(LOG4J_PROP_FILE)


# In[8]:


# run spark
spark = SparkSession    .builder    .appName(APP_NAME)    .master("k8s://https://10.32.7.103:6443")    .config("spark.driver.host", LOCAL_IP)    .config("spark.driver.bindAddress", "0.0.0.0")    .config("spark.executor.instances", "2")    .config("spark.executor.cores", '3')    .config("spark.memory.fraction", "0.8")    .config("spark.memory.storageFraction", "0.6")    .config("spark.executor.memory", '3g')    .config("spark.driver.memory", "3g")    .config("spark.driver.maxResultSize", "1g")    .config("spark.kubernetes.memoryOverheadFactor", "0.3")    .config("spark.driver.extraJavaOptions", EXTRA_JAVA_OPTIONS)    .config("spark.kubernetes.namespace", "kpasala-309602")    .config("spark.kubernetes.driver.label.appname", APP_NAME)    .config("spark.kubernetes.executor.label.appname", APP_NAME)    .config("spark.kubernetes.container.image", "node03.st:5000/spark-executor:kpasala-309602")    .config("spark.local.dir", "/tmp/spark")    .config("spark.driver.extraClassPath", "/home/jovyan/shared-data/my-project-name-jar-with-dependencies.jar")    .config("spark.executor.extraClassPath", "/home/jovyan/shared-data/my-project-name-jar-with-dependencies.jar")    .config("spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-tmp-spark.mount.path", "/tmp/spark")    .config("spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-tmp-spark.mount.readOnly", "false")    .config("spark.kubernetes.executor.volumes.hostPath.depdir.mount.path", "/home/jovyan/shared-data")    .config("spark.kubernetes.executor.volumes.hostPath.depdir.options.path", "/nfs/shared")    .config("spark.kubernetes.executor.volumes.hostPath.depdir.options.type", "Directory")    .config("spark.kubernetes.executor.volumes.hostPath.depdir.mount.readOnly", "true")    .getOrCreate()

import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords
from pyspark.ml.feature import StopWordsRemover
stopwords = stopwords.words("russian")

stopwords_broadcast = spark.sparkContext.broadcast(stopwords) 






schema = StructType(\
    [\
        StructField("id", DoubleType()),\
        StructField("date", TimestampType()),\
        StructField("text", StringType()),\
        StructField("sex", IntegerType()),\
        StructField("age", IntegerType()),\
        StructField("age_group", StringType()),\
    ])



lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka-svc:9092").option("subscribe",'task2_kpasala').load()

df = lines.selectExpr("CAST(value AS STRING)")
df = df.select('value', from_json("value", schema).alias("data"))
df = df.select("value", "data.id",  "data.date", "data.text", \
               "data.sex", "data.age", "data.age_group")

remover = StopWordsRemover(inputCol="text_words", outputCol="filtered_words", stopWords=stopwords)
df = df.withColumn('text_words', split(col('text'), ' '))
df = remover.transform(df)
df = df.withColumn('words_count', size(col('filtered_words')))

sex = [1,2]
age_group = ["0-18", "18-27", "27-40", "40-60", "60Above"]
window_lst = ["1 hour", "1 day", "1 week"]

topic_names = []
for gender in sex:
    for group in age_group:
        for win in window_lst:
            name = str(gender) + '_' + group + '_' + win.replace(" ", "")
            topic_names.append(name)
            stream = df.filter((df.sex == gender) & (df.age_group == group))\
            .drop("text", "id", "age", "text_words", "filtered_words")\
            .withWatermark("date", win).groupBy(window("date", win)).agg(sum("words_count").alias("total_words"))\
            .withColumn('value', to_json(struct(col("*"))))\
            .writeStream\
            .option("checkpointLocation", "file:///home/jovyan/nfs-home/checkpoint/{}".format(name))\
            .outputMode("complete").format("kafka").option("kafka.bootstrap.servers", "kafka-svc:9092")\
            .option("topic", name).start()
            
        

print(spark.streams.active)    
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(\
    bootstrap_servers=['kafka-svc:9092'],\
    auto_offset_reset='earliest',\
    enable_auto_commit=True,\
    group_id='kpasala',\
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))\
)

consumer.subscribe(topic_names)


for msg in consumer:
    print(msg.value)



spark.stop()
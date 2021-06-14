#!/usr/bin/env python
# coding: utf-8

# In[31]:


from kafka import KafkaProducer
import json
from random import randint
import time
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from datetime import date
import os
import socket
from collections import Counter, OrderedDict
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, length, when, col, broadcast
from pyspark.sql.types import MapType, BooleanType, IntegerType, LongType, StringType, ArrayType, FloatType, StructType, StructField
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
from jinja2 import Environment, FileSystemLoader


# In[2]:


# setting constants
APP_NAME = "kpasala-309602"
NORMALIZED_APP_NAME = APP_NAME.replace('/', '_').replace(':', '_')

APPS_TMP_DIR = os.path.join(os.getcwd(), "tmp")
APPS_CONF_DIR = os.path.join(os.getcwd(), "conf")
APPS_LOGS_DIR = os.path.join(os.getcwd(), "logs")
LOG4J_PROP_FILE = os.path.join(APPS_CONF_DIR, "pyspark-log4j-{}.properties".format(NORMALIZED_APP_NAME))
LOG_FILE = os.path.join(APPS_LOGS_DIR, 'pyspark-{}.log'.format(NORMALIZED_APP_NAME))
EXTRA_JAVA_OPTIONS = "-Dlog4j.configuration=file://{} -Dspark.hadoop.dfs.replication=1 -Dhttps.protocols=TLSv1.0,TLSv1.1,TLSv1.2,TLSv1.3"    .format(LOG4J_PROP_FILE)

LOCAL_IP = socket.gethostbyname(socket.gethostname())


# In[3]:


# preparing configuration files from templates
for directory in [APPS_CONF_DIR, APPS_LOGS_DIR, APPS_TMP_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

env = Environment(loader=FileSystemLoader('/opt'))
template = env.get_template("pyspark_log4j.properties.template")
template    .stream(logfile=LOG_FILE)    .dump(LOG4J_PROP_FILE)


# In[4]:


# setting up 4 cores for executing spark operations
SPARK_ADDRESS = "local[4]"

spark = SparkSession    .builder    .appName(APP_NAME)    .master(SPARK_ADDRESS)    .config('spark.ui.port', "4040")    .config("spark.memory.fraction", "0.8")    .config("spark.memory.storageFraction", "0.6")    .config("spark.driver.memory", "4g")    .config("spark.driver.extraJavaOptions", EXTRA_JAVA_OPTIONS)    .config("spark.executor.memory", "6g")    .getOrCreate()


# In[5]:


followers_posts_df = spark.read.json("file:///home/jovyan/shared-data/bigdata20/followers_posts_api_final.json")
followers_info_df = spark.read.json("file:///home/jovyan/shared-data/bigdata20/followers_info.json")


# In[6]:


# followers_posts_df.printSchema()


# In[7]:


# followers_info_df.printSchema()


# In[8]:


from datetime import date, datetime
from dateutil.relativedelta import *
def age(dob): 
    if (dob is not None) and (len(dob.split('.'))==3):
        birth = datetime.strptime(dob, '%d.%m.%Y')
        now = date.today()
        age =  relativedelta(now, birth)
        return age.years
    return None

udfAge = udf(age)


# In[11]:


# age('15.10.1996')


# In[20]:


followers_info = followers_info_df.select('id' ,'sex', 'bdate').withColumn('age', udfAge('bdate')).drop('bdate')


# In[21]:


# followers_info.show(5)


# In[17]:


followers_posts = followers_posts_df.select('id', 'text', 'date')


# In[19]:


# followers_posts.show(5)


# In[32]:


df = followers_posts.join(followers_info, ["id"])
df = df.where(df.text != '')



df = df.withColumn("age_group", when(col("age").isNull() | (col("age") < F.lit(18)), F.lit("0-18")).when(col("age").between(F.lit(18), F.lit(26)), F.lit("18-27")).when(col("age").between(F.lit(27), F.lit(39)), F.lit("27-40")).when(col("age").between(F.lit(40), F.lit(59)), F.lit("40-60")).when(col("age") >= 60, F.lit("60Above")))

df = df.orderBy('date')

print(df.count())
# In[44]:


# df.show(10)


# In[45]:


producer = KafkaProducer(bootstrap_servers="kafka-svc:9092", 
                         value_serializer=str.encode)
topicname = 'task2_kpasala'


# In[47]:


for data in df.rdd.toLocalIterator():
    value = json.dumps(data.asDict(), ensure_ascii=False)
    producer.send(topicname, value)
    producer.flush()


spark.stop()
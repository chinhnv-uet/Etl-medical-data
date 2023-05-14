from operators.medicThaiBinh.dimOperator.baseLoadOperator import baseLoadDim
from airflow.models.baseoperator import BaseOperator
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from airflow import settings
from airflow.hooks.base import BaseHook

class loadType1(baseLoadDim): #load mongo to clickhouse
    def __init__(self, spark, db, sourceName, selectCol, dropDup, rename, idCol, desName):
        super().__init__()
        self.spark = spark
        self.db = db
        self.sourceName = sourceName
        self.selectCol = selectCol
        self.dropDup = dropDup
        self.rename = rename
        self.idCol = idCol
        self.desName = desName
        
    def load(self):
        uri = "mongodb://{}:{}@{}:{}/{}".format(self.db["mongoTb"]["usr"], self.db["mongoTb"]["pwd"], self.db["mongoTb"]["host"], self.db["mongoTb"]["port"], self.db["mongoTb"]["db"])
        
        df = self.spark.read.format("mongo").option("uri", f"{uri}.{self.sourceName}?authSource=admin").load()
        df = df.select(self.selectCol)
        df = df.drop_duplicates(self.dropDup)
        for k in self.rename.keys():
            df = df.withColumnRenamed(k, self.rename[k])
        df = df.withColumn(self.idCol, F.monotonically_increasing_id())
        # df = df.select(self.desName)
        
        options = {
            'url': "jdbc:clickhouse://{}:{}/{}".format(self.db["clhTb"]["host"], self.db["clhTb"]["port"], self.db["clhTb"]["db"]),
            'dbtable': self.desName,
            'isolationLevel': 'NONE'
        }
        oldRecord = self.spark.read.format("jdbc").options(**options).load().select(df.schema.names)
        res = df.subtract(oldRecord)
        res.write.mode("append").format("jdbc").options(**options).save()
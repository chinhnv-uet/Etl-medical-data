from airflow.models.baseoperator import BaseOperator
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from operators.medicThaiBinh.dimOperator.loadTypeOne import loadType1
from operators.medicThaiBinh.dimOperator.loadTypeTwo import loadType2
from airflow.hooks.base import BaseHook
import os
import json
import time

class loadBranch5(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info("Start branch 5")
        spark = SparkSession.builder.master("local[1]") \
            .appName("loadData") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

        AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
        file_path = os.path.join(AIRFLOW_HOME, "plugins", "operators", "medicThaiBinh", "db", "tableDim.json")
        f = open(file_path, 'r')
        connections = json.load(f)
        f.close()
        #load db
        db_path = os.path.join(AIRFLOW_HOME, "plugins", "operators", "medicThaiBinh", "db", "connectDb.json")
        file = open(db_path, 'r')
        db = json.load(file)
        file.close()
    
        listDim = ["doituongtt", "donvitinh", "nhomdichvu", "trangthai", "xutrikhambenh", "hinhthucravien", "quanhuyen", "phuongxa"]
        for task in listDim:
            self.log.info(f"Start load {task}")
            start = time.time()
            newTask = loadType1(spark=spark, db=db, **connections[task])
            newTask.load()
            end = time.time()
            self.log.info(f"Done load {task}, took {round(end-start, 2)} (s)")
        
        # update dim table in spark after insert
        # self.conn.updateDim()
        self.log.info("Insert dimensional table by spark success")
        
from airflow.models.baseoperator import BaseOperator
import pandas as pd
from pyspark.sql import functions as F
from operators.medicThaiBinh.db.sparkTb import Connect
import time


def loadDimDate(spark, conn):
    tmp = list(pd.date_range(start='1900-01-01',
               end='2036-12-31', freq='D').strftime('%Y%m%d'))
    d = []
    for i in range(len(tmp)):
        d.append((tmp[i], i))
    deptColumns = ['date', 'IdDate']
    dateDf = spark.createDataFrame(data=d, schema=deptColumns)

    dateDf = dateDf.withColumn('Ngay', F.substring('date', 7, 2))\
        .withColumn('Thang', F.substring('date', 5, 2))\
        .withColumn('Nam', F.substring('date', 1, 4))\
        .withColumnRenamed('date', 'NgayFull')

    # insert new record
    tmp = spark.read.format("jdbc").options(
        **conn.optionsDate).load().select(dateDf.schema.names)
    res = dateDf.subtract(tmp)
    res.write.mode("append").format("jdbc").options(**conn.optionsDate).save()


def loadDimTime(spark, conn):
   # dim time
    lst = []
    for h in range(24):
        tmp = str(h) if h > 9 else ("0"+str(h))
        for m in range(60):
            tmp2 = tmp
            tmp2 += str(m) if (m > 9) else ('0' + str(m))
            for s in range(60):
                tmp3 = tmp2
                tmp3 += str(s) if (s > 9) else ('0' + str(s))
                lst.append(tmp3)
    times = []
    for i in range(len(lst)):
        times.append((lst[i], i))
    column = ['ThoiGianFull', 'IdTime']
    timeDf = spark.createDataFrame(data=times, schema=column)
    timeDf = timeDf.withColumn('Gio', F.substring('ThoiGianFull', 1, 2))
    timeDf.write.mode("ignore").format(
        "jdbc").options(**conn.optionsTime).save()


class loadBranch3(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.conn = Connect()
        spark = self.conn.getSpark()
        start = time.time()
        self.log.info("Start load date")
        loadDimDate(spark, self.conn)
        self.log.info("Start load time")
        loadDimTime(spark, self.conn)
        end = time.time()
        self.log.info(f"Done load date, time, took {round(end - start, 2)} (s)")

# from airflow.models.baseoperator import BaseOperator
# from sqlalchemy import create_engine
# import pandas as pd
# import numpy as np
# from pyspark.sql import functions as F
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf
# from pyspark.sql.types import StringType
# from operators.medicThaiBinh.sparkTb import Connect
# from loadType1 import loadType1

# def loadDimBenhNhan(spark, conn):
#     dfBenhNhan = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.p_thongtindieutri_new?authSource=admin").load()
#     benhNhan = dfBenhNhan.select('MaBN', 'HoBenhNhan', 'ChuLotBenhNhan', 'TenBenhNhan', 'Tuoi', 'NgaySinh', 'NamSinh',
#                                  'DienThoaiDiDong', 'SoNha', 'QuocTich', 'NamSinhBo', 'NamSinhMe', 'NgayCapCMND').drop_duplicates(['MaBN'])
#     benhNhan = benhNhan.withColumnRenamed('MaBN', 'MaBenhNhan') \
#         .withColumnRenamed('HoBenhNhan', 'Ho') \
#         .withColumnRenamed('ChuLotBenhNhan', 'TenLot') \
#         .withColumnRenamed('TenBenhNhan', 'Ten') \
#         .withColumnRenamed('DienThoaiDiDong', 'SDT') \
#         .withColumnRenamed('NgayCapCMND', 'NgayCapCanCuoc')
#     benhNhan = benhNhan.withColumn(
#         "IdBenhNhan", F.monotonically_increasing_id())
    
#     #find new record
#     oldRecord = spark.read.format("jdbc").options(**conn.optionsBenhNhan).load().select(benhNhan.schema.names)
#     res = benhNhan.subtract(oldRecord)
#     res.write.mode("append").format("jdbc").options(**conn.optionsBenhNhan).save()
    
    
# def loadDimDate(spark, conn):
#     tmp = list(pd.date_range(start='1900-01-01', end='2036-12-31', freq='D').strftime('%Y%m%d'))
#     d = []
#     for i in range(len(tmp)):
#         d.append((tmp[i], i))
#     deptColumns = ['date', 'IdDate']
#     dateDf = spark.createDataFrame(data=d, schema = deptColumns)

#     dateDf = dateDf.withColumn('Ngay', F.substring('date', 7, 2))\
#         .withColumn('Thang', F.substring('date', 5, 2))\
#         .withColumn('Nam', F.substring('date', 1, 4))\
#         .withColumnRenamed('date', 'NgayFull')
        
#     # insert new record
#     tmp = spark.read.format("jdbc").options(**conn.optionsDate).load().select(dateDf.schema.names)
#     res = dateDf.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsDate).save()
    
# def loadDimTime(spark, conn):
#    # dim time
#     lst = []
#     for h in range(24):
#         tmp = str(h) if h > 9 else ("0"+str(h))
#         for m in range(60):
#             tmp2 = tmp
#             tmp2 += str(m) if (m > 9) else ('0' + str(m))
#             for s in range(60):
#                 tmp3 = tmp2
#                 tmp3 += str(s) if (s > 9) else ('0' + str(s))
#                 lst.append(tmp3)
#     times = []
#     for i in range(len(lst)):
#         times.append((lst[i], i))
#     column = ['ThoiGianFull', 'IdTime']
#     timeDf = spark.createDataFrame(data=times, schema = column)
#     timeDf = timeDf.withColumn('Gio', F.substring('ThoiGianFull', 1, 2))
#     timeDf.write.mode("ignore").format("jdbc").options(**conn.optionsTime).save()
    
# def loadDimKhoa(spark, conn): 
#     # dim khoa
#     merge_chi_dinh = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.merge_chi_dinh?authSource=admin").load()
#     khoaDf = merge_chi_dinh.select(
#         'MA_KHOA_CHI_DINH', 'TEN_KHOA_CHI_DINH').drop_duplicates(['MA_KHOA_CHI_DINH'])
#     khoaDf = khoaDf.withColumnRenamed('MA_KHOA_CHI_DINH', 'MaKhoaPhong')\
#         .withColumnRenamed('TEN_KHOA_CHI_DINH', 'TenKhoa')\
#         .withColumn("IdKhoa", F.monotonically_increasing_id())
#     # khoaDf.write.mode('ignore').format('jdbc').options(**conn.optionsKhoa).save()
    
#     # insert new record
#     tmp = spark.read.format("jdbc").options(**conn.optionsKhoa).load().select(khoaDf.schema.names)
#     res = khoaDf.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsKhoa).save()
    
# def loadDimPhong(spark, conn):
#     # dim phong
#     merge_chi_dinh = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.merge_chi_dinh?authSource=admin").load()
#     phongDf = merge_chi_dinh.select(
#         'MA_PHONG_CHI_DINH', 'TEN_PHONG_CHI_DINH').drop_duplicates(['MA_PHONG_CHI_DINH'])
#     phongDf = phongDf.withColumnRenamed('MA_PHONG_CHI_DINH', 'MaKhoaPhong')\
#         .withColumnRenamed('TEN_PHONG_CHI_DINH', 'TenPhong')\
#         .withColumn("IdPhong", F.monotonically_increasing_id())
#     # phongDf.write.mode('ignore').format('jdbc').options(**conn.optionsPhong).save()
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsPhong).load().select(phongDf.schema.names)
#     res = phongDf.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsPhong).save()

# def loadDimBacSi(spark, conn):
#     ttdv = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.thongtindichvu?authSource=admin").load()
#     bs = ttdv.select(
#         'MaBacSiChiDinh', 'TenBacSiChiDinh').drop_duplicates(['MaBacSiChiDinh', 'TenBacSiChiDinh'])
#     bs = bs.withColumnRenamed('MaBacSiChiDinh', 'MaBacSi')\
#         .withColumnRenamed('TenBacSiChiDinh', 'HoTenBacSi')\
#         .withColumn("IdBacSi", F.monotonically_increasing_id())
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsBacSi).load().select(bs.schema.names)
#     res = bs.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsBacSi).save()

# def loadDimCskcb(spark, conn):
#     hd = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.HoaDon?authSource=admin").load()
#     hd = hd.select(
#         'MaCSKCB', 'TenCSKCB').drop_duplicates(['MaCSKCB', 'TenCSKCB'])
#     hd = hd.withColumn("IdCSKCB", F.monotonically_increasing_id())
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsCskcb).load().select(hd.schema.names)
#     res = hd.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsCskcb).save()

# def loadDimDichVu(spark, conn):
#     dv = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.thongtindichvu?authSource=admin").load()
#     dv = dv.select(
#         'MaDichVu', 'TenDichVu').drop_duplicates(['MaDichVu', 'TenDichVu'])
#     dv = dv.withColumn("IdDichVu", F.monotonically_increasing_id())
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsDichVu).load().select(dv.schema.names)
#     res = dv.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsDichVu).save()

# def loadDimGioiTinh(spark, conn):
#     df = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.p_thongtindieutri_new?authSource=admin").load()
#     df = df.select(
#         'GioiTinh').drop_duplicates(['GioiTinh'])
#     df = df.withColumn("IdGioiTinh", F.monotonically_increasing_id())
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsGioiTinh).load().select(df.schema.names)
#     res = df.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsGioiTinh).save()
    
#         # coll = db['p_thongtindieutri_new'].distinct(key='GioiTinh')
#     # df = pd.DataFrame()
#     # df['GioiTinh'] = list(coll)
#     # df['IdGioiTinh'] = range(len(df))
#     # client.insert_df(table='DIM_GIOITINH', database='MEDIC_THAIBINH_DWH', df=df)
    
# def loadDimLoaiBenhAn(spark, conn):
#     df = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.p_thongtindieutri_new?authSource=admin").load()
#     df = df.select(
#         'LoaiBenhAn').drop_duplicates(['LoaiBenhAn'])
#     df = df.withColumnRenamed('LoaiBenhAn', 'TenLoaiBenhAn')\
#         .withColumn("IdLoaiBenhAn", F.monotonically_increasing_id())
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsLoaiBenhAn).load().select(df.schema.names)
#     res = df.subtract(tmp)
#     # res.write.mode("append").format("jdbc").options(**conn.optionsLoaiBenhAn).save()
    
# def loadDimLoaiPhieuThu(spark, conn):
#     df = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.HoaDon?authSource=admin").load()
#     df = df.select(
#         'MaLoaiPhieuThu', 'LoaiPhieuThu').drop_duplicates(['MaLoaiPhieuThu'])
#     df = df.withColumnRenamed('LoaiPhieuThu', 'TenLoaiPhieuThu')\
#     .withColumn("IdLoaiPhieuThu", F.monotonically_increasing_id())
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsLoaiPhieuThu).load().select(df.schema.names)
#     res = df.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsLoaiPhieuThu).save()

# def loadDimHoatChat(spark, conn):
#     df = spark.read.format("mongo").option(
#         "uri", "mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.ChiDinhDichVu?authSource=admin").load()
#     df = df.select(
#         'MaHoatChat', 'TenHoatChat').drop_duplicates(['MaHoatChat', 'TenHoatChat'])
#     df = df.withColumn("IdHoatChat", F.monotonically_increasing_id())
    
#     tmp = spark.read.format("jdbc").options(**conn.optionsHoatChat).load().select(df.schema.names)
#     res = df.subtract(tmp)
#     res.write.mode("append").format("jdbc").options(**conn.optionsHoatChat).save()

# def loadDimTable(conn):
#     spark = conn.getSpark()
#     loadDimBenhNhan(spark, conn)
#     loadDimDate(spark, conn)
#     print('Done dim date')
#     #loadDimTime(spark, conn)
#     print('Done dim time')
#     loadDimKhoa(spark, conn)
#     print('Done dim khoa')
#     # loadDimPhong(spark, conn)
#     # print('Done dim phong')
#     # loadDimBacSi(spark, conn)
#     # print("Done dim bac si")
#     # loadDimCskcb(spark, conn)
#     # print("Done dim cskcb")
#     # loadDimDichVu(spark, conn)
#     # loadDimGioiTinh(spark, conn)
#     # loadDimLoaiBenhAn(spark, conn)
#     # loadDimLoaiPhieuThu(spark, conn)
#     # loadDimHoatChat(spark, conn)
#     # thieu: dantoc, doituongtt, doituongvaovien, dvt, loaidichvu, trangthai, xutrikhambenh
    


# class loadDimOperator(BaseOperator):
#     def __init__(self, *args, **kwargs) -> None:
#         super().__init__(*args, **kwargs)
        
#     def execute(self, context):
#         spark = SparkSession.builder.master("local[4]") \
#         .appName("loadData") \
#         .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
#         .getOrCreate()
        
#         benhnhan = loadType1(spark=spark, sourceName='p_thongtindieutri_new', selectCol=['MaBN', 'HoBenhNhan', 'ChuLotBenhNhan', 'TenBenhNhan', 'Tuoi', 'NgaySinh', 'NamSinh',
#                                  'DienThoaiDiDong', 'SoNha', 'QuocTich', 'NamSinhBo', 'NamSinhMe', 'NgayCapCMND'], dropDup=['MaBN'], 
#                              rename={'MaBN': 'MaBenhNhan', 'HoBenhNhan' : 'Ho', 'ChuLotBenhNhan': 'TenLot', 'TenBenhNhan': 'Ten', 'DienThoaiDiDong': 'SDT', 'NgayCapCMND': 'NgayCapCanCuoc'},
#                              idCol='IdBenhNhan',
#                              desName='DIM_BENHNHAN')
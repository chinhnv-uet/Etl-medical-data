from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from airflow.hooks.base import BaseHook
import json
import os

class Connect():
    def __init__(self) -> None:
        #load db
        AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
        db_path = os.path.join(AIRFLOW_HOME, "plugins", "operators", "medicThaiBinh", "db", "connectDb.json")
        file = open(db_path, 'r')
        db = json.load(file)
        file.close()
        self.uriMongo = "mongodb://{}:{}@{}:{}/{}".format(db["mongoTb"]["usr"], db["mongoTb"]["pwd"], db["mongoTb"]["host"], db["mongoTb"]["port"], db["mongoTb"]["db"])
        uri = "jdbc:clickhouse://{}:{}/{}".format(db["clhTb"]["host"], db["clhTb"]["port"], db["clhTb"]["db"])
        
        self.optionsBenhNhan = {
            'url': uri,
            'dbtable': 'DIM_BENHNHAN',
            'isolationLevel': 'NONE'
        }
        self.optionsBenhChinhRaKhoa = {
            'url': uri,
            'dbtable': 'DIM_BENHCHINHRAKHOA',
            'isolationLevel': 'NONE'
        }
        self.optionsBenhChinhRaVien = {
            'url': uri,
            'dbtable': 'DIM_BENHCHINHRAVIEN',
            'isolationLevel': 'NONE'
        }
        self.optionsDate = {
            'url': uri,
            'dbtable': 'DIM_DATE',
            'isolationLevel': 'NONE'
        }
        self.optionsTime = {
            'url': uri,
            'dbtable': 'DIM_TIME',
            'isolationLevel': 'NONE'
        }
        self.optionsKhoa = {
            'url': uri,
            'dbtable': 'DIM_KHOA',
            'isolationLevel': 'NONE'
        }
        self.optionsPhong = {
            'url': uri,
            'dbtable': 'DIM_PHONG',
            'isolationLevel': 'NONE'
        }
        self.optionsDichVu = {
            'url': uri,
            'dbtable': 'DIM_DICHVU',
            'isolationLevel': 'NONE'
        }
        self.optionsLoaiDichVu = {
            'url': uri,
            'dbtable': 'DIM_LOAIDICHVU',
            'isolationLevel': 'NONE'
        }
        self.optionsCskcb = {
            'url': uri,
            'dbtable': 'DIM_CSKCB',
            'isolationLevel': 'NONE'
        }
        self.optionsLoaiBenhAn = {
            'url': uri,
            'dbtable': 'DIM_LOAIBENHAN',
            'isolationLevel': 'NONE'
        }
        self.optionsDoiTuongTT = {
            'url': uri,
            'dbtable': 'DIM_DOITUONGTT',
            'isolationLevel': 'NONE'
        }
        self.optionsDoiTuongVaoVien = {
            'url': uri,
            'dbtable': 'DIM_DOITUONGVAOVIEN',
            'isolationLevel': 'NONE'
        }
        self.optionsTrangThai = {
            'url': uri,
            'dbtable': 'DIM_TRANGTHAI',
            'isolationLevel': 'NONE'
        }
        self.optionsTTXN = {
            'url': uri,
            'dbtable': 'FACT_THONGTINXETNGHIEM',
            'isolationLevel': 'NONE'
        }
        self.optionsQuanHuyen = {
            'url': uri,
            'dbtable': 'DIM_QUANHUYEN',
            'isolationLevel': 'NONE'
        }
        self.optionsPhuongXa = {
            'url': uri,
            'dbtable': 'DIM_PHUONGXA',
            'isolationLevel': 'NONE'
        }
        self.optionsBacSi = {
            'url': uri,
            'dbtable': 'DIM_BACSI',
            'isolationLevel': 'NONE'
        }
        self.optionsDVT = {
            'url': uri,
            'dbtable': 'DIM_DONVITINH',
            'isolationLevel': 'NONE'
        }
        self.optionsDanToc = {
            'url': uri,
            'dbtable': 'DIM_DANTOC',
            'isolationLevel': 'NONE'
        }
        self.optionsGioiTinh = {
            'url': uri,
            'dbtable': 'DIM_GIOITINH',
            'isolationLevel': 'NONE'
        }
        self.optionsHinhThucRaVien = {
            'url': uri,
            'dbtable': 'DIM_HINHTHUCRAVIEN',
            'isolationLevel': 'NONE'
        }
        self.optionsHinhThucThanhToan = {
            'url': uri,
            'dbtable': 'DIM_HINHTHUCTT',
            'isolationLevel': 'NONE'
        }
        self.optionsHoatChat = {
            'url': uri,
            'dbtable': 'DIM_HOATCHAT',
            'isolationLevel': 'NONE'
        }
        self.optionsLoaiPhieuThu = {
            'url': uri,
            'dbtable': 'DIM_LOAIPHIEUTHU',
            'isolationLevel': 'NONE'
        }
        self.optionsLoaiPTTT = {
            'url': uri,
            'dbtable': 'DIM_LOAIPTTT',
            'isolationLevel': 'NONE'
        }
        self.optionsNgheNghiep = {
            'url': uri,
            'dbtable': 'DIM_NGHENGHIEP',
            'isolationLevel': 'NONE'
        }
        self.optionsNhomDichVu = {
            'url': uri,
            'dbtable': 'DIM_NHOMDICHVU',
            'isolationLevel': 'NONE'
        }
        self.optionsXuTriKhamBenh = {
            'url': uri,
            'dbtable': 'DIM_XUTRIKHAMBENH',
            'isolationLevel': 'NONE'
        }
        self.optionsFactHoaDon = {
            'url': uri,
            'dbtable': 'FACT_HOADON',
            'isolationLevel': 'NONE'
        }
        self.optionsTinhThanh = {
            'url': uri,
            'dbtable': 'DIM_TINHTHANH',
            'isolationLevel': 'NONE'
        }
        self.optionsFactThongTinDieuTri = {
            'url': uri,
            'dbtable': 'FACT_THONGTINDIEUTRI',
            'isolationLevel': 'NONE'
        }
        self.optionsFactHoaDon = {
            'url': uri,
            'dbtable': 'FACT_HOADON',
            'isolationLevel': 'NONE'
        }
        self.factChiDinhDv = {
            'url': uri,
            'dbtable' : 'FACT_CHIDINHDICHVU',
            'isolationLevel' : 'NONE'
        }

        spark = SparkSession.builder.master("local[1]") \
            .appName("loadData") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
        self.spark = spark
        
        self.date = spark.read.format('jdbc').options(**self.optionsDate).load()
        self.dt = spark.read.format('jdbc').options(**self.optionsDate).load()
        self.tt = spark.read.format('jdbc').options(**self.optionsTime).load()
        self.timeTable = spark.read.format('jdbc').options(**self.optionsTime).load()
        self.loaidv = spark.read.format('jdbc').options(**self.optionsLoaiDichVu).load()
        self.loaiba = spark.read.format('jdbc').options(**self.optionsLoaiBenhAn).load()
        self.cskcb = spark.read.format('jdbc').options(**self.optionsCskcb).load()
        self.phong = spark.read.format('jdbc').options(**self.optionsPhong).load()
        self.doituongtt = spark.read.format('jdbc').options(**self.optionsDoiTuongTT).load()
        self.khoa = spark.read.format('jdbc').options(**self.optionsKhoa).load()
        self.benhnhan = spark.read.format('jdbc').options(**self.optionsBenhNhan).load()
        self.quanhuyen = spark.read.format('jdbc').options(**self.optionsQuanHuyen).load()
        self.dvt = spark.read.format('jdbc').options(**self.optionsDVT).load()
        self.dichvu = spark.read.format('jdbc').options(**self.optionsDichVu).load()
        self.trangthai = spark.read.format('jdbc').options(**self.optionsTrangThai).load()
        self.nghenghiep = spark.read.format('jdbc').options(**self.optionsNgheNghiep).load()
        self.nhomdv = spark.read.format('jdbc').options(**self.optionsNhomDichVu).load()
        self.hoatchat = spark.read.format('jdbc').options(**self.optionsHoatChat).load()
        self.benhChinhRaVien = spark.read.format(
            'jdbc').options(**self.optionsBenhChinhRaVien).load()
        self.doituongvaovien = spark.read.format(
            'jdbc').options(**self.optionsDoiTuongVaoVien).load()
        self.xutrikhambenh = spark.read.format(
            'jdbc').options(**self.optionsXuTriKhamBenh).load()
        self.gioitinh = spark.read.format('jdbc').options(**self.optionsGioiTinh).load()
    
    def getSpark(self):
        return self.spark
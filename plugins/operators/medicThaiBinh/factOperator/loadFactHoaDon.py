from airflow.models.baseoperator import BaseOperator
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from operators.medicThaiBinh.db.sparkTb import Connect

class loadFactHoaDon(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
     
    def execute(self, context):
        conn = Connect()
        spark = conn.getSpark()
        
        data = spark.read.format("mongo").option("uri", f"{conn.uriMongo}.thongtinhoadonchitiet_1?authSource=admin").load()
        df = data.select('MaCSKCB', 'SoTien', 'TenDichVu', 'DVT', 'TenKhoaChiDinh', 'TenKhoaThuTien', 'ThoiGianChiDinh', 'TenNhomDichVu', 'TenDoiTuongThanhToan', 'GiaDichVu', 'GiaBHYT', 'DonGia', 'TyLeBHYTThanhToan', 'MucHuong', 'SoLuong', 'ThanhTien', 'BNTra', 'BNDCT', 'BHYTTra', 'Mien', 'HaoPhi', 'LoaiBenhAn')
        df = df.join(conn.cskcb, df.MaCSKCB == conn.cskcb.MaCSKCB, 'left').select(df["*"], conn.cskcb['IdCSKCB'])
        df = df.join(conn.dichvu, df.TenDichVu == conn.dichvu.TenDichVu, 'left').select(df['*'], conn.dichvu['IdDichVu'])
        df = df.join(conn.dvt, df.DVT == conn.dvt.DonViTinh, 'left').select(df['*'], conn.dvt['IdDonViTinh'])
        df = df.join(conn.khoa, df.TenKhoaChiDinh == conn.khoa.TenKhoa, 'left').select(df['*'], conn.khoa['IdKhoa'].alias('IdKhoaChiDinh'))
        k = spark.read.format('jdbc').options(**conn.optionsKhoa).load()
        df = df.join(k, df.TenKhoaThuTien == k.TenKhoa, 'left').select(df['*'], k['IdKhoa'].alias('IdKhoaThuTien'))
        
        #udf spark
        def getTimeStandard(s):
            s = str(s)
            if len(s) < 4:
                return "0"
            res = s[-4:]
            res += '00'
            return res
        
        TimeStandardUDF = udf(lambda x:getTimeStandard(x),StringType())
        
        df = df.withColumn('NgayChiDinh', F.substring('ThoiGianChiDinh', 1, 8))\
                .withColumn('GioChiDinh', TimeStandardUDF(col("ThoiGianChiDinh")))
        df = df.join(conn.nhomdv, df.TenNhomDichVu == conn.nhomdv.NhomDichVu, 'left').select(df['*'], conn.nhomdv['IdNhomDichVu'])
        df = df.join(conn.doituongtt, df.TenDoiTuongThanhToan == conn.doituongtt.DoiTuongTT, 'left').select(df['*'], conn.doituongtt['IdDoiTuongTT'])
        df = df.join(conn.loaiba, df.LoaiBenhAn == conn.loaiba.TenLoaiBenhAn, 'left').select(df['*'], conn.loaiba['IdLoaiBenhAn'])
        df = df.join(conn.dt, df.NgayChiDinh == conn.dt.NgayFull, 'left').select(df['*'], conn.dt['IdDate'].alias('ngaychidinh1'))
        df = df.drop('NgayChiDinh')
        df = df.withColumnRenamed('ngaychidinh1', 'NgayChiDinh')
        df = df.join(conn.tt, df.GioChiDinh == conn.tt.ThoiGianFull, 'left').select(df['*'], conn.tt['IdTime'].alias('giochidinh1'))
        df = df.drop('GioChiDinh')
        df = df.withColumnRenamed('giochidinh1', 'GioChiDinh')
        # df.printSchema()
        load = df.select('IdCSKCB', 'SoTien', 'IdDIchVu', 'IdDonViTinh', 'IdKhoaChiDinh', 'IdKhoaThuTien', 'NgayChiDinh', 'GioChiDinh', 'IdNhomDichVu', 'IdDoiTuongTT', 'GiaDichVu', 'GiaBHYT', 'DonGia', 'TyLeBHYTThanhToan', 'MucHuong', 'SoLuong', 'ThanhTien', 'BNTra', 'BNDCT', 'BHYTTra', 'Mien', 'HaoPhi', 'IdLoaiBenhAn')
        load = load.withColumn('IdFactHoaDon', F.monotonically_increasing_id())
        # load.printSchema()
        
        tmp = spark.read.format("jdbc").options(**conn.optionsFactHoaDon).load().select(load.schema.names)
        res = load.subtract(tmp)
        res.write.mode("append").format("jdbc").options(**conn.optionsFactHoaDon).save()
        
        # load.write.mode("append").format("jdbc").options(**conn.optionsFactHoaDon).save()
        self.log.info('Done fact hoa don')
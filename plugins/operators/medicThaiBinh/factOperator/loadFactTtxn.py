from airflow.models.baseoperator import BaseOperator
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from operators.medicThaiBinh.db.sparkTb import Connect

class loadFactTtxn(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
     
    def execute(self, context):
        conn = Connect()
        spark = conn.getSpark()
        
        name = 'thongtinxetnghiem_1'
        data = spark.read.format("mongo").option("uri", f"{conn.uriMongo}.{name}?authSource=admin").load()
        data = data.select('NgayDongBo', 'MaKhoaChiDinh', 'MaLoaiDichVu', 'GiaDichVu', 'LoaiBenhAn', 'TenCSKCB',
                        'MaKhoaChiDinh', 'MaPhongChiDinh', 'MaNhomDichVu', 'TenDoiTuongThanhToan', 'ThoiGianChiDinh', 'TrangThai')
        #udf spark
        def getTimeStandard(s):
            s = str(s)
            if len(s) < 4:
                return "0"
            res = s[-4:]
            res += '00'
            return res
        
        TimeStandardUDF = udf(lambda x:getTimeStandard(x),StringType())
        data2 = data.withColumn('DateDongBo', F.substring('NgayDongBo', 1, 8))\
                    .withColumn('TimeDongBo', F.substring('NgayDongBo', 9, 6))\
                    .withColumn('NgayChiDinh', F.substring('ThoiGianChiDinh', 1, 8))\
                    .withColumn('GioChiDinh', TimeStandardUDF(col("ThoiGianChiDinh")))
                    
        dateTab = spark.read.format('jdbc').options(**conn.optionsDate).load()
        df = data2.join(dateTab, data2.DateDongBo ==  dateTab.NgayFull,"left")
        df = df.drop("Ngay","Thang","Nam", "NgayFull")
        df = df.withColumnRenamed(existing='IdDate', new='IdNgayDongBo')
        df = df.join(conn.timeTable, df.TimeDongBo ==  conn.timeTable.ThoiGianFull,"left")
        df = df.drop('Gio', 'ThoiGianFull')
        df = df.withColumnRenamed(existing='IdTime', new='IdGioDongBo')
        
        df = df.join(conn.dt, df.NgayChiDinh == conn.dt.NgayFull, 'left').select(df["*"], conn.dt['IdDate'].alias('IdNgayChiDinh'))
        # df = df.drop("Ngay","Thang","Nam", "NgayFull")
        # df = df.withColumnRenamed(existing='IdDate', new='IdNgayChiDinh')
        df = df.join(conn.tt, df.GioChiDinh == conn.tt.ThoiGianFull, 'left').select(df["*"], conn.tt['IdTime'].alias('IdGioChiDinh'))
        df = df.join(conn.loaidv, df.MaLoaiDichVu == conn.loaidv.MaLoaiDichVu, 'left').select(df["*"], conn.loaidv['IdLoaiDichVu'])
        df = df.join(conn.loaiba, df.LoaiBenhAn == conn.loaiba.TenLoaiBenhAn, 'left').select(df["*"], conn.loaiba['IdLoaiBenhAn'])
        df = df.join(conn.cskcb, df.TenCSKCB == conn.cskcb.TenCSKCB, 'left').select(df["*"], conn.cskcb['IdCSKCB'])
        df = df.join(conn.phong, df.MaPhongChiDinh == conn.phong.MaKhoaPhong, 'left').select(df["*"], conn.phong['IdPhong'].alias('IdPhongChiDinh'))
        df = df.join(conn.khoa, df.MaKhoaChiDinh == conn.khoa.MaKhoaPhong, 'left').select(df["*"], conn.khoa['IdKhoa'].alias('IdKhoaChiDinh'))
        df = df.join(conn.doituongtt, df.TenDoiTuongThanhToan == conn.doituongtt.DoiTuongTT, 'left').select(df["*"], conn.doituongtt['IdDoiTuongTT'])
        df = df.join(conn.trangthai, df.TrangThai == conn.trangthai.TenTrangThai, 'left').select(df["*"], conn.trangthai['IdTrangThai'])
        finalTab = df.select('IdNgayDongBo', 'IdGioDongBo', 'IdKhoaChiDinh', 'IdLoaiDichVu', 'GiaDichVu', 'idLoaiBenhAn', 'IdCSKCB', 'IdPhongChiDinh', 'IdDoiTuongTT', 'IdNgayChiDinh', 'IdGioChiDinh', 'IdTrangThai')
        finalTab = finalTab.withColumn("IdFactThongTinXetNghiem", F.monotonically_increasing_id())
        finalTab = finalTab.withColumnRenamed("IdKhoaChiDinh", "IdKhoa")\
                            .withColumnRenamed("IdNgayChiDinh", "NgayChiDinh")\
                            .withColumnRenamed("IdGioChiDinh", "GioChiDinh")
                            
        tmp = spark.read.format("jdbc").options(**conn.optionsTTXN).load().select(finalTab.schema.names)
        res = finalTab.subtract(tmp)
        res.write.mode("append").format("jdbc").options(**conn.optionsTTXN).save()
        
        # finalTab.write.mode("append").format('jdbc').options(**conn.optionsTTXN).save()
        self.log.info('Done fact thong tin xet nghiem')
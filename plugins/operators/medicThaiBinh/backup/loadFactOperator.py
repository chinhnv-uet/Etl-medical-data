from airflow.models.baseoperator import BaseOperator
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from operators.medicThaiBinh.db.sparkTb import Connect

def loadFactTtxn(spark, conn):
    name = 'thongtinxetnghiem_1'
    data = spark.read.format("mongo").option("uri", f"mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.{name}?authSource=admin").load()
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
    print('Done fact thong tin xet nghiem')

def loadFactCddv(spark, conn):
    # insert fact chi dinh dich vu
    data = spark.read.format("mongo").option("uri", f"mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.ChiDinhDichVu?authSource=admin").load()
    df = data.select('MaBN', 'DVT', 'BHYTTra', 'BNDCT', 'GiaBHYT', 'GiaDichVu', 'NgayDuyetBHYT', 'NgayDuyetKeToan', 'SoLuong', 'ThanhTien', 'TyLeBHYTThanhToan', 'HamLuong', 'MaQuanHuyen', 'LoaiBenhAn', 'TenCSKCB', 'TenDichVu', 'TenDoiTuongThanhToan', 'TenKhoaChiDinh', 'TenLoaiDichVu', 'TenNgheNghiep', 'TenNhomDichVu', 'MaPhongChiDinh', 'TrangThai', 'TenHoatChat', 'ThoiGianLayMau', 'ThoiGianKetQua', 'ThoiGianChiDinh', 'NgayDongBo')
    df = df.join(conn.quanhuyen, df.MaQuanHuyen == conn.quanhuyen.MaQuanHuyen, 'left').select(df["*"], conn.quanhuyen['IdQuanHuyen'])
    df = df.join(conn.benhnhan, df.MaBN == conn.benhnhan.MaBenhNhan, 'left').select(df["*"], conn.benhnhan['IdBenhNhan'])
    df = df.join(conn.dvt, df.DVT == conn.dvt.DonViTinh, 'left').select(df["*"], conn.dvt['IdDonViTinh'])
    df = df.join(conn.loaiba, df.LoaiBenhAn == conn.loaiba.TenLoaiBenhAn, 'left').select(df["*"], conn.loaiba['IdLoaiBenhAn'])
    df = df.join(conn.cskcb, df.TenCSKCB == conn.cskcb.TenCSKCB, 'left').select(df["*"], conn.cskcb['IdCSKCB'])
    df = df.join(conn.dichvu, df.TenDichVu == conn.dichvu.TenDichVu, 'left').select(df["*"], conn.dichvu['IdDichVu'])
    df = df.join(conn.doituongtt, df.TenDoiTuongThanhToan == conn.doituongtt.DoiTuongTT, 'left').select(df["*"], conn.doituongtt['IdDoiTuongTT'])
    df = df.join(conn.khoa, df.TenKhoaChiDinh == conn.khoa.TenKhoa, 'left').select(df["*"], conn.khoa['IdKhoa'].alias('IdKhoaChiDinh'))
    df = df.join(conn.loaidv, df.TenLoaiDichVu == conn.loaidv.TenLoaiDichVu, 'left').select(df["*"], conn.loaidv['IdLoaiDichVu'])
    df = df.join(conn.nghenghiep, df.TenNgheNghiep == conn.nghenghiep.NgheNghiep, 'left').select(df["*"], conn.nghenghiep['IdNgheNghiep'])
    df = df.join(conn.nhomdv, df.TenNhomDichVu == conn.nhomdv.NhomDichVu, 'left').select(df["*"], conn.nhomdv['IdNhomDichVu'])
    df = df.join(conn.phong, df.MaPhongChiDinh == conn.phong.MaKhoaPhong, 'left').select(df["*"], conn.phong['IdPhong'].alias('IdPhongChiDinh'))
    df = df.join(conn.trangthai, df.TrangThai == conn.trangthai.TenTrangThai, 'left').select(df["*"], conn.trangthai['IdTrangThai'])
    df = df.join(conn.hoatchat, df.TenHoatChat == conn.hoatchat.TenHoatChat, 'left').select(df["*"], conn.hoatchat['IdHoatChat'])
    df = df.withColumn('NgayLayMau', F.substring('ThoiGianLayMau', 1, 8))\
                .withColumn('GioLayMau', F.substring('ThoiGianLayMau', 9, 6))
    df = df.withColumn('NgayKetQua', F.substring('ThoiGianKetQua', 1, 8))\
                .withColumn('GioKetQua', F.substring('ThoiGianKetQua', 9, 6))
    df = df.withColumn('NgayChiDinh', F.substring('ThoiGianChiDinh', 1, 8))\
                .withColumn('GioChiDinh', F.substring('ThoiGianChiDinh', 9, 6))
    df = df.withColumn('DateDongBo', F.substring('NgayDongBo', 1, 8))
    df = df.join(conn.date, df.DateDongBo == conn.date.NgayFull, 'left').select(df["*"], conn.date['IdDate'])
    df.drop("NgayDongBo", "DateDongBo")
    load = df.select('IdDate', 'IdQuanHuyen', 'BHYTTra', 'BNDCT', 'IdBenhNhan', 'IdDonViTinh', 'GiaBHYT', 'GiaDichVu', 'IdLoaiBenhAn', 'IdCSKCB', 'IdDichVu', 'IdDoiTuongTT', 'IdKhoaChiDinh', 'IdLoaiDichVu', 'IdNgheNghiep', 'IdNhomDichVu', 'IdPhongChiDinh', 'NgayDuyetBHYT', 'NgayDuyetKeToan', 'SoLuong', 'ThanhTien', 'NgayChiDinh', 'GioChiDinh', 'NgayKetQua', 'GioKetQua', 'NgayLayMau', 'GioLayMau', 'IdTrangThai', 'TyLeBHYTThanhToan', 'HamLuong', 'IdHoatChat')
    load = load.withColumn('IdFactChiDinhDichVu', F.monotonically_increasing_id())
    tmp = spark.read.format("jdbc").options(**conn.factChiDinhDv).load().select(load.schema.names)
    res = load.subtract(tmp)
    res.write.mode("append").format("jdbc").options(**conn.factChiDinhDv).save()
    
    # load.write.mode("append").format("jdbc").options(**factChiDinhDv).save()
    
    print('Done fact chi dinh dich vu')
    #TODO: #join happen duplicated, #end > #begin
    #TODO: id ko tang dan ???

def loadFactHoaDon(spark, conn):
    data = spark.read.format("mongo").option("uri", f"mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.thongtinhoadonchitiet_1?authSource=admin").load()
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
    print('Done fact hoa don')
    
def loadFactTtdt(spark, conn):
    # ### Insert fact thongtindieutri
    data = spark.read.format("mongo").option("uri", f"mongodb://root:89Oc6q5!tFNEXYQO@10.56.10.100:27018/thaibinh.p_thongtindieutri_new?authSource=admin").load()
    data_explored = data.withColumn("Khoa", F.explode("DieuTriKhoa"))
    dataStandard = data_explored.select( 
                                    "BenhChinhRaVien",
                                    "NgayDongBo",
                                    "MaBN",
                                    "NgayHeThong",
                                    "Khoa.Khoa",
                                    "Khoa.Phong",
                                    "Khoa.ThoiGianVaoKhoa",
                                    "Khoa.ThoiGianRaKhoa",
                                    "Khoa.DoiTuongVaoVien",
                                    "Khoa.LoaiBenhAn",
                                    "Khoa.HinhThucVaoVien",
                                    "Khoa.XuTriKhamBenh",
                                    "NgayHeThong",
                                    "Khoa.ChuyenVienTheoYeuCau",
                                    "Khoa.ThoiGianChuyenVien",
                                    "Khoa.ThoiGianTuVong",
                                    "Khoa.TuVong",
                                    "Khoa.SoLanCoThai",
                                    "Khoa.SoConTrong1LanSinh",
                                    "Khoa.NgayKinhCuoiKy",
                                    "Khoa.NgaySInhDuKien",
                                    "Khoa.TienLuongDe",
                                    "Khoa.TuNgay",
                                    "Khoa.DenNgay",
                                    "GioiTinh",
                                    "MaCSKCB",
                                    "MaNgheNghiep",
                                    "TenQuanHuyen",
                                    "TenTinhThanh",
                                    "MoCapCuu",
                                    "NgayBatDauDieuTriARV",
                                    "NgayBatDauDieuTriINH",
                                    "NgayBatDauDieuTriLAO",
                                    "NgayBatDauDieuTriThuocARV",
                                    "NgayHenKhamKeTiep",
                                    "NgayLaySTT",
                                    "NgayPhatThuoc",
                                    "NgayRaVien",
                                    "NgayVaoVien",
                                    "TrangThai")
    joinedData = dataStandard.join(conn.benhChinhRaVien, dataStandard.BenhChinhRaVien == conn.benhChinhRaVien.TenBenhChinhRaVien, 'left').select(dataStandard['*'], conn.benhChinhRaVien['IdBenhChinhRaVien'])
    joinedData = joinedData.drop("BenhChinhRaVien")
    joinedData = joinedData.withColumn('DateDongBo', F.substring('NgayDongBo', 1, 8))
    joinedData = joinedData.join(conn.dt, joinedData.DateDongBo == conn.dt.NgayFull, 'left').select(joinedData['*'], conn.dt['IdDate'])
    joinedData = joinedData.join(conn.benhnhan, joinedData.MaBN == conn.benhnhan.MaBenhNhan, 'left').select(joinedData['*'], conn.benhnhan['IdBenhNhan'])
    joinedData = joinedData.join(conn.khoa, joinedData.Khoa == conn.khoa.TenKhoa, 'left').select(joinedData['*'], conn.khoa['IdKhoa'])
    joinedData = joinedData.join(conn.phong, joinedData.Phong == conn.phong.TenPhong, 'left').select(joinedData['*'], conn.phong['IdPhong'])
    joinedData = joinedData.withColumn('NgayVaoKhoa', F.substring('ThoiGianVaoKhoa', 1, 8))\
            .withColumn('GioVaoKhoa', F.substring('ThoiGianVaoKhoa', 9, 6))
    joinedData = joinedData.withColumn('NgayRaKhoa', F.substring('ThoiGianRaKhoa', 1, 8))\
            .withColumn('GioRaKhoa', F.substring('ThoiGianRaKhoa', 9, 6))
    joinedData = joinedData.join(conn.doituongvaovien, joinedData.DoiTuongVaoVien == conn.doituongvaovien.TenDoiTuongVaoVien, 'left').select(joinedData['*'], conn.doituongvaovien['IdDoiTuongVaoVien'])
    joinedData = joinedData.join(conn.loaiba, joinedData.LoaiBenhAn == conn.loaiba.TenLoaiBenhAn, 'left').select(joinedData['*'], conn.loaiba['IdLoaiBenhAn'])
    joinedData = joinedData.join(conn.xutrikhambenh, joinedData.XuTriKhamBenh == conn.xutrikhambenh.XuTriKhamBenh, 'left').select(joinedData['*'], conn.xutrikhambenh['IdXuTriKhamBenh'])
    joinedData = joinedData.withColumn('NgayTiep', F.substring('NgayHeThong', 1, 8))\
            .withColumn('GioTiep', F.substring('NgayHeThong', 9, 6))
    joinedData = joinedData.withColumn('NgayChuyenVien', F.substring('ThoiGianChuyenVien', 1, 8))\
            .withColumn('GioChuyenVien', F.substring('ThoiGianChuyenVien', 9, 6))
    joinedData = joinedData.withColumn('NgayTuVong', F.substring('ThoiGianTuVong', 1, 8))\
            .withColumn('GioTuVong', F.substring('ThoiGianTuVong', 9, 6))
    joinedData = joinedData.join(conn.gioitinh, joinedData.GioiTinh == conn.gioitinh.GioiTinh, 'left').select(joinedData['*'], conn.gioitinh['IdGioiTinh'])
    joinedData = joinedData.join(conn.cskcb, joinedData.MaCSKCB == conn.cskcb.MaCSKCB, 'left').select(joinedData['*'], conn.cskcb['IdCSKCB'])
    joinedData = joinedData.join(conn.nghenghiep, joinedData.MaNgheNghiep == conn.nghenghiep.MaNgheNghiep, 'left').select(joinedData['*'], conn.nghenghiep['IdNgheNghiep'])
    joinedData = joinedData.join(conn.quanhuyen, joinedData.TenQuanHuyen == conn.quanhuyen.TenQuanHuyen, 'left').select(joinedData['*'], conn.quanhuyen['IdQuanHuyen'])
    joinedData = joinedData.join(conn.trangthai, joinedData.TrangThai == conn.trangthai.TenTrangThai, 'left').select(joinedData['*'], conn.trangthai['IdTrangThai'])
    loads = joinedData.select('IdBenhChinhRaVien', 'IdDate', 'IdBenhNhan',
                            'IdKhoa', 'IdPhong', 'NgayVaoKhoa',
                            'GioVaoKhoa', 'NgayRaKhoa', 'GioRaKhoa',
                            'IdDoiTuongVaoVien', 'IdLoaiBenhAn', 'IdXuTriKhamBenh',
                            'NgayTiep', 'GioTiep', 'ChuyenVienTheoYeuCau', 'NgayChuyenVien',
                            'GioChuyenVien', 'NgayTuVong', 'GioTuVong', 'TuVong', 'SoLanCoThai',
                            'SoConTrong1LanSinh', 'NgayKinhCuoiKy', 'NgaySInhDuKien', 'TienLuongDe',
                            'TuNgay', 'DenNgay', 'IdGioiTinh', 'IdCSKCB', 'IdNgheNghiep',
                            'IdQuanHuyen', 'MoCapCuu', 'NgayBatDauDieuTriARV',
                            'NgayBatDauDieuTriINH', 'NgayBatDauDieuTriLAO', 'NgayBatDauDieuTriThuocARV',
                            'NgayHenKhamKeTiep', 'NgayLaySTT', 'NgayPhatThuoc', 'NgayRaVien', 'NgayVaoVien', 'IdTrangThai')
    loads = loads.withColumnRenamed('SoConTrong1LanSinh', 'SoConSinhTrongMotLan')\
                .withColumnRenamed('NgaySinhDuKien', 'NgaySInhDuKien')
    loads = loads.withColumn('IdFactThongTinDieuTri', F.monotonically_increasing_id())
    loads.count()
    
    tmp = spark.read.format("jdbc").options(**conn.optionsFactThongTinDieuTri).load().select(loads.schema.names)
    res = loads.subtract(tmp)
    res.write.mode("append").format("jdbc").options(**conn.optionsFactThongTinDieuTri).save()
    print('Done fact thong tin dieu tri')
    
def loadFactTable(conn):
    spark = conn.getSpark()
    loadFactTtxn(spark, conn)
    loadFactCddv(spark, conn)
    loadFactHoaDon(spark, conn)
    loadFactTtdt(spark, conn)

class loadFactOperator(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
     
    def execute(self, context):
        conn = Connect()
        loadFactTable(conn)
        
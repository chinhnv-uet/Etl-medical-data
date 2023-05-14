from airflow.models.baseoperator import BaseOperator
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from operators.medicThaiBinh.db.sparkTb import Connect

class loadFactTtdt(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
     
    def execute(self, context):
        conn = Connect()
        spark = conn.getSpark()
        # ### Insert fact thongtindieutri
        data = spark.read.format("mongo").option("uri", f"{conn.uriMongo}.p_thongtindieutri_new?authSource=admin").load()
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
        self.log.info('Done fact thong tin dieu tri')
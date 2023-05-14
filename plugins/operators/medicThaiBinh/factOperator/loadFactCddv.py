from airflow.models.baseoperator import BaseOperator
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from operators.medicThaiBinh.db.sparkTb import Connect
from airflow.hooks.base import BaseHook

class loadFactCddv(BaseOperator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        
     
    def execute(self, context):
        conn = Connect()
        spark = conn.getSpark()

        # insert fact chi dinh dich vu
        data = spark.read.format("mongo").option("uri", f"{conn.uriMongo}.ChiDinhDichVu?authSource=admin").load()
        print("Read ok")
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
        self.log.info("Join ok")
        load = df.select('IdDate', 'IdQuanHuyen', 'BHYTTra', 'BNDCT', 'IdBenhNhan', 'IdDonViTinh', 'GiaBHYT', 'GiaDichVu', 'IdLoaiBenhAn', 'IdCSKCB', 'IdDichVu', 'IdDoiTuongTT', 'IdKhoaChiDinh', 'IdLoaiDichVu', 'IdNgheNghiep', 'IdNhomDichVu', 'IdPhongChiDinh', 'NgayDuyetBHYT', 'NgayDuyetKeToan', 'SoLuong', 'ThanhTien', 'NgayChiDinh', 'GioChiDinh', 'NgayKetQua', 'GioKetQua', 'NgayLayMau', 'GioLayMau', 'IdTrangThai', 'TyLeBHYTThanhToan', 'HamLuong', 'IdHoatChat')
        load = load.withColumn('IdFactChiDinhDichVu', F.monotonically_increasing_id())
        tmp = spark.read.format("jdbc").options(**conn.factChiDinhDv).load().select(load.schema.names)
        res = load.subtract(tmp)
        # res = res.limit(200000)
        # print("Start write:", res.count())
        res.write.mode("append").format("jdbc").options(**conn.factChiDinhDv).save()
        
        # load.write.mode("append").format("jdbc").options(**factChiDinhDv).save()
        
        self.log.info('Done fact chi dinh dich vu')
        #TODO: #join happen duplicated, #end > #begin
        #TODO: id ko tang dan ???
        
        
#         delete from MEDIC_THAIBINH_DWH.DIM_BACSI;
# delete from MEDIC_THAIBINH_DWH.DIM_BENHCHINHRAKHOA;
# delete from MEDIC_THAIBINH_DWH.DIM_BENHCHINHRAVIEN;
# delete from MEDIC_THAIBINH_DWH.DIM_BENHCHINHVAOKHOA;
# delete from MEDIC_THAIBINH_DWH.DIM_BENHNHAN;
# delete from MEDIC_THAIBINH_DWH.DIM_CHANDOANRAPHONG;
# delete from MEDIC_THAIBINH_DWH.DIM_CSKCB;
# delete from MEDIC_THAIBINH_DWH.DIM_DANTOC;
# delete from MEDIC_THAIBINH_DWH.DIM_DATE;
# delete from MEDIC_THAIBINH_DWH.DIM_DICHVU;
# delete from MEDIC_THAIBINH_DWH.DIM_DOITUONGTT;
# delete from MEDIC_THAIBINH_DWH.DIM_DOITUONGVAOVIEN;
# delete from MEDIC_THAIBINH_DWH.DIM_DONVITINH;
# delete from MEDIC_THAIBINH_DWH.DIM_GIOITINH;
# delete from MEDIC_THAIBINH_DWH.DIM_HINHTHUCRAVIEN;
# delete from MEDIC_THAIBINH_DWH.DIM_HINHTHUCTT;
# delete from MEDIC_THAIBINH_DWH.DIM_HOATCHAT;
# delete from MEDIC_THAIBINH_DWH.DIM_KHOA;
# delete from MEDIC_THAIBINH_DWH.DIM_LOAIBENHAN;
# delete from MEDIC_THAIBINH_DWH.DIM_LOAIDICHVU;
# delete from MEDIC_THAIBINH_DWH.DIM_LOAIPHIEUTHU;
# delete from MEDIC_THAIBINH_DWH.DIM_LOAIPTTT;
# delete from MEDIC_THAIBINH_DWH.DIM_NGHENGHIEP;
# delete from MEDIC_THAIBINH_DWH.DIM_NHOMDICHVU;
# delete from MEDIC_THAIBINH_DWH.DIM_PHONG;
# delete from MEDIC_THAIBINH_DWH.DIM_PHUONGXA;
# delete from MEDIC_THAIBINH_DWH.DIM_QUANHUYEN;
# delete from MEDIC_THAIBINH_DWH.DIM_TIME;
# delete from MEDIC_THAIBINH_DWH.DIM_TRANGTHAI;
# delete from MEDIC_THAIBINH_DWH.DIM_XUTRIKHAMBENH;
# delete from MEDIC_THAIBINH_DWH.FACT_CHIDINHDICHVU;
# delete from MEDIC_THAIBINH_DWH.FACT_HOADON;
# delete from MEDIC_THAIBINH_DWH.FACT_THONGTINDIEUTRI;
# delete from MEDIC_THAIBINH_DWH.FACT_THONGTINXETNGHIEM;

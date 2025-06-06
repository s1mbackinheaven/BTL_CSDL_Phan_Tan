# Hệ Thống Phân Mảnh Dữ Liệu (Data Fragmentation System)

Dự án này triển khai một hệ thống phân mảnh dữ liệu trong môi trường cơ sở dữ liệu phân tán sử dụng PostgreSQL và Python.

## Tính Năng

1. **Load Dữ Liệu (loadratings)**
   - Đọc và xử lý dữ liệu từ file ratings.dat
   - Tải dữ liệu vào cơ sở dữ liệu PostgreSQL

2. **Phân Mảnh Theo Khoảng (Range Partition)**
   - Phân chia dữ liệu dựa trên khoảng giá trị rating
   - Tạo và quản lý các bảng phân mảnh

3. **Phân Mảnh Round Robin**
   - Phân phối dữ liệu đều giữa các phân mảnh
   - Cân bằng tải dữ liệu

4. **Chèn Dữ Liệu**
   - Hỗ trợ chèn dữ liệu vào phân mảnh range
   - Hỗ trợ chèn dữ liệu vào phân mảnh round robin

## Yêu Cầu Hệ Thống

- Python 3.8+
- PostgreSQL 12+
- Các thư viện Python (xem requirements.txt)

## Cài Đặt

1. Clone repository:
```bash
git clone <repository-url>
```

2. Cài đặt dependencies:
```bash
pip install -r requirements.txt
```

3. Cấu hình PostgreSQL:
   - Tạo database mới
   - Cập nhật thông tin kết nối trong file cấu hình

## Sử Dụng

1. Load dữ liệu:
```python
loadratings()
```

2. Tạo phân mảnh range:
```python
rangepartition(numberofpartitions)
```

3. Tạo phân mảnh round robin:
```python
roundrobinpartition(numberofpartitions)
```

4. Chèn dữ liệu mới:
```python
rangeinsert(userid, itemid, rating)
roundrobininsert(userid, itemid, rating)
```

## Testing

Sử dụng Assignment1Tester.py để chạy các test case:
```bash
python Assignment1Tester.py
```

## Cấu Trúc File Python

1. **Interface.py**
   - File chính chứa các hàm interface để tương tác với hệ thống (code thảm khảo)
   - Triển khai các hàm: loadratings, rangepartition, roundrobinpartition, rangeinsert, roundrobininsert
   - Xử lý kết nối và tương tác với PostgreSQL

2. **Assignment1Tester.py**
   - File kiểm thử tự động
   - Chứa các test case để kiểm tra tính đúng đắn của các hàm
   - Kiểm tra các trường hợp biên và xử lý lỗi

3. **solution.py**
   - File chứa code chính của dự án (code làm)
   - Có thể test với file ratings.dat (data lớn, chạy chậm) hoặc test_data.dat (data nhỏ, chạy nhanh) bằng cách thay đổi đường dẫn file
   - Triển khai các hàm: loadratings, rangepartition, roundrobinpartition, rangeinsert, roundrobininsert
   - Xử lý kết nối database và các thao tác với PostgreSQL 
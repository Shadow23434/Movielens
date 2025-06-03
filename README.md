# MovieLens Database Partitioning Project

### Tổng quan
Dự án này triển khai các chiến lược phân vùng cơ sở dữ liệu cho tập dữ liệu MovieLens sử dụng PostgreSQL. Nó minh họa cả hai kỹ thuật phân vùng theo phạm vi (range partitioning) và phân vùng luân phiên (round-robin partitioning) để xử lý dữ liệu đánh giá phim.

### Yêu cầu
- Python 3.3.12.3
- PostgreSQL
- Các gói Python cần thiết:
  - psycopg2

### Hướng dẫn cài đặt môi trường trên Ubuntu

#### 1. Cài đặt Python và các công cụ cần thiết
```bash
# Cập nhật package list
sudo apt update

# Cài đặt Python và các công cụ cần thiết
sudo apt install python3 python3-pip python3-venv postgresql postgresql-contrib
```

#### 2. Cài đặt PostgreSQL
```bash
# Khởi động PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Tạo database và user
sudo -u postgres psql
postgres=# CREATE DATABASE dds_assgn1;
postgres=# CREATE USER postgres WITH PASSWORD '1234';
postgres=# GRANT ALL PRIVILEGES ON DATABASE dds_assgn1 TO postgres;
postgres=# \q
```

#### 3. Thiết lập môi trường Python
```bash
# Clone repository (nếu chưa có)
git clone <repository-url>
cd movielens

# Tạo môi trường ảo
python3 -m venv venv

# Kích hoạt môi trường ảo
source venv/bin/activate

# Cài đặt các package cần thiết
pip install psycopg2-binary
```

#### 4. Kiểm tra cài đặt
```bash
# Kiểm tra Python version
python --version

# Kiểm tra PostgreSQL
psql -U postgres -d dds_assgn1 -h localhost
```

### Thiết lập Cơ sở dữ liệu
1. Tạo cơ sở dữ liệu PostgreSQL tên `dds_assgn1`
2. Thông số kết nối mặc định:
   - Host: localhost
   - Port: 5432
   - User: postgres
   - Password: 1234
   - Database: dds_assgn1

### Cấu trúc Dự án
```
movielens/
├── src/
│   ├── main.py
│   ├── database/
│   ├── partitioning/
│   └── utils/
└── tests/
    └── test_data.dat
```

### Tính năng
- Phân vùng theo phạm vi cho dữ liệu đánh giá
- Phân vùng luân phiên
- Chèn dữ liệu với cả hai phương pháp phân vùng
- Tạo thống kê phân vùng

### Cách sử dụng
1. Đảm bảo PostgreSQL đang chạy và cơ sở dữ liệu đã được tạo
2. Đặt file dữ liệu đánh giá vào đường dẫn `tests/test_data.dat`
3. Chạy script chính:
```bash
# Đảm bảo môi trường ảo đã được kích hoạt
source venv/bin/activate

# Chạy script
python src/main.py
``` 
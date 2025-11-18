#  Hệ Thống ETL Tự Động Thu Thập, Lưu Trữ, Xử Lý và Phân Tích Dữ Liệu Facebook

Dự án này xây dựng một hệ thống ETL tự động để thu thập dữ liệu quảng cáo Facebook thông qua Graph API, lưu trữ dữ liệu thô trên Google Cloud Storage (GCS), xử lý bằng Apache Spark, nạp vào Google BigQuery và trực quan hóa dữ liệu trên Power BI.  
Hệ thống được orchestration bằng Apache Airflow và triển khai hoàn toàn trên Docker.

---

##  1. Tổng quan dự án

Hệ thống cho phép:

- Tự động **thu thập dữ liệu quảng cáo Facebook** (Spend, Impression, Click, CPM, CTR, …)  
- Lưu trữ dữ liệu thô dạng JSON trên **Google Cloud Storage**  
- Xử lý, chuẩn hóa dữ liệu bằng **Apache Spark**  
- Nạp dữ liệu đã xử lý vào **Google BigQuery**  
- Kết nối và trực quan hóa trên **Power BI**  
- Vận hành hoàn toàn tự động thông qua **Apache Airflow**  

---

##  2. Công nghệ sử dụng

- **Facebook Graph API**  
- **Apache Airflow**  
- **Docker**  
- **Google Cloud Storage (GCS)**  
- **Apache Spark**  
- **Google BigQuery**  
- **Power BI**

---

##  3. Cấu trúc thư mục

~~~bash
Facebookv1/
│
├── assests/                # hình ảnh hệ thống
├── dags/                  # chứa file dag
├── logs/  
├── plugins                              
├── Makefile    # Các lệnh make hỗ trợ
├── scripts
├── secrets
├── docker-compose.yaml
├── dockerfile
├── makefile
├── requirement.txt                 
└── README.md               # File tài liệu
~~~

---

##  4. Bắt đầu với dự án

###  Yêu cầu trước

- Docker & Docker Compose  
- Python 3.9+  
- Tài khoản GCP + Service Account JSON Key  
- Facebook Marketing API Token  

---

##  5. Khởi chạy dự án

 **Chạy các lệnh bên dưới trong thư mục `Facebookv1`.**

Clone dự án:

~~~bash
git clone ...
cd Facebookv1
~~~

Cấp quyền cho Airflow ghi logs:

~~~bash
chmod 777 -R ./airflow/logs
~~~

Khởi tạo & chạy hệ thống Docker:

~~~bash
make init && make up
~~~

---

##  6. Các lệnh Make hỗ trợ

| Lệnh | Chức năng |
|------|-----------|
| `make init` | Khởi tạo môi trường |
| `make up` | Khởi chạy toàn bộ service Docker |
| `make ui` | Mở Airflow Web UI |
| `make down` | Dừng toàn bộ hệ thống |

Bạn có thể xem đầy đủ trong **Makefile**.

---

##  7. Chạy dự án

~~~bash
make init
make up
~~~

Mở giao diện Airflow:

~~~bash
make ui
~~~

---

## 📊 8. Luồng dữ liệu (ETL Pipeline)

1. **Extract**  
   Airflow → Facebook Graph API → JSON lưu vào GCS.

2. **Transform**  
   Spark → đọc JSON từ GCS → xử lý → chuẩn hóa → nạp BigQuery.

3. **Load**  
   BigQuery → Power BI kết nối trực quan hóa.

---

## 📄 Giấy phép
Dự án sử dụng cho mục đích học tập và nghiên cứu.  

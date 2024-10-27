# Hệ Thống Phân Tích Cổ Phiếu Realtime

Kho lưu trữ này chứa mã cho hệ thống phân tích cổ phiếu theo thời gian thực. Hệ thống được xây dựng bằng **Python**, **Kafka**, **Spark Streaming**, **Postgres** và **Apache Grafana**. Môi trường được xây dựng bằng Docker Compose để dễ dàng tạo các dịch vụ cần thiết trong các container Docker.

---


## Kiến Trúc

![image](https://github.com/user-attachments/assets/302a2dc0-9581-4804-848d-d0d76fa7e6c3)

## Thành phần

- **main.py**: Đây là tập lệnh Python chính.Có nhiệm vụ lấy dữ liệu cổ phiếu (chọn nhà cung cấp) từ thư viện vnstock3.Sau đó biến đổi (Transform) phù hợp để send vào Kafka.
- **ride.py**: Đây là tập lệnh với mục đích chủ yếu để tạo 1 class phục vụ cho việc biến đổi dữ liệu để lưu vào kafka
- **spark_streaming.py**: Đây là tập lệnh với mục đích xử lý dữ liệu trực tiếp.Gồm những nhiệm vụ như đọc dữ liệu trực tiếp từ topic kafka,tạo những table trong cơ sở dữ liệu Postgres,biến đổi dataframe thành những dữ liệu mong muốn rồi lưu (**writeStream**) vào trong những bảng vừa tạo.Rồi đống dữ liệu đó, chúng ta sẽ bắt đầu setup **Apache Grafana** để tiến hành trực quan hoá - phân tích dữ liệu.
- **docker-compose.yml**: Đây là tệp lệnh để setup môi trường cần có trong project này bằng docker

## Cài Đặt

### Yêu Cầu Hệ Thống

- Các yêu cầu hệ thống cần thiết (ví dụ: Python 3.8+, Docker, các thư viện phụ thuộc...)

### Hướng Dẫn Cài Đặt

1. Clone repository:
    ```bash
    git clone https://github.com/username/project-name.git
    cd project-name
    ```
2. Cài đặt các thư viện phụ thuộc:
    ```bash
    pip install -r requirements.txt
    ```

## Hướng Dẫn Sử Dụng

Hướng dẫn chi tiết cách sử dụng dự án, bao gồm các ví dụ chạy và giải thích các tham số chính.

Ví dụ:
```bash
python main.py --option value

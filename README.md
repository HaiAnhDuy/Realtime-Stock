# Hệ Thống Phân Tích Cổ Phiếu Realtime

Kho lưu trữ này chứa mã cho hệ thống phân tích cổ phiếu theo thời gian thực. Hệ thống được xây dựng bằng **Python**, **Kafka**, **Spark Streaming**, **Postgres** và **Apache Grafana**. Môi trường được xây dựng bằng Docker Compose để dễ dàng tạo các dịch vụ cần thiết trong các container Docker.

---


## Kiến Trúc

![image](https://github.com/user-attachments/assets/302a2dc0-9581-4804-848d-d0d76fa7e6c3)

## Thành phần

- **main.py**: Đây là tập lệnh Python chính.Có nhiệm vụ lấy dữ liệu cổ phiếu (chọn nhà cung cấp) từ thư viện vnstock3.Sau đó biến đổi (Transform) phù hợp để send vào Kafka.
- **ride.py**: Đây là tập lệnh với mục đích chủ yếu là tạo 1 class phục vụ cho việc biến đổi dữ liệu để lưu vào kafka
- **spark_streaming.py**: Đây là tập lệnh với mục đích xử lý dữ liệu trực tiếp.Gồm những nhiệm vụ như đọc dữ liệu trực tiếp từ topic kafka,tạo những table trong cơ sở dữ liệu Postgres,biến đổi dataframe thành những dữ liệu mong muốn rồi lưu (**writeStream**) vào trong những bảng vừa tạo.Rồi đống dữ liệu đó, chúng ta sẽ bắt đầu setup **Apache Grafana** để tiến hành trực quan hoá - phân tích dữ liệu.
- **docker-compose.yml**: Đây là tệp lệnh để setup môi trường cần có trong project này bằng docker.

## Cài Đặt

### Yêu Cầu Hệ Thống

- Các yêu cầu hệ thống cần thiết (ví dụ: Python 3.8+, Docker, các thư viện phụ thuộc...)

### Hướng Dẫn Cài Đặt

1. Clone repository:
    ```bash
    git clone https://github.com/HaiAnhDuy/Realtime-Stock.git
    cd project-name
    ```
2. Cài đặt các môi trường phụ thuộc:
    ```bash
    docker compose up -d
    ```
3. Xem các table có trong Postgres (đảm bảo đang chạy docker):
   ```bash
    docker exec -it postgres bash
    psql -U postgres -d voting;
    \d
    ```
4. Xem dữ liệu có trong Kafka (đảm bảo đang chạy docker):
   ```bash
    docker exec -it broker bash
    kafka-console-consumer --bootstrap-server localhost:29092 --topic stock_topic --from-beginning
    ```

## Hướng Dẫn Sử Dụng
1.Chạy file **main.py**:
Ví dụ:
```bash
python main.py
```
2.Chạy file **spark_streaming.py** (chạy song song 2 file):
Ví dụ:
```bash
python spark_streaming.py
```
3.Truy cập đường dẫn localhost:3000 để xem biểu đồ, phân tích dữ liệu (tk:admin; mk:admin)
Ví dụ:
- **Giá cổ phiếu trung bình trong 1 ngày**
![vag_by_day](https://github.com/user-attachments/assets/84a5b8e2-bfad-4498-8a1b-c11773d72db1)
- **Xu Hướng Cổ Phiếu (Dựa trên Open và Close)**
![trend_by_day](https://github.com/user-attachments/assets/02511aa7-8d4e-42b2-8542-5511b5e8efd6)
- **Khối lượng giao dịch cổ phiếu theo tháng**
![volume_by_months](https://github.com/user-attachments/assets/0fffe2ba-d881-48c8-8075-9b5ec6991838)
- **Kiểm tra cổ phiếu có tăng đột biến theo tuần(tính theo 5 ngày thay vì 7 ngày vì sàn giao dịch đóng cửa thứ 7 và chủ nhật)**
![mutation_check_5_days](https://github.com/user-attachments/assets/6b16f2b8-f200-452a-8ec4-428bd8ff5371)



# DE-Project---University-exam-scores-2020-data-processing-using-Apache-Spark-on-AWS

Trong project này, tôi xử lý tập dữ liệu 900k bản ghi (kích thước 2.4GB) với Apache Spark và các dịch vụ trong hệ sinh thái AWS, với các bước như sau:

1. Ban đầu, tôi có file dữ liệu 75k bản ghi, kích thước 200MB (dữ liệu nằm trong raw_data.txt).
  Tôi sử dụng file fake_data.py đã tạo để tạo thêm 12 lần số bản ghi từ dữ liệu ban đầu), và được tập dữ liệu có 900k bản ghi, kích thước 2.4GB.
  Dữ liệu được lưu trữ trong Amazon S3.

2. Tạo cụm Spark với các Node là các máy ảo EC2 để xử lý tập dữ liệu trên.

3. Dữ liệu sạch được lưu trữ trong Amazon RDS for MySQL và file .csv để phục vụ cho việc trực quan hóa và phân tích dữ liệu.

4. Xây dựng trang web (ở đây, tôi sử dụng dữ liệu thật từ điểm thi THPT Quốc gia 2023 thay vì điểm thi THPT Quốc gia 2020).
Đường dẫn đến project:  https://github.com/lhduc02/Web-Project---Look-up-test-scores-and-rankings-in-Vietnam-National-High-School-Exam-2020




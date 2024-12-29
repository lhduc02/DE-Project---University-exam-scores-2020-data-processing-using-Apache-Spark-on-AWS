# DE-Project---University-exam-scores-2020-data-processing-using-Apache-Spark-on-AWS

Trong project này, tôi xử lý tập dữ liệu 900k bản ghi (kích thước 2.4GB) với Apache Spark và các dịch vụ trong hệ sinh thái AWS, với các bước như sau:

1. Ban đầu, tôi có file dữ liệu 75k bản ghi, kích thước 200MB (dữ liệu nằm trong raw_data.txt).
  Tôi sử dụng file fake_data.py đã tạo để tạo thêm 12 lần số bản ghi từ dữ liệu ban đầu), và được tập dữ liệu có 900k bản ghi, kích thước 2.4GB.
  Dữ liệu được lưu trữ trong Amazon S3.

2. Tạo cụm Spark với các Node là các máy ảo EC2 để xử lý tập dữ liệu trên.

3. Dữ liệu sạch được lưu trữ trong Amazon RDS for MySQL và file .csv để phục vụ cho việc trực quan hóa và phân tích dữ liệu.

4. Xây dựng trang web với Streamlit để thí sinh có thể tra cứu điểm thi và vị trí xếp hạng của khối thi nào đó ở tỉnh thí sinh dự thi và trên toàn quốc. Ngoài ra, thí sinh có thể biết được mức điểm tương đương của mình vào năm 2020 (dựa trên top điểm của thí sinh, ví dụ năm 2020, thí sinh này có mức điểm 26.5 nằm trong top 2.91% điểm khối A toàn quốc, và năm năm 2019, thí sinh chỉ cần 24.2 điểm là đã có thể nằm trong top 2.91% điểm khối A toàn quốc).

  Đường dẫn đến project:  https://github.com/lhduc02/Web-Project---Look-up-test-scores-and-rankings-in-Vietnam-National-High-School-Exam-2020


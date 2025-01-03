# DE-Project---University-exam-scores-2020-data-processing-using-Apache-Spark-on-AWS

## Giới thiệu 
Trong project này, tôi xử lý tập dữ liệu 900k bản ghi (kích thước 2.4GB) với Apache Spark và các dịch vụ trong hệ sinh thái AWS, với các bước như sau:

1. Ban đầu, tôi có file dữ liệu 75k bản ghi, kích thước 200MB (dữ liệu nằm trong raw_data.txt).
  Tôi sử dụng file fake_data.py đã tạo để tạo thêm 12 lần số bản ghi từ dữ liệu ban đầu), và được tập dữ liệu có 900k bản ghi, kích thước 2.4GB.
  Dữ liệu được lưu trữ trong Amazon S3.

2. Tạo cụm Spark với các Node là các máy ảo EC2 để xử lý tập dữ liệu trên.

3. Dữ liệu sạch được lưu trữ trong Amazon RDS for MySQL và file .csv để phục vụ cho việc trực quan hóa và phân tích dữ liệu.

4. Xây dựng trang web với Streamlit để thí sinh có thể tra cứu điểm thi và vị trí xếp hạng của khối thi nào đó ở tỉnh thí sinh dự thi và trên toàn quốc. Ngoài ra, thí sinh có thể biết được mức điểm tương đương của mình vào năm 2020 (dựa trên top điểm của thí sinh, ví dụ năm 2020, thí sinh này có mức điểm 26.5 nằm trong top 2.91% điểm khối A toàn quốc, và năm năm 2019, thí sinh chỉ cần 24.2 điểm là đã có thể nằm trong top 2.91% điểm khối A toàn quốc).

  Đường dẫn đến project:  https://github.com/lhduc02/Web-Project---Look-up-test-scores-and-rankings-in-Vietnam-National-High-School-Exam-2020

---
## Cách xây dựng cụm Spark với Amazon EC2
* Tạo thủ công các máy ảo EC2 phiên bản t3.medium (4GB, 2 VCPU) với hệ điều hành Amazon Linux. Sau đó cài đặt các phần mềm cần thiết. Cần đặt các máy ảo EC2 TRONG CÙNG 1 Security Group để có thể truy cập giữa các máy ảo. Sau đó, mở các cổng 22, 4040, 7077, 8080, 8081 với source là 0.0.0.0/0.

* Cài đặt JAVA trên cả các máy ảo.
sudo yum update -y
sudo yum install -y java-11-amazon-corretto
java -version

* Cài đặt Python trên cả các máy ảo.
sudo yum install python3-pip -y

* Cài đặt Spark trên cả các máy ảo.
    - Tải Spark từ trang chủ.
wget https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar -xvf spark-3.5.3-bin-hadoop3.tgz
sudo mv spark-3.5.3-bin-hadoop3 /usr/local/spark
    - Cấu hình biến môi trường và kiểm tra
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
source ~/.bashrc
spark-shell --version

* Cấu hình Spark trên các Node
    - Trên cả Master Node và Worker Node, mở file spark-env.sh:
    sudo nano $SPARK_HOME/conf/spark-env.sh

    Sau đó ghi nội dung sau vào file:
	export SPARK_MASTER_HOST=<Master-Node-Private-IP>
	export JAVA_HOME=<path/to/JAVA_HOME>
    Ví dụ như:
    export SPARK_MASTER_HOST=172.31.31.107
    export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto

    Để đọc file xem nhập địa chỉ đã đúng chưa:
    cat $SPARK_HOME/conf/spark-env.sh

    Trên Master Node, mở file:
    sudo nano $SPARK_HOME/conf/slaves

    - Trên Master Node, mở file:
    sudo nano $SPARK_HOME/conf/slaves

    Sau đó ghi nội dung sau vào file:
    <Worker-Node-1-Private-IP>
    <Worker-Node-2-Private-IP>
    Ví dụ như:
    172.31.17.95	# EC2 node 1
    172.31.11.217	# EC2 node 2
    
    Để đọc file xem nhập địa chỉ đã đúng chưa:
    cat $SPARK_HOME/conf/slaves

* Cấu hình SSH giữa các Node
    - Cài đặt SSH trên tất cả các instances:
	sudo yum install -y openssh-server openssh-clients

    - Tạo SSH key trên Master Node:
	ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

    - Show private IP ở Master Node: cat ~/.ssh/id_rsa.pub

    - Copy SSH key từ Master Node sang Worker Node:
    ssh-copy-id -i ~/.ssh/id_rsa.pub ec2-user@<Worker-Node-Private-IP>

    - Thêm private IP của Master Node vào file ~/.ssh/authorized_keys trên Master Node:
    nano ~/.ssh/authorized_keys

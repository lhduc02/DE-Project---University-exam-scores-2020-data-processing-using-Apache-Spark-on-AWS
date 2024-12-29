from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, col, to_date, expr, when, lit, rand, floor, substring, length
from pyspark.sql.types import StringType, FloatType
import html

# Đường dẫn tới tệp dữ liệu
input_file = "s3a://student-test-score-data/scores_data_900k_row.txt"

# Đọc bảng unicode
chars = []
codes = []
with open("/home/ec2-user/unicode.txt", mode="r", encoding="utf8") as file:
    unicode_table = file.read().split("\n")
    for i in unicode_table:
        i = i.split()
        chars.append(i[0])
        codes.append(i[1])

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Data Processing") \
    .config("spark.jars", "mysql-connector-j-8.4.0.jar") \
    .master("spark://172.31.31.107:7077") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

# Đọc dữ liệu và chuyển vào RDD
data_rdd = spark.sparkContext.textFile(input_file)

# Chia dữ liệu thành các partition
data_rdd = data_rdd.repartition(10)

# Định nghĩa các hàm xử lý dữ liệu
def replace_unicode(text):
    for i in range(len(chars)):
        text = text.replace(codes[i], chars[i])
    return text[:-2]

def unescape_html(text):
    return html.unescape(text)

def process_dob(dob):
    return dob[:-2]

def process_score(score):
    score = score[:-2]
    score = replace_unicode(score)
    for i in range(len(score)):
        if score[i:i+2] == "&#":
            score = score[:i] + html.unescape(score[i:i+5]) + score[i+6:]
    score = score.replace(":", "")
    score = score.replace("KHXH ", "KHXH   ")
    score = score.replace("KHTN ", "KHTN   ")
    score_list = score.split("   ")
    data = []
    for subject in ["Toán", "Ngữ văn", "KHXH", "KHTN", "Lịch sử", "Địa lí", "GDCD", "Sinh học", "Vật lí", "Hóa học", "Tiếng Anh"]:
        if subject in score_list:
            data.append(str(float(score_list[score_list.index(subject) + 1])))
        else:
            data.append("")
    return " ".join(data)

def to_float(value):
    if value.strip() == "":
        return None
    return float(value)

def normal(txt):
    return txt

# Chuyển RDD sang DataFrame
def rdd_to_df(rdd):
    df = rdd.toDF(["name", "dob", "score"])
    return df

# Định nghĩa các UDF
replace_unicode_udf = udf(replace_unicode, StringType())
unescape_html_udf = udf(unescape_html, StringType())
process_dob_udf = udf(process_dob, StringType())
process_score_udf = udf(process_score, StringType())
to_float_udf = udf(to_float, FloatType())
normal_udf = udf(normal, StringType())

# Thông tin kết nối MySQL
url = "jdbc:mysql://mydb.cfgiqouyoed5.ap-southeast-2.rds.amazonaws.com:3306/big_data"
properties = {
    "user": "lhduc02",
    "password": "****",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Xử lý dữ liệu
filtered_rdd = data_rdd.filter(lambda line: len(line.strip()) > 0) \
    .map(lambda line: line.strip()) \
    .filter(lambda line: line.startswith("b'<!DOCTYPE html>")) \
    .map(lambda line: line.split("\\n")) \
    .filter(lambda arr: len(arr) == 90) \
    .map(lambda arr: (arr[61].strip(), arr[64].strip(), arr[67].strip()))

data_df = rdd_to_df(filtered_rdd)

processed_df = data_df.withColumn("name", replace_unicode_udf("name")) \
    .withColumn("name", unescape_html_udf("name")) \
    .withColumn("score", process_score_udf("score")) \
    .withColumn("dob", process_dob_udf("dob")) \
    .withColumn("toan", split(normal_udf("score"), " ")[0]) \
    .withColumn("ngu_van", split(normal_udf("score"), " ")[1]) \
    .withColumn("ngoai_ngu", split(normal_udf("score"), " ")[10]) \
    .withColumn("lich_su", split(normal_udf("score"), " ")[4]) \
    .withColumn("dia_ly", split(normal_udf("score"), " ")[5]) \
    .withColumn("gdcd", split(normal_udf("score"), " ")[6]) \
    .withColumn("khxh", split(normal_udf("score"), " ")[2]) \
    .withColumn("sinh_hoc", split(normal_udf("score"), " ")[7]) \
    .withColumn("vat_li", split(normal_udf("score"), " ")[8]) \
    .withColumn("hoa_hoc", split(normal_udf("score"), " ")[9]) \
    .withColumn("khtn", split(normal_udf("score"), " ")[3])

processed_df = processed_df.drop('score')

# Thêm cột 'dob'
processed_df = processed_df.withColumn("dob", to_date(col("dob"), "dd/MM/yyyy"))
processed_df = processed_df.na.replace("", None)

# Tạo cột 'sbd' với giá trị ngẫu nhiên từ 1000000 đến 64999999 (trừ những sbd có ãm tỉnh là 20 vì không tồn tại tỉnh này)
processed_df = processed_df.withColumn(
    "sbd",
    when(
        rand() < 0.35,  # 50% khả năng chọn khoảng đầu tiên
        (floor(rand() * (19999999 - 1000001 + 1)) + 1000001).cast("int")
    ).otherwise(  # 50% khả năng chọn khoảng thứ hai
        (floor(rand() * (64999999 - 21000001 + 1)) + 21000001).cast("int")
    )
)

# Tạo cột 'ma_tinh' là 6 ký tự cuối của 'sbd'
processed_df = processed_df.withColumn("ma_tinh", expr("substring(sbd, 1, length(sbd) - 6)"))

# Danh sách các tỉnh
tinh = [
    'Hà Nội', 'Hồ Chí Minh', 'Hải Phòng', 'Đà Nẵng', 'Hà Giang', 'Cao Bằng', 'Lai Châu', 'Lào Cai', 'Tuyên Quang',
    'Lạng Sơn', 'Bắc Kạn', 'Thái Nguyên', 'Yên Bái', 'Sơn La', 'Phú Thọ', 'Vĩnh Phúc', 'Quảng Ninh', 'Bắc Giang',
    'Bắc Ninh', 'Hải Dương', 'Hưng Yên', 'Hòa Bình', 'Hà Nam', 'Nam Định', 'Thái Bình', 'Ninh Bình', 'Thanh Hóa',
    'Nghệ An', 'Hà Tĩnh', 'Quảng Bình', 'Quảng Trị', 'Thừa Thiên-Huế', 'Quảng Nam', 'Quảng Ngãi', 'Kon Tum', 'Bình Định',
    'Gia Lai', 'Phú Yên', 'Đắk Lắk', 'Khánh Hòa', 'Lâm Đồng', 'Bình Phước', 'Bình Dương', 'Ninh Thuận', 'Tây Ninh',
    'Bình Thuận', 'Đồng Nai', 'Long An', 'Đồng Tháp', 'An Giang', 'Bà Rịa-Vũng Tàu', 'Tiền Giang', 'Kiên Giang', 
    'Cần Thơ', 'Bến Tre', 'Vĩnh Long', 'Trà Vinh', 'Sóc Trăng', 'Bạc Liêu', 'Cà Mau', 'Điện Biên', 'Đắk Nông', 'Hậu Giang'
]

# Tạo cột 'tinh' dựa trên 'ma_tinh'
case_expr = (
    "CASE "
    + " ".join(
        [
            f"WHEN ma_tinh = {i+1} THEN '{tinh[i]}'" if i < 19 else f"WHEN ma_tinh = {i+2} THEN '{tinh[i]}'"
            for i in range(len(tinh))
        ]
    )
    + " END"
)
processed_df = processed_df.withColumn("tinh", expr(case_expr))

# Tạo cột 'khu_vuc' của thí sinh dựa trên thông tin về tỉnh thành
khu_vuc_map = {
    'Trung du và miền núi phía Bắc': [
        "Hà Giang", "Cao Bằng", "Bắc Kạn", "Tuyên Quang", "Lào Cai", "Yên Bái", "Thái Nguyên",
        "Lạng Sơn", "Bắc Giang", "Phú Thọ", "Điện Biên", "Lai Châu", "Sơn La", "Hòa Bình"
    ],
    'Đồng bằng sông Hồng': [
        "Hà Nội", "Vĩnh Phúc", "Bắc Ninh", "Quảng Ninh", "Hải Dương", "Hải Phòng",
        "Hưng Yên", "Thái Bình", "Hà Nam", "Nam Định", "Ninh Bình"
    ],
    'Bắc Trung Bộ': [
        "Thanh Hóa", "Nghệ An", "Hà Tĩnh", "Quảng Bình", "Quảng Trị", "Thừa Thiên-Huế"
    ],
    'Duyên hải Nam Trung Bộ': [
        "Quảng Nam", "Đà Nẵng", "Quảng Ngãi", "Bình Định", "Phú Yên", "Khánh Hòa", "Ninh Thuận", "Bình Thuận"
    ],
    'Tây Nguyên': [
        "Kon Tum", "Gia Lai", "Đắk Lắk", "Đắk Nông", "Lâm Đồng"
    ],
    'Đông Nam Bộ': [
        "Hồ Chí Minh", "Đồng Nai", "Bình Dương", "Bà Rịa-Vũng Tàu", "Bình Phước", "Tây Ninh"
    ],
    'Đồng bằng sông Cửu Long': [
        "Long An", "Đồng Tháp", "An Giang", "Kiên Giang", "Cà Mau", "Bạc Liêu", "Sóc Trăng",
        "Bến Tre", "Trà Vinh", "Vĩnh Long", "Tiền Giang", "Hậu Giang", "Cần Thơ"
    ]
}

case_expr_khu_vuc = "CASE " + " ".join(
    [f"WHEN tinh IN ({', '.join([repr(tinh) for tinh in tinh_list])}) THEN '{kv}'" for kv, tinh_list in khu_vuc_map.items()]
) + " ELSE NULL END"
processed_df = processed_df.withColumn("khu_vuc", expr(case_expr_khu_vuc))

# Tạo cột 'so_bai_thi' là số môn thi có điểm (NOT NULL) của thí sinh
mon_thi = ["toan", "ngu_van", "ngoai_ngu", "lich_su", "dia_ly", "gdcd", "sinh_hoc", "vat_li", "hoa_hoc"]
processed_df = processed_df.withColumn("so_bai_thi", sum(col(mon).isNotNull().cast("int") for mon in mon_thi))

# Thêm cột 'nam_thi' với giá trị mặc định là 2020
processed_df = processed_df.withColumn("nam_thi", lit(2020))

# Thêm cột 'ban_khtn' với giá trị 1 nếu có điểm của 1 trong 3 môn Vật lí, Hóa học, Sinh học
processed_df = processed_df.withColumn(
    "ban_khtn", when((col("vat_li").isNotNull()) | (col("hoa_hoc").isNotNull()) | (col("sinh_hoc").isNotNull()), 1).otherwise(0)
)

# Thêm cột 'ban_khxh' với giá trị 1 nếu có điểm của 1 trong 3 môn Lịch sử, Địa lí, GDCD
processed_df = processed_df.withColumn(
    "ban_khxh", when((col("lich_su").isNotNull()) | (col("dia_ly").isNotNull()) | (col("gdcd").isNotNull()), 1).otherwise(0)
)

print(processed_df.show())

# Lưu DataFrame vào MySQL
processed_df.write.jdbc(url=url, table="du_lieu_thi_sinh", mode="append", properties=properties)

print("Hoàn tất xử lý.")

# Đóng SparkSession
spark.stop()

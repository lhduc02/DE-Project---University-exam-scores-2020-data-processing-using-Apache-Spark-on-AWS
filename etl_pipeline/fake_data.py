# Mở file1 để đọc
with open("C:\\Users\ADMIN\\Repo\\Data_processing\\raw_data.txt", mode="r", encoding="utf8") as file1:
    # Mở file2 để ghi
    with open("C:\\Users\ADMIN\\Repo\\Data_processing\\scores_data_900k_row.txt", mode="a", encoding="utf8") as file2:
        # Lặp lại 12 lần
        for i in range(12):
            # Di chuyển con trỏ về đầu file1
            file1.seek(0)
            # Đọc nội dung của file1
            file1_content = file1.read()
            # Ghi nội dung của file1 vào file2
            file2.write(file1_content)


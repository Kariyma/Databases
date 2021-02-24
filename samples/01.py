import mysql.connector

conn = mysql.connector.connect(
    host='192.168.1.103',
    user='py',
    passwd='py'
)

print(conn)
cursor = conn.cursor()
print(cursor)
cursor.execute('CREATE DATABASE my_test_db123 CHARACTER SET utf8 COLLATE utf8_unicode_ci')

'''
cursor.execute("SELECT * FROM table_name")
# Получаем данные.
row = cursor.fetchone()
print(row)

# Разрываем подключение.
conn.close()
'''
from mysql.connector import connect, Error

try:
    with connect(
        host="localhost",
        user=input("Имя пользователя: "),
        password='py',
    ) as connection:
        print(connection)
except Error as e:
    print(e)

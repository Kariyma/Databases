import getpass as gp
from mysql.connector import connect, Error


try:
    with connect(
        host="localhost",
        password=getpass("Пароль: "),
        user=input("Имя пользователя: ")
    ) as connection:
        print(connection)
except Error as e:
    print(e)

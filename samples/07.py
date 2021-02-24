from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config
import datetime


def insert_car(id_a, name, make, model, year, timestamp):
    query = "INSERT INTO table_name(id_a, name, make, model, year, timestamp) "
    "VALUES (%s,%s,%s,%s,%s,%s)"
    args = (id_a, name, make, model, year, timestamp)

    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query, args)

        if cursor.lastrowid:
            print('last insert id', cursor.lastrowid)
        else:
            print('last insert id not found')

        conn.commit()
    except Error as error:
        print('Error')
        print(error)

    finally:
        cursor.close()
        conn.close()


def main():
    insert_car(6, 'Запорожец2', 'Украина', 'ЗАЗ', '2020', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
    assigneelist = [111, 5555, 2552, 666]

if __name__ == '__main__':
    main()

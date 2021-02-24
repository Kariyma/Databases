import mysql.connector
from mysql.connector import Error


def connect():
    """ Connect to MySQL database 192.168.1.103"""
    try:
        conn = mysql.connector.connect(host='localhost',
                                       database='python',
                                       user='py',
                                       password='py')
        if conn.is_connected():
            print('Connected to MySQL database')

    except Error as e:
        print(e)

    finally:
        conn.close()
        print('Close')


if __name__ == '__main__':
    connect()

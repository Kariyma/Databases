from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config


def create_table():
    query = "CREATE TABLE `python`.`st_tasks` ( `key` INT NOT NULL AUTO_INCREMENT , `assignee` INT NOT NULL , \
            `status` VARCHAR(60) NOT NULL , `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , \
            `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , PRIMARY KEY (`key`)) ENGINE = InnoDB;"
    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Error as error:
        print('Error')
        print(error)
    finally:
        cursor.close()
        conn.close()


def drop_table():
    query = "DROP TABLE `python`.`st_tasks`;"
    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Error as error:
        print('Error')
        print(error)
    finally:
        cursor.close()
        conn.close()


def insert_car(id_a, name, make, model, year, timestamp):
    query = "INSERT INTO table_name(id_a, name, make, model, year, timestamp) VALUES (%s,%s,%s,%s,%s,%s)"
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


if __name__ == '__main__':
    create_table()

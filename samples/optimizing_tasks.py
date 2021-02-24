from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config
import numpy as np


def create_table():
    query = "CREATE TABLE IF NOT EXISTS `python`.`st_tasks` ( `key` INT NOT NULL AUTO_INCREMENT , \
            `assignee` INT NOT NULL , `status` VARCHAR(60) NOT NULL , \
            `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , \
            `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , PRIMARY KEY (`key`)) ENGINE = InnoDB;"
    try:
        db_config = read_db_config()
        with MySQLConnection(**db_config) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
    except Error as error:
        print('Error')
        print(error)


def drop_table():
    query = "DROP TABLE IF EXISTS `python`.`st_tasks`;"
    try:
        db_config = read_db_config()
        with MySQLConnection(**db_config) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
    except Error as error:
        print('Error')
        print(error)


def fill_table_tests_data(number_total_tasks: int, number_total_assignee: int):
    print('Всего задач в таблице', number_total_tasks)
    print('Всего исполнителей', number_total_assignee)


def optimizing_tasks(assignee_list: list, number_day_red_line: int):
    print('Список исполнителей для оптимизации', *assignee_list)
    print('Сколько дней простоя оптимизируем', number_day_red_line)


def main():
    number_total_tasks = 100
    number_total_assignee = 20
    number_day_red_line = 14
    number_assignee_optimizing = 10

    assignee_list = np.random.choice(range(1, number_total_assignee+1), number_assignee_optimizing, False)

    create_table()

    fill_table_tests_data(number_total_tasks, number_total_assignee)

    optimizing_tasks(assignee_list, number_day_red_line)

    # drop_table()
    print('I\'m main')


if __name__ == '__main__':
    main()

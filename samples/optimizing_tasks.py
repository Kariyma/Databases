from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config
import numpy as np
import datetime


def create_table(full_table_name):
    query = "CREATE TABLE IF NOT EXISTS " + full_table_name + " ( `key` INT NOT NULL AUTO_INCREMENT , \
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


def drop_table(full_table_name):
    query = "DROP TABLE IF EXISTS " + full_table_name + ";"
    try:
        db_config = read_db_config()
        with MySQLConnection(**db_config) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
    except Error as error:
        print('Error')
        print(error)


def create_table_st_tasks(full_table_name: str, number_total_tasks: int, number_total_assignee: int, status_list: list,
                          deep=181):
    """
    Создаём таблицу задач с именем full_table_name и заполняем её случайными данными.
    :param full_table_name: Полное имя таблицы (с базой данных)
    :param number_total_tasks: Всего записей в таблице
    :param number_total_assignee: Всего возможных исполнителей задачи. Генерируем случайное число в этом пределе.
    :param status_list: Список возможных статусов задачи. Выбираем случайным образом из него значение.
    :param deep: Глубина таблицы в днях от текущей даты.
    :return:
    """
    '''
    print("Полное имя базы данных", full_table_name)
    print('Список возможных статусов', *status_list)
    print('Всего задач в таблице', number_total_tasks)
    print('Всего исполнителей', number_total_assignee)
    print('Глубина таблицы в днях', deep)
    '''

    query = "CREATE TABLE IF NOT EXISTS " + full_table_name + " ( `key` INT NOT NULL AUTO_INCREMENT , \
            `assignee` INT NOT NULL , `status` VARCHAR(60) NOT NULL , \
            `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , \
            `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , PRIMARY KEY (`key`)) ENGINE = InnoDB;"
    try:
        db_config = read_db_config()
        with MySQLConnection(**db_config) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            print('Создана таблица', full_table_name)
            for task in range(number_total_tasks):
                cur_assignee = np.random.randint(1, number_total_assignee+1)
                cur_status = status_list[np.random.randint(len(status_list))]

                cur_date_created = datetime.datetime.now() - datetime.timedelta(np.random.randint(deep))
                cur_date_update = cur_date_created + datetime.timedelta(np.random.randint((datetime.datetime.now() -
                                                                                           cur_date_created).days)+1)
                '''
                print('=========== Вычисленно ===============')
                print('cur_assignee', cur_assignee)
                print('cur_status', cur_status)
                print('cur_date_created', cur_date_created)
                print('cur_date_update', cur_date_update)
                '''

                query = "INSERT INTO " + full_table_name + " (`assignee`, `status`, `updated`, `created`) \
                VALUES (%s,%s,%s,%s)"
                args = (cur_assignee, cur_status, str(cur_date_update), str(cur_date_created))

                cursor.execute(query, args)
                conn.commit()
    except Error as error:
        print('Error')
        print(error)


def test(full_table_name: str, number_total_tasks: int, number_total_assignee: int, status_list: list, deep=181):
    print("Полное имя базы данных", full_table_name)
    print('Список возможных статусов', *status_list)
    print('Всего задач в таблице', number_total_tasks)
    print('Всего исполнителей', number_total_assignee)
    print('Глубина таблицы в днях', deep)

    for task in range(number_total_tasks):
        cur_assignee = np.random.randint(1, number_total_assignee)
        cur_status = status_list[np.random.randint(len(status_list))]

        cur_date_created = datetime.datetime.now() - datetime.timedelta(np.random.randint(deep))
        cur_date_update = cur_date_created + datetime.timedelta(np.random.randint((datetime.datetime.now() -
                                                                                   cur_date_created).days))
        print('=========== Вычисленно для записи {} ==============='.format(task))
        print('cur_assignee', cur_assignee)
        print('cur_status', cur_status)
        print('cur_date_created', cur_date_created)
        print('cur_date_update', cur_date_update)


def fill_table_tests_data(full_table_name: str, number_total_tasks: int, number_total_assignee: int, status_list: list):
    # query = "INSERT INTO %s (`key`, `assignee`, `status`, `updated`, `created`)"
    # "VALUES (%s,%s,%s,%s,%s,%s)"
    # args = (full_table_name, 'NULL', np.random.randint(1, number_total_assignee),
    #        status_list[np.random.randint(len(status_list))], model, year, timestamp)

    print("Полное имя базы данных", full_table_name)
    print('Список возможных статусов', *status_list)
    print('Всего задач в таблице', number_total_tasks)
    print('Всего исполнителей', number_total_assignee)


def optimizing_tasks(full_table_name, assignee_list: list, number_day_red_line: int):
    print("Полное имя базы данных", full_table_name)
    print('Список исполнителей для оптимизации', *assignee_list)
    print('Сколько дней простоя оптимизируем', number_day_red_line)


def main():
    number_total_tasks = 100
    number_total_assignee = 20
    number_day_red_line = 14
    number_assignee_optimizing = 10
    status_list = ['Open', 'On support side', 'Verifying', 'Close', 'Skip', 'Nothing']
    optimising_status_list = ['Open', 'On support side', 'Verifying']
    full_table_name = 'python.test_st_tasks'

    assignee_list = np.random.choice(range(1, number_total_assignee+1), number_assignee_optimizing, False)

    create_table(full_table_name)

    fill_table_tests_data(full_table_name, number_total_tasks, number_total_assignee, status_list)

    optimizing_tasks(full_table_name, assignee_list, number_day_red_line)

    # drop_table(full_table_name)
    print(*optimising_status_list)
    print('I\'m main')


if __name__ == '__main__':
    # main()
    create_table_st_tasks('test_st_tasks100', 100, 7, ['Open', 'On support side', 'Verifying', 'Close'])

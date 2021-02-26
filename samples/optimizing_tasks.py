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

                query = "INSERT INTO " + full_table_name + " (`assignee`, `status`, `updated`, `created`) \
                VALUES (%s,%s,%s,%s)"

                args = (cur_assignee, cur_status, str(cur_date_update), str(cur_date_created))

                cursor.execute(query, args)
                conn.commit()
    except Error as error:
        print('Error')
        print(error)


def optimizing_tasks(full_table_name, assignee_list: list, optimising_status_list: list, number_day_red_line: int):
    """
    Оптимизируем нагрузку по задачам
    :param full_table_name: Полное имя таблицы с задачами
    :param assignee_list: Список исполнителей чьи задачи оптимизируем, и между которыми оптимизируем
    :param optimising_status_list: Список статусов задач попадающих под оптимизацию
    :param number_day_red_line: Количество дней назад от текущей даты. Задачи, в которых дата обновления меньше,
     подлежат оптимизации
    :return:
    """
    # Запрос на получение задач подлежащих оптимизации
    query = "SELECT * FROM %s where DATEDIFF(NOW(), updated) > %s and status in (%s) and assignee in (%s);"
    args = (full_table_name, number_day_red_line, optimising_status_list, assignee_list)
    print("Полное имя базы данных", full_table_name)
    print('Список исполнителей для оптимизации', *assignee_list)
    print('Статусы задач, подлежащих оптимизации', *optimising_status_list)
    print('Сколько дней простоя оптимизируем', number_day_red_line)


def main():
    full_table_name = 'python.test_st_tasks100'
    assignee_list = [2, 3, 4, 6]
    optimising_status_list = ['Open', 'On support side', 'Verifying']
    number_day_red_line = 30

    # create_table_st_tasks('test_st_tasks100', 100, 7, ['Open', 'On support side', 'Verifying', 'Close'])

    optimizing_tasks(full_table_name, assignee_list, optimising_status_list, number_day_red_line)

    # drop_table(full_table_name)
    # print(*optimising_status_list)


if __name__ == '__main__':
    main()

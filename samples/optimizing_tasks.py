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
    # Запрос на создание таблицы задач подлежащих оптимизации
    query = "CREATE TEMPORARY TABLE tasks_to_optimizing (PRIMARY KEY (`key`)) \
            SELECT *, assignee as assignee_new FROM " + full_table_name + " where DATEDIFF(NOW(), updated) > " \
            + str(number_day_red_line) + " and status in " + str(tuple(optimising_status_list)) + " and assignee in " \
            + str(tuple(assignee_list)) + ";"
    try:
        db_config = read_db_config()
        with MySQLConnection(**db_config) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            query = "SELECT * FROM tasks_to_optimizing;"
            cursor.execute(query)
            tasks_to_optimizing = cursor.fetchall()
            total_tasks = cursor.rowcount
            min_workload = int(np.floor(total_tasks/len(assignee_list)))

            # Запрос на получения таблицы оптимизации таблицы исполнителей
            query = "CREATE TEMPORARY TABLE table_optimizing \
                    SELECT tt.*,  ((tt.total - tt.mini) IN (0, 1)) AS optim, \
                    IF(tt.total <= tt.mini, tt.mini - tt.total, tt.mini + 1 - tt.total) AS imper, \
                    ((tt.total <= tt.mini)*2 - 1) AS facul \
                    FROM ( \
                    SELECT GROUP_CONCAT(`key`) AS `keys`, assignee, COUNT(*) AS total, " \
                    + str(min_workload) + " AS mini \
                    FROM tasks_to_optimizing GROUP BY assignee ORDER BY total DESC) \
                    AS tt ORDER BY imper ASC, tt.total DESC;"
            cursor.execute(query)

            query = "SELECT * FROM table_optimizing;"
            cursor.execute(query)
            table_optimizing = cursor.fetchall()
            to_normal = 0
            for row in table_optimizing:
                to_normal += row[5]
            if to_normal != 0:
                query = "UPDATE table_optimizing SET imper = imper + facul, facul = 0 \
                        WHERE -facul = " + str(to_normal) + " limit " + str(to_normal) + ";"
                cursor.execute(query)
                conn.commit()
            query = "SELECT * FROM table_optimizing WHERE NOT optim;"
            cursor.execute(query)
            table_optimizing = cursor.fetchall()
            print(table_optimizing)
            tasks_del = []
            tasks_update = []
            for row in table_optimizing:
                print('{0:_^30}'.format(row[0]), '{0:_^5}'.format(row[1]), '{0:_^5}'.format(row[2]),
                      '{0:_^5}'.format(row[3]), '{0:_^5}'.format(row[4]), '{0:_^5}'.format(row[5]),
                      '{0:_^5}'.format(row[6]))
                list_tasks = row[0].split(',')
                assignee = row[1]
                imper = row[5]
                for i in range(int(np.fabs(imper))):
                    task = []
                    if imper < 0:
                        task.append(list_tasks.pop())
                        task.append(assignee)
                        tasks_del.append(task)
                    elif imper > 0:
                        task = tasks_del.pop()
                        task.append(assignee)
                        tasks_update.append(task)
            print('tasks_del', tasks_del, 'длина', len(tasks_del))
            print('tasks_update', tasks_update, 'длина', len(tasks_update))
            assert len(tasks_del) == 0, 'Распределенны не все отобранные задачи!'

            for u in tasks_update:
                # query = "UPDATE `tasks_to_optimizing` SET `assignee_new` = " + str(u[2]) + \
                #        " WHERE `tasks_to_optimizing`.`key` = " + str(u[0]) + ";"
                query = "UPDATE `tasks_to_optimizing` SET `assignee_new` = " + str(u[2]) + \
                        " WHERE `tasks_to_optimizing`.`key` = " + str(u[0]) + ";"
                cursor.execute(query)
                conn.commit()
            query = "SELECT `key`, `assignee_new` as assignee FROM tasks_to_optimizing;"
            cursor.execute(query)
            optimized_tasks_table = cursor.fetchall()
            for row in optimized_tasks_table:
                print(row)
    except Error as error:
        print('Error')
        print(error)


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

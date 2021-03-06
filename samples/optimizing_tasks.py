from mysql.connector import MySQLConnection, Error

# модуль для чтения параметров доступа к серверу и базе данных из файла
from python_mysql_dbconfig import read_db_config

import numpy as np
import datetime


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
    # Запрос на создание временной таблицы задач подлежащих оптимизации
    is_temporary = 'TEMPORARY'
    # is_temporary = ''
    query = "CREATE " + is_temporary + " TABLE `tasks_to_optimizing` (PRIMARY KEY (`key`)) \
            SELECT `key`, `assignee`, assignee AS assignee_old FROM " + full_table_name \
            + " WHERE DATEDIFF(NOW(), `updated`) > " + str(number_day_red_line) \
            + " AND `status` IN " + str(tuple(optimising_status_list)) + " AND `assignee` IN " \
            + str(tuple(assignee_list)) + ";"
    try:
        db_config = read_db_config()
        with MySQLConnection(**db_config) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            if cursor.rowcount == 0:
                print('Не найдены задачи отвечающие заданным условиям:')
                print('исполнитель задачи входит в список исполнителей \033[32m{}\033[0m;\n'
                      'задача дольше \033[32m{}\033[0m дней находится в одном изстатусов \033[32m{}\033[0m.'
                      .format(assignee_list, number_day_red_line, optimising_status_list))
                print('В оптимизации нет необходимости.')
                return
            query = "SELECT COUNT(`key`), count(DISTINCT `assignee`), `assignee` FROM `tasks_to_optimizing`;"
            cursor.execute(query)
            # tasks_to_optimizing = cursor.fetchall()
            tasks_to_optimizing = cursor.fetchone()
            # total_tasks = cursor.rowcount
            total_tasks = int(tasks_to_optimizing[0])
            real_assignee_number = int(tasks_to_optimizing[1])
            if real_assignee_number == 1:
                print('Из списка исполнителей \033[32m{}\033[0m найден только один (\033[31m{}\033[0m), '
                      'задачи которого дольше \033[32m{}\033[0m дней находятся в статусах \033[32m{}\033[0m.'
                      .format(assignee_list, tasks_to_optimizing[2], number_day_red_line, optimising_status_list))
                print('В оптимизации нет необходимости.')
                return
            min_workload = int(np.floor(total_tasks/real_assignee_number))

            # Запрос на получение временной таблицы оптимизации таблицы исполнителей
            query = "CREATE " + is_temporary + " TABLE `table_optimizing` \
                    SELECT tt.*,  ((tt.total - tt.mini) IN (0, 1)) AS optim, \
                    IF(tt.total <= tt.mini, tt.mini - tt.total, tt.mini + 1 - tt.total) AS imper, \
                    ((tt.total <= tt.mini)*2 - 1) AS facul \
                    FROM ( \
                    SELECT GROUP_CONCAT(`key`) AS `keys`, `assignee`, COUNT(*) AS total, " \
                    + str(min_workload) + " AS mini \
                    FROM `tasks_to_optimizing` GROUP BY `assignee` ORDER BY `total` DESC) \
                    AS tt ORDER BY `optim` ASC, `imper` ASC, tt.total DESC;"
            cursor.execute(query)

            '''
            Нормализация таблицы оптимизации.
            Если в таблице оптимизации количество задач, которые надо забрать у исполнителей, не равно количеству 
            задач, которые надо добавить исполнителям (to_normal на равен нулю), тогда приводим это соотношение к 
            равентсву -  прибавляем к минимуму добполтительное число (+1 или -1).
            '''
            query = "SELECT sum(imper) AS `to_normal` FROM table_optimizing;"
            cursor.execute(query)
            table_optimizing = cursor.fetchone()
            to_normal = int(table_optimizing[0])
            if to_normal != 0:
                query_u = "UPDATE `table_optimizing` SET `imper` = `imper` + `facul`, `facul` = 0, `optim` = 0 \
                        WHERE -`facul` = " + str(to_normal) + " limit " + str(int(np.fabs(to_normal))) + ";"
                cursor.execute(query_u)
                conn.commit()
            cursor.execute(query)
            table_optimizing = cursor.fetchone()
            to_normal = int(table_optimizing[0])
            assert to_normal == 0, 'Нормализация таблицы оптимизации задач не удалась!'

            ''' Формируем массив перереспределения task_update. 
                task_update[0] - ключ задачи
                task_update[1] - текущий исполнитель
                task_update[2] - новый исполнитель
            '''
            query = "SELECT `keys`, `assignee`, `imper` FROM table_optimizing WHERE NOT `optim`;"
            cursor.execute(query)
            table_optimizing = cursor.fetchall()
            print(table_optimizing)

            tasks_del = []
            tasks_update = []
            for row in table_optimizing:
                print('{0:_^30}'.format(row[0]), '{0:_^5}'.format(row[1]), '{0:_^5}'.format(row[2]))
                list_tasks = row[0].split(',')
                assignee = row[1]
                imper = row[2]
                for i in range(int(np.fabs(imper))):
                    task = {}
                    if imper < 0:
                        task.update({'key': list_tasks.pop()})
                        task.update({'assignee_old': assignee})
                        tasks_del.append(task)
                    elif imper > 0:
                        task = tasks_del.pop()
                        task.update({'assignee_new': assignee})
                        tasks_update.append(task)

            assert len(tasks_del) == 0, 'Распределенны не все отобранные задачи!'
            for row_update in tasks_update:
                print(row_update)

            # Вносим изменения в таблицу с задачами подлежащими оптимизации
            for u in tasks_update:
                query = "UPDATE `tasks_to_optimizing` SET `assignee` = " + str(u['assignee_new']) + \
                        ", `assignee_old` = " + str(u['assignee_old']) + \
                        " WHERE `tasks_to_optimizing`.`key` = " + str(u['key']) + ";"
                cursor.execute(query)
                conn.commit()

            # Получаем искомую выборку по оптимизации распределия задач
            query = "SELECT `key`, `assignee` as assignee FROM tasks_to_optimizing;"
            cursor.execute(query)
            optimized_tasks_table = cursor.fetchall()
            optimized_tasks_dictionary = []
            optimized_tasks_dictionary_row = {}
            for row in optimized_tasks_table:
                optimized_tasks_dictionary_row.update({cursor.column_names[0]: row[0]})
                optimized_tasks_dictionary_row.update({cursor.column_names[1]: row[1]})
                optimized_tasks_dictionary.append(optimized_tasks_dictionary_row)
                print(optimized_tasks_dictionary_row)
    except Error as error:
        print('Error')
        print(error)


def main():
    # Если задача из таблицы full_table_name исполнителя из списка assignee_list дольше чем number_day_red_line дней
    # находися в одном из статуов optimising_status_list она подлежит оптимизации
    full_table_name = 'python.test_st_tasks100'                         # Полное имя базы данных
    assignee_list = [4, 6]                                              # Список исполнителей задач для оптимизации > 1
    optimising_status_list = ['Open', 'On support side', 'Verifying']   # Cписок статусов задач для оптимизации
    number_day_red_line = 10                                            # Количество дней

    test_input_data = {'Полное имя базы данных': full_table_name, 'Список исполнителей': assignee_list,
                       'Список статусов задач': optimising_status_list}

    # Testing the input data for validity
    error = False
    message_error = ''
    for test in test_input_data:
        if len(test_input_data[test]) == 0:
            error += True
            message_error += '\033[31mПараметр "' + test + '" не может быть пустым.\033[0m\n'
    if len(test_input_data['Список исполнителей']) == 1:
        error += True
        message_error += '\033[32mВ параметре "Список исполнителей" должно быть больше чем одно (1) значение.\033[0m\n'
    if type(number_day_red_line) != int:
        error += True
        message_error += '\033[31mНедопустимое значение ' + str(number_day_red_line) + \
                         ' для параметра "Количество дней". Ожидается целое число.\033[0m\n'
    elif number_day_red_line < 0:
        error += True
        message_error += '\033[31mНедопустимое значение ' + str(number_day_red_line) + \
                         ' для параметра "Количество дней". Ожидается положительное число.\033[0m\n'
    if error:
        print(message_error)
        return

    # Создание тестовой таблицы со случайными данными
    # create_table_st_tasks('test_st_tasks100', 100, 7, ['Open', 'On support side', 'Verifying', 'Close'])

    # Оптимизируем распределение задач из таблицы по заданным условиям
    optimizing_tasks(full_table_name, assignee_list, optimising_status_list, number_day_red_line)


if __name__ == '__main__':
    main()

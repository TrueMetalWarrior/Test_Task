import datetime
from psycopg2 import connect
import psycopg2.extras
from DB_config import load_config

connectionDB = None
cursor = None


def add_processed_at(doc): # Функция добавления значения в поле processed_at в таблице documents
    cursor.execute("""
                UPDATE documents
                SET processed_at = %s
                WHERE doc_id = %s
            """, (datetime.datetime.now(), doc['doc_id']))
    connectionDB.commit()
    return


def update_data(doc, data_rows): # Функция обновления данных в таблице data
    list_of_objects = [row['object'] for row in data_rows] # Получаем список объектов из таблицы data для sql-запроса
    str_of_objects_for_sql = get_list_for_sql(list_of_objects) # Приводим полученный список к виду для sql-запроса
    str_operation_details_for_sql = "" # Подготавливаем строку для функции SET sql-запроса
    list_new_values_for_data = list() # Подготавливаем массив данных для замены в таблице data
    for key, value in doc['document_data']['operation_details'].items(): #Перебор аналогичен функции check_data, перебор осуществляем для подготовки sql-запроса
        str_operation_details_for_sql += f"""{key} = %s\n""" # Формируем построчные запросы для обновления полей таблицы дата формата owner = %s
        list_new_values_for_data.append(value['new']) # Записываем в массив новое значение из operation_details
    cursor.execute(f"""  
                    UPDATE data SET
                    {str_operation_details_for_sql}
                    WHERE object IN ({str_of_objects_for_sql})
                """, tuple(list_new_values_for_data)) # Выполняем запрос на обновление таблицы data. %s позволяет подавать любое значение для записи в таблицу
    connectionDB.commit()                             # и psycopg2 сам определит тип данных. Для работы функции нужно подать множество, поэтому преобразуем list в tuple


def get_oldest_document(): #Функция запроса данных из таблицы documents согласно условию
    cursor.execute("""
                            SELECT * FROM documents 
                            WHERE document_type = 'transfer_document'
                            AND processed_at is NULL
                            ORDER BY recieved_at ASC
                            """)
    return cursor.fetchone()


def get_list_for_sql(list_of_objects): # Функция формирования данных в корректный для sql-запроса вид: "'object1', 'object2',.., object_n'"
    return "'" + "', '".join(list_of_objects) + "'"


def get_data_rows(doc): # Функция получения данных из таблицы data
    str_of_objects_for_sql = get_list_for_sql(doc['document_data']['objects']) # Получаем корректый для sql-запроса список объектов, взятый из поля document_data таблицы documents
    cursor.execute(f"""
                        SELECT * FROM data
                        WHERE parent in ({str_of_objects_for_sql}) 
                        OR object in ({str_of_objects_for_sql})""") # Запрос объектов из таблицы data согласно полю document_data таблицы documents
    return cursor.fetchall()


def check_data(doc, data_rows): # Функция проверки данных на соответствие полю operation_details таблицы documents
    for row in data_rows: # Построчная обработка данных, взятых из таблицы data
        for key, value in doc['document_data']['operation_details'].items(): # Перебираем пары ключ-значение поля operation_details (owner: new, old и status: new, old)
            if row[key] != value['old']: # Сравниваем значения из поля operation_details таблицы documents со значениями из полей owner и status таблицы data
                return False
    return True


def process_single_document():
    doc = get_oldest_document()
    if not doc: # Проверка наличия данных после запроса
        raise ValueError("Документа нет")
    data_rows = get_data_rows(doc)
    if not data_rows: # Проверка наличия данных после запроса
        raise ValueError(f"Не удалось получить данные из таблицы data в документе {doc['doc_id']}")

    if check_data(doc, data_rows):
        update_data(doc, data_rows)
        add_processed_at(doc)
        return True
    else:
        add_processed_at(doc)
        return False


if __name__ == '__main__':
    try:
        config = load_config()  # Функция для загрузки данных из ".ini" файла для подключения к БД
        connectionDB = connect(**config)    # Подключаемся к БД
        cursor = connectionDB.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # Инициализируем курсор для работы с запросами SQL
                                                                                    # Используем RealDictCursor, чтобы при запросе данные выдавались в формате словаря
        print(process_single_document())

        cursor.close()
        connectionDB.close()
    except Exception as Error:
        print(Error)

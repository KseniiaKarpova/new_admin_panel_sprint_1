import sqlite3
from contextlib import contextmanager
import os
from typing import List
import psycopg2
from psycopg2.extensions import connection as _connection
from dotenv import load_dotenv
from data import tables
from dataclasses import fields, astuple


load_dotenv()


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


class SqlExecuter:
    def __init__(self, connect):
        self.connect = connect

    def extract_data(self, table_name: str, datatype) -> List:
        # получение  всех данных из таблицы
        try:
            # название колонок
            colums_name = [field.name for field in fields(datatype)]

            curs = self.connect.cursor()
            curs.execute(f"SELECT {', '.join(colums_name)} FROM {table_name};")

            # сохранение полученных записей в тип датакласса
            result = []
            for row in curs.fetchall():
                if isinstance(row, tuple):
                    result.append(datatype(*row))
                else:
                    result.append(datatype(**row))
            return result

        except Exception as e:
            print(f'Не удалось получить данные для {datatype}. {e}')
            return []


class PostgresSaver(SqlExecuter):
    """Обработка данных для Postgres """
    def get_count_rows(self, table_name: str, colums_name: str) -> int:
        return len(self.extract_data(table_name, colums_name))

    def save(self, table_name: str, data):
        # получение названий колонок
        colums_name = [field.name for field in fields(data[0])]
        col_count = ', '.join(['%s'] * len(colums_name))

        # название колонок по котором могут быть конфликты при insert
        conflict_name_colums = [name for name in colums_name if '_id' in name]
        conflict_name_colums = 'id' if len(conflict_name_colums) == 0 \
            else ', '.join(conflict_name_colums)

        # загрузка данных в таблицу
        curs = self.connect.cursor()
        bind_values = [astuple(row) for row in data]
        query = f'''INSERT INTO content.{table_name} ({", ".join(colums_name)}) 
            VALUES ({col_count}) 
            ON CONFLICT ({conflict_name_colums}) DO NOTHING;'''
        curs.executemany(query, bind_values)


class SQLiteExtractor(SqlExecuter):
    """Обработка данных из sqlite3 """


def load_from_sqlite(connection: sqlite3.Connection,
                     pg_conn: _connection,
                     n: int):
    """Основной метод загрузки данных из SQLite в Postgres"""
    try:
        # Обработчики запросов к каждой БД
        postgres_saver = PostgresSaver(pg_conn)
        sqlite_extractor = SQLiteExtractor(connection)

        # по каждой таблице собираем данные
        for table in tables:
            # Получение данных из sqlite3
            data = sqlite_extractor.extract_data(table, tables[table])
            count_rows_sqlite = len(data)

            # Кол-во записей до вставки в Postgres
            count_before = postgres_saver.get_count_rows(table, tables[table])

            # разделение на batch по n элементов
            for i in range(0, count_rows_sqlite, n):
                postgres_saver.save(table, data)

            count_after = postgres_saver.get_count_rows(table, tables[table])

            if count_after - count_before != count_rows_sqlite:
                print('Данные потерялись или были дубли')
            else:
                print('Данные успешно загружены')

    except Exception as e:
        print(e)


if __name__ == '__main__':
    # Данные для подключения к БД
    db_path = 'db.sqlite'

    dsn = {
        'dbname': os.environ.get('dbname'),
        'user': os.environ.get('user'),
        'password': os.environ.get('password'),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': os.environ.get('DB_HOST', 5432),
        'options': '-c search_path=content',
    }

    # n - Кол-во элементов в одном батче
    n = 100

    # Создание соединений с Базами Данных
    try:
        with (conn_context(db_path) as sqlite_conn,
              psycopg2.connect(**dsn) as pg_conn):
            load_from_sqlite(sqlite_conn, pg_conn, n)
    except Exception as e:
        print(f"Не удалось подключиться к базе данных.\n{e}")

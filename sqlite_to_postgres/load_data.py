import sqlite3
from contextlib import contextmanager
import os
import psycopg2
from psycopg2.extensions import connection as _connection
from dotenv import load_dotenv
from data import tables
from Logger import logger
from data_execution import PostgresSaver, SQLiteExtractor


load_dotenv()


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@contextmanager
def conn_context_pg(settings: dict):
    conn = psycopg2.connect(**settings)
    yield conn
    conn.commit()


def load_from_sqlite(connection: sqlite3.Connection,
                     pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    try:
        # Обработчики запросов к каждой БД
        postgres_saver = PostgresSaver(pg_conn)
        sqlite_extractor = SQLiteExtractor(connection)

        # по каждой таблице собираем данные
        for table in tables:
            # Получение данных из sqlite3
            data = sqlite_extractor.extract_data(table,
                                                 tables[table].get('type'))
            count_rows_sqlite = len(data)

            # Кол-во записей до вставки в Postgres
            count_before = postgres_saver.get_count_rows(table,
                                                         tables[table].get('type'))

            postgres_saver.save(table,
                                data,
                                tables[table].get('conflict_name_colums'))

            count_after = postgres_saver.get_count_rows(table,
                                                        tables[table].get('type'))

            if count_after - count_before != count_rows_sqlite:
                logger.info('Данные потерялись или были дубли')
            else:
                logger.info('Данные успешно загружены')

    except Exception as e:
        logger.exception(e)


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

    # Создание соединений с Базами Данных
    try:
        with (conn_context(db_path) as sqlite_conn,
              conn_context_pg(dsn) as pg_conn):
            load_from_sqlite(sqlite_conn, pg_conn)

    except Exception as e:
        logger.exception(f"Не удалось подключиться к базе данных.\n{e}")

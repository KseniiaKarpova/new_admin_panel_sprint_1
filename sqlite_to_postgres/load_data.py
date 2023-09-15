import sqlite3
from contextlib import contextmanager
import os
import psycopg2
from psycopg2.extensions import connection as _connection
from dotenv import load_dotenv
from data import tables
from Logger import logger
from data_execution import PostgresSaver, SQLiteExtractor
import gc

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
                     pg_conn: _connection,
                     n=100):
    """Основной метод загрузки данных из SQLite в Postgres"""
    try:
        # Обработчики запросов к каждой БД
        postgres_saver = PostgresSaver(pg_conn)
        sqlite_extractor = SQLiteExtractor(connection)

        # по каждой таблице собираем данные
        for table in tables:
            # Получение данных из sqlite3
            count_rows_sqlite = sqlite_extractor.count_rows(table)

            # Кол-во записей до вставки в Postgres
            count_before = postgres_saver.count_rows(table)

            #выгрузка данных частями на основе UUID (послений смивол)
            for i in 'abcdefghijklmnopqrstuvwxyz0123456789':
                data = sqlite_extractor.extract_data(table,
                                                     tables[table].get('type'),
                                                     i,
                                                     n)
                count_part=len(data)
                if count_part > 0:
                    for i in range(0, count_part, n):
                        postgres_saver.save(table,
                                            data[i:i+n],
                                            tables[table].get('conflict_name_colums'))

                #освободить оперативку
                gc.collect()
            count_after = postgres_saver.count_rows(table)

            if count_after - count_before != count_rows_sqlite:
                logger.info(f'При загрузке в {table}   данные потерялись или были дубли')
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

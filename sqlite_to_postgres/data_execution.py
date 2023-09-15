from typing import List
from psycopg2.extras import execute_batch
from dataclasses import fields, astuple
from Logger import logger


class SqlExecuter:
    def __init__(self, connect):
        self.connect = connect

    def extract_data(self, table_name: str, datatype, i, n=100) -> List:
        # получение  всех данных из таблицы
        try:
            # название колонок
            colums_name = [field.name for field in fields(datatype)]

            curs = self.connect.cursor()
            curs.execute(f"SELECT {', '.join(colums_name)} FROM {table_name} where id like '%{i}';")

            # сохранение полученных записей в тип датакласса
            result = []
            while True:
                rows = curs.fetchmany(n)
                if rows:
                    for row in rows:
                        if isinstance(row, tuple):
                            result.append(datatype(*row))
                        else:
                            result.append(datatype(**row))
                else:
                    break
            curs.close()
            return result

        except Exception as e:
            logger.exception(f'Не удалось получить данные для {datatype}. {e}')
            return []

    def count_rows(self, table_name: str) -> int:
        try:
            curs = self.connect.cursor()
            curs.execute(f"SELECT COUNT(*) FROM {table_name};")

            count_rows = curs.fetchall()
            curs.close()
            return count_rows[0][0]

        except Exception as e:
            logger.exception(f'Не удалось получить кол-во данных из {table_name}. {e}')
            return 0



class PostgresSaver(SqlExecuter):
    """Обработка данных для Postgres """
    def get_count_rows(self, table_name: str, colums_name: str) -> int:
        return len(self.extract_data(table_name, colums_name))

    def save(self, table_name: str, data, conflict_name_colums: List[str]):
        try:
            # получение названий колонок
            colums_name = [field.name for field in fields(data[0])]
            col_count = ', '.join(['%s'] * len(colums_name))

            # загрузка данных в таблицу
            curs = self.connect.cursor()
            bind_values = [astuple(row) for row in data]
            query = f'''INSERT INTO content.{table_name}  
                ({", ".join(colums_name)}) 
                VALUES ({col_count}) 
                ON CONFLICT ({", ".join(conflict_name_colums)}) DO NOTHING;'''
            execute_batch(curs, query, bind_values)

            curs.close()
        except Exception as e:
            logger.exception(e)


class SQLiteExtractor(SqlExecuter):
    """Обработка данных из sqlite3 """

import psycopg2
import config


class DbConnection:
    """
    При создании экземпляра класса происходит подключение к базе данных.
    Параметры для подключения берутся из config.py.
    """

    def __init__(self, db_name):
        self.conn = psycopg2.connect(**config.foreign_db_params(db_name))
        self.curs = self.conn.cursor()

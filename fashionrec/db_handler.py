import sys
from typing import *
import pymysql
from pymysql.cursors import DictCursor

class DatabaseHandler:
    def __init__(self, host, password, user, db, port):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db
        self.conn = None

    def open_connection(self):
        """Connect to MySQL Database."""
        try:
            if self.conn is None:
                self.conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    passwd=self.password,
                    db=self.db,
                    port=self.port,
                    cursorclass=DictCursor,
                    connect_timeout=5,
                    charset='utf8',
                    use_unicode=True
                )
        except pymysql.MySQLError as e:
            print(e)
            sys.exit()
            
            
    def execute(self, query, args: tuple = None, fetch_one = False) -> List:
        try:
            self.open_connection()
            with self.conn.cursor() as cursor:
                cursor.execute(query, args)
                if fetch_one:
                    result = cursor.fetchone()
                else:
                    result = list(cursor.fetchall())
                cursor.close()
                return result
        except pymysql.MySQLError as e:
            print(e)
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None
    
    def executemany(self, query, args: List[tuple] = None):
        assert self.is_init
        try:
            self.open_connection()
            with self.conn.cursor() as cursor:
                cursor.executemany(query, args)
                result = cursor.fetchall()
                cursor.close()
                return result
        except pymysql.MySQLError as e:
            print(e)
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

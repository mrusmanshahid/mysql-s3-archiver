import pymysql
import logging

class Database:

    def __init__(self) -> None:
        pass

    def establish_connection(self, host, user, password, port):
        connection = pymysql.connect(host=host, 
                                     user=user,
                                     password=password,
                                     port=int(port),
                                     db='mysql',
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection

    def execute(self, con, sql):
        if not con:
            logging.error(f"No database connection, can't proceed")
            return
        with con.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
            con.commit()
            return result

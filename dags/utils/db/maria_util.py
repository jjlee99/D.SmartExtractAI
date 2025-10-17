from airflow.providers.mysql.hooks.mysql import MySqlHook

class DBUtil:
    def __init__(self, conn_id='maria_db_conn'):
        self.conn_id = conn_id

    def execute_query(self, query, params=None, dictionary=False, fetch=False, return_id=False):
        """
        쿼리를 실행하고 결과를 반환합니다.
        :param query: 실행할 SQL 쿼리
        :param params: 쿼리 파라미터 (튜플 또는 딕셔너리)
        :param fetch: 결과를 조회해서 반환할지 여부 (True: SELECT 등, False: INSERT/UPDATE/DELETE 등)
        :param dictionary: 결과를 딕셔너리 리스트로 받을지 여부 (fetch=True일 때 의미)
        :param return_id: INSERT 시 생성된 PK 반환 여부
        :return: 결과 데이터 (fetch=True일 때)
        """
        hook = MySqlHook(mysql_conn_id=self.conn_id)

        if fetch:
            if dictionary:
                df = hook.get_pandas_df(sql=query, parameters=params)
                result = df.to_dict(orient="records")
            else:
                result = hook.get_records(sql=query, parameters=params)
            return result
        else:
            conn = hook.get_conn()
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                result = None
                if return_id:
                    result = cursor.lastrowid
                conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                raise
            finally:
                cursor.close()
                conn.close()

    def execute_many_query(self, query, params_list):
        """
        Bulk Insert 쿼리를 실행합니다.
        :param query: 실행할 SQL 쿼리
        :param params_list: 파라미터 리스트 (튜플의 리스트)
        """
        hook = MySqlHook(mysql_conn_id=self.conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.executemany(query, params_list)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()


def execute(query, params=None, dictionary=False, fetch=False, return_id=False):
    db = DBUtil(conn_id='maria_db_conn')
    results = db.execute_query(query, params=params, fetch=fetch, return_id=return_id, dictionary=dictionary)
    return results


def execute_many(query, params_list):
    db = DBUtil(conn_id='maria_db_conn')
    db.execute_many_query(query, params_list)

#bak
# from airflow.providers.mysql.hooks.mysql import MySqlHook

# class DBUtil:
#     def __init__(self, conn_id='maria_db_conn'):
#         self.conn_id = conn_id

#     def execute_query(self, query, params=None, dictionary=False, fetch=False, return_id=False):
#         """
#         쿼리를 실행하고 결과를 반환합니다.
#         :param query: 실행할 SQL 쿼리
#         :param params: 쿼리 파라미터 (튜플 또는 딕셔너리)
#         :param fetch: 결과를 조회해서 반환할지 여부 (True: SELECT 등, False: INSERT/UPDATE/DELETE 등)
#         :return: 결과 데이터 (fetch=True일 때)
#         """
#         hook = MySqlHook(mysql_conn_id=self.conn_id)
#         conn = hook.get_conn()
#         cursor = conn.cursor(dictionary=dictionary)
#         try:
#             cursor.execute(query, params)
#             result = None
#             if fetch:
#                 result = cursor.fetchall()
#             elif return_id:
#                 result = cursor.lastrowid  # PK 반환
#             conn.commit()
#             return result
#         except Exception as e:
#             conn.rollback()
#             raise e
#         finally:
#             cursor.close()
#             conn.close()

#     def execute_many_query(self, query, params_list):
#         """
#         Bulk Insert 쿼리를 실행합니다.
#         :param query: 실행할 SQL 쿼리
#         :param params_list: 파라미터 리스트 (튜플의 리스트)
#         """
#         hook = MySqlHook(mysql_conn_id=self.conn_id)
#         conn = hook.get_conn()
#         cursor = conn.cursor()
#         try:
#             cursor.executemany(query, params_list)  # Bulk Insert
#             conn.commit()
#         except Exception as e:
#             conn.rollback()
#             raise e
#         finally:
#             cursor.close()
#             conn.close()

# def execute(query, params=None, dictionary=False, fetch=False, return_id=False):
#     db = DBUtil(conn_id='maria_db_conn')
#     results = db.execute_query(query, params=params, fetch=fetch, return_id=return_id, dictionary=dictionary)
#     return results

# def execute_many(query, params_list):
#     db = DBUtil(conn_id='maria_db_conn')
#     db.execute_many_query(query, params_list)

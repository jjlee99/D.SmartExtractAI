from logging import exception
from typing import Any, Union
from utils.db.maria_util import execute, execute_many

#insert만 벌크 실행 가능
def insert_map(key, params:Union[list,tuple]=None, fetch:bool=False, return_id:bool=False):
    map = {
        "insertRun":"INSERT INTO TB_AF_RUN(DAG_ID, RUN_ID, START_DATE, STATUS) VALUES (%s, %s, current_timestamp(), 'P')", # P(진행중)
        "insertTargetFile":"INSERT INTO TB_AF_TARGET(RUN_ID, TARGET_ID, CONTENT) VALUES (%s, %s, %s)",
        "insertClassifyResult":"INSERT INTO TB_AF_TARGET(RUN_ID, TARGET_ID, CONTENT) VALUES (%s, %s, %s)",
        "insertTranslateLog":"INSERT INTO TB_OCR_TRN_LOG (TRN_TABLE_NM, TRN_TABLE_PK, TRN_COL_ID, ORI_TEXT, TRN_TEXT) VALUES (%s, %s, %s, %s, %s)",
        "insertComplete":"INSERT INTO TB_AF_COMPLETE (RUN_ID,CONTENT,USE_YN) VALUES (%s,%s,'Y')",
        "insertCompleteMap":"INSERT INTO TB_AF_COMPLETE_MAP (COMPLETE_ID, TABLE_ID, PK_VALUE) VALUES (%s, %s, %s)",
        "insertCreateUnready":"INSERT INTO TB_AI_CREATE (DOC_CLASS_ID,LAYOUT_CLASS_ID,STATUS) VALUES (%s,%s,'U')", # U(미준비)
    }
    if isinstance(params, list):  # 벌크 삽입(벌크 입력은 return_id 지원 안함)
        print("bulk insert query : ",map[key],params)
        return execute_many(map[key], params_list=params) 
    elif isinstance(params, tuple):  # 단일 삽입
        print("insert query : ",map[key],params)
        return execute(map[key], params=params, fetch=fetch, return_id=return_id)
    else :
        print("error", "파라미터가 list나 tuple이 아닙니다.")
        raise ValueError("파라미터가 list나 tuple이 아닙니다.")
    

def update_map(key, params:tuple=None):
    map = {
        "updateRunEnd":"UPDATE TB_AF_RUN SET END_DATE = CURRENT_TIMESTAMP(), STATUS = %s WHERE DAG_ID = %s AND RUN_ID = %s ", # SATATUS : U(미준비),H(높은우선순위),W(대기),P(진행중),C(완료),E(오류),D(비활성화)
        "updateTargetContent":"UPDATE TB_AF_TARGET SET CONTENT = %s WHERE RUN_ID = %s AND TARGET_ID = %s ",
        "updateTargetContentDetail":"UPDATE TB_AF_TARGET SET CONTENT = JSON_SET(CONTENT, %s, %s) WHERE RUN_ID = %s AND TARGET_ID = %s ",
        "updateClassifyAiInfo":"UPDATE TB_DI_LAYOUT_CLASS SET CLASSIFY_AI_INFO = %s WHERE LAYOUT_CLASS_ID = %s ",
        "updateCreateReady":"UPDATE TB_AI_CREATE SET STATUS = 'W' WHERE STATUS = 'U' ", # U(비활성화),W(대기)
        "updateCreateStart":"UPDATE TB_AI_CREATE SET START_DATE = CURRENT_TIMESTAMP(), STATUS = 'P' WHERE CREATE_ID = %s ", # P(진행중)
        "updateCreateStatus":"UPDATE TB_AI_CREATE SET STATUS = %s WHERE CREATE_ID = %s ", # SATATUS : U(미준비),H(높은우선순위),W(대기),P(진행중),C(완료),E(오류),D(비활성화)
        "updateCreateEnd":"UPDATE TB_AI_CREATE SET END_DATE = CURRENT_TIMESTAMP(), STATUS = %s WHERE CREATE_ID = %s ", # E(오류),C(완료)
        
        "updateCompleteContent":"UPDATE TB_AF_COMPLETE SET content = JSON_SET(content, %s, %s), updt = current_timestamp() WHERE complete_id = %s ",
    }

    print(" query : ",map[key],params)
    execute(map[key], params=params, fetch=False)
    

def select_list_map(key, params:tuple=None, dictionary:bool=False):
    map = {
        "selectLayoutList": ("SELECT A.LAYOUT_CLASS_ID, A.LAYOUT_NM, A.DOC_CLASS_ID, A.IMG_PREPROCESS_INFO, A.CLASSIFY_AI_INFO "+
                "FROM TB_DI_LAYOUT_CLASS AS A "
                "WHERE A.DOC_CLASS_ID = %s AND A.USE_YN='Y' "
                "ORDER BY A.LAYOUT_ORDR, A.LAYOUT_CLASS_ID "
            ,['layout_class_id','layout_name','doc_class_id','img_preprocess_info','classify_ai_info']
        ),
        "selectSectionList": ("SELECT A.SECTION_CLASS_ID, A.SECTION_NM, A.SECTION_TYPE, A.SEPARATE_SECTION_INFO, A.SEPARATE_BLOCK_INFO, A.OCR_INFO, A.CLEANSING_INFO, A.STRUCTURING_INFO "+
                "FROM TB_DI_SECTION_CLASS AS A "+
                "WHERE A.LAYOUT_CLASS_ID = %s "+
                "ORDER BY A.SECTION_ORDR, A.SECTION_CLASS_ID "
            ,['section_class_id','section_name','section_type','separate_area','separate_block','ocr','cleansing','structuring']
        ),
        "selectBlockList": ("SELECT A.BLOCK_ROW_NUM, A.BLOCK_COL_NUM, A.BLOCK_TYPE, A.DEFAULT_TEXT, C.TABLE_NM, B.COLUMN_NM "+
                "FROM TB_DI_BLOCK_CLASS A LEFT OUTER JOIN TB_DS_COLUMN B ON A.COLUMN_ID =B.COLUMN_ID "+
                "LEFT OUTER JOIN TB_DS_TABLE C ON B.TABLE_ID=C.TABLE_ID "+
                "WHERE A.SECTION_CLASS_ID = %s "+
                "ORDER BY A.BLOCK_ROW_NUM, A.BLOCK_COL_NUM "
            ,['block_row_num','block_col_num','block_type','default_text','table_name','column_name']
        ),
        "selectCommonCrctnList": ("SELECT A.ERROR_TEXT, A.CRRCT_TEXT  "+
                "FROM TB_DI_COMMON_CRCTN A "
            ,['error_text','crrct_text']
        ),
        "selectSectionParent": ("SELECT A.DOC_CLASS_ID, A.LAYOUT_CLASS_ID "
                "FROM VW_DI_DOC_LAYOUT_SECTION A "
                "WHERE A.SECTION_CLASS_ID = %s "
                ,['doc_class_id','layout_class_id']
        ),
        "selectSectiontypelist": ("SELECT A.SECTION_CLASS_ID, A.SECTION_TYPE "
                "FROM TB_DI_SECTION_CLASS A "
                "WHERE A.LAYOUT_CLASS_ID = %s "
                ,['section_class_id','section_type']
        ),
        "selectRelatedTableList": ("SELECT A.TABLE_ID, A.TABLE_NM, A.PARENT_TABLE_ID, B.TABLE_NM AS PARENT_TABLE_NM, F.COLUMN_NM AS ID_COL_NAME "+
                "FROM TB_DS_TABLE A LEFT JOIN TB_DS_TABLE B ON A.PARENT_TABLE_ID = B.TABLE_ID "
                "INNER JOIN TB_DS_COLUMN C ON A.TABLE_ID = C.TABLE_ID "
                "INNER JOIN TB_DI_BLOCK_CLASS D ON C.COLUMN_ID = D.COLUMN_ID "
                "INNER JOIN VW_DI_DOC_LAYOUT_SECTION E ON D.SECTION_CLASS_ID = E.SECTION_CLASS_ID "
                "LEFT JOIN (SELECT TABLE_ID, COLUMN_NM FROM TB_DS_COLUMN X WHERE X.IS_PK = 'Y') F ON F.TABLE_ID = A.TABLE_ID "
                "WHERE E.DOC_CLASS_ID = %s "
                "GROUP BY A.TABLE_ID, A.TABLE_NM, A.PARENT_TABLE_ID, B.TABLE_NM, F.COLUMN_NM "
                ,['table_id','table_name','parent_table_id','parent_table_name','id_col_name']
        ),
        "selectBreakCreate": ("SELECT B.CREATE_ID, B.STATUS, A.LAYOUT_CLASS_ID, A.LAYOUT_NM, A.DOC_CLASS_ID, A.IMG_PREPROCESS_INFO, A.CLASSIFY_AI_INFO, A.TEMPLATE_FILE_PATH "
                "FROM TB_DI_LAYOUT_CLASS AS A INNER JOIN TB_AI_CREATE B ON A.LAYOUT_CLASS_ID=B.LAYOUT_CLASS_ID "+
                "WHERE B.END_DATE IS NULL AND B.STATUS NOT IN ('U','W','H','D') " # U(미준비),W(대기),H(높은우선순위),D(비활성화)   
            ,['create_id','first_status','layout_class_id','layout_name','doc_class_id','img_preprocess_info','classify_ai_info','template_file_path']
        ),
        "selectBreakRun": ("SELECT A.RUN_ID, A.DAG_ID, A.START_DATE, A.END_DATE, A.STATUS "+
                "FROM TB_AF_RUN A"+
                "WHERE A.DAG_ID = %s AND A.RUN_ID = %s AND A.END_DATE IS NULL "
            ,['run_id']
        ),
    }
    print(" query : ",map[key][0],params)
    result = execute(map[key][0], params=params, fetch=True, dictionary=dictionary)
    print("result : ",result)
    if dictionary:
        return result
    else:
        return tuples_to_dicts(result,map[key][1])

def select_row_map(key, params:tuple=None, dictionary:bool=False):
    map = {
        "selectSectionParent": ("SELECT A.DOC_CLASS_ID, A.LAYOUT_CLASS_ID "
                "FROM VW_DI_DOC_LAYOUT_SECTION A "
                "WHERE A.SECTION_CLASS_ID = %s "
                ,['doc_class_id','layout_class_id']
        ),
        "selectLayoutInfo": ("SELECT A.LAYOUT_CLASS_ID, A.LAYOUT_NM, A.DOC_CLASS_ID, A.IMG_PREPROCESS_INFO, A.CLASSIFY_AI_INFO, A.TEMPLATE_FILE_PATH "
                "FROM TB_DI_LAYOUT_CLASS AS A "
                "WHERE A.LAYOUT_CLASS_ID = %s"
            ,['layout_class_id','layout_name','doc_class_id','img_preprocess_info','classify_ai_info','template_file_path']
        ),
        "selectSectionInfo": ("SELECT A.SECTION_CLASS_ID,A.LAYOUT_CLASS_ID,A.SECTION_NM,A.SECTION_DESC,A.SEPARATE_SECTION_INFO, "
                "A.SEPARATE_BLOCK_INFO,A.OCR_INFO,A.CLEANSING_INFO,A.RGDT,A.UPDT,A.SECTION_ORDR,A.STRUCTURING_INFO,A.SECTION_TYPE "
                "FROM TB_DI_SECTION_CLASS AS A "
                "WHERE A.SECTION_CLASS_ID = %s "
            ,['section_class_id','layout_class_id','section_nm','section_desc','separate_section_info','separate_block_info',
                'ocr_info','cleansing_info','rgdt','updt','section_ordr','structuring_info','section_type']
        ),
        "selectMultiRowInfo": ("SELECT MIN(B.BLOCK_ROW_NUM) AS minnum,MAX(B.BLOCK_ROW_NUM) AS maxnum " +
                "FROM TB_DI_SECTION_CLASS A LEFT OUTER JOIN TB_DI_BLOCK_CLASS B ON A.SECTION_CLASS_ID=B.SECTION_CLASS_ID AND B.BLOCK_TYPE='val' "+
                "WHERE A.SECTION_TYPE='MULTI_ROW' AND A.SECTION_CLASS_ID=%s "
                ,['minnum','maxnum']
        ),
        "selectStaticBlockInfo": ("SELECT A.SECTION_CLASS_ID, A.SECTION_TYPE, B.BLOCK_CLASS_ID, B.DEFAULT_TEXT, B.BLOCK_BOX, B.BLOCK_ROW_NUM, B.BLOCK_COL_NUM "+
                "FROM TB_DI_SECTION_CLASS A LEFT OUTER JOIN TB_DI_BLOCK_CLASS B ON A.SECTION_CLASS_ID = B.SECTION_CLASS_ID "
                "WHERE B.BLOCK_TYPE = 'key' AND A.LAYOUT_CLASS_ID = %s "
                "ORDER BY (A.SECTION_TYPE = 'STATIC_TEXT') DESC "
                "LIMIT 1 "
                ,['section_class_id','section_type','block_class_id','default_text','block_box','row','col']
        ),
        "selectNextLayoutInfo": ("SELECT B.CREATE_ID, B.STATUS, A.LAYOUT_CLASS_ID, A.LAYOUT_NM, A.DOC_CLASS_ID, A.IMG_PREPROCESS_INFO, A.CLASSIFY_AI_INFO, A.TEMPLATE_FILE_PATH "
                "FROM TB_DI_LAYOUT_CLASS AS A INNER JOIN TB_AI_CREATE B ON A.LAYOUT_CLASS_ID=B.LAYOUT_CLASS_ID "+
                "WHERE B.STATUS IN ('W','H') ORDER BY CASE WHEN B.STATUS = 'H' THEN 1 WHEN B.STATUS = 'W' THEN 2 ELSE 3 END, B.RGDT LIMIT 1 " # W(대기),H(높은우선순위)
            ,['create_id','first_status','layout_class_id','layout_name','doc_class_id','img_preprocess_info','classify_ai_info','template_file_path']
        ),
    }
    print(" query : ",map[key][0],params)
    result = execute(map[key][0], params=params, fetch=True, dictionary=dictionary)
    print("result : ",result)
    if dictionary:
        return result
    else:
        return tuples_to_dicts(result,map[key][1])[0] if result else None  # 값이 없으면 None 반환

def select_one_map(key, params:tuple=None):
    map = {
        "selectPatternInfo":"SELECT A.FORMAT_INFO "+
                "FROM TB_DS_COLUMN A INNER JOIN TB_DI_BLOCK_CLASS B ON A.COLUMN_ID=B.COLUMN_ID "+
                "WHERE B.SECTION_CLASS_ID = %s AND BLOCK_ROW_NUM= %s AND BLOCK_COL_NUM = %s "+
                "ORDER BY B.BLOCK_CLASS_ID DESC LIMIT 1 ",
        "selectBlockCrctnMatched":"SELECT NVL(B.CRRCT_TEXT,%s) AS TEXT "+
                "FROM TB_DI_BLOCK_CLASS A LEFT JOIN TB_DI_BLOCK_CRCTN B ON A.BLOCK_CLASS_ID=B.BLOCK_CLASS_ID AND B.ERROR_TEXT=%s "+
                "WHERE A.SECTION_CLASS_ID=%s AND A.BLOCK_ROW_NUM=%s AND A.BLOCK_COL_NUM=%s " +
                "ORDER BY B.CRCTN_ORDR DESC LIMIT 1 ",
    }
    print(" query : ",map[key], params)
    result = execute(map[key], params=params, fetch=True)
    if result:
        print("result : ",result[0][0])
        return result[0][0]  # 값 1개만 반환
    else:
        return None 

#공통
def tuples_to_dicts(rows, columns):
    """
    튜플 기반 결과를 딕셔너리 리스트로 변환합니다.
    
    :param rows: list of tuple - DB 결과 행들
    :param columns: list of str - 컬럼명 리스트
    :return: list of dict - 컬럼명을 키로 하는 딕셔너리 리스트
    """
    result = []
    for row in rows:
        if len(row) != len(columns):
            raise ValueError(f"컬럼 수 불일치: row의 컬럼 수 {len(row)}와 columns의 수 {len(columns)}가 다릅니다.")
        result.append(dict(zip(columns, row)))
    return result


#에외처리용
def select_doc_class_id(params:list) -> int:
    if not params and len(params) == 0:
        return None
    placeholders = ','.join(['%s'] * len(params)) 
    query = f"""
        SELECT DOC_CLASS_ID FROM VW_DI_DOC_LAYOUT_SECTION 
         WHERE LAYOUT_CLASS_ID IN ({placeholders}) 
         GROUP BY DOC_CLASS_ID ORDER BY COUNT(*) DESC 
         FETCH FIRST 1 ROW ONLY 
    """
    print(" query : ",query, params)
    result = execute(query, params=params, fetch=True)
    if result:
        return result[0][0]  # 값 1개만 반환
    else:
        return None 


def insert_structed_ocr_result(doc_class_id:str, structed_doc:dict=None, complete_id:int=None):
    select_query = """
        SELECT B.TABLE_ID, B.TABLE_NM, A.COLUMN_NM AS PK, C.TABLE_NM AS PARENT_TABLE_NM, D.COLUMN_NM AS PARENT_PK 
            FROM TB_DS_COLUMN A 
            INNER JOIN TB_DS_TABLE B ON A.TABLE_ID=B.TABLE_ID 
            LEFT OUTER JOIN (TB_DS_TABLE C INNER JOIN TB_DS_COLUMN D ON C.TABLE_ID=D.TABLE_ID AND D.IS_PK='Y') 
            ON B.PARENT_TABLE_ID=C.TABLE_ID 
            WHERE B.TABLE_ID IN ( 
                SELECT DISTINCT Z.TABLE_ID FROM VW_DI_DOC_LAYOUT_SECTION X 
                INNER JOIN TB_DI_BLOCK_CLASS Y ON X.SECTION_CLASS_ID=Y.SECTION_CLASS_ID 
                INNER JOIN TB_DS_COLUMN Z ON Z.COLUMN_ID=Y.COLUMN_ID 
                WHERE X.DOC_CLASS_ID= %s 
                ) 
            AND A.IS_PK='Y' """
    dict_key_list = ['table_id','table_name','pk','parent_table_name','parent_pk']
    params = (doc_class_id,)
    print(" query : ",select_query,params)
    result = execute(select_query, params=params, fetch=True)
    print("result : ",result)
    table_list = tuples_to_dicts(result,dict_key_list)
    bulk_map_list = []
    # 부모가 없는 테이블 먼저 작업
    pk_map = {}
    print("===================",structed_doc)
    
    for table_info in table_list:
        if table_info["parent_table_name"] is None:
            # 부모테이블이 없어 단독으로 넣을 수 있는 테이블 먼저 데이터 삽입 및 PK 얻기
            table_id = table_info["table_id"]
            table_name = table_info["table_name"]
            pk_name = table_info["pk"]

            records = structed_doc.get(table_name, [])
            if not records:
                print(f"경고: {table_name} 테이블에 삽입할 데이터가 없습니다.")
                continue
            elif len(records)>1:
                print(f"경고: 대표 테이블인 {table_name}에는 1건씩만 입력 가능합니다.")
                return "error"
            
            # pk를 제외한 컬럼 목록 준비
            col_list = list(records[0].keys())
            if pk_name in col_list: # PK는 항상 autoincrease
                col_list.remove(pk_name)
            # 구조화를 위한 코드는 제외 후 작업
            if '_ID' in col_list:
                col_list.remove("_ID")
            if '_PAR_ID' in col_list:
                col_list.remove("_PAR_ID")
            # 실제 DB에 없는 컬럼은 제외
            select_query = """
                SELECT A.COLUMN_NM FROM TB_DS_COLUMN A WHERE A.TABLE_ID = %s AND IS_EXIST='N'
                """
            params = (table_id,)
            dict_key_list = ['col_nm']
            print(" query : ",select_query,params)
            result = execute(select_query, params=params, fetch=True)
            not_exist_col_list = tuples_to_dicts(result,dict_key_list)
            for row in not_exist_col_list:
                if row["col_nm"] in col_list:
                    col_list.remove(row["col_nm"])
            if not col_list:
                print(f"경고: {table_name} 테이블에 삽입할 데이터가 없습니다.")
                continue

            col_placeholders = ', '.join(['%s'] * len(col_list))
            col_names_quoted = ', '.join([f"`{col}`" for col in col_list])
            insert_sql = f"INSERT INTO `{table_name}` ({col_names_quoted}) VALUES ({col_placeholders})"
            print(insert_sql)
            for record in records:
                col_values = [record.get(col, {}).get("structed_text", "") for col in col_list]
                pk_value = execute(insert_sql, params=col_values, return_id=True)
                pk_map.setdefault(table_name, {}).setdefault(pk_name, []).append(pk_value)
                if complete_id:
                    bulk_map_list.append((complete_id, table_id, pk_value))
            print(f"{table_name}에 데이터 삽입 완료. pk:{pk_value}")
    
    # 부모가 있고 pk_map에 pk가 있는 테이블 작업
    for table_info in table_list:
        if table_info["parent_table_name"]:
            # TB_OCR_BILD_BASIC_INFO에 먼저 데이터 삽입 및 BILD_SEQ_NUM 얻기
            table_id = table_info["table_id"]
            table_name = table_info["table_name"]
            pk_name = table_info["pk"]
            parent_table_name = table_info["parent_table_name"]
            parent_pk_name = table_info["parent_pk"]

            records = structed_doc.get(table_name, [])
            if not records:
                print(f"경고: {table_name} 테이블에 삽입할 데이터가 없습니다.")
                continue
            # pk, 부모pk를 제외한 컬럼 목록 준비
            col_list = list(records[0].keys())
            if pk_name in col_list: # PK는 항상 autoincrease
                col_list.remove(pk_name)
            if parent_pk_name in col_list: # 부모PK는 별도 입력
                col_list.remove(parent_pk_name)
            # 구조화를 위한 코드는 제외 후 작업
            if '_ID' in col_list:
                col_list.remove("_ID")
            if '_PAR_ID' in col_list:
                col_list.remove("_PAR_ID")
            
            if not col_list:
                print(f"경고: {table_name} 테이블에 삽입할 데이터가 없습니다.")
                continue

            col_placeholders = ', '.join(['%s'] * len(col_list))
            col_names_quoted = ', '.join([f"`{col}`" for col in col_list])
            parent_pk_placeholder = ', %s'
            parent_pk_name_quoted = f', {parent_pk_name}'
            insert_sql = f"INSERT INTO `{table_name}` ({col_names_quoted}{parent_pk_name_quoted}) VALUES ({col_placeholders}{parent_pk_placeholder})"
            for record in records:
                col_values = [record.get(col, {}).get("structed_text", "") for col in col_list]
                parent_pk_value = pk_map.get(parent_table_name,{}).get(parent_pk_name,"")[0]
                col_values.append(parent_pk_value)
                pk_value = execute(insert_sql, params=col_values, return_id=True)
                print(f"{table_name}에 데이터 삽입 완료. pk:{pk_value}")
                if complete_id:
                    bulk_map_list.append((complete_id, table_id, pk_value))
    # TB_AF_COMPLETE_MAP 벌크 삽입
    if len(bulk_map_list) > 0:
        complete_map_sql = "INSERT INTO TB_AF_COMPLETE_MAP (COMPLETE_ID, TABLE_ID, PK_VAL) VALUES (%s, %s, %s)"
        execute_many(complete_map_sql, params_list=bulk_map_list)

    

def select_translate_target_list(table_name: str, id_col_name: str):
    """
    번역 대상 테이블과 컬럼을 선택합니다.
    :return: 번역 대상 테이블과 컬럼의 리스트
    """
    col_values = (table_name,)
    query = f"""SELECT B.*
    FROM {table_name} B
    LEFT JOIN TB_OCR_TRN_LOG A 
    ON B.{id_col_name} = A.TRN_TABLE_PK 
    AND A.TRN_TABLE_NM = %s
    WHERE A.TRN_TABLE_PK IS NULL;
    """
    print(" query : ",query, col_values)
    list = execute(query, params=col_values, fetch=True, dictionary=True)
    return list 

def select_complete_map_data(complete_id:str):
    """
    번역 대상 테이블과 컬럼을 선택합니다.
    :return: 번역 대상 테이블과 컬럼의 리스트
    """
    select_complete_query = """
    SELECT DISTINCT M.TABLE_ID, T.TABLE_NM, C.COLUMN_NM
    FROM TB_AF_COMPLETE_MAP M
    INNER JOIN TB_DS_TABLE T ON M.TABLE_ID = T.TABLE_ID
    INNER JOIN TB_DS_COLUMN C ON C.TABLE_ID = T.TABLE_ID AND C.IS_PK = 'Y'
    WHERE M.COMPLETE_ID = %s
    """
    table_list = execute(select_complete_query, params=(complete_id,), fetch=True, dictionary=True)
    data_map = {}
    for table_info in table_list:
        table_id = table_info["TABLE_ID"]
        table_name = table_info["TABLE_NM"]
        pk_column_name = table_info["COLUMN_NM"]
        query = f"""
        SELECT B.*
        FROM TB_AF_COMPLETE_MAP A
        INNER JOIN {table_name} B ON B.{pk_column_name} = A.PK_VAL
        WHERE A.COMPLETE_ID = %s and A.TABLE_ID = %s 
        """
        col_values = (complete_id, table_id)
        print(" query : ",query, col_values)
        rows = execute(query, params=col_values, fetch=True, dictionary=True)
        data_map[table_name] = rows  # 테이블명 → 데이터 리스트
    return data_map

def update_for_translate(table_name: str, id_col_name:str, latest_id: str, update_target: dict, update_origin_at:bool=False):
    """
    번역된 컬럼을 원본 테이블에 업데이트합니다.
    
    :param updates: dict - 컬럼명과 번역된 텍스트의 딕셔너리
    """        
    if update_origin_at:
        # 원본 테이블 업데이트
        set_clauses = [f"{col} = %s" for col in update_target.keys()]
        set_values = [v[1] for v in update_target.values()]
        
        update_query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {id_col_name} = %s;"
        update_values = set_values + [latest_id]
        execute(update_query, params=update_values)

    insert_log_query = "INSERT INTO TB_OCR_TRN_LOG (TRN_TABLE_NM, TRN_TABLE_PK, TRN_COL_ID, ORI_TEXT, TRN_TEXT) VALUES (%s, %s, %s, %s, %s)"
    params_list = [
        (table_name, latest_id, col, ori_text, trn_text)
        for col, (ori_text, trn_text) in update_target.items()
    ]
    execute_many(insert_log_query, params_list=params_list)

    return

def select_doc_class_id_list():
    """
    DAG 동적 생성을 위해 TB_DI_DOC_CLASS 테이블에서 DOC_CLASS_ID 목록을 조회합니다.
    """
    query = """
        SELECT T1.DOC_CLASS_ID, T1.DOC_NM
        FROM dococr.TB_DI_DOC_CLASS AS T1
        INNER JOIN dococr.TB_DI_LAYOUT_CLASS AS T2
        WHERE T1.DOC_CLASS_ID = T2.DOC_CLASS_ID
        AND T2.USE_YN = 'Y'
        WHERE USE_YN = 'Y'
        ORDER BY DOC_CLASS_ID
    """

    # execute 함수는 이미 maria_db_conn을 사용하도록 되어 있습니다.
    # fetch=True: 결과를 조회합니다. (튜플 리스트 반환: [(1,), (2,), ...])
    # dictionary=False: 튜플 형태로 반환하여 DAG 생성 로직에서 처리합니다.
    print(" query : ", query)
    try:
        result = execute(query, fetch=True, dictionary=True)

        info_columns = [] 
        # 결과가 없으면 빈 리스트 반환
        return result if result else [], info_columns
    except Exception as e:
        print(f"❌ TB_DI_DOC_CLASS 조회 오류: {e}")
        # Airflow DAG 로드 실패를 유도하기 위해 예외를 다시 발생시킵니다.
        raise e
    
    #쿼리 추가
def select_infos_for_dag_generation():
    """
    DAG 동적 생성을 위해 TB_DI_DOC_CLASS 및 TB_DI_LAYOUT_CLASS 테이블에서 필요한 정보를 조회합니다.
    """
    
    query = """SELECT T2.DOC_CLASS_ID, T2.DOC_NM AS DOC_NAME FROM TB_DI_DOC_CLASS T2 ORDER BY T2.DOC_CLASS_ID"""
    columns = ["doc_class_id","doc_name"]
    result = execute(query, fetch=True)
    return tuples_to_dicts(result,columns)

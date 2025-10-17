-- 상위 계층 정보 조회를 위한 뷰
CREATE VIEW VW_DI_DOC_LAYOUT_SECTION AS
SELECT
    dc.DOC_CLASS_ID,
    dc.DOC_CLASS_NAME,
    lc.LAYOUT_CLASS_ID,
    lc.LAYOUT_CLASS_NAME,
    lc.LAYOUT_ORDER,
    sc.SECTION_CLASS_ID,
    sc.SECTION_NAME,
    sc.SECTION_DESC
FROM TB_DI_DOC_CLASS dc
JOIN TB_DI_LAYOUT_CLASS lc ON dc.DOC_CLASS_ID = lc.DOC_CLASS_ID
JOIN TB_DI_SECTION_CLASS sc ON lc.LAYOUT_CLASS_ID = sc.LAYOUT_CLASS_ID;

-- 값이 저장될 데이터 검색을 위해 필요함
CREATE OR REPLACE VIEW TB_DI_BLOCK_HIERARCHY AS
WITH RECURSIVE BlockHierarchy AS (
    -- 1. 시작점: BLOCK_TYPE = 'val'
    SELECT
        bc.BLOCK_CLASS_ID,
        bc.SECTION_CLASS_ID,
        sc.SECTION_NAME,
        bc.BLOCK_ROW_NUM,
        bc.BLOCK_COL_NUM,
        bc.BLOCK_TYPE,
        bc.PARENT_BLOCK_ID,
        bc.DEFAULT_TEXT,
        1 AS LEVEL -- 계층 깊이 추가
    FROM TB_DI_BLOCK_CLASS bc
    LEFT JOIN TB_DI_SECTION_CLASS sc ON bc.SECTION_CLASS_ID = sc.SECTION_CLASS_ID
    WHERE bc.BLOCK_TYPE = 'val'

    UNION ALL

    -- 2. 재귀적으로 상위 블록을 따라감
    SELECT
        parent.BLOCK_CLASS_ID,
        parent.SECTION_CLASS_ID,
        sc.SECTION_NAME,
        parent.BLOCK_ROW_NUM,
        parent.BLOCK_COL_NUM,
        parent.BLOCK_TYPE,
        parent.PARENT_BLOCK_ID,
        parent.DEFAULT_TEXT,
        child.LEVEL + 1 AS LEVEL
    FROM TB_DI_BLOCK_CLASS parent
    LEFT JOIN TB_DI_SECTION_CLASS sc ON parent.SECTION_CLASS_ID = sc.SECTION_CLASS_ID
    INNER JOIN BlockHierarchy child ON child.PARENT_BLOCK_ID = parent.BLOCK_CLASS_ID
)
SELECT * FROM BlockHierarchy;
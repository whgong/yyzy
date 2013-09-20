SET SCHEMA = MD;

CREATE PROCEDURE MD.P_ETL_VALIDLOADFIELD (
    IN TGTTABLE    VARCHAR(128),
    IN LOADTYPE    VARCHAR(1),
    IN LOADFIELD    VARCHAR(255),
    INOUT LOADVALUE    VARCHAR(255),
    OUT RUNSTATUS    INTEGER,
    INOUT MESSAGE    VARCHAR(4000),
    OUT SQL    VARCHAR(4000) )
  SPECIFIC SQL120113203901100
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN
    --定义系统变量
    DECLARE SQLSTATE CHAR(5);
    DECLARE SQLCODE INTEGER;
    
    DECLARE STMT VARCHAR(4000);
    DECLARE ERR_MSG VARCHAR(4000);
    DECLARE STRFIELD VARCHAR(255);
    DECLARE NFIELD INTEGER DEFAULT 0;
    DECLARE NVALUE INTEGER DEFAULT 0;
    DECLARE DFIELD INTEGER DEFAULT 0;
    DECLARE I INTEGER DEFAULT 0;
    
    --定义游标
    DECLARE C1 CURSOR FOR S1;
    
    --定义异常处理
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION   
    SET ERR_MSG='系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';  ';
    
    SET MESSAGE=MESSAGE||RTRIM(CHAR(CURRENT TIMESTAMP))||'  开始验证加载类型和加载字段是否有效……  ';
    
    IF LOADTYPE IN ('I','P','D','W','A') THEN
      SET MESSAGE=MESSAGE||'加载类型('||LOADTYPE||')目前支持;  ';
      IF RTRIM(COALESCE(LOADFIELD,''))='' THEN
        SET RUNSTATUS=2;
        SET MESSAGE=MESSAGE||'需要定义加载字段;  ';
      ELSE
        SET STRFIELD=RTRIM(LOADFIELD);
        IF RIGHT(STRFIELD,1)=',' THEN 
          SET STRFIELD=SUBSTR(STRFIELD,1,LENGTH(STRFIELD)-1);
        END IF;
        SET I=0;
        WHILE I<LENGTH(STRFIELD) DO
          IF SUBSTR(STRFIELD,I,1)=',' THEN 
            SET NFIELD=NFIELD+1;
          END IF; 
          SET I=I+1;
        END WHILE;
        SET NFIELD=NFIELD+1;
        
        SET ERR_MSG='';
        SET STMT='SELECT COUNT(*) FROM SYSCAT.COLUMNS WHERE RTRIM(TABSCHEMA)||''.''||TABNAME='''||TGTTABLE||''' AND COLNAME IN ('''||REPLACE(STRFIELD,',',''',''')||''')';
        PREPARE S1 FROM STMT;
    
        --打开游标
        OPEN C1;
          FETCH C1 INTO DFIELD;
        CLOSE C1;
        IF ERR_MSG<>'' THEN
          SET RUNSTATUS=1;
          SET MESSAGE=MESSAGE||ERR_MSG;
          SET SQL=STMT;
        ELSEIF NFIELD<>DFIELD THEN
          SET RUNSTATUS=2;
          SET MESSAGE=MESSAGE||'('||STRFIELD||')中有部分或全部字段在目标数据表中不存在;  ';
          SET SQL=STMT;
        ELSE
          SET RUNSTATUS=0;
          SET MESSAGE=MESSAGE||'('||STRFIELD||')中的全部字段在目标数据表中存在;  ';
          SET SQL='';
        END IF;

        IF RUNSTATUS<>0 THEN RETURN 0; END IF;

        SET STRFIELD=RTRIM(COALESCE(LOADVALUE,''));
        IF RIGHT(STRFIELD,1)=',' THEN 
          SET STRFIELD=SUBSTR(STRFIELD,1,LENGTH(STRFIELD)-1);
        END IF;
        SET I=0;
        WHILE I<LENGTH(STRFIELD) DO
          IF SUBSTR(STRFIELD,I,1)=',' THEN 
            SET NVALUE=NVALUE+1;
          END IF; 
          SET I=I+1;
        END WHILE;
        SET NVALUE=NVALUE+1;
        IF STRFIELD='' THEN
          SET NVALUE=0;
        END IF;

        CASE LOADTYPE
          WHEN 'I' THEN
            IF NVALUE<>NFIELD AND NVALUE<>0 THEN 
              SET RUNSTATUS=2;
              SET MESSAGE=MESSAGE||'加载数值与加载字段中的个数不一致;  ';
            END IF;
          WHEN 'P' THEN
            IF NFIELD NOT IN (1,2,3) THEN
              SET RUNSTATUS=2;
              SET MESSAGE=MESSAGE||'此类型只允许加载字段有一个(年/日期)、二个(年、月)、三个(年、月、日);  ';
            ELSEIF NVALUE NOT IN (1,2) AND NVALUE<>0 THEN
              SET RUNSTATUS=2;
              SET MESSAGE=MESSAGE||'此类型只允许加载数值有一个(开始日期)、二个(开始日期、结束日期);  ';
            ELSEIF NVALUE<>0 THEN
              IF NVALUE=1 THEN
                SET LOADVALUE=MD.F_ETL_GETLOADDATE(STRFIELD);
              ELSE
                SET LOADVALUE=MD.F_ETL_GETLOADDATE(SUBSTR(STRFIELD,1,LOCATE(',',STRFIELD)-1))||','||MD.F_ETL_GETLOADDATE(SUBSTR(STRFIELD,LOCATE(',',STRFIELD)+1));
              END IF;
            END IF;
          WHEN 'D' THEN
            IF NFIELD<>1 THEN
              SET RUNSTATUS=2;
              SET MESSAGE=MESSAGE||'此类型只允许一个加载字段;  ';
            END IF;
          WHEN 'W' THEN
            IF NFIELD<>1 THEN
              SET RUNSTATUS=2;
              SET MESSAGE=MESSAGE||'此类型只允许一个加载字段;  ';
            END IF;
        END CASE;
      END IF;
    ELSEIF LOADTYPE IN ('C','V','S') THEN
      SET MESSAGE=MESSAGE||'加载类型('||LOADTYPE||')目前支持;  ';
      IF LOADTYPE='S' AND COALESCE(LOADVALUE,'')<>'' THEN
        IF LENGTH(LOADVALUE)<>10 THEN
          SET LOADVALUE=MD.F_ETL_GETLOADDATE(LOADVALUE);
        END IF;        
      END IF;
    ELSE
      SET RUNSTATUS=2;
      SET MESSAGE=MESSAGE||'加载类型('''||COALESCE(LOADTYPE,'Null')||''')目前不支持;  ';
    END IF;
    
 END;

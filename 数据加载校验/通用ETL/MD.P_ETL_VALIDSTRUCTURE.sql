SET SCHEMA = MD;

CREATE PROCEDURE MD.P_ETL_VALIDSTRUCTURE (
    IN TGTTABLE    VARCHAR(128),
    IN SRCTABLE    VARCHAR(128),
    OUT RUNSTATUS    INTEGER,
    INOUT MESSAGE    VARCHAR(4000),
    OUT SQL    VARCHAR(4000) )
  SPECIFIC SQL120113203611000
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
    
    DECLARE STMT        VARCHAR(4000);
    DECLARE ERR_MSG     VARCHAR(4000);
    DECLARE ISCOL       INTEGER ;
    DECLARE ISTYPE      INTEGER ;
    
    --定义游标
    DECLARE C1 CURSOR FOR S1;
    
    --定义异常处理
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION   
    SET ERR_MSG='系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';  ';
    
    SET MESSAGE=MESSAGE||RTRIM(CHAR(CURRENT TIMESTAMP))||'  开始验证目标数据表和源数据表的结构是否一致……  ';
    
    --判断源表和目标表的字段名称是否一致
    SET ERR_MSG='';
    SET STMT='SELECT COUNT(*) FROM (Select COLNAME from SYSCAT.COLUMNS WHERE RTRIM(TABSCHEMA)||''.''||TABNAME='''||TGTTABLE||''' AND COLNAME<>''LSBH'')AS A FULL JOIN (SELECT COLNAME FROM SYSCAT.COLUMNS WHERE RTRIM(TABSCHEMA)||''.''||TABNAME='''||SRCTABLE||''' AND COLNAME<>''LSBH'')AS B ON A.COLNAME=B.COLNAME WHERE A.COLNAME<>B.COLNAME OR A.COLNAME IS NULL OR B.COLNAME IS NULL';
    PREPARE s1 FROM stmt;
    
    --打开游标；
    OPEN C1;
    FETCH C1 INTO ISCOL;
    CLOSE C1;
    IF ISCOL<>0 AND ERR_MSG='' THEN
      SET RUNSTATUS=2;
      SET MESSAGE=MESSAGE||'1.源表目标表字段名称不一致;  ';
      SET SQL=STMT;
    ELSEIF ERR_MSG<>'' THEN
     SET RUNSTATUS=1;
     SET MESSAGE=MESSAGE||'1.'||ERR_MSG;
     SET SQL=STMT;
    ELSE 
     SET RUNSTATUS=0;
     SET MESSAGE=MESSAGE||'1.源表目标表字段名称一致;  ';
     SET SQL='';
    END IF;
    
    IF RUNSTATUS<>0 THEN RETURN 0;
    END IF;
    
    /*--判断源表和目标表字段类型是否一致;
    SET ERR_MSG='';
    SET STMT='SELECT SUM( CASE WHEN REPLACE(A.TYPENAME,''CHARACTER'',''VARCHAR'')=REPLACE(B.TYPENAME,''CHARACTER'',''VARCHAR'') AND A.LENGTH>=B.LENGTH AND A.SCALE>=B.SCALE THEN 0 WHEN B.TYPENAME=''SMALLINT'' AND A.TYPENAME=''INTEGER'' THEN 0 WHEN B.TYPENAME=''INTEGER'' AND A.TYPENAME=''DOUBLE'' THEN 0 WHEN B.TYPENAME=''INTEGER'' AND A.TYPENAME=''DECIMAL'' AND A.LENGTH-A.SCALE>=10 THEN 0 WHEN B.TYPENAME=''DECIMAL'' AND B.SCALE=0 AND B.LENGTH<10 AND A.TYPENAME=''INTEGER'' THEN 0 WHEN REPLACE(B.TYPENAME,''CHARACTER'',''VARCHAR'')=''VARCHAR'' AND B.LENGTH=10 AND A.TYPENAME=''DATE'' THEN 0 ELSE 1 END ) FROM ( Select * from SYSCAT.COLUMNS WHERE RTRIM(TABSCHEMA)||''.''||TABNAME='''||TGTTABLE||''' AND COLNAME<>''LSBH'') AS A, ( Select * from SYSCAT.COLUMNS WHERE RTRIM(TABSCHEMA)||''.''||TABNAME='''||SRCTABLE||''' ) AS B WHERE A.COLNAME=B.COLNAME';
        PREPARE s1 FROM stmt;
    OPEN C1;
    FETCH C1 INTO ISTYPE;
    CLOSE C1;
    IF ISTYPE<>0 AND ERR_MSG='' THEN
     SET RUNSTATUS=2;
     SET MESSAGE=MESSAGE||'2.源表目标表字段类型不一致;  ';
     SET SQL=STMT;
    ELSEIF ERR_MSG<>'' THEN
     SET RUNSTATUS=1;
     SET MESSAGE=MESSAGE||'2.'||ERR_MSG;
     SET SQL=STMT;
    ELSE
     SET RUNSTATUS=0;
     SET MESSAGE=MESSAGE||'2.源表目标表字段类型一致;  ';
     SET SQL='';
    END IF;
    */
 END;

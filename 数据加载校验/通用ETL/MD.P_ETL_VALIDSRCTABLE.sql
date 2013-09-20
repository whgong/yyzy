SET SCHEMA = MD;

CREATE PROCEDURE MD.P_ETL_VALIDSRCTABLE (
    IN SRCTABLE    VARCHAR(128),
    OUT RUNSTATUS    INTEGER,
    INOUT MESSAGE    VARCHAR(4000),
    OUT SQL    VARCHAR(4000) )
  SPECIFIC SQL120113203705500
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
--存储过程
  BEGIN
    --定义系统变量
    DECLARE SQLSTATE CHAR(5);
    DECLARE SQLCODE INTEGER;

    --定义
    DECLARE STMT1 VARCHAR(1000);
    DECLARE STMT2 VARCHAR(1000);
    DECLARE STMT3 VARCHAR(1000);
     DECLARE ERR_MSG VARCHAR(1000) DEFAULT '';
    DECLARE ISEXIST INTEGER DEFAULT 0;
    DECLARE ISAUTH  INTEGER DEFAULT 0;
    DECLARE ISUSE INTEGER DEFAULT 0;
    DECLARE STATUS INTEGER;
    DECLARE TABAUTH INTEGER;
    DECLARE DBAUTH INTEGER;

    --定义动态游标
    DECLARE c1 CURSOR FOR s1;
    DECLARE c2 CURSOR FOR s2;
    DECLARE C3 CURSOR FOR s3;
    
    --定义异常处理
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION   
      SET ERR_MSG = '系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';  ';

    SET MESSAGE=MESSAGE||RTRIM(CHAR(CURRENT TIMESTAMP))||'  开始验证源数据表('||SRCTABLE||')……  ';
        
    SET ERR_MSG='';
    --检查源数据表是否存在  
    SET stmt1 = 'SELECT COUNT(*) FROM SYSCAT.TABLES WHERE RTRIM(TABSCHEMA)||''.''||TABNAME='''||SRCTABLE||'''';
    PREPARE s1 FROM stmt1;
    
    --打开游标
    OPEN c1;
      FETCH c1 INTO ISEXIST;
    CLOSE c1;
    IF ISEXIST=0 AND ERR_MSG='' THEN
      SET RUNSTATUS=2;
      SET MESSAGE=MESSAGE||'1.源数据表不存在;  ';
      SET SQL=STMT1;
    ELSEIF ERR_MSG<>'' THEN 
      SET RUNSTATUS=1;
      SET MESSAGE=MESSAGE||'1.'||ERR_MSG;
    ELSE
      SET RUNSTATUS=0;
        SET MESSAGE=MESSAGE||'1.源数据表存在;  ';
      SET SQL='';
    END IF;
      
      IF RUNSTATUS<>0 THEN RETURN 0;
      END IF;

    SET ERR_MSG='';
    --检查源数据表是否可用 
    SET stmt2 = 'SELECT COUNT(*) FROM SYSCAT.TABLES WHERE RTRIM(TABSCHEMA)||''.''||TABNAME='''||SRCTABLE||'''  AND STATUS<>''X''';
    PREPARE s2 FROM stmt2;
    
    --打开游标
    OPEN c2;
     FETCH c2 INTO ISUSE;
    CLOSE c2;
    IF ISUSE=0 AND ERR_MSG='' THEN
      SET RUNSTATUS=2;
      SET MESSAGE=MESSAGE||'2.源数据表不可用;  ';
      SET SQL=STMT2;
    ELSEIF ERR_MSG<>'' THEN 
      SET RUNSTATUS=1;
      SET MESSAGE=MESSAGE||'2.'||ERR_MSG;
      SET SQL=STMT2;
    ELSE
      SET RUNSTATUS=0;
        SET MESSAGE=MESSAGE||'2.源数据表可用;  ';
      SET SQL='';
    END IF;

     IF RUNSTATUS<>0 THEN RETURN 0;
     END IF  ;

    SET ERR_MSG='';       
    --SET stmt3 = 'SELECT A.DBAUTH+B.TABAUTH FROM 
    --   (SELECT COUNT(*) AS DBAUTH FROM SYSCAT.DBAUTH  WHERE DBADMAUTH<>''N''AND GRANTEE=''ETLUSR'') AS A,
    --   (SELECT COUNT(*) AS TABAUTH FROM SYSCAT.TABAUTH WHERE SELECTAUTH<>''N'' AND (GRANTEE=''ETLUSR'' OR GRANTEE=''PUBLIC'')  AND RTRIM(TABSCHEMA)||''.''||TABNAME='''||SRCTABLE||''') AS B';
    SET STMT3='SELECT COUNT(*) AS DBAUTH FROM SYSCAT.DBAUTH  WHERE DBADMAUTH<>''N'' AND GRANTEE=''ETLUSR''';
    PREPARE s3 FROM stmt3;  
    OPEN c3;
     FETCH c3 INTO DBAUTH;
    CLOSE c3;
    SET STMT3='SELECT COUNT(*) AS TABAUTH FROM SYSCAT.TABAUTH WHERE SELECTAUTH<>''N'' AND (GRANTEE=''ETLUSR'' OR GRANTEE=''PUBLIC'')  AND RTRIM(TABSCHEMA)||''.''||TABNAME='''||SRCTABLE||''' ';
    PREPARE s3 FROM stmt3;  
    OPEN c3;
      FETCH c3 INTO TABAUTH;
    CLOSE c3;
    SET ISAUTH=DBAUTH+TABAUTH;
      
    IF ISAUTH=0 AND ERR_MSG='' THEN
      SET STMT3='SELECT COUNT(*) AS DBAUTH FROM SYSCAT.DBAUTH  WHERE DBADMAUTH<>''N'' AND GRANTEE=''ETLUSR'''|| ',' || 'SELECT COUNT(*) AS TABAUTH FROM SYSCAT.TABAUTH WHERE SELECTAUTH<>''N'' AND (GRANTEE=''ETLUSR'' OR GRANTEE=''PUBLIC'')  AND RTRIM(TABSCHEMA)||''.''||TABNAME='''||SRCTABLE||''' ' ;
      SET RUNSTATUS=2;
      SET MESSAGE=MESSAGE||'3.源数据表没有查询权限;  ';
      SET SQL=STMT3;
    ELSEIF ERR_MSG<>'' THEN 
      SET RUNSTATUS=1;
      SET MESSAGE=MESSAGE||'3.'||ERR_MSG;
      SET SQL=STMT3;
    ELSE
      SET RUNSTATUS=0;
        SET MESSAGE=MESSAGE||'3.源数据表有查询权限;  ';
      SET SQL='';
    END IF;
    
END;

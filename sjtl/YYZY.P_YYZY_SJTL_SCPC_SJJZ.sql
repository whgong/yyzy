DROP PROCEDURE YYZY.P_YYZY_SJTL_SCPC_SJJZ;

SET SCHEMA YYZYUSR ;

SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,YYZYUSR;

CREATE PROCEDURE YYZY.P_YYZY_SJTL_SCPC_SJJZ
 (OUT OUT_JG VARCHAR(1)
 ) 
  SPECIFIC YYZY.SQL130917171607800
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
  BEGIN
/*
  --系统接入投料数据
  INSERT INTO YYZY.T_YYZY_SJTL_SCPC
  select A.SJTLZBDM, TLSJ, A.PFPHDM, A.PHSCPC
  from YYZY.V_YYZY_SJTL_SCPC a
    left join (SELECT PFPHDM,MAX(TLSJ) AS ZDTLSJ FROM YYZY.T_YYZY_SJTL_SCPC GROUP BY PFPHDM) b
    on a.PFPHDM = b.PFPHDM
  where a.tlsj > value(B.zdtlsj,'1990-01-01 00:00:00.000000');
  COMMIT;
*/

  --领料批次 -> 核销数据接口表
  INSERT INTO YYZY.T_YYZY_SJTL_SCPC(TLSJ,PFPHDM,PHSCPC)
  WITH TMP AS (
    SELECT PFPHDM,MAX(ZDTLSJ) ZDTLSJ 
    FROM YYZY.T_YYZY_SJTL_SCPC_RZJL 
    WHERE (PFPHDM,GXSJ) IN (SELECT PFPHDM,MAX(GXSJ) AS GXSJ FROM YYZY.T_YYZY_SJTL_SCPC_RZJL GROUP BY PFPHDM) 
    GROUP BY PFPHDM
  )
  SELECT  MAX(LOAD_TIME) AS TLSJ,A.PFPHDM,SUM(PHSCPC)  AS PHSCPC
  FROM YYZY.T_YYZY_SJTL_SCPC_TMP A
    LEFT JOIN TMP B
      ON A.PFPHDM=B.PFPHDM 
  WHERE DATE(A.LOAD_TIME)>DATE(B.ZDTLSJ)  
    AND A.PHSCPC>0 AND A.ZYBJ=1
  GROUP BY  DATE(LOAD_TIME),A.PFPHDM
  ;
  
  --记录最大确认日期
  INSERT INTO YYZY.T_YYZY_SJTL_ZDQRSJ(GXSJ,PFPHDM,YQRSJ)
  SELECT CURRENT DATE AS GXSJ,PFPHDM,MAX(TLSJ) YQRSJ
  FROM YYZY.T_YYZY_SJTL_SCPC_TMP WHERE ZYBJ=1  GROUP BY PFPHDM
  ;
  
  --记录核销最大日期
  INSERT INTO YYZY.T_YYZY_SJTL_SCPC_RZJL(PFPHDM,ZDTLSJ)
  SELECT PFPHDM,MAX(TLSJ) AS ZDTLSJ
  FROM YYZY.T_YYZY_SJTL_SCPC
  GROUP BY PFPHDM
  ;

  COMMIT;
  
  SET OUT_JG='0';
 
 END;

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SJTL_SCPC_SJJZ
(
  VARCHAR(1)
) 
  TO USER DB2INST2 WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SJTL_SCPC_SJJZ
(
  VARCHAR(1)
) 
  TO USER ETLUSR WITH GRANT OPTION;


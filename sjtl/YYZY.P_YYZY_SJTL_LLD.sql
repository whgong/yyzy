/*********
 相对v2修改部分：
 修改56行;
 修改v2版phscpc为0时rsl表会产生null值bug，可以通过过滤phscpc>0 或者修改tmp2表
 WHERE PC1<=JSPCS AND PC2>=QSPCS and pc1<pc2
*********/
/**
DROP PROCEDURE YYZY.P_YYZY_SJTL_LLD;
*/
SET SCHEMA ETLUSR  ;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_SJTL_LLD(
  IN V_TLSJ1 DATE, 
  IN V_TLSJ2 DATE,
  IN V_SCXBJ INTEGER, 
  OUT OP_MSG INTEGER
) 
  SPECIFIC YYZY.P_YYZY_SJTL_LLD
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
BEGIN  
  DECLARE SQLCODE INT ;
  DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
  DECLARE MSG varCHAR(500) ;
  
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
  SET MSG='SQLCODE='||CHAR(SQLCODE)||';SQLSTATE='||SQLSTATE;
  
  IF MSG<>'00000' THEN 
    SET OP_MSG=0; 
    return; 
  ELSE 
    SET OP_MSG=1; 
  END IF; 
  
  DECLARE GLOBAL TEMPORARY TABLE SESSION.T_YYZY_SJTL_YEAR(
    TLSJ DATE,
    PFPHDM INTEGER NOT NULL,
    PHSCPC DECIMAL(18, 6),
    YTLPC DECIMAL(18, 6)
  ) WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED; 
  
  INSERT INTO SESSION.T_YYZY_SJTL_YEAR(TLSJ,PFPHDM,PHSCPC,YTLPC)
  WITH TMP AS (
    SELECT A.TLSJ,A.PFPHDM,A.PHSCPC,VALUE(B.YTLPC,0) AS YTLPC
    FROM YYZY.T_YYZY_SJTL_SCPC_TMP A
      LEFT JOIN YYZY.T_YYZY_SJTL_YEAR  B 
        ON A.PFPHDM=B.PFPHDM AND YEAR(TLSJ)=B.NF
      LEFT JOIN (
        select pfphdm,yqrsj 
        from YYZY.T_YYZY_SJTL_ZDQRSJ 
        WHERE (PFPHDM,GXSJ) IN (SELECT PFPHDM,MAX(GXSJ) AS GXSJ FROM YYZY.T_YYZY_SJTL_ZDQRSJ GROUP BY PFPHDM)
      ) C ON A.PFPHDM=C.PFPHDM
    WHERE A.TLSJ >C.YQRSJ AND A.TLSJ<=V_TLSJ2	 AND A.PHSCPC>0
  ) 
  SELECT A.TLSJ,A.PFPHDM,A.PHSCPC,
    SUM(CASE WHEN B.TLSJ<A.TLSJ THEN B.PHSCPC ELSE 0 END)+A.YTLPC AS YTLPC 
  FROM TMP A 
    LEFT JOIN TMP B ON A.PFPHDM=B.PFPHDM AND YEAR(A.TLSJ)=YEAR(B.TLSJ) 
    LEFT JOIN ( 
      select pfphdm,yqrsj 
      from YYZY.T_YYZY_SJTL_ZDQRSJ 
      WHERE (PFPHDM,GXSJ) IN (SELECT PFPHDM,MAX(GXSJ) AS GXSJ FROM YYZY.T_YYZY_SJTL_ZDQRSJ GROUP BY PFPHDM) 
    ) C ON A.PFPHDM=C.PFPHDM 
  WHERE A.TLSJ >C.YQRSJ AND A.TLSJ<=V_TLSJ2 AND B.TLSJ >C.YQRSJ AND B.TLSJ<=V_TLSJ2 
  GROUP BY A.TLSJ,A.PFPHDM,A.PHSCPC,A.YTLPC 
  ;
  
  DELETE FROM YYZY.T_YYZY_SJTL_LLD WHERE SCXBJ=V_SCXBJ; 
  INSERT INTO YYZY.T_YYZY_SJTL_LLD(TLSJ, PFPHDM,PPBJ,PFPHMC, PHSCPC, SCXBJ, YYDM, YYMC, YYCDMC, YYNF,YYPC, LLSL) 
  WITH TMP1 AS (
    SELECT A.TLSJ,A.PFPHDM,C.PPBJ,A.PHSCPC,(A.YTLPC+1) AS PC1,(A.YTLPC+A.PHSCPC) AS PC2
    FROM SESSION.T_YYZY_SJTL_YEAR A
      INNER JOIN YYZY.T_YYZY_PFPH_SCX C ON A.PFPHDM=C.PFPHDM AND C.SCXBJ=V_SCXBJ 
    WHERE  A.PHSCPC>0
  ),
  TMP2 AS (
    SELECT A.TLSJ,A.PFPHDM,A.PPBJ,A.PHSCPC,A.PC1,A.PC2,B.LSBH
    FROM TMP1 A 
    LEFT JOIN (
      SELECT PFPHDM,QSPCS,JSPCS,NF,YF,LSBH 
      FROM YYZY.T_YYZY_PFDXXB 
      WHERE (PFPHDM,NF,YF,BBH) IN (SELECT PFPHDM,NF,YF,MAX(BBH) FROM YYZY.T_YYZY_PFDXXB WHERE  SFFS='1' GROUP BY PFPHDM,NF,YF) 
        AND SFFS='1' 
    ) B 
      ON A.PFPHDM=B.PFPHDM 
    WHERE PC1<=JSPCS AND PC2>=QSPCS 
  ),
  TMP3 AS (
    SELECT A.TLSJ,A.PFPHDM,A.PPBJ,A.PHSCPC,A.PC1,A.PC2,A.LSBH,B.KSPC,B.JSPC,B.YYDM,B.YYPC,B.DPXS 
    FROM TMP2 A
      LEFT JOIN  YYZY.T_YYZY_PFDB B  
        ON A.LSBH=B.PFDXXBLSBH AND PC1<=JSPC AND PC2>=KSPC
  ),
  TMP4 AS (
    SELECT A.TLSJ,A.PFPHDM,A.PPBJ,A.PHSCPC,A.PC1,A.PC2,
      CASE WHEN KSPC=KSPC_M THEN A.PC1 ELSE KSPC END AS KSPC,
      CASE WHEN JSPC=JSPC_M AND A.PC2<=JSPC_M THEN A.PC2 ELSE JSPC END AS JSPC,
      A.YYDM,A.YYPC,A.DPXS
    FROM TMP3  A
      LEFT JOIN (
        SELECT TLSJ,PFPHDM,MIN(KSPC) AS KSPC_M,MAX(JSPC) AS JSPC_M FROM TMP3 GROUP BY TLSJ,PFPHDM
      ) B 
        on A.PFPHDM=B.PFPHDM AND A.TLSJ=B.TLSJ
  ),
  RSL AS (
    SELECT A.TLSJ,A.PFPHDM,A.PPBJ,A.PHSCPC,A.YYDM,A.YYPC,
      SUM(CASE WHEN C.YYDM IS NOT NULL THEN (JSPC-KSPC+1)*DPXS*B.PZCS ELSE  (JSPC-KSPC+1)*DPXS END) AS LLSL
    FROM TMP4 A 
      LEFT JOIN YYZY.T_YYZY_SJTL_XL_CFG B ON A.PFPHDM=B.PFPHDM 
      LEFT JOIN (SELECT YYDM FROM YYZY.T_YYZY_YYZDBMX where yycdmc='新疆' AND YYLBBS IN ('2025','2026')) C
        ON A.YYDM=C.YYDM
    GROUP BY A.TLSJ,A.PFPHDM,A.PPBJ,A.PHSCPC,A.YYDM,A.YYPC
  )
  SELECT  A.TLSJ,A.PFPHDM,A.PPBJ,D.PFPHMC,A.PHSCPC,B.SCXBJ,A.YYDM,
    SUBSTR(char(YYNF),3,2)||YYCDMC||YYDJMC YYMC,C.YYCDMC,C.YYNF,A.YYPC,A.LLSL
  FROM RSL  A
    LEFT JOIN YYZY.T_YYZY_PFPH_SCX B ON A.PFPHDM=B.PFPHDM 
    LEFT JOIN  YYZY.T_YYZY_YYZDBMX C ON A.YYDM=C.YYDM 
    LEFT JOIN DIM.T_DIM_YYZY_PFPH D ON A.PFPHDM=D.PFPHDM 
  WHERE A.TLSJ BETWEEN V_TLSJ1 and V_TLSJ2 
  ;
  END;

COMMENT ON PROCEDURE YYZY.P_YYZY_SJTL_LLD( 
  DATE,
  DATE, 
  INTEGER, 
  INTEGER
) IS '投料领料计算';

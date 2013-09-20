SET SCHEMA = JYHSF;

CREATE PROCEDURE JYHSF.P_JYHSF_SJJY ( OUT OUT_ZT INTEGER )
  SPECIFIC SQL130711114123300
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN    
    SET OUT_ZT=1;
    delete from JYHSF.T_JYHSF_SJJY_LOG;
    commit;
            
    --日生产计划取整校验
    INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
    SELECT '日生产计划取整异常',
            'JYHSF.T_JYHSF_RSCJHB',
            '配方牌号代码：'||RTRIM(CHAR(PFPHDM))||',开始日期：'||RTRIM(CHAR(KSRQ))||',结束时间：'||RTRIM(CHAR(JSRQ))||
            ',平均计划产量：'||RTRIM(CHAR(JHCL_AVG))||',平均计划批次：'||RTRIM(CHAR(JHPC_AVG))
    FROM JYHSF.T_JYHSF_RSCJHB WHERE JHPC_AVG-DEC(JHPC_AVG)  <>0  
            OR JHCL_AVG-DEC(JHCL_AVG)  <>0 ;
    COMMIT;
     
     
    --执行配方取整校验
    INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
    SELECT '执行配方取整异常',
            'JYHSF.T_JYHSF_ZXPF',
            '配方牌号代码：'||RTRIM(CHAR(PFPHDM))||',角色代码：'||RTRIM(CHAR(JSDM))||',开始日期：'||RTRIM(CHAR(KSRQ))||',结束时间：'||RTRIM(CHAR(JSRQ))||
            ',烟叶分配量'||RTRIM(CHAR(YYFPL))||',开始使用量'||RTRIM(CHAR(KSSYL))||',结束使用量'||RTRIM(CHAR(JSSYL))
    FROM JYHSF.T_JYHSF_ZXPF WHERE YYFPL-DEC(YYFPL)<>0;
    COMMIT;
    
    --执行配方开始结束使用量检验
    INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
    WITH YSSJ AS (
        SELECT PFPHDM,JSDM,RQ,SUM(SYL) AS SYL FROM
        (
        ( SELECT PFPHDM,JSDM,RQ,QRBJ,SYL
        FROM  (
        SELECT PFPHDM,JSDM,KSRQ AS RQ, CASE WHEN KSRQ=JSRQ THEN 0 ELSE 1 END AS QRBJ,KSSYL AS SYL
        FROM JYHSF.T_JYHSF_ZXPF A , DIM.T_DIM_YYZY_DATE B
        WHERE B.RIQI=A.KSRQ AND A.YYFPL>=0
        UNION ALL
        SELECT PFPHDM,JSDM,JSRQ AS RQ, CASE WHEN KSRQ=JSRQ THEN 0 ELSE 2 END AS QRBJ,JSSYL AS SYL
        FROM JYHSF.T_JYHSF_ZXPF A , DIM.T_DIM_YYZY_DATE B
        WHERE B.RIQI=A.JSRQ AND A.YYFPL>=0
        )
        GROUP BY  PFPHDM,JSDM,RQ,QRBJ,SYL
        )
        ) GROUP BY PFPHDM,JSDM,RQ
        ),YYXQL AS (
        SELECT A.PFPHDM,B.JSDM,A.JHPC_AVG,B.DPXS ,RQ,A.JHPC_AVG*B.DPXS AS XQL
        FROM (
        SELECT A.PFPHDM,A.JHPC_AVG,B.RIQI AS RQ
        FROM JYHSF.T_JYHSF_RSCJHB A,DIM.T_DIM_YYZY_DATE B
        WHERE B.RIQI BETWEEN A.KSRQ AND A.JSRQ  ) A
        LEFT JOIN ( SELECT PFPHDM,JSDM,DPXS,KSRQ,JSRQ FROM JYHSF.T_JYHSF_JSXX) B
        ON A.PFPHDM =B.PFPHDM AND A.RQ BETWEEN B.KSRQ AND B.JSRQ) 
    SELECT '执行配方开始结束使用量异常',
           'JYHSF.T_JYHSF_ZXPF，JYHSF.T_JYHSF_RSCJHB，JYHSF.T_JYHSF_JSXX',
           '配方牌号代码：'||RTRIM(CHAR(A.PFPHDM))||'，角色代码：'||RTRIM(CHAR(A.JSDM))||
           '，日期：'||RTRIM(CHAR(A.RQ))||',使用量（假）：'||RTRIM(CHAR(A.SYL))||',需求量（真）'||RTRIM(CHAR(B.XQL))
        FROM YSSJ A LEFT JOIN YYXQL B
        ON A.PFPHDM=B.PFPHDM AND A.JSDM=B.JSDM AND A.RQ = B.RQ
        WHERE A.SYL<>B.XQL;
        COMMIT;
        
    
    --执行配方分配量校验
    INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
    WITH TMP AS (
    SELECT PFPHDM,JSDM,CASE WHEN AD1 < BD1 THEN BD1 ELSE AD1 END
    KSRQ ,CASE WHEN AD2 > BD2 THEN BD2 ELSE AD2 END JSRQ
    ,DPXS, JHPC_AVG,XHYZL 
    FROM ( 
    SELECT A.PFPHDM,JSDM, A.KSRQ AD1 ,A.JSRQ
    AD2,B.KSRQ BD1 ,B.JSRQ BD2,DPXS ,B.JHPC_AVG  , 
    CASE  WHEN A.PFPHDM =16 
    THEN JHCL_AVG*DPXS/100*1.00 
    ELSE JHPC_AVG * DPXS * 1.00  END XHYZL
    FROM (
    SELECT PFPHDM,JSDM, KSRQ , JSRQ , DPXS 
    FROM JYHSF.T_JYHSF_JSXX 
    ) A 
    INNER JOIN JYHSF.T_JYHSF_RSCJHB B 
    ON A.KSRQ <=B.JSRQ 
    AND B.KSRQ<=A.JSRQ AND A.PFPHDM =B.PFPHDM 
    ) T  
    ORDER BY PFPHDM,JSDM,CASE WHEN AD1 < BD1 THEN BD1 ELSE AD1 END 
    ), 
    TMP2 AS (
    SELECT PFPHDM, JSDM, YYDM, YYNF,KCLX,YYFPL, KSRQ+1 DAYS KSRQ, 
    JSRQ -1 DAYS JSRQ,KSSYL,JSSYL,KSRQ ZKKSRQ ,JSRQ ZKJSRQ 
    FROM JYHSF.T_JYHSF_ZXPF 
    WHERE YYFPL>=0 
    ),
    TMP3 AS (
    SELECT TMP2.PFPHDM,TMP2.JSDM,YYDM, YYNF, KCLX, YYFPL, ZKKSRQ, 
    ZKJSRQ,KSSYL,JSSYL,TMP.XHYZL, 
    CASE WHEN TMP2.KSRQ BETWEEN TMP.KSRQ AND TMP.JSRQ 
    THEN TMP2.KSRQ 
    ELSE TMP.KSRQ 
    END KSRQ,
    CASE WHEN TMP2.JSRQ BETWEEN TMP.KSRQ AND TMP.JSRQ 
    THEN TMP2.JSRQ 
    ELSE TMP.JSRQ 
    END JSRQ 
    FROM TMP 
    RIGHT JOIN TMP2 
    ON TMP.PFPHDM = TMP2.PFPHDM 
    AND TMP.JSDM = TMP2.JSDM 
    AND NOT(TMP.JSRQ<TMP2.KSRQ OR TMP.KSRQ>TMP2.JSRQ) 
    ), 
    TMP4 AS ( 
    SELECT PFPHDM,JSDM,YYDM, YYNF, KCLX, ZKKSRQ, ZKJSRQ,YYFPL, KSSYL,JSSYL, 
    CASE WHEN JSRQ>=KSRQ THEN (DAYS(JSRQ)-DAYS(KSRQ)+1)*XHYZL ELSE 0 END HY 
    FROM TMP3
    ), 
    TMP5 AS ( 
    SELECT  PFPHDM, JSDM, YYDM, YYNF, KCLX, ZKKSRQ, 
    ZKJSRQ, YYFPL, KSSYL, JSSYL,SUM(HY) HY 
    FROM TMP4 
    GROUP BY  PFPHDM,JSDM,YYDM, YYNF,KCLX,YYFPL,ZKKSRQ , ZKJSRQ, KSSYL,JSSYL
    ) ,
    TMP6 AS ( 
    SELECT PFPHDM,JSDM,YYDM, YYNF,KCLX,ZKKSRQ, ZKJSRQ ,YYFPL, 
    CASE WHEN ZKKSRQ=ZKJSRQ  THEN KSSYL+HY ELSE KSSYL+JSSYL+HY END HY 
    FROM TMP5
    ) ,
    TMP7 AS (
    SELECT PFPHDM,JSDM,YYDM, YYNF,KCLX,ZKKSRQ  , ZKJSRQ ,YYFPL,HY ,YYFPL-HY CHA 
    FROM TMP6
    )
    SELECT '执行配方分配量异常',
           'JYHSF.T_JYHSF_ZXPF，JYHSF.T_JYHSF_RSCJHB，JYHSF.T_JYHSF_JSXX',
           '配方牌号代码：'||RTRIM(CHAR(PFPHDM))||'，角色代码：'||RTRIM(CHAR(JSDM))||
           '，烟叶代码：'||RTRIM(CHAR(YYDM))||'，烟叶年份：'||RTRIM(CHAR(YYNF))||
           '，库存类型：'||RTRIM(CHAR(KCLX))||'，开始日期：'||RTRIM(CHAR(ZKKSRQ))||
           '，结束日期：'||RTRIM(CHAR(ZKJSRQ))||'，烟叶分配量：'||RTRIM(CHAR(YYFPL))||
           '，耗用：'||RTRIM(CHAR(HY))||'，分配量-耗用'||RTRIM(CHAR(CHA))
           FROM TMP7 WHERE ABS(CHA)>0;
    COMMIT;
    
    --库存及烟碱表中烟叶代码缺少检验
    INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
    SELECT DISTINCT '库存及烟碱表中烟叶代码缺少',
           'JYHSF.T_JYHSF_ZXPF，JYHSF.T_JYHSF_KCJYJXX',
           '烟叶代码：'||RTRIM(CHAR(YYDM))
    FROM JYHSF.T_JYHSF_ZXPF  WHERE yydm not in
   (select DISTINCT YYDM from JYHSF.T_JYHSF_KCJYJXX);
    COMMIT;
        
    
/*-- --库存及烟碱表同烟叶同批次数据检测
--     INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
--     SELECT '库存及烟碱表中存在同烟叶同批次数据',
--            'JYHSF.T_JYHSF_KCJYJXX',
--            '烟叶代码：'||RTRIM(CHAR(YYDM))||'，复烤批次：'||RTRIM(CHAR(FKPC))
--     FROM JYHSF.T_JYHSF_KCJYJXX
--                  WHERE (YYDM,FKPC) IN (
--                 SELECT YYDM, FKPC
--                 FROM JYHSF.T_JYHSF_KCJYJXX
--                 GROUP BY YYDM, FKPC
--                 HAVING COUNT(YYDM)>1 )
--                   ORDER BY YYDM, FKP;
--     COMMIT;*/

    --执行配方的分配大于库存
    INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
    SELECT '库存箱数少于执行配方分配量',
           'JYHSF.T_JYHSF_ZXPF，JYHSF.T_JYHSF_KCJYJXX',
           '烟叶代码：'||RTRIM(CHAR(A.YYDM))||'执行配方分配量：'||RTRIM(CHAR(A.FPL))||
           '库存库存量：'||RTRIM(CHAR(B.KCL))
     FROM  (SELECT YYDM,SUM(YYFPL) AS FPL FROM JYHSF.T_JYHSF_ZXPF GROUP BY YYDM) AS A
          ,(SELECT A.YYDM,SUM(KCXS) AS KCL FROM JYHSF.T_JYHSF_KCJYJXX AS A GROUP BY A.YYDM) AS B
            WHERE A.YYDM=B.YYDM
          AND B.KCL<A.FPL;
    COMMIT;

    
--     --检测多批次同烟碱
--     INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
--     SELECT '库存及烟碱表中存在同批次同烟碱数据',
--            'JYHSF.T_JYHSF_KCJYJXX',
--            '烟叶代码：'||RTRIM(CHAR(YYDM))||'，复烤批次：'||RTRIM(CHAR(FKPC))||'，烟碱：'||RTRIM(CHAR(YJ))
--            FROM JYHSF.T_JYHSF_KCJYJXX
--                       WHERE (YYDM,YJ) IN (
--                     SELECT YYDM, YJ
--                     FROM JYHSF.T_JYHSF_KCJYJXX
--                     GROUP BY YYDM, YJ
--                     HAVING COUNT(YYDM)>1)
--                       ORDER BY YYDM, FKP;
--     COMMIT;

    --检验库存烟碱表异常库存箱数
    INSERT INTO JYHSF.T_JYHSF_SJJY_LOG( LOG_TITLE, LOG_TABLE , LOG_COMMIT )
    SELECT '库存烟碱表中的库存箱数为空',
           'JYHSF.T_JYHSF_KCJYJXX',
           '烟叶代码：'||RTRIM(CHAR(YYDM))||'，复烤批次：'||RTRIM(CHAR(FKPC))
    FROM  JYHSF.T_JYHSF_KCJYJXX where YYDM is null or kcxs is null or kcxs=0;
    COMMIT;
     
    IF(SELECT COUNT(*) FROM JYHSF.T_JYHSF_SJJY_LOG) <>0
    THEN 
       SET OUT_ZT =2;
    END IF;
     
END;

COMMENT ON PROCEDURE JYHSF.P_JYHSF_SJJY( INTEGER ) IS '均匀化算法数据校验';

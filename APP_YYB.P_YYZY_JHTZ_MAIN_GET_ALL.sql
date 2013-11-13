drop PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_ALL;
SET SCHEMA ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_ALL ( ) 
  SPECIFIC APP_YYB.SQL130711172312200
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
  --存储过程  
BEGIN 
  --定义系统变量  
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  DECLARE MSG VARCHAR(1000); 
  
  --定义
  DECLARE MIN_KSRQ, LOOP_DATE, TODAY_DATE ,START_DATE, END_DATE DATE ;   
  
  /* 每年1月1日,自动更新执行配方维护表库存类型,三年生产计划,增加率 */
  SET TODAY_DATE=CURRENT DATE;
  
  IF TODAY_DATE=TODAY_DATE - DAYOFYEAR(TODAY_DATE) DAY + 1 DAY THEN 
    --1.修改执行配方维护表库存类型
    UPDATE YYZY.T_YYZY_ZXPF_WHB SET KCLX=1 WHERE YYNF=YEAR(CURRENT DATE)-1 AND KCLX=3;
    
    --2.修改三年生产计划（待增加）
    
    --3.修改增长率（待增加）
    
  END IF;
  
  --备份角色调整表
  INSERT INTO YYZY.T_YYZY_JSTZ_WHB_BAK (PFPHDM, JSDM, KSRQ, JSRQ, DPXS, ZYBJ, BBH)
  SELECT PFPHDM, JSDM, KSRQ, JSRQ, DPXS, ZYBJ, BBH
    FROM YYZY.T_YYZY_JSTZ_WHB
   WHERE (PFPHDM,JSDM,BBH) IN (
        SELECT PFPHDM,JSDM,MAX(BBH) 
        FROM YYZY.T_YYZY_JSTZ_WHB 
        WHERE ZYBJ='1' AND CURRENT DATE BETWEEN KSRQ AND JSRQ 
        GROUP BY PFPHDM,JSDM
      ) 
     AND ZYBJ='1'
     AND CURRENT DATE BETWEEN KSRQ AND JSRQ
  ;
  
  --保留1个月的角色调整表备份数据
  DELETE FROM YYZY.T_YYZY_JSTZ_WHB_BAK WHERE LOAD_TIME<CURRENT TIMESTAMP - 1 MONTH;
  
  INSERT INTO YYZY.T_YYZY_JZ_RZ(BZ, MBB, JZSJ, SFCG)
  VALUES('开始加载','',CURRENT TIMESTAMP,1);
  COMMIT;

--  --依次调用各个计划的存储过程
--  --三年生产计划，全量加载
--  --CALL APP_YYB.P_YYZY_JHJZ_3YEAR();
--  /* 2013-02-04 修改 ,3年计划中的当年年计划加载, 否则前台页面 研发->烟叶资源保障分析系统->综合查询->年生计划(分厂) 当年数据及当年年份指向的跳转页面数据会显示过期数据 */ 
  INSERT INTO YYZY.T_YYZY_SNSCJH_1 (TOBACCOID, JHNF, PZMC, YHBS, CJDM, JHCL, PPMC, BBH, BBRQ)
  WITH YSSJ AS (
    select A.TOBACCOID,
      INT(A.CYEAR) AS JHNF, 
      B.STANDARDNM AS PZMC,
      D.YHBS, 
      D.SCCJDM AS CJDM,
      SUM(A.QUANTITY) AS JHCL,
      C.brandnm as PPMC,
      E.BBH+1 AS BBH,
      DATE(SUBSTR(A.C_CREATETIME,1,4)||'-'||SUBSTR(A.C_CREATETIME,5,2)||'-'||SUBSTR(A.C_CREATETIME,7,2)) AS BBRQ
    from HDS_CXQJ.N_CXQJ_O_REGION_YEARPLAN AS A
      LEFT JOIN HDS_TF01.N_TF01_TCM21 AS B
        ON A.TOBACCOID=B.STANDARDID
      LEFT JOIN HDS_TF01.N_TF01_TCM08 AS C
        ON C.brandid=substr(A.TOBACCOID,14,3)
      LEFT JOIN dim.t_dim_yyzy_pfph AS D
        ON SUBSTR(A.TOBACCOID,17,2)=D.YHBS
      LEFT JOIN (SELECT MAX(BBH) AS BBH FROM YYZY.T_YYZY_SNSCJH_1) AS E
        ON 1=1
    WHERE INT(A.CYEAR)=YEAR(CURRENT DATE)
      AND INT(C_CREATETIME)=(SELECT MAX(INT(C_CREATETIME)) FROM HDS_CXQJ.N_CXQJ_O_REGION_YEARPLAN)
    GROUP BY A.TOBACCOID, B.STANDARDNM, C.brandnm, A.CYEAR, A.C_CREATETIME, E.BBH, D.YHBS, D.SCCJDM
  ),SNSC_BBRQ AS (  --原三年计划最大版本日期
    SELECT MAX(BBRQ) AS BBRQ FROM YYZY.T_YYZY_SNSCJH_1
  ),NSC_BBRQ AS (   --生产处年计划最大版本日期
    SELECT MAX(BBRQ) AS BBRQ FROM YSSJ 
  ),SCJHBBRQ AS (   --生产计划最大版本日期处理
    SELECT A.BBRQ AS A,B.BBRQ AS B FROM SNSC_BBRQ AS A,NSC_BBRQ AS B WHERE A.BBRQ=B.BBRQ
  )
  /* 当生产处发布新的年计划时才会进行更新 */
  SELECT TOBACCOID, JHNF, PZMC, YHBS, CJDM, JHCL, PPMC, BBH, BBRQ 
  FROM YSSJ WHERE NOT EXISTS (select 1 from SCJHBBRQ)
  ;
  
  --年生产计划，全量加载 
  CALL APP_YYB.P_YYZY_JHJZ_YEAR(); 
  --季生产计划，加载更新当年数据 
  --CALL APP_YYB.P_YYZY_JHJZ_QUARTER();
  
  --季生产计划,2012年04月28日注释!!
  --CALL APP_YYB.P_YYZY_JHJZ_J();
  
  --月生产计划，全量加载
  CALL APP_YYB.P_YYZY_JHJZ_MONTH();
  
  
  --取得循环的开始日期
  SELECT MAX(JHRQ) INTO MIN_KSRQ 
  FROM YYZY.T_YYZY_RSCJH_LSB
    WHERE PFPHDM NOT IN(SELECT PFPHDM FROM YYZY.T_YYZY_WJG_PHSD)     --2011-11-17
  ;
  
  SELECT MAX(DQRQ)+1 DAY INTO START_DATE
  FROM YYZY.T_YYZY_YYKC_NEW
  ; 
  
  SET LOOP_DATE=MIN_KSRQ;
  WHILE MIN_KSRQ<START_DATE-1 DAY DO 
    --更新日生产计划历史
    INSERT INTO YYZY.T_YYZY_RSCJH_LSB(PFPHDM,JHRQ,JHCL,JHPC,BBRQ)
    SELECT PFPHDM,KSRQ,JHCL_AVG,JHPC_AVG,BBRQ 
    FROM YYZY.T_YYZY_RSCJHB_WHB 
    WHERE KSRQ=MIN_KSRQ+1 DAY; 
    
    --备份历史数据(只保留一个月)
    DELETE FROM YYZY.T_YYZY_RSCJHB_WHB_BAK
    WHERE BBRQ<=CURRENT DATE-DAY(CURRENT DATE) DAY - 1 MONTH;
    
    DELETE FROM YYZY.T_YYZY_ZXPF_WHB_BAK
    WHERE BBRQ<=CURRENT DATE-DAY(CURRENT DATE) DAY - 1 MONTH;
    
    INSERT INTO YYZY.T_YYZY_RSCJHB_WHB_BAK
    SELECT * 
    FROM YYZY.T_YYZY_RSCJHB_WHB ;
/*  
    --更新YYZY.T_YYZY_RSCJH_LSB表中 81#膨丝；91膨丝
    UPDATE YYZY.T_YYZY_RSCJH_LSB SET JHCL=0,JHPC=0 WHERE PFPHDM IN (72,73);
*/
    INSERT INTO YYZY.T_YYZY_ZXPF_WHB_BAK
    (
      PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,SDBH,ZXSX,TDSX,KSSYL,JSSYL,
      ZLYYBJ,ZPFBJ,FJCHSX,FJCHXX,KCLX,BBRQ,LOAD_TIME
    ) 
    SELECT 
      PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,SDBH,ZXSX,TDSX,
      KSSYL,JSSYL,ZLYYBJ,ZPFBJ,FJCHSX,FJCHXX,KCLX,BBRQ,(SELECT MAX(LOAD_TIME) FROM YYZY.T_YYZY_ZXPF_WHB) AS LOAD_TIME
    FROM YYZY.T_YYZY_ZXPF_WHB
    ;
    
    --复制上一天的日生产计划表, 调整角色表时使用，2013年8月17日增加.
    DELETE FROM YYZY.T_YYZY_TMP_RSCJHB_WHB_PRI; 
    INSERT INTO YYZY.T_YYZY_TMP_RSCJHB_WHB_PRI 
    SELECT * FROM YYZY.T_YYZY_RSCJHB_WHB; 
    
    --日生产计划增量加载 
    DELETE FROM YYZY.T_YYZY_RSCJHB_WHB; 
    CALL APP_YYB.P_YYZY_JHJZ_R(MIN_KSRQ+2 DAY,MSG); 
    
    --外加工锁定 
    DELETE FROM YYZY.T_YYZY_RSCJH_LSB WHERE JHRQ >= CURRENT DATE; --2013-04-10修改，保证外加工维护数据的加入 
    CALL YYZY.P_YYZY_WJG_PHSD(1,MSG);   -- 2011-11-17 
    CALL YYZY.P_YYZY_WJG_PHSD(2,MSG);   -- 2011-11-17 

/* --在DS中调度
    --实际投料数据(暂用历史计划作为实际投料),2013年8月15日增加.
    DELETE FROM YYZY.T_YYZY_SJTL_SCPC WHERE DATE(TLSJ)>=CURRENT_DATE - (DAY(CURRENT_DATE) - 1) DAY;
    INSERT INTO YYZY.T_YYZY_SJTL_SCPC(TLSJ, PFPHDM, PHSCPC)
    SELECT TIMESTAMP(TRIM(CHAR(JHRQ))||' 00:00:00.000000') AS TLSJ,PFPHDM, SUM(VALUE(JHPC,0)) AS PHSCPC
    FROM YYZY.T_YYZY_RSCJH_LSB
    WHERE PFPHDM NOT IN(SELECT PFPHDM FROM YYZY.T_YYZY_WJG_PHSD)
      AND JHRQ >= CURRENT_DATE - (DAY(CURRENT_DATE) - 1) DAY
      AND JHRQ < MIN_KSRQ+2 DAY
    GROUP BY TIMESTAMP(TRIM(CHAR(JHRQ))||' 00:00:00.000000'), PFPHDM
    ORDER BY TLSJ DESC, PFPHDM
    ;
    --总生产批次,2013年8月15日增加.
    DELETE FROM YYZY.T_YYZY_SJTL_YEAR WHERE NF>=YEAR(CURRENT_DATE)-1;
    INSERT INTO YYZY.T_YYZY_SJTL_YEAR(NF, PFPHDM, YTLPC)
    SELECT YEAR(TLSJ) AS NF,PFPHDM, SUM(PHSCPC) AS YTLPC
    FROM YYZY.T_YYZY_SJTL_SCPC
    WHERE YEAR(TLSJ)>=YEAR(CURRENT_DATE)-1
      AND DATE(TLSJ)>=(
          SELECT DATE(CSZ)
          FROM YYZY.T_YYZY_STCS
          WHERE CSMC = 'ZSPFFSQSRQ'
          FETCH FIRST 1 ROW ONLY
        )
      AND DATE(TLSJ) < MIN_KSRQ+2 DAY
    GROUP BY YEAR(TLSJ),PFPHDM
    ;
    */
    --日生产计划取整 2013年8月15日修改, 由制丝均匀性移至此 
    CALL YYZY.P_YYZY_RSCJHB_WHB_PCQZ(); 
    
    --生产计划修正,2013年8月15日增加.
--    CALL YYZY.P_YYZY_RSCJH_JHXZ(YEAR(MIN_KSRQ+2 DAY),MONTH(MIN_KSRQ+2 DAY));
    
--    CALL YYZY.P_YYZY_TMP_10YPFQDBD;    
    
	--分组加工生产计划
    call YYZY.P_YYZY_SCJH_FZJG;
    
    --角色更新(按最新的计划调整)
    --修正生产计划
    CALL YYZY.P_YYZY_SDPF_JSGX();
    
    --剩余投料库存处理 201309-29新增
    CALL YYZY.P_YYZY_SJTL_SYPCCL;
    
    --删除分配量为 -1 的情况
    DELETE FROM YYZY.T_YYZY_ZXPF_WHB WHERE YYFPL=-1;
    
    --锁定配方历史切割
    CALL YYZY.P_YYZY_LSPF6(MIN_KSRQ+1 DAY); --6要素
    CALL YYZY.P_YYZY_LSPF7(MIN_KSRQ+1 DAY); --7要素
    
    --锁定配方更新
    CALL YYZY.P_YYZY_SDPFGX6(); --6要素
    CALL YYZY.P_YYZY_SDPFGX7(); --7要素
    
    --更新ZXPF_LSB数据
    MERGE INTO YYZY.T_YYZY_ZXPF_LSB AS E  
    USING
      (
        SELECT 
          PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,ZXSX,TDSX, 
          KSSYL,JSSYL,ZLYYBJ,ZPFBJ,BBRQ,FJCHSX,FJCHXX,KCLX 
        FROM YYZY.T_YYZY_ZXPF_WHB 
        WHERE KSRQ=MIN_KSRQ+1 DAY 
          -- 2013年8月20日修改
          AND PFPHDM NOT IN (SELECT PFPHDM FROM JYHSF.T_JYHSF_ZSPF union select pfphdm from JYHSF.T_JYHSF_ZSPF_SDB) 
          --AND YYFPL>0  
          --2013-01-01 修改，部分砖块可能会由于生产计划等原因导致分配量为0，属于正常情况，若过滤掉这些数据，则执行配方历史数据会中断
      ) AS M 
        ON (E.PFPHDM,E.JSDM,E.YYDM,E.YYNF,E.KCLX,E.JSRQ)=
            (M.PFPHDM,M.JSDM,M.YYDM,M.YYNF,M.KCLX,M.KSRQ-1 DAY)
      WHEN MATCHED THEN 
        UPDATE SET (E.YYFPL,E.JSRQ,E.ZXSX,E.JSSYL,E.ZLYYBJ,E.ZPFBJ)= 
            (E.YYFPL+M.KSSYL,M.KSRQ,M.ZXSX,M.KSSYL,M.ZLYYBJ,M.ZPFBJ)
      WHEN NOT MATCHED THEN
        INSERT(PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,ZXSX,KSSYL,JSSYL,ZLYYBJ,ZPFBJ,KCLX)
        VALUES(M.PFPHDM,M.JSDM,M.YYDM,M.YYNF,M.KSRQ,M.KSRQ,
            M.KSSYL,M.ZXSX,M.KSSYL,M.KSSYL,M.ZLYYBJ,M.ZPFBJ,M.KCLX)
    ;
    
    --执行配方烟叶分配
    SELECT MAX(JSRQ) INTO END_DATE 
    FROM YYZY.T_YYZY_RSCJHB_WHB
    ; 
    --CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR(MIN_KSRQ+2 DAY,END_DATE,0); 
    
    SET MIN_KSRQ=MIN_KSRQ+1 DAY;
  
  END WHILE; 
  
  -- 其它分析模块数据加载
  -- 配方分析 
--  CALL APP_YYB.P_YYZY_PFFX_WHB(CURRENT DATE + 1 MONTH - DAY(CURRENT DATE) DAY + 1 DAY,MSG); 
  -- 产能分析
--  CALL APP_YYB.P_YYZY_CNFX_MAIN(0);
--  CALL APP_YYB.P_YYZY_CNFX_MAIN(1);
--  CALL APP_YYB.P_YYZY_CNFX_MX(MSG);
 
  -- 下月1日库存更新 
  CALL YYZY.P_YYZY_SJJZ_XYKC(START_DATE,MSG);
  --调度
  CALL YYZY.P_YYZY_SJJZ_DD(MSG);
  
  INSERT INTO YYZY.T_YYZY_JZ_RZ(BZ, MBB, JZSJ, SFCG)
  VALUES('加载完成','',CURRENT TIMESTAMP,1);
  
  -- 插入已经转历史的 外加工牌号 ，量 为 0 ，防止砖墙图报错
  INSERT INTO YYZY.T_YYZY_RSCJHB_WHB
  SELECT PFPHDM,'2013-12-31' AS KSRQ,'2013-12-31' AS JSRQ,0 AS JHCL_AVG,0 AS JHPC_AVG,'2011-12-06' AS BBH
  FROM YYZY.T_YYZY_WJG_PHSD 
  WHERE PFPHDM NOT IN(
      SELECT PFPHDM
      FROM YYZY.T_YYZY_RSCJHB_WHB 
      WHERE PFPHDM IN(SELECT PFPHDM FROM YYZY.T_YYZY_WJG_PHSD)
    )
  ;

  /* 2013-01-24 增加配方图相关校验 */ 
  CALL YYZY.P_YYZY_ZXPF_JC();

  /* 2013-02-22 修改，将牌号合并的设定加入到月生产计划,否则月配方单无法显示牌号合并后的正确数据 */ 
  --备份数据
  /* 2013-04-07修改，月度产量牌号不进行不合并。
  insert into YYZY.T_YYZY_YSCJH_BAK (YSCJHDM, JHNF, JHYF, YHBS, CJDM, BRIEFNM, WKSPDM, QYDM, JHCL, ZYBJ, QYBBH, XGBZ, BBH, BBRQ, KSRQ, JSRQ,PZMC, PZDM, PPDM)
  SELECT YSCJHDM, JHNF, JHYF, YHBS, CJDM, BRIEFNM, WKSPDM,QYDM, JHCL, ZYBJ, QYBBH, XGBZ, BBH, BBRQ, KSRQ, JSRQ, PZMC, PZDM, PPDM FROM YYZY.T_YYZY_YSCJH WHERE JHNF=YEAR(CURRENT DATE);
  DELETE FROM YYZY.T_YYZY_YSCJH_BAK WHERE LOAD_TIME<CURRENT TIMESTAMP - 1 MONTH;

  --修改数据
  UPDATE YYZY.T_YYZY_YSCJH AS A SET (A.YHBS,A.CJDM)=(SELECT B.YHBS,int(B.CJDM) FROM YYZY.T_YYZY_YS_PH27 AS B WHERE A.YHBS=B.YHBS_SRC AND B.ZYBJ='1' AND B.YHBS_SRC=B.YHBS)
  WHERE A.YHBS IN (SELECT YHBS_SRC FROM YYZY.T_YYZY_YS_PH27 WHERE YHBS_SRC=YHBS AND YHBS NOT IN ('26','17'))
    AND (jhnf,jhyf,bbh) in (select jhnf,jhyf,max(bbh) from YYZY.T_YYZY_YSCJH WHERE ZYBJ='1' group by jhnf,jhyf)
    AND ZYBJ='1' 
  ;
*/ 

  --3月14日更新,将库存数据每天更新至库存锁定表,以便执行配方图耗用表能够查询最新的库存数据 
  DELETE FROM YYZY.T_YYZY_YYKC_SDB;
  INSERT INTO YYZY.T_YYZY_YYKC_SDB(YYDM, YYMC, YYNF, YYKCJS)
  SELECT YYDM, YYMC, YYNF, YYKCJS 
  FROM YYZY.V_YYZY_YYKC_WHB
  ;
  
  --均匀化算法数据加载
  CALL JYHSF.P_JYHSF_SJJZ();
  
--2013-09-27 添加  
MERGE INTO JYHSF.T_JYHSF_KCJYJXX A 
USING (
SELECT YYDM,FKPC,YJ FROM JYHSF.T_JYHSF_KCJYJXX 
WHERE YYDM IN  (
SELECT YYDM 
  FROM JYHSF.T_JYHSF_KCJYJXX 
 WHERE YJ IS NOT NULL 
  GROUP BY YYDM HAVING COUNT(YYDM)>1)
  AND FKPC='1'
) B
ON A.YYDM=B.YYDM AND A.YJ IS NULL   
WHEN MATCHED THEN
UPDATE SET A.YJ=B.YJ;
  
insert into JYHSF.T_JYHSF_KCJYJXX(YYDM, FKPC, YJ, KCXS, KCLX)
values('0','1',null,999999,null);
  
END;

GRANT EXECUTE ON PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_ALL ( ) 
  TO USER DB2INST1 WITH GRANT OPTION;


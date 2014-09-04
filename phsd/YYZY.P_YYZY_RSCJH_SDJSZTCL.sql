drop PROCEDURE YYZY.P_YYZY_RSCJH_SDJSZTCL; 

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_RSCJH_SDJSZTCL 
( 
) 
  SPECIFIC PROC_YYZY_RSCJH_SDJSZTCL 
  LANGUAGE SQL
  NOT DETERMINISTIC
  NO EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
LB_MAIN:
BEGIN ATOMIC
  /* DECLARE SYSTEM VARIABLES */
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  DECLARE V_SQLSTATE CHAR(5); 
  DECLARE I_SQLCODE INTEGER; 
  DECLARE SQL_CUR_AT_END INTEGER; 
  DECLARE SQL_STMT VARCHAR(2000); 
  /* DECLARE USER-DEFINED VARIABLES */ 
  -- DECLARE V_SEPARATOR VARCHAR(50) DEFAULT ','; 

  /* DECLARE STATIC CURSOR */
  -- DECLARE C1 CURSOR /*WITH RETURN*/ FOR
  --   SELECT DISTINCT NAME, CREATOR, TYPE
  --   FROM SYSIBM.SYSTABLES
  --   ORDER BY TYPE,CREATOR,NAME
  -- ;
  /* DECLARE DYNAMIC CURSOR */
  -- DECLARE C2 CURSOR FOR S2;
  /* DECLARE EXCEPTION HANDLE */
  /*
  DECLARE UNDO HANDLER FOR SQLEXCEPTION
  BEGIN 
    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
    ; 
  END; 
  */
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  DECLARE GLOBAL TEMPORARY TABLE TB_SDJSPC --锁定结束批次
  (jsrq DATE, pfphdm INTEGER, jspc double, jhpc double) with replace on commit preserve rows not logged; 
  DECLARE GLOBAL TEMPORARY TABLE T_YYZY_RSCJHB_WHB --日生产计划临时表
    like YYZY.T_YYZY_RSCJHB_WHB with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  --清理临时表数据
  delete from session.TB_SDJSPC;
  
  --计算结束批次
  insert into session.TB_SDJSPC(pfphdm, jsrq, jspc, jhpc) 
  with 
  tmp_jsrq as (
    select pfphdm, max(jsrq) as jsrq 
    from YYZY.T_YYZY_ZXPF_SDB group by pfphdm 
  )
  , tmp_jssyl as ( 
    select PFPHDM, JSRQ, sum(JSSYL) as jssyl 
    from YYZY.T_YYZY_ZXPF_SDB 
    where (pfphdm, jsrq)in(select pfphdm, jsrq from tmp_jsrq) 
      and jssyl<>0 
    group by PFPHDM, JSRQ 
  )
  , tmp_zxs as ( 
    select m.PFPHDM, sum(DPXS) as zxs 
    from YYZY.T_YYZY_JSTZ_WHB as m 
      inner join tmp_jsrq as c 
        on m.pfphdm = c.pfphdm and c.jsrq between m.ksrq and m.jsrq 
    where m.zybj = '1' 
    group by m.pfphdm 
  )
  , tmp_jssypc as ( --计算结束使用批次
  select t1.pfphdm, t1.jsrq, jssyl / zxs as pc 
  from tmp_jssyl as t1 
    inner join tmp_zxs as t2 
      on t1.pfphdm = t2.pfphdm 
  ) 
  , tmp_rpc as ( --计算结束日期当天计划批次
    select m.PFPHDM, JHPC_AVG as jhpc
    from YYZY.T_YYZY_RSCJHB_WHB as m
      inner join tmp_jsrq as c
        on m.pfphdm = c.pfphdm
        and c.jsrq between m.ksrq and m.jsrq
  )
  , tmp_fztph as ( --获取锁定非整天的牌号
    select pfphdm
    from (
      select pfphdm, pc*-1 from tmp_jssypc 
      union all
      select pfphdm, jhpc from tmp_rpc where pfphdm in (select pfphdm from tmp_jssypc)
    ) as t(pfphdm, pc) 
    group by pfphdm 
    having sum(pc)>0 
  ) 
  select m.pfphdm, c1.jsrq, c2.pc, c3.jhpc
  from tmp_fztph as m 
    left join tmp_jsrq as c1 on m.pfphdm = c1.pfphdm 
    left join tmp_jssypc as c2 on m.pfphdm = c2.pfphdm 
    left join tmp_rpc as c3 on m.pfphdm = c3.pfphdm 
  ;
  --end of 计算结束批次
  
  lpf1 : --start of 调整结束日期的批次
  for v1 as c1 cursor for
    select pfphdm, jsrq, jspc, jhpc, minksrq
    from session.TB_SDJSPC as m 
      left join (
        select PFPHDM, min(KSRQ) as ksrq 
        from YYZY.T_YYZY_RSCJHB_WHB 
        group by pfphdm 
      ) as c(pfphdm1,minksrq) 
        on m.pfphdm = c.pfphdm1
  do 
    delete from session.T_YYZY_RSCJHB_WHB; 
    insert into session.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, JHCL_AVG, JHPC_AVG, BBRQ) 
    with params(p_rq, p_pfph, p_pc, p_pc1) as ( 
      values (v1.jsrq, v1.pfphdm, v1.jspc, v1.jhpc) 
    ) 
    , params1(p_pfph, p_rq, p_rq1, p_pc, p_pc1) as ( 
      select p_pfph, p_rq , p_rq + 1 day, p_pc - p_pc1, (p_pc - p_pc1)*-1 
      from params 
    ) 
    , t_rjh as ( --取出与调整部分相交的日计划数据
      select PFPHDM, KSRQ, JSRQ, JHCL_AVG, JHPC_AVG, BBRQ 
      from YYZY.T_YYZY_RSCJHB_WHB, params1 
      where pfphdm = p_pfph and (p_rq1 between ksrq and jsrq or p_rq between ksrq and jsrq) 
    ) 
    select pfphdm, p_rq1 + 1 day as ksrq, jsrq, JHCL_AVG, JHPC_AVG, BBRQ 
    from t_rjh, params1 
    where jsrq > p_rq1 --调整日期的之后部分,可能无数据
    union all 
    select pfphdm, ksrq, p_rq - 1 day as jsrq, JHCL_AVG, JHPC_AVG, BBRQ 
    from t_rjh, params1 
    where ksrq < p_rq --调整日期的之前部分,可能无数据
    union all 
    select pfphdm, p_rq as ksrq, p_rq as jsrq, 
      JHCL_AVG, JHPC_AVG + p_pc, BBRQ 
    from t_rjh, params1 
    where p_rq between ksrq and jsrq --锁定结束日期调整:减少结束日期内的多余计划
    union all 
    select pfphdm, p_rq1 as ksrq, p_rq1 as jsrq, 
      JHCL_AVG, JHPC_AVG + p_pc1, BBRQ 
    from t_rjh, params1 
    where p_rq1 between ksrq and jsrq --锁定结束日期+1天调整:增加结束日期内的多余计划
    order by ksrq, jsrq 
    ; 
    
    --数据入库
    delete from YYZY.T_YYZY_RSCJHB_WHB 
    where pfphdm = v1.pfphdm and (v1.jsrq between ksrq and jsrq or v1.jsrq + 1 day between ksrq and jsrq); 
    insert into YYZY.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, JHCL_AVG, JHPC_AVG, BBRQ) 
    select pfphdm, ksrq, jsrq, JHCL_AVG, JHPC_AVG, BBRQ 
    from session.T_YYZY_RSCJHB_WHB 
    ;
  end for lpf1; 
  
  --同步jyhsf
  DELETE FROM  JYHSF.T_JYHSF_RSCJHB; 
  INSERT INTO  JYHSF.T_JYHSF_RSCJHB( PFPHDM, KSRQ, JSRQ, JHCL_AVG, JHPC_AVG, BBRQ) 
  SELECT PFPHDM, KSRQ, JSRQ, JHCL_AVG, JHPC_AVG, BBRQ 
  FROM YYZY.T_YYZY_RSCJHB_WHB 
  ; 
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_RSCJH_SDJSZTCL() IS '锁定结束整天处理';


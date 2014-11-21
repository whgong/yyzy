drop PROCEDURE YYZY.P_YYZY_RSCJH_SDYCCL; 

SET SCHEMA = ETLUSR;

SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_RSCJH_SDYCCL 
( 
) 
  SPECIFIC PROC_YYZY_RSCJH_SDYCCL 
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
  DECLARE GLOBAL TEMPORARY TABLE TB_SDSYPC --锁定剩余批次
  (pfphdm INTEGER,sypc double) with replace on commit preserve rows not logged; 
  DECLARE GLOBAL TEMPORARY TABLE TB_SYNJHPC --3月内计划批次
  (pfphdm INTEGER, mjsrq date, JHPC double) with replace on commit preserve rows not logged; 
  DECLARE GLOBAL TEMPORARY TABLE T_YYZY_RSCJHB_WHB like YYZY.T_YYZY_RSCJHB_WHB 
  with replace on commit preserve rows not logged; 
  
  
  /* SQL PROCEDURE BODY */
  --清理临时表数据
  delete from session.TB_SDSYPC;
  delete from session.TB_SYNJHPC;
  delete from session.T_YYZY_RSCJHB_WHB;
  
  --计算剩余锁定批次
  insert into session.TB_SDSYPC(pfphdm, sypc)
  with 
  t_fspc as (
    select PFPHDM, max(JSPCS) as fsjspcs 
    from YYZY.T_YYZY_PFDXXB 
    where (pfphdm, nf, yf, bbh) in ( 
          select pfphdm, nf, yf, max(bbh) 
          from YYZY.T_YYZY_PFDXXB 
          group by pfphdm, nf, yf 
        ) 
    group by pfphdm 
  ) 
  select pfphdm, sum(sdpc) 
  from ( 
      select PFPHDM, YTLPC*-1 
      from YYZY.V_YYZY_SJTL_YEAR 
      union all 
      select PFPHDM, fsjspcs 
      from t_fspc 
    ) as t(pfphdm, sdpc) 
  group by pfphdm 
  having sum(sdpc)>0 
  ; 
  --end of 根据配方图锁定部分计算剩余锁定批次
  
  --计算截止3月内的生产批次
  insert into session.TB_SYNJHPC(pfphdm, mjsrq, jhpc) 
  with tb_minksrq3m as ( --计算最小ksrq
    select pfphdm, minksrq + 3 month - day(minksrq + 3 month) day as mjsrq 
    from ( 
      select PFPHDM, min(ksrq) as minksrq 
      from JYHSF.T_JYHSF_ZSPF_SDB
      group by pfphdm 
    ) as t1
  )
  select pfphdm, mjsrq, sum((days(jsrq)-days(ksrq)+1)*JHPC_AVG) as jhpc
  from (
    select j.PFPHDM, mjsrq ,KSRQ, (case when jsrq>mjsrq then mjsrq else jsrq end) as jsrq, JHPC_AVG
    from YYZY.T_YYZY_RSCJHB_WHB as j 
    inner join tb_minksrq3m as f
      on j.pfphdm = f.pfphdm
      and ksrq <= mjsrq
  ) as t2
  group by pfphdm, mjsrq
  ;
  -- end of 计算截止3月内的生产批次
  
  --增加计划批次
  insert into session.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, JHCL_AVG, JHPC_AVG, BBRQ)
  with tb_phdypc as ( 
    select pfphdm, max(mjsrq) as mjsrq, sum(SYPC) - sum(jhpc) as dypc 
    from (
      select PFPHDM, MJSRQ, JHPC, 0 as SYPC 
      from session.TB_SYNJHPC 
      union all 
      select PFPHDM, cast(null as date) as mjsrq, 0 as JHPC, SYPC 
      from session.TB_SDSYPC 
    ) as t1 
    where not exists (
        select 1 from JYHSF.T_JYHSF_PFPXB 
        where sdlx = '1' and pfphdm = t1.pfphdm
      )
    group by pfphdm 
    having sum(SYPC)-sum(jhpc) > 0 
  )
  , tb_1 as ( 
    select j.PFPHDM, KSRQ, JSRQ, JHCL_AVG, JHPC_AVG, BBRQ, mjsrq, dypc 
    from YYZY.T_YYZY_RSCJHB_WHB as j
      inner join tb_phdypc as d 
        on j.pfphdm = d.pfphdm and mjsrq between ksrq and jsrq 
  )
  select PFPHDM, mjsrq as KSRQ, mjsrq as JSRQ, 
    (JHPC_AVG + dypc)*(select dpcl 
                        from YYZY.T_YYZY_DPCLB 
                        where pfphdm = tb_1.pfphdm 
                        order by nf desc, yf desc 
                        fetch first 1 row only
                        ) as jhcl_avg, 
    JHPC_AVG + dypc as jhpc_avg, BBRQ 
  from tb_1 
  union all 
  select PFPHDM, mjsrq + 1 day as KSRQ, JSRQ, JHCL_AVG, JHPC_AVG, BBRQ 
  from tb_1 
  where mjsrq < jsrq 
  union all 
  select PFPHDM, KSRQ, mjsrq - 1 day as JSRQ, JHCL_AVG, JHPC_AVG, BBRQ 
  from tb_1 
  where mjsrq > ksrq 
  order by pfphdm, ksrq, jsrq 
  ; 
  
  delete from YYZY.T_YYZY_RSCJHB_WHB 
  where (pfphdm,ksrq, jsrq) in (
      select pfphdm, min(ksrq), max(jsrq) 
      from session.T_YYZY_RSCJHB_WHB 
      group by pfphdm 
    )
  ;
  insert into YYZY.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, JHCL_AVG, JHPC_AVG, BBRQ) 
  select pfphdm, ksrq, jsrq, JHCL_AVG, JHPC_AVG, BBRQ 
  from session.T_YYZY_RSCJHB_WHB 
  ;
  
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_RSCJH_SDYCCL() IS '锁定溢出部分处理 锁定部分压缩至3月内';


drop PROCEDURE yyzy.p_yyzy_rscjh_kbbz; 

SET SCHEMA = ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR; 

CREATE PROCEDURE yyzy.p_yyzy_rscjh_kbbz( 
  in IP_KSRQ date, 
  in IP_JSRQ date, 
  IN IP_PFPHDM INTEGER
) 
  SPECIFIC PROC_YYZY_rscjh_kbbz 
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
  declare lc_d_jhny date;
  DECLARE lc_d_jhksrq, lc_d_jhjsrq date; 
  DECLARE lc_d_yksrq, lc_d_yjsrq date; 
  DECLARE lc_n_scpc_o,lc_n_scpc_ls,lc_n_scpc, lc_n_ytlpc, lc_n_sjscpc, lc_n_syscpc decimal(18,6); 
  declare lc_n_ts, lc_n_pjs, lc_n_ys int;
  declare lc_i_i1, lc_i_lcm integer;
  declare lc_i_js, lc_i_ys integer;

  /* DECLARE STATIC CURSOR */
  -- DECLARE C1 CURSOR /*WITH RETURN*/ FOR
  --   SELECT DISTINCT NAME, CREATOR, TYPE
  --   FROM SYSIBM.SYSTABLES
  --   ORDER BY TYPE,CREATOR,NAME
  -- ;
  /* DECLARE DYNAMIC CURSOR */
  -- DECLARE C2 CURSOR FOR S2;
  /* DECLARE EXCEPTION HANDLE */
--  DECLARE UNDO HANDLER FOR SQLEXCEPTION
--  BEGIN 
--    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
--    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
--      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
--      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
--    ; 
--  END; 
--  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  DECLARE GLOBAL TEMPORARY TABLE T_YYZY_RSCJHB_WHB like YYZY.T_YYZY_RSCJHB_WHB 
  with replace on commit preserve rows not logged; 
  
  DECLARE GLOBAL TEMPORARY TABLE T_YYZY_RSCJHB (
    pfphdm integer,
    jhrq date
  ) 
  with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  ----------------------------------------------------------------------------------------
  insert into session.T_YYZY_RSCJHB(pfphdm, jhrq)
  with rq_ycz as (
    select pfphdm, riqi 
    from YYZY.T_YYZY_RSCJHB_WHB, dim.t_dim_yyzy_date 
    where riqi between ksrq and jsrq 
      and pfphdm = IP_PFPHDM 
  )
  , rq_all as (
    select pfphdm, riqi
    from (select distinct pfphdm from YYZY.T_YYZY_RSCJHB_WHB), dim.t_dim_yyzy_date
    where riqi between IP_KSRQ and IP_JSRQ
      and pfphdm = IP_PFPHDM 
  )
  , rq_dcl as (
    select pfphdm, riqi from rq_all 
    except 
    select pfphdm, riqi from rq_ycz 
  )
  select pfphdm, riqi 
  from rq_dcl
  ; 
  
  insert into YYZY.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, bbrq)
  with rjh_xh as (
    select pfphdm, jhrq, rownumber()over(partition by pfphdm order by jhrq) as xh
    from session.T_YYZY_RSCJHB
  )
  , tb_jh_cir(pfphdm, jhrq, xh, xh1) as (
    select pfphdm, jhrq, xh, 1 as xh
    from rjh_xh
    where xh = 1
    union all
    select c.pfphdm, c.jhrq, c.xh,
      (case when m.jhrq + 1 day = c.jhrq then m.xh1 else c.xh end) as xh1
    from rjh_xh as c, tb_jh_cir as m
    where m.xh+1 = c.xh 
      and m.pfphdm = c.pfphdm
  )
  select pfphdm, min(jhrq) as ksrq, max(jhrq) as jsrq, 0 , 0,
    (select max(bbrq) from YYZY.T_YYZY_RSCJHB_WHB)
  from tb_jh_cir
  group by pfphdm, xh1
  ;
  
END LB_MAIN; 

COMMENT ON PROCEDURE yyzy.p_yyzy_rscjh_kbbz(date, date, INTEGER) IS '日生产计划 空白部分补全'; 

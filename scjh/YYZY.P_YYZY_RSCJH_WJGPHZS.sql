
drop PROCEDURE YYZY.P_YYZY_RSCJH_WJGPHZS;
SET SCHEMA ETLUSR ;

SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_RSCJH_WJGPHZS
 (IN IP_STARTDATE DATE
 ) 
  SPECIFIC YYZY.PROC_YYZY_RSCJH_WJGPHZS
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  NO EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
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
  DECLARE GLOBAL TEMPORARY TABLE tb_yscjh_wjg
  (
    pfphdm INTEGER,
    jhnf integer,
    jhyf integer,
    jhcl decimal(18,6),
    yksrq date,
    yjsrq date,
    jhcl_avg decimal(18,6)
  ) with replace on commit preserve rows not logged
  ; 
  DECLARE GLOBAL TEMPORARY TABLE t_yyzy_rscjhb_whb like yyzy.t_yyzy_rscjhb_whb with replace on commit preserve rows not logged;
  
  /* SQL PROCEDURE BODY */
  delete from session.tb_yscjh_wjg;
  delete from session.t_yyzy_rscjhb_whb;
-----------------------------------------------------------------------------------
  --外加工月计划获取
  insert into session.tb_yscjh_wjg(pfphdm, jhnf, jhyf, jhcl)
  WITH tb_PPGG AS (
    select PPGGID, JHNF,CJMC,CJDM,PPMC,PPDM,YHBS,JYGG,CZR ,BBH,BBRQ, pfphdm
    from DIM.T_DIM_YYZY_WJGPPGG
    WHERE (JHNF,CJDM,PPDM,YHBS,JYGG,BBH) IN (
        SELECT JHNF,CJDM,PPDM,YHBS,JYGG,MAX(BBH) 
        FROM DIM.T_DIM_YYZY_WJGPPGG 
        GROUP BY JHNF,CJDM,PPDM,YHBS,JYGG
      )
  )
  , tb_YSCJH_WJG AS (
      select PPGGID, JHNF, JHYF, JHCL, ZSBJ, CZR, BBH, BBRQ
        from YYZY.T_YYZY_YSCJH_WJG as m
      WHERE (PPGGID,JHNF,JHYF,BBH) IN (
          SELECT PPGGID,JHNF,JHYF,MAX(BBH) 
          FROM YYZY.T_YYZY_YSCJH_WJG 
          GROUP BY PPGGID,JHNF,JHYF
        )
  )
  select c.pfphdm, m.jhnf, m.jhyf, m.jhcl
  from tb_YSCJH_WJG as m
    inner join tb_PPGG as c
      on m.PPGGID = c.ppggid 
  where m.jhnf>=year(IP_STARTDATE)
    and m.jhyf>=month(IP_STARTDATE)
  ;
  
  update session.tb_yscjh_wjg
  set yksrq = date(to_date(char(jhnf*100*100+jhyf*100+1),'YYYYMMDD'))
  ;
  update session.tb_yscjh_wjg
  set yjsrq = yksrq + 1 month - 1 day
  ;
  update session.tb_yscjh_wjg
  set jhcl_avg = jhcl*1.000000/(days(yjsrq)-days(yksrq)+1)
  ;
  
  lp1:
  for v1 as c1 cursor for
    select pfphdm, jhnf, jhyf, yksrq, yjsrq, jhcl_avg
    from session.tb_yscjh_wjg
    order by jhnf, jhyf
  do
    --指定月份中增加外加工烟叶制丝计划
    delete from session.t_yyzy_rscjhb_whb;
    insert into session.t_yyzy_rscjhb_whb(pfphdm, ksrq, jsrq , jhcl_avg, jhpc_avg)
    with 
    tb_yscjh_wjg as (
      select *
      from session.tb_yscjh_wjg
      where pfphdm = v1.pfphdm and jhnf = v1.jhnf and jhyf = v1.jhyf
    )
    ,tb_clksrq as (
      select m.pfphdm,m.ksrq,y.yksrq-1 day as jsrq, m.jhcl_avg,m.jhpc_avg 
      from yyzy.t_yyzy_rscjhb_whb as m
        inner join tb_yscjh_wjg as y 
          on m.pfphdm = y.pfphdm and (y.yksrq > m.ksrq and y.yksrq <= m.jsrq)
      union all
      select m.pfphdm,y.yksrq as ksrq,m.jsrq, m.jhcl_avg as jhcl_avg,m.jhpc_avg 
      from yyzy.t_yyzy_rscjhb_whb as m
        inner join tb_yscjh_wjg as y 
          on m.pfphdm = y.pfphdm and (y.yksrq > m.ksrq and y.yksrq <= m.jsrq)
      union all
      select m.pfphdm,m.ksrq,m.jsrq, m.jhcl_avg,m.jhpc_avg 
      from yyzy.t_yyzy_rscjhb_whb as m
        inner join tb_yscjh_wjg as y 
          on m.pfphdm = y.pfphdm and not(y.yksrq > m.ksrq and y.yksrq <= m.jsrq)
    )
    , tb_cljsrq as (
      select m.pfphdm, m.ksrq, y.yjsrq as jsrq, m.jhcl_avg as jhcl_avg, m.jhpc_avg
      from tb_clksrq as m
      inner join tb_yscjh_wjg as y 
        on m.pfphdm = y.pfphdm and (y.yjsrq >= m.ksrq and y.yjsrq < m.jsrq)
      union all
      select m.pfphdm, y.yjsrq+1 day as ksrq, m.jsrq, m.jhcl_avg, m.jhpc_avg
      from tb_clksrq as m
      inner join tb_yscjh_wjg as y 
        on m.pfphdm = y.pfphdm and (y.yjsrq >= m.ksrq and y.yjsrq < m.jsrq)
      union all
      select m.pfphdm, m.ksrq, m.jsrq, m.jhcl_avg, m.jhpc_avg
      from tb_clksrq as m
      inner join tb_yscjh_wjg as y 
        on m.pfphdm = y.pfphdm and not(y.yjsrq >= m.ksrq and y.yjsrq < m.jsrq)
    )
    select m.pfphdm, m.ksrq, m.jsrq, m.jhcl_avg+value(y.jhcl_avg,0) as jhcl_avg, jhpc_avg
    from tb_cljsrq as m
      left join tb_yscjh_wjg as y
        on m.pfphdm = y.pfphdm and (m.ksrq<=y.yjsrq and y.yksrq<=m.jsrq)
    order by 1,2,3 
    ;
    
    --入目标表
    delete from yyzy.t_yyzy_rscjhb_whb as e where exists (select 1 from session.t_yyzy_rscjhb_whb where pfphdm = e.pfphdm);
    insert into yyzy.t_yyzy_rscjhb_whb(pfphdm, ksrq, jsrq , jhcl_avg, jhpc_avg)
    select pfphdm, ksrq, jsrq , jhcl_avg, jhpc_avg
    from session.t_yyzy_rscjhb_whb;

  end for lp1;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_RSCJH_WJGPHZS(DATE) IS '日生产计划 外加工牌号制丝';
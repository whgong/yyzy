/*
--drop table YYZY.T_YYZY_TMP_RSCPCB;
create table YYZY.T_YYZY_TMP_RSCPCB 
(
  pfphdm INTEGER,
  jhrq date,
  jhpc decimal(18,6)
)
in ts_reg_16k;
*/
--drop PROCEDURE YYZY.P_YYZY_RSCJH_RJHXZ;

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_RSCJH_RJHXZ
( 
  IN IP_PFPHDM integer, 
  IN IP_I_NF integer, 
  IN IP_I_YF integer
)
  SPECIFIC PROC_YYZY_RSCJH_RJHXZ
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
  declare lc_d_yksrq, lc_d_yjsrq, lc_d_rjhksrq date;
  declare lc_d_mxjjbrq date;
  declare lc_i_yjhnf, lc_i_yjhyf integer;
  declare lc_i_dmbl decimal(18,6);

  /* DECLARE STATIC CURSOR */
--  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
--    select * from tb;
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
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  --配方牌号列表临时表
  DECLARE GLOBAL TEMPORARY TABLE tb_pfphlb
  (
    pfphdm integer
  ) with replace on commit preserve rows not logged;
  --需弥补数量临时表
  DECLARE GLOBAL TEMPORARY TABLE tb_dmbjhs
  (
    pfphdm integer,
    mbpc decimal(18,6)
  ) with replace on commit preserve rows not logged; 
  --月计划临时表
  DECLARE GLOBAL TEMPORARY TABLE tb_yscjh
  (
    pfphdm integer,
    jhpc decimal(18,6)
  ) with replace on commit preserve rows not logged; 
  DECLARE GLOBAL TEMPORARY TABLE T_YYZY_RSCJHB_WHB like YYZY.T_YYZY_RSCJHB_WHB with replace on commit preserve rows not logged; 
  --外加工月计划临时表
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
-----------------------------------------------------------------------------------------------------------
  /* SQL PROCEDURE BODY */
  --过程中使用变量赋值
  set lc_d_rjhksrq = value((select min(ksrq) from YYZY.T_YYZY_RSCJHB_WHB),date('1980-01-01'));
  set lc_i_yjhnf = IP_I_NF; 
  set lc_i_yjhyf = IP_I_YF; 
  set lc_d_yksrq = date(to_date(char(lc_i_yjhnf*100*100 + lc_i_yjhyf*100 + 1),'YYYYMMDD'));
  set lc_d_yjsrq = lc_d_yksrq + 1 month - 1 day;
----------------------------------------------------------------------------------------------------------
--  --外加工月计划获取
--  delete from session.tb_yscjh_wjg;
--  insert into session.tb_yscjh_wjg(pfphdm, jhnf, jhyf, jhcl)
--  WITH tb_PPGG AS (
--    select PPGGID, JHNF,CJMC,CJDM,PPMC,PPDM,YHBS,JYGG,CZR ,BBH,BBRQ, pfphdm
--    from DIM.T_DIM_YYZY_WJGPPGG
--    WHERE (JHNF,CJDM,PPDM,YHBS,JYGG,BBH) IN (
--        SELECT JHNF,CJDM,PPDM,YHBS,JYGG,MAX(BBH) 
--        FROM DIM.T_DIM_YYZY_WJGPPGG 
--        GROUP BY JHNF,CJDM,PPDM,YHBS,JYGG
--      )
--  )
--  , tb_YSCJH_WJG AS (
--      select PPGGID, JHNF, JHYF, JHCL, ZSBJ, CZR, BBH, BBRQ
--        from YYZY.T_YYZY_YSCJH_WJG as m
--      WHERE (PPGGID,JHNF,JHYF,BBH) IN (
--          SELECT PPGGID,JHNF,JHYF,MAX(BBH) 
--          FROM YYZY.T_YYZY_YSCJH_WJG 
--          GROUP BY PPGGID,JHNF,JHYF
--        )
--  )
--  select c.pfphdm, m.jhnf, m.jhyf, m.jhcl
--  from tb_YSCJH_WJG as m
--    inner join tb_PPGG as c
--      on m.PPGGID = c.ppggid 
--  where m.jhnf=lc_i_yjhnf
--    and m.jhyf=lc_i_yjhyf
--  ;
--  
--  --处理月生产计划
--  delete from session.tb_yscjh;
--  insert into session.tb_yscjh(pfphdm,jhpc)
--  with tb_yjh_maxbb as 
--  (
--    SELECT LSBH, YSCJHDM, int(JHNF) as jhnf, int(JHYF) as jhyf, YHBS, CJDM, BRIEFNM, 
--      WKSPDM, QYDM, JHCL, ZYBJ, QYBBH, XGBZ, BBH, BBRQ, 
--      KSRQ, JSRQ, PZMC, PZDM, PPDM 
--    FROM YYZY.T_YYZY_YSCJH
--    where (JHNF, JHYF, bbh)in (
--      select JHNF, JHYF, max(bbh)
--      from YYZY.T_YYZY_YSCJH
--      group by JHNF, JHYF
--    )
--  )
--  , tb_yjh_pfph as 
--  (
--    select jhnf, jhyf, pfphdm, sum(JHCL) as jhcl
--    from tb_yjh_maxbb as m
--      left join DIM.T_DIM_YYZY_PFPH_PHCF as p
--        on m.yhbs = p.yhbs
--        and m.cjdm = p.sccjdm
--        and p.jsrq>=current_date
--    group by jhnf, jhyf,pfphdm
--  )
--  , tb_yjh_hz as (
--    select jhnf, jhyf, pfphdm, sum(JHCL) as jhcl
--    from (
--        select pfphdm, jhnf, jhyf, jhcl from tb_yjh_pfph
--        union all
--        select pfphdm, jhnf, jhyf, jhcl from session.tb_yscjh_wjg --外加工月计划计划
--      ) as t
--    group by jhnf, jhyf, pfphdm
--  ) 
--  , tb_yjhpc as 
--  (
--    select jhnf, jhyf, m.pfphdm, jhcl, round(jhcl*1.00000/dpcl,0) as jhpc
--    from tb_yjh_hz as m
--      left join (
--        select pfphdm,dpcl
--        from yyzy.t_yyzy_dpclb 
--        where (pfphdm,nf*100+yf)in(
--          select pfphdm,max(nf*100+yf) 
--          from yyzy.t_yyzy_dpclb 
--          group by pfphdm
--          )
--      ) as d on m.pfphdm = d.pfphdm 
--  )
--  select pfphdm, sum(jhpc) as jhpc
--  from tb_yjhpc
--  where jhnf = lc_i_yjhnf and jhyf = lc_i_yjhyf --年月过滤
--    and pfphdm = IP_PFPHDM 
--  group by pfphdm
--  ;
  
  if IP_PFPHDM in (select pfphdm from YYZY.T_YYZY_YSCJHPC where jhny = lc_d_yksrq) 
      and IP_PFPHDM in (select pfphdm from YYZY.T_YYZY_SJTL_SCPC) 
--                        where date(tlsj) between lc_d_yksrq and lc_d_rjhksrq - 1 day) 
      and IP_PFPHDM in (select pfphdm from YYZY.T_YYZY_TMP_RSCPCB) 
  then
    --计算需要弥补批次
    delete from session.tb_dmbjhs;
    insert into session.tb_dmbjhs(pfphdm, mbpc) 
    with tb_sjtlpc(pfphdm,phscpc) as --当月已投料批次
    (
      select pfphdm,sum(phscpc) as phscpc --只有在当月能取得值
      from 
        (
          select tlsj, pfphdm, phscpc
          from YYZY.T_YYZY_SJTL_SCPC
          union all
          select '2013-08-01 00:00:00.123456' as tlsj,
            pfphdm, phscpc
          from YYZY.T_YYZY_SJTL_SCPC
          where tlsj = '2013-07-31 00:00:00.123456'
        ) as t
      where date(tlsj) between lc_d_yksrq and lc_d_rjhksrq - 1 day
        and pfphdm = IP_PFPHDM 
      group by pfphdm
      union all
      select pfphdm, sum(tqllpc) as tqllpc --提前领料数量
      from YYZY.T_YYZY_YSCJH_TQLLL 
      where pfphdm = IP_PFPHDM 
        and jhny = lc_d_yksrq - 1 month
      group by pfphdm
    )
    , tb_rscjhpc as --当月剩余生产批次
    (
      select pfphdm, sum(jhpc) as jhpc 
      from YYZY.T_YYZY_TMP_RSCPCB
      where pfphdm = IP_PFPHDM
        and jhrq between lc_d_yksrq and lc_d_yjsrq --2013-09-18修改bug
      group by pfphdm
    )
    , tb_jhsyl as ( --弥补数量
      select pfphdm, sum(pc) as mbpc
      from 
        (
          select pfphdm, -1*(case when phscpc<0 then 0 else phscpc end) 
          from tb_sjtlpc 
          union all
          select pfphdm, -1*jhpc from tb_rscjhpc 
          union all
          select pfphdm, sum(jhpc) from YYZY.T_YYZY_YSCJHPC -- 20131127修改月计划批次取值位置
          where pfphdm = IP_PFPHDM and jhny = lc_d_yksrq 
          group by pfphdm 
        ) as tb(pfphdm, pc) 
      group by pfphdm 
    )
    select pfphdm, mbpc 
    from tb_jhsyl
    ;
    
    set lc_i_dmbl = value((select sum(mbpc) from session.tb_dmbjhs where pfphdm = IP_PFPHDM),0);
    
    --------------------------------------------------------------------------------------------
    loop_f1: --对当月范围内逐条修正计划
    for v1 as c1 cursor for
      select pfphdm, jhrq, jhpc
      from YYZY.T_YYZY_TMP_RSCPCB
      where jhrq<=lc_d_yjsrq
      order by jhrq desc --从最后一天开始弥补
      for update of jhpc
    do
      if lc_i_dmbl = 0 then --需弥补量为0
        leave loop_f1; --结束修正计划处理
      elseif lc_i_dmbl >0 then --需弥补量大于0
        --在最后一天增加弥补量
        update YYZY.T_YYZY_TMP_RSCPCB 
        set jhpc = jhpc+lc_i_dmbl 
        where current of c1 
        ;
        set lc_i_dmbl = 0;
      elseif lc_i_dmbl*-1 > v1.jhpc then --需弥补量小于0,且当天计划不够弥补
        --弥补量减少，当天计划置0
        set lc_i_dmbl = lc_i_dmbl + v1.jhpc;
        
        update YYZY.T_YYZY_TMP_RSCPCB
        set jhpc = 0
        where current of c1
        ;
      elseif lc_i_dmbl*-1 <= v1.jhpc then --需弥补量小于0,且当天计划足够弥补
        --调整当天计划，弥补量置0
        update YYZY.T_YYZY_TMP_RSCPCB 
        set jhpc = jhpc+lc_i_dmbl 
        where current of c1 
        ; 
        set lc_i_dmbl = 0; 
      end if; 
    end for loop_f1; --以上为 对当月范围内逐条修正计划
  end if; 
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_RSCJH_RJHXZ(integer, integer, integer) IS '日生产计划修正'; 

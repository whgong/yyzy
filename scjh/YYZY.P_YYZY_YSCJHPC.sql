--drop PROCEDURE YYZY.P_YYZY_YSCJHPC; 

SET SCHEMA = ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR; 

CREATE PROCEDURE YYZY.P_YYZY_YSCJHPC( 
  in ip_nf integer, 
  in ip_yf integer 
) 
  SPECIFIC PROC_YYZY_YSCJHPC 
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
  DECLARE GLOBAL TEMPORARY TABLE scpcxl_n(
    pfphdm INTEGER, 
    jhrq date, 
    pch integer 
  ) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  ----------------------------------------------------------------------------------------
  --中间变量
  set lc_d_jhny = date(to_date(char(ip_nf*100+ip_yf),'YYYYMM'));
  ----------------------------------------------------------------------------------------
  --清除数据
  delete from YYZY.T_YYZY_YSCJHPC where jhny = lc_d_jhny and jhlx not in ('A'); 
  ----------------------------------------------------------------------------------------
  --1常规计划
  insert into YYZY.T_YYZY_YSCJHPC(PFPHDM, JHNY, JHPC, JHLX)
  with tb_yjh_maxbb as 
  ( 
    SELECT LSBH, YSCJHDM, jhnf, jhyf, YHBS, CJDM, BRIEFNM, WKSPDM, 
      QYDM, JHCL, ZYBJ, QYBBH, XGBZ, BBH, BBRQ, KSRQ, JSRQ, PZMC, PZDM, PPDM 
    FROM YYZY.T_YYZY_YSCJH 
    where (JHNF, JHYF, bbh)in ( 
        select JHNF, JHYF, max(bbh) 
        from YYZY.T_YYZY_YSCJH 
        group by JHNF, JHYF 
      ) 
      and int(jhnf) = ip_nf and int(jhyf) = ip_yf 
  ) 
  , tb_yjh_pfph as 
  (
    select jhnf, jhyf, pfphdm, sum(JHCL) as jhcl 
    from tb_yjh_maxbb as m 
      left join DIM.T_DIM_YYZY_PFPH as p 
        on m.yhbs = p.yhbs 
        and m.cjdm = p.sccjdm 
        and p.jsrq>=current_date 
    group by jhnf, jhyf,pfphdm 
  )
  , tb_yjhpc as ( 
    select date(to_date(char(jhnf*100+jhyf),'YYYYMM')) as jhny,
      pfphdm, jhcl, jhcl * 1.000000 / YYZY.F_DPCL(pfphdm) as jhpc
    from tb_yjh_pfph 
  )
  select pfphdm, jhny, round(jhpc,0) as jhpc, '1' as jhlx
  from tb_yjhpc
  where pfphdm is not null --bug修改 2014-02-18增加
  ;
  ----------------------------------------------------------------------------------------
  --2外加工本地制丝
  insert into YYZY.T_YYZY_YSCJHPC(PFPHDM, JHNY, JHPC, JHLX)
  WITH tb_WJGPPGG AS ( 
    select PPGGID, JHNF,CJMC,CJDM,PPMC,PPDM,YHBS,JYGG,CZR ,BBH,BBRQ, pfphdm 
    from DIM.T_DIM_YYZY_WJGPPGG 
    WHERE (JHNF,CJDM,PPDM,YHBS,JYGG,BBH) IN ( 
        SELECT JHNF,CJDM,PPDM,YHBS,JYGG,MAX(BBH) 
        FROM DIM.T_DIM_YYZY_WJGPPGG 
        GROUP BY JHNF,CJDM,PPDM,YHBS,JYGG 
      ) 
  ) 
  , tb_YSCJH_WJG_1 AS ( 
    select PPGGID, JHNF, JHYF, JHCL, ZSBJ, CZR, BBH, BBRQ 
    from YYZY.T_YYZY_YSCJH_WJG as m 
    WHERE (PPGGID,JHNF,JHYF,BBH) IN ( 
        SELECT PPGGID,JHNF,JHYF,MAX(BBH) 
        FROM YYZY.T_YYZY_YSCJH_WJG 
        GROUP BY PPGGID,JHNF,JHYF 
      ) 
      and JHNF = ip_nf and JHYF = ip_yf
  ) 
  , tb_YSCJH_WJG as ( 
    select c.pfphdm, m.jhnf, m.jhyf, m.jhcl 
    from tb_YSCJH_WJG_1 as m 
      inner join tb_WJGPPGG as c 
        on m.PPGGID = c.ppggid 
  ) 
  , tb_YSCJHpc_WJG as (
    select date(to_date(char(jhnf*100+jhyf),'YYYYMM')) as jhny,
      pfphdm, jhcl, jhcl *1.00000 / YYZY.F_DPCL(pfphdm) as jhpc
    from tb_YSCJH_WJG as m
  )
  select pfphdm, JHNY, round(jhpc,0) as jhpc, '2' as jhlx
  from tb_YSCJHPC_WJG
  ;
  ----------------------------------------------------------------------------------------
  --3分组加工
  loopf1: --轮询每笔分组加工记录 
  for v1 as c1 cursor for 
    select PFPHDM, NF, YF, ZYFZJGBJ, SCPC 
    from YYZY.T_YYZY_FZJG_PC 
    where zybj = '1' 
      and nf = ip_nf and yf = ip_yf 
  do 
    set lc_n_scpc_o = coalesce( 
        (select sum(jhpc) 
          from YYZY.T_YYZY_YSCJHPC 
          where jhlx in('1', 'A') 
            and pfphdm = v1.pfphdm 
            and jhny = lc_d_jhny 
        ),
        0
      )
    ; 
    
    case v1.ZYFZJGBJ 
      when '1' then --整月分组加工
        set lc_n_scpc = lc_n_scpc_o; 
      when '0' then --部分分组加工 
        set lc_n_scpc = case when v1.SCPC<=lc_n_scpc_o then v1.SCPC else lc_n_scpc_o end; 
    end case; 
    
    -----------------------------------------------------------------------------------
    --若当月合并牌号的生产批次为0, 终止该次循环，进入下一次循环 
    if lc_n_scpc <=0 then
      ITERATE loopf1; 
    end if;
    
    -----------------------------------------------------------------------------------
    --多个分组的最小公倍数计算 
    set lc_i_i1 = 1;
    loopf2: 
    for v2 as c2 cursor for 
      select bl 
      from YYZY.T_YYZY_FZJG_PHB 
      where pfphdm = v1.pfphdm 
        and lc_d_jhny between ksrq and jsrq 
      order by bl
    do 
      if lc_i_i1=1 then 
        set lc_i_lcm = v2.bl; 
      elseif lc_i_i1!=1 then 
        set lc_i_lcm = MATH.F_LCM(v2.bl ,lc_i_lcm); 
      end if; 
      set lc_i_i1 = lc_i_i1 + 1; 
    end for loopf2; --以上为 多个分组的最小公倍数计算 
    -----------------------------------------------------------------------------------
    --生产批次按最小公倍数向上取整 
    set lc_n_scpc = int(round(math.f_xsqz(round(lc_n_scpc,0), lc_i_lcm),0)); 
    -----------------------------------------------------------------------------------
    loopf3: --依次计算各分组的生产计划批次 
    for v3 as c3 cursor for 
      select FZPHDM, BL 
      from YYZY.T_YYZY_FZJG_PHB 
      where pfphdm = v1.pfphdm 
        and lc_d_jhny between ksrq and jsrq 
      order by bl desc 
    do 
      insert into YYZY.T_YYZY_YSCJHPC(PFPHDM, JHNY, JHPC, JHLX)
      values(v3.fzphdm, lc_d_jhny, lc_n_scpc / v3.bl, '3');
    end for loopf3; --以上为 依次计算各分组的生产计划批次 
    
  end for loopf1; --以上为 轮询每笔分组加工记录 
  
  ----------------------------------------------------------------------------------------
  --4外加工
  --暂无
  
END LB_MAIN; 

COMMENT ON PROCEDURE YYZY.P_YYZY_YSCJHPC(integer, integer) IS '月生产计划批次'; 

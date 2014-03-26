--drop PROCEDURE YYZY.P_YYZY_LSPF_JSBH; 

SET SCHEMA = ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR; 

CREATE PROCEDURE YYZY.P_YYZY_LSPF_JSBH 
( 
  IN IP_PFPHDM INTEGER, 
  IN IP_RQ DATE, 
  IN IP_PC DECIMAL(18,6) 
) 
  SPECIFIC PROC_YYZY_LSPF_JSBH 
  LANGUAGE SQL 
  NOT DETERMINISTIC 
  NO EXTERNAL ACTION 
  MODIFIES SQL DATA 
  CALLED ON NULL INPUT 
LB_MAIN: 
BEGIN ATOMIC 
--start of main 
  /* DECLARE SYSTEM VARIABLES */
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  DECLARE V_SQLSTATE CHAR(5); 
  DECLARE I_SQLCODE INTEGER; 
  DECLARE SQL_CUR_AT_END INTEGER; 
  DECLARE SQL_STMT VARCHAR(2000); 
  /* DECLARE USER-DEFINED VARIABLES */ 
  declare lc_i_dfppc, lc_i_bhqpc,lc_i_clpc decimal(18,6);
  declare lc_d_bhrq date; 
  declare lc_i_dpxs1, lc_i_dpxs2 integer;
  declare lc_i_kspc decimal(18,6);
  declare lc_i_rqks, rqjs date;
  declare lc_n_yclpc decimal(18,6); 
  declare lc_b_jsbhflg smallint;
  /* DECLARE STATIC CURSOR */
--  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
--    select COLUMNS from TABLES;
  
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
  declare GLOBAL TEMPORARY TABLE tmp_jsbh( 
    pfphdm integer, 
    jsdm integer, 
    bhrq date,
    bhqpc decimal(18,6)
  ) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  set lc_i_dfppc = IP_PC; 
  ---------------------------------------------------------------------------------------------
  --计算角色变化批次
  delete from session.tmp_jsbh; 
  insert into session.tmp_jsbh(pfphdm, jsdm, bhrq, bhqpc)
  with t_ksrqjsrq as ( --计算判断角色的时间范围
    select pfphdm, IP_RQ as pdksrq, max(jsrq) as pdjsrq
    from YYZY.T_YYZY_RSCJHB_WHB 
    group by pfphdm 
  )
  , t_jstz_yx as ( --过滤有效的角色
    select pfphdm, jsdm, ksrq, jsrq, dpxs 
    from YYZY.T_YYZY_JSTZ_WHB 
    where zybj = '1' and pfphdm = IP_PFPHDM 
  ) 
  , t_jsbhd as ( --计划变化日期
    --ksrq介于判断范围内，且前后角色不一致
    select pfphdm, jsdm, ksrq 
    from t_jstz_yx 
    where (pfphdm,ksrq) in ( 
        select distinct m.pfphdm, m.ksrq 
        from t_jstz_yx as m 
          inner join t_ksrqjsrq as k 
            on m.pfphdm = k.pfphdm 
          left join t_jstz_yx as c 
            on m.pfphdm = c.pfphdm 
            and m.jsdm = c.jsdm 
            and m.ksrq = c.jsrq + 1 day 
        where coalesce(m.dpxs,0) <> coalesce(c.dpxs,0) 
          and m.ksrq between k.pdksrq and k.pdjsrq 
      ) 
    union 
    --jsrq + 1 day介于判断范围内，且前后角色不一致
    select pfphdm, jsdm, jsrq + 1 day as ksrq 
    from t_jstz_yx 
    where (pfphdm,jsrq) in ( 
        select distinct m.pfphdm, m.jsrq 
        from t_jstz_yx as m 
          inner join t_ksrqjsrq as k 
            on m.pfphdm = k.pfphdm 
          left join t_jstz_yx as c 
            on m.pfphdm = c.pfphdm 
            and m.jsdm = c.jsdm 
            and c.ksrq = m.jsrq + 1 day 
        where coalesce(m.dpxs,0) <> coalesce(c.dpxs,0) 
          and m.jsrq + 1 day between k.pdksrq and k.pdjsrq 
      ) 
  ) 
  , t_scpc_ph as ( --计划批次拆分到日,便于计算
    select PFPHDM, riqi as jhrq, JHPC_AVG as scpc --??????需扣减
    from YYZY.T_YYZY_RSCJHB_WHB as m 
      inner join DIM.T_DIM_YYZY_DATE as d 
        on d.riqi between m.ksrq and m.jsrq 
    where JHPC_AVG<>0 
  ) 
  , t_jsbhpc as ( --计算变化日期前的批次
    select pfphdm, jsdm, ksrq, 
      (select sum(scpc) from t_scpc_ph where pfphdm = m.pfphdm and jhrq<m.ksrq) as pc 
    from t_jsbhd as m 
    order by pfphdm, jsdm, ksrq 
  ) 
  select * from t_jsbhpc 
  where pc is not null and pc>0 
  order by pfphdm, pc, jsdm, ksrq 
  ; 
  
  ---------------------------------------------------------------------
  loopw1: --start of 循环处理直至未核销批次内没有角色变化
  while 
    (select count(*) from session.tmp_jsbh 
      where pfphdm = IP_PFPHDM and IP_PC>bhqpc 
    ) > 0 
  do --body of 循环处理直至未核销批次内没有角色变化
    ----------------------------------------------------------------
    --获取变化前批次和变化日期
    set lc_i_bhqpc = cast(null as decimal(18,6)); 
    set lc_d_bhrq = cast(null as date); 
    select bhrq, bhqpc into lc_d_bhrq, lc_i_bhqpc 
    from session.tmp_jsbh 
    where pfphdm = IP_PFPHDM and IP_PC>bhqpc 
    order by bhqpc 
    fetch first 1 row only 
    ; 
    set lc_i_clpc = coalesce(lc_i_bhqpc,0) - coalesce(lc_n_yclpc,0); 
    ---------------------------------------------------------------
    --当前天与前一天角色是否一致
    set lc_b_jsbhflg = 0; 
    select count(*) into lc_b_jsbhflg 
    from 
      ( 
        ( 
        select PFPHDM, JSDM, KSRQ, JSRQ, DPXS from YYZY.T_YYZY_JSTZ_WHB 
        where zybj = '1' and pfphdm = IP_PFPHDM and IP_RQ between ksrq and jsrq 
        except 
        select PFPHDM, JSDM, KSRQ, JSRQ, DPXS from YYZY.T_YYZY_JSTZ_WHB 
        where zybj = '1' and pfphdm = IP_PFPHDM and IP_RQ - 1 day between ksrq and jsrq 
        ) 
        union 
        ( 
        select PFPHDM, JSDM, KSRQ, JSRQ, DPXS from YYZY.T_YYZY_JSTZ_WHB 
        where zybj = '1' and pfphdm = IP_PFPHDM and IP_RQ - 1 day between ksrq and jsrq 
        except 
        select PFPHDM, JSDM, KSRQ, JSRQ, DPXS from YYZY.T_YYZY_JSTZ_WHB 
        where zybj = '1' and pfphdm = IP_PFPHDM and IP_RQ between ksrq and jsrq 
        ) 
      ) as t 
    ; 
    if lc_b_jsbhflg>0 then --若前后两天的角色不一致
      if  (select coalesce(sum(YYSYL),0) from YYZY.T_YYZY_ZXPF_LSB_R 
            where pfphdm = IP_PFPHDM and syrq = IP_RQ - 1 day)=0 and 
          (select coalesce(sum(YYSYL),0) from JYHSF.T_JYHSF_ZSPF_LSB_R 
            where pfphdm = IP_PFPHDM and syrq = IP_RQ - 1 day)=0 
      then --前一天是否有核销数量?
        --若无分配量, 调整昨天的角色
        update YYZY.T_YYZY_JSTZ_WHB 
        set ksrq = ksrq - 1 day 
        where pfphdm = IP_PFPHDM and zybj = '1' and ksrq = IP_RQ 
        ; 
        update YYZY.T_YYZY_JSTZ_WHB 
        set jsrq = jsrq - 1 day 
        where pfphdm = IP_PFPHDM and zybj = '1' and jsrq = IP_RQ - 1 day 
        ; 
        delete from YYZY.T_YYZY_JSTZ_WHB 
        where zybj = '1' and pfphdm = IP_PFPHDM and ksrq>jsrq 
        ; 
      else --若有分配量,不能调整角色
        SIGNAL SQLSTATE '99999' --抛出异常
          SET MESSAGE_TEXT = 'user-defined exception:unprocessed condition, multiply role in one day' 
        ; 
      end if; 
    end if; 
    --------------------------------------------------------------
    --前一天IP_RQ - 1 day的计划增加lc_i_clpc
    merge into YYZY.T_YYZY_RSCJH_LSB as t 
    using (values(IP_RQ - 1 day, lc_i_clpc, lc_i_clpc*yyzy.f_dpcl(IP_PFPHDM))) as s(jhrq, jhpc, jhcl) 
      on t.jhrq = s.jhrq and t.pfphdm = IP_PFPHDM 
    when matched then 
      update set JHCL = s.jhcl + t.jhcl, 
        JHPC = s.jhpc + t.jhpc, 
        BBRQ = current_date 
    when not matched then 
      insert(PFPHDM, JHRQ, JHCL, JHPC, BBRQ) 
      values(IP_PFPHDM, s.jhrq, s.jhcl, s.jhpc, current_date) 
    ; 
    insert into YYZY.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, jhpc_avg)
    values(IP_PFPHDM,IP_RQ - 1 day,IP_RQ - 1 day,-1*lc_i_clpc); 
    
    --分配lc_i_clpc至前一天IP_RQ - 1 day
    loopf1: --start of 分配牌号的所有角色
    for v1 as c1 cursor for 
      select jsdm from YYZY.T_YYZY_JSTZ_WHB 
      where pfphdm = IP_PFPHDM and zybj = '1' 
        and IP_RQ - 1 day between ksrq and jsrq 
    do -- body of 分配牌号的所有角色
      call YYZY.P_YYZY_LSPF6PHJS_RQ(IP_PFPHDM, v1.jsdm, IP_RQ - 1 day, lc_i_clpc); 
      call YYZY.P_YYZY_LSPF7PHJS_RQ(IP_PFPHDM, v1.jsdm, IP_RQ - 1 day, lc_i_clpc); 
    end for loopf1; --end of 分配牌号的所有角色
    
    set lc_n_yclpc = coalesce(lc_n_yclpc,0) + coalesce(lc_i_clpc,0); 
    -----------------------------------------------------------------
    --删除已处理批次
    delete from session.tmp_jsbh 
    where pfphdm = IP_PFPHDM and bhqpc = lc_i_bhqpc 
    ; 
    set lc_i_dfppc = lc_i_dfppc - lc_i_clpc; 
    -----------------------------------------------------------------
    --更新角色变化日期至当天
    update YYZY.T_YYZY_JSTZ_WHB 
    set ksrq = IP_RQ  
    where pfphdm = IP_PFPHDM and zybj = '1' and ksrq = lc_d_bhrq 
    ; 
    update YYZY.T_YYZY_JSTZ_WHB 
    set jsrq = IP_RQ - 1 day 
    where pfphdm = IP_PFPHDM and zybj = '1' and jsrq = lc_d_bhrq - 1 day 
    ; 
    delete from YYZY.T_YYZY_JSTZ_WHB 
    where zybj = '1' and pfphdm = IP_PFPHDM 
      and ksrq>jsrq 
    ; 
  end while loopw1 --end of 循环处理直至未核销批次内没有角色变化
  ; 
  ---------------------------------------------------------------------------------
  --正常处理无角色部分
  --修改当天IP_RQ历史计划为lc_i_dfppc
  delete from YYZY.T_YYZY_RSCJH_LSB 
  where pfphdm = IP_PFPHDM and jhrq = IP_RQ 
  ;
  insert into YYZY.T_YYZY_RSCJH_LSB(PFPHDM, JHRQ, JHCL, JHPC, BBRQ)
  values(IP_PFPHDM, IP_RQ, lc_i_dfppc*yyzy.f_dpcl(IP_PFPHDM), lc_i_dfppc, current_date)
  ;
  insert into YYZY.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, jhpc_avg)
  values(IP_PFPHDM,IP_RQ,IP_RQ,-1*lc_i_dfppc);
  
  --分配lc_i_dfppc至当天IP_RQ
  loopf2: --start of 分配牌号的所有角色
  for v2 as c2 cursor for 
    select jsdm from YYZY.T_YYZY_JSTZ_WHB 
    where pfphdm = IP_PFPHDM and zybj = '1' 
      and IP_RQ between ksrq and jsrq 
  do -- body of 分配牌号的所有角色
    call YYZY.P_YYZY_LSPF6PHJS_RQ(IP_PFPHDM, v2.jsdm, IP_RQ, lc_i_dfppc); 
    call YYZY.P_YYZY_LSPF7PHJS_RQ(IP_PFPHDM, v2.jsdm, IP_RQ, lc_i_dfppc); 
  end for loopf2 --end of 分配牌号的所有角色
  ; 
--end of main
END LB_MAIN 
; 

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF_JSBH( 
    integer, date, decimal(18,6) 
  ) IS '历史配方 角色变化处理'
; 

drop PROCEDURE YYZY.P_YYZY_SCJH_FZJG; 

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_SCJH_FZJG() 
  SPECIFIC PROC_YYZY_SCJH_FZJG
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
  DECLARE lc_d_jhksrq, lc_d_jhjsrq date; 
  DECLARE lc_d_yksrq, lc_d_yjsrq date; 
  DECLARE lc_n_scpc_o,lc_n_scpc, lc_n_ytlpc, lc_n_sjscpc, lc_n_syscpc decimal(18,6); 
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
  --清理临时表
  delete from YYZY.T_YYZY_TMP_RSCPCB; 
  delete from YYZY.T_YYZY_TMP_RSCJHB_WHB; 
  ---------------------------------------------------------------------------------------
  --计算计划的开始、结束时间
  select min(ksrq),max(jsrq) into lc_d_jhksrq, lc_d_jhjsrq 
  from YYZY.T_YYZY_RSCJHB_WHB 
  ; 
-----------------------------------------------------------------------------------------
  loopf1: --轮询每笔分组加工记录 
  for v1 as c1 cursor for 
    select PFPHDM, NF, YF, ZYFZJGBJ, SCPC 
    from YYZY.T_YYZY_FZJG_PC 
    where zybj = '1' 
      and (nf*100+yf)>=(year(lc_d_jhksrq)*100+month(lc_d_jhksrq))
  do 
    -----------------------------------------------------------------------------------
    --获得生产批次
    set lc_d_yksrq = date(to_date(char(nf*100*100+yf*100+1),'YYYYMMDD'));
    set lc_d_yjsrq = lc_d_yksrq + 1 month - 1 day;
    set lc_d_yksrq = (case when lc_d_yksrq<lc_d_jhksrq then lc_d_jhksrq else lc_d_yksrq end);
    
    set lc_n_scpc_o = 0; 
    select sum((days(jsrq)-days(ksrq)+1)*jhpc_avg) into lc_n_scpc_o 
    from ( 
        select PFPHDM, JHCL_AVG, JHPC_AVG, 
          (case when ksrq<ksd then ksd else ksrq end) as KSRQ, 
          (case when jsrq>jsd then jsd else jsrq end) as JSRQ 
        from YYZY.T_YYZY_RSCJHB_WHB, 
          (values(lc_d_yksrq, lc_d_yjsrq)) as tb_cs(ksd,jsd) 
        where pfphdm = v1.pfphdm and (ksrq<=jsd and jsrq>=ksd) 
      ) as t 
    ; 
    case v1.ZYFZJGBJ 
      when '1' then --整月分组加工
        set lc_n_scpc = lc_n_scpc_o; 
      when '0' then --部分分组加工 
        set lc_n_scpc = v1.SCPC; 
    end case; 
    -----------------------------------------------------------------------------------
    --多个分组的最小公倍数计算 
    set lc_i_i1 = 1;
    loopf2: 
    for v2 as c2 cursor for 
      select bl 
      from YYZY.T_YYZY_FZJG_PHB 
      where pfphdm = v1.pfphdm 
        and lc_d_yksrq between ksrq and jsrq 
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
    set lc_n_scpc = int(round(math.f_xsqz(lc_n_scpc, lc_i_lcm),0)); 
    -----------------------------------------------------------------------------------
    loopf3: --依次计算各分组的生产计划批次 
    for v3 as c3 cursor for 
      select FZPHDM, BL 
      from YYZY.T_YYZY_FZJG_PHB 
      where pfphdm = v1.pfphdm 
        and lc_d_yksrq between ksrq and jsrq 
      order by bl desc 
    do 
      set lc_n_ytlpc = coalesce( --分组加工牌号的该月已投料批次
          (select sum(PHSCPC) from YYZY.T_YYZY_SJTL_SCPC 
            where date(tlsj) between lc_d_yjsrq - (day(lc_d_yjsrq) - 1) day and lc_d_yjsrq 
              and pfphdm = v3.FZPHDM 
          ),0 
        ) 
      ; 
      -- 分组加工牌号实际计划批次 
      -- 情况1：整月分组加工:合并牌号计划 / 倍率 (已在计划批次取整模块做了修正,此处无需再次抵扣) 
      -- 情况2：部分分组加工:合并牌号计划 / 倍率 - 分组加工牌号已投料数量 
      set lc_n_sjscpc = (case when v1.ZYFZJGBJ='1' then lc_n_scpc/v3.bl else lc_n_scpc / v3.bl - lc_n_ytlpc end); 
      -- 合并牌号的剩余计划批次 
      -- 情况1:整月分组加工:0; 情况2:部分分组加工: 合并牌号原计划批次 - 分组加工牌号实际计划批次 * 倍率 
      set lc_n_syscpc = (case when v1.ZYFZJGBJ='1' then 0 else lc_n_scpc_o - lc_n_sjscpc * v3.bl end); 
      set lc_n_syscpc = (case when lc_n_syscpc<0 then 0 else lc_n_syscpc end); 
      -- 计划批次的平铺
      set lc_i_js = int(lc_n_sjscpc * 1.000000 / (days(lc_d_yjsrq) - days(lc_d_yksrq) + 1)); --基数 
      set lc_i_ys = mod(int(lc_n_sjscpc), (days(lc_d_yjsrq) - days(lc_d_yksrq) + 1)); --余数 
      insert into YYZY.T_YYZY_TMP_RSCPCB(pfphdm, jhrq, jhpc) 
      select v3.FZPHDM as pfphdm, RIQI as jhrq, 
        (case 
          when riqi between lc_d_yjsrq - (lc_i_ys - 1) day and lc_d_yjsrq 
            then lc_i_js + 1 --月末部分批次 = 基数+1
          when riqi between lc_d_yksrq and lc_d_yjsrq - lc_i_ys day 
            then lc_i_js --月头部分批次 = 基数
          else 0 
        end) as jhpc 
      from DIM.T_DIM_YYZY_DATE 
      where riqi between lc_d_yksrq and lc_d_yjsrq 
      ;
    end for loopf3; --以上为 依次计算各分组的生产计划批次 
    -----------------------------------------------------------------------------------
    --处理原合并规格的生产批次
    delete from YYZY.T_YYZY_RSCJHB_WHB 
    where pfphdm = v1.pfphdm 
      and (ksrq>=lc_d_yksrq and jsrq<=lc_d_yjsrq)
    ;
    update YYZY.T_YYZY_RSCJHB_WHB 
    set jsrq = lc_d_yksrq -  1 day
    where pfphdm = v1.pfphdm 
      and lc_d_yksrq between ksrq and jsrq
    ;
    update YYZY.T_YYZY_RSCJHB_WHB 
    set ksrq = lc_d_yjsrq + 1 day
    where pfphdm = v1.pfphdm 
      and lc_d_yjsrq between ksrq and jsrq
    ;
    set lc_i_js = int(lc_n_syscpc * 1.000000 / (days(lc_d_yjsrq) - days(lc_d_yksrq) + 1)); 
    set lc_i_ys = mod(int(lc_n_syscpc), (days(lc_d_yjsrq) - days(lc_d_yksrq) + 1)); 
    insert into YYZY.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, jhpc_avg) 
    values 
      (v1.pfphdm, lc_d_yjsrq - (lc_i_ys - 1) day, lc_d_yjsrq, lc_i_js+1), 
      (v1.pfphdm, lc_d_yksrq, lc_d_yjsrq - lc_i_ys day, lc_i_js) 
    ; 
    delete from YYZY.T_YYZY_RSCJHB_WHB where pfphdm = v1.pfphdm and ksrq>jsrq; 
    
  end for loopf1; 
  -----------------------------------------------------------------------------------
  -- 分组加工牌号每日计划 -> 时间段计划
  --补全分组加工牌号生产计划缺少的日期 
  insert into YYZY.T_YYZY_TMP_RSCPCB(pfphdm, jhrq, jhpc) 
  select pfphdm, riqi, 0 
  from ( 
      select distinct pfphdm 
      from YYZY.T_YYZY_TMP_RSCPCB 
    ) as p 
  inner join DIM.T_DIM_YYZY_DATE as d on 1=1 
  where riqi between lc_d_jhksrq and lc_d_jhjsrq 
  except
  select pfphdm, jhrq, 0 
  from YYZY.T_YYZY_TMP_RSCPCB 
  ; 
  --处理分组加工牌号生产计划 合并相同批次数的日期
  insert into YYZY.T_YYZY_RSCJHB_WHB(pfphdm, ksrq, jsrq, jhpc_avg) 
  with tb_jhr as ( 
    select pfphdm, jhrq, jhpc, 
      rownumber()over(partition by pfphdm order by jhrq) as xh 
    from YYZY.T_YYZY_TMP_RSCPCB as tb 
  ) 
  , tb_jh_cir(pfphdm, jhrq, jhpc, xh, xh1) as (
    select pfphdm, jhrq, jhpc, xh, 1 
    from tb_jhr 
    where xh = 1 
    union all 
    select a.pfphdm, a.jhrq, a.jhpc, a.xh, 
      (case when a.jhpc = b.jhpc then b.xh1 else a.xh end) as xh1 
    from tb_jhr as a, tb_jh_cir as b 
    where a.xh = b.xh+1 
      and a.pfphdm = b.pfphdm 
  )
  select pfphdm, min(jhrq) as ksrq, max(jhrq) as jsrq, jhpc 
  from tb_jh_cir 
  group by pfphdm, jhpc, xh1 
  ;
  -----------------------------------------------------------------------------------
  --处理计划产量
  update YYZY.T_YYZY_RSCJHB_WHB as t 
  set jhcl_avg = jhpc_avg * (select dpcl 
                from YYZY.T_YYZY_DPCLB 
                where pfphdm = t.pfphdm 
                order by nf desc, yf desc 
                fetch first 1 row only)
  where jhcl_avg is null and jhpc_avg is not null
  ; 
  --处理版本日期
  update YYZY.T_YYZY_RSCJHB_WHB as t
  set bbrq = (select max(bbrq) from YYZY.T_YYZY_RSCJHB_WHB)
  where bbrq is null
  ; 
  
END LB_MAIN; 

COMMENT ON PROCEDURE YYZY.P_YYZY_SCJH_FZJG() IS '生产计划分组加工'; 

/*
--DROP TABLE YYZY.T_YYZY_TMP_SJTLXS;
CREATE TABLE YYZY.T_YYZY_TMP_SJTLXS(
  RQ date,
  PFPHDM integer,
  JSDM integer,
  TLSL decimal(18,6)
) 
  in ts_reg_16k
; 
*/
-------------------------------------------------
drop PROCEDURE YYZY.P_YYZY_LSPF6PHJS;

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_LSPF6PHJS
( 
  IN IP_KSRQ DATE,
  IN IP_JSRQ DATE,
  IN IP_PFZXRQ DATE,
  IN IP_PFPHDM INTEGER,
  IN IP_JSDM INTEGER,
  IN IP_TLSL DECIMAL(18,6)
)
  SPECIFIC PROC_YYZY_LSPF6PHJS
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
  declare lc_i_pfphdm, lc_i_jsdm integer;
  declare lc_n_yhyl, lc_n_tlsl,lc_n_yyfpl, lc_n_zxsx decimal(18,6);
  declare lc_i_tdsx integer;
  declare lc_c_yydm varchar(20); 
  declare lc_i_yynf, lc_i_kclx integer; 
  declare lc_d_ksrq, lc_d_jsrq date;
  
  declare lc_c_yydm_k varchar(20); 
  declare lc_i_yynf_k, lc_i_kclx_k integer; 
  declare lc_c_ZLYYBJ, lc_c_ZPFBJ char(1);

  /* DECLARE STATIC CURSOR */
--  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
--    select pfphdm, jsdm
--    from YYZY.T_YYZY_ZXPF_WHB_SD
--    WHERE PFPHDM IN(select distinct pfphdm from JYHSF.T_JYHSF_ZSPF)
--    group by pfphdm, jsdm
--    order by pfphdm, jsdm
--  ;
  DECLARE C2 CURSOR /*WITH RETURN*/ FOR
    select tdsx, yyfpl
    from YYZY.T_YYZY_ZXPF_SDB
    where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
    order by tdsx
  ;
  
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
  
  /* SQL PROCEDURE BODY */
  --回退到多余处理的日期---------------------------------------------------
  set lc_n_tlsl = value( --获取前一天的投料数量 
      (select sum(tlsl) from YYZY.T_YYZY_TMP_SJTLXS 
        where rq = IP_KSRQ-1 day 
          and pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
      ),0 
    ) 
  ; 
  loopf2: 
  for v2 as c2 cursor for
    select yyfpl, kssyl, jssyl, ksrq, jsrq
    from YYZY.T_YYZY_ZXPF_LSB
    where jsrq >= IP_KSRQ-1 day 
      and pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
    order by jsrq desc, ksrq desc, zxsx desc
    for update of jssyl,jsrq,ksrq
  do 
    update YYZY.T_YYZY_ZXPF_LSB as t 
    set jsrq = IP_KSRQ-1 day,
      jssyl = (case when yyfpl<lc_n_tlsl then yyfpl else lc_n_tlsl end)
    where current of c2
    ;
    update YYZY.T_YYZY_ZXPF_LSB
    set ksrq = jsrq
    where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
      and ksrq>jsrq
    ;
    if v2.yyfpl < lc_n_tlsl then
      set lc_n_tlsl = lc_n_tlsl - v2.yyfpl;
    end if;
  end for loopf2;
  
  --开始扣减---------------------------------------
  loopf1: --按天轮询新增投料记录
  for v1 as c1 cursor for 
    select riqi as rq, value(sum(tlsl),0) as tlsl 
    from DIM.T_DIM_YYZY_DATE as m 
      left join (
          select * from YYZY.T_YYZY_TMP_SJTLXS 
          where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
            and rq between IP_KSRQ and IP_JSRQ 
        ) as c 
        on m.riqi = c.rq 
    where m.riqi between IP_KSRQ and IP_PFZXRQ
    group by riqi 
    order by riqi 
  do 
    if((select count(*) from YYZY.T_YYZY_JSTZ_WHB 
          where zybj = '1' and pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
          and v1.rq between ksrq and jsrq)=0) then
      leave loopf1; --判断当天是否有角色, 若无角色退出当天分配
    end if;
    set lc_n_tlsl = v1.tlsl; --获取一天的投料量 
    
    loopw2: --轮询锁定烟叶
    while 1=1 do 
      --获取待扣减烟叶 
      delete from YYZY.T_YYZY_ZXPF_SDB --删除异常数据,否则可能出现死循环
      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
        and yyfpl<=0
      ;
      values ('-1',-1,-1,'0','0',0,-1) 
        into lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_n_yyfpl, lc_i_tdsx 
      ; 
      select yydm, yynf, kclx, zlyybj, zpfbj, yyfpl, tdsx 
          into lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_n_yyfpl, lc_i_tdsx 
      from YYZY.T_YYZY_ZXPF_SDB 
      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
      order by ksrq, jsrq, tdsx 
      fetch first 1 row only
      ; 
      
      if lc_c_yydm_k='-1' then --锁定部分已扣减完,使用未锁定部分
--        delete from YYZY.T_YYZY_ZXPF_WHB --删除异常数据,否则可能出现死循环
--        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
--          and yyfpl <= 0
--        ;
        select yydm, yynf, kclx, zlyybj, zpfbj, yyfpl 
            into lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_n_yyfpl
        from YYZY.T_YYZY_ZXPF_WHB 
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
          and yyfpl > 0 
        order by ksrq, jsrq, tdsx 
        fetch first 1 row only
        ; 
--        if lc_c_yydm_k='-1' then --无烟叶可扣减,退出该牌号角色的处理
--          leave loopf1; 
--        end if;
        
      end if;
      
      --获取历史中的最后一条记录
      values ('-1',-1,-1,date('1980-01-01'),date('1980-01-01'),-1) 
        into lc_c_yydm, lc_i_yynf, lc_i_kclx, lc_d_ksrq, lc_d_jsrq, lc_n_zxsx
      ;
      select yydm, yynf, kclx, ksrq, jsrq, zxsx 
          into lc_c_yydm, lc_i_yynf, lc_i_kclx, lc_d_ksrq, lc_d_jsrq, lc_n_zxsx
      from YYZY.T_YYZY_ZXPF_LSB 
      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
      order by jsrq desc, ksrq desc, zxsx desc 
      fetch first 1 row only 
      ;
      
      if lc_n_tlsl = 0 and lc_c_yydm<>'-1' then --若当天扣减投料量为0
        update YYZY.T_YYZY_ZXPF_LSB as t --延长最后一个砖块
        set jsrq = v1.rq, jssyl=0 
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
          and yydm = lc_c_yydm and yynf = lc_i_yynf 
          and ksrq = lc_d_ksrq and jsrq = lc_d_jsrq 
          and kclx = lc_i_kclx and zxsx = lc_n_zxsx 
        ;
        leave loopw2; --结束该天的数量分配
      elseif lc_c_yydm_k = '-1' then --无烟叶可扣减,退出该牌号角色的处理
        leave loopw2; --结束该天的数量分配
      elseif lc_n_yyfpl > lc_n_tlsl then --若足够扣减投料量
        if (lc_c_yydm_k = lc_c_yydm and lc_i_yynf_k = lc_i_yynf 
            and lc_i_kclx = lc_i_kclx_k 
            ) then -- 若扣减烟叶与历史匹配
          update YYZY.T_YYZY_ZXPF_LSB as t --更新已有记录
          set t.yyfpl = t.yyfpl+lc_n_tlsl, t.jsrq = v1.RQ, t.jssyl = lc_n_tlsl
          where pfphdm = IP_PFPHDM and jsdm = IP_JSDM and yydm = lc_c_yydm 
            and yynf = lc_i_yynf and kclx = lc_i_kclx and zxsx = lc_n_zxsx
          ;
        else -- 若未匹配
          insert into YYZY.T_YYZY_ZXPF_LSB --新增记录
          values( 
            IP_PFPHDM, IP_JSDM, lc_c_yydm_k, lc_i_yynf_k, v1.RQ, v1.RQ, lc_n_tlsl,
            value((select sum(jssyl) from YYZY.T_YYZY_ZXPF_LSB 
                where pfphdm=IP_PFPHDM and jsdm=IP_JSDM and jsrq = v1.RQ),0), 
            lc_n_tlsl, lc_n_tlsl, lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_i_kclx_k 
          )
          ;
        end if;
        
        update YYZY.T_YYZY_ZXPF_SDB --锁定表中扣除已抵扣数量
        set yyfpl = yyfpl - lc_n_tlsl
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
          and tdsx = lc_i_tdsx
        ;
        
        leave loopw2; --结束改天的数量分配
        
      else --若不够或刚够扣减投料量
        if (lc_c_yydm_k = lc_c_yydm and lc_i_yynf_k = lc_i_yynf 
            and lc_i_kclx = lc_i_kclx_k
            ) then -- 若扣减烟叶与历史匹配 
          update YYZY.T_YYZY_ZXPF_LSB as t --更新已有记录
          set yyfpl = yyfpl+lc_n_yyfpl, jssyl = lc_n_yyfpl, jsrq = v1.RQ 
          where pfphdm = IP_PFPHDM and jsdm = IP_JSDM and yydm = lc_c_yydm 
            and yynf = lc_i_yynf and kclx = lc_i_kclx and zxsx = lc_n_zxsx
          ;
        else -- 若未匹配
          insert into YYZY.T_YYZY_ZXPF_LSB --新增记录
          values (
            IP_PFPHDM, IP_JSDM, lc_c_yydm_k, lc_i_yynf_k, v1.RQ, v1.RQ, lc_n_yyfpl, 
            value((select sum(jssyl) from YYZY.T_YYZY_ZXPF_LSB 
              where pfphdm=IP_PFPHDM and jsdm=IP_JSDM and jsrq = v1.RQ),0), 
            lc_n_yyfpl, lc_n_yyfpl, lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_i_kclx_k 
          )
          ;
        end if;

        delete from YYZY.T_YYZY_ZXPF_SDB --删除使用完的烟叶
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
          and tdsx = lc_i_tdsx
        ;
        set lc_n_tlsl = lc_n_tlsl -lc_n_yyfpl; --扣除已抵扣数量
        
        if lc_n_tlsl <= 0 then --若剩余的投料数量为0
          leave loopw2; --结束该天的数量分配
        end if;
        
      end if; 
      
    end while loopw2; 
    
  end for loopf1; 
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF6PHJS( 
    date, date, date, integer, integer,  decimal(18,6) 
  ) IS '6要素历史配方 牌号角色'
;

-------------------------------------------------------------------------------------
drop PROCEDURE YYZY.P_YYZY_LSPF6;

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_LSPF6
( 
  IN IP_PFZXRQ DATE
)
  SPECIFIC PROC_YYZY_LSPF6
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
  declare lc_i_pfphdm, lc_i_jsdm integer;
  declare lc_n_yhyl, lc_n_tlsl,lc_n_yyfpl decimal(18,6);
  declare lc_i_tdsx integer;
  declare lc_d_tlksrq, lc_d_tljsrq date;

  /* DECLARE STATIC CURSOR */
--  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
--    select pfphdm, jsdm
--    from YYZY.T_YYZY_ZXPF_SDB
--    WHERE PFPHDM IN (select distinct pfphdm from JYHSF.T_JYHSF_ZSPF_SDB)
--    group by pfphdm, jsdm
--    order by pfphdm, jsdm
--  ;
  
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
  DECLARE GLOBAL TEMPORARY TABLE t_yyzy_sjtll (
    rq date,
    pfphdm integer,
    jsdm integer,
    tlsl decimal(18,6)
  ) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */ 
  delete from YYZY.T_YYZY_TMP_SJTLXS; 
  insert into YYZY.T_YYZY_TMP_SJTLXS 
  select date(tlsj) as tlrq,m.pfphdm,jsdm,sum(phscpc * dpxs) as tlsl 
  from YYZY.T_YYZY_SJTL_SCPC as m 
    inner join YYZY.T_YYZY_JSTZ_WHB as j 
      on m.pfphdm = j.pfphdm 
      and date(m.tlsj) between j.ksrq and j.jsrq 
  where j.zybj = '1' 
    and date(tlsj) >= ( 
          select date(CSZ) 
          from YYZY.T_YYZY_STCS 
          where csmc = 'ZSPFFSQSRQ' 
          fetch first 1 row only 
        ) 
  group by date(tlsj),m.pfphdm,jsdm 
  ;
  
  lp1: 
  for v1 as c1 cursor for 
    select distinct pfphdm 
    from JYHSF.T_JYHSF_ZSPF_SDB 
    union 
    select distinct pfphdm 
    from JYHSF.T_JYHSF_ZSPF 
  do 
    --step1:计算ksrq,jsrq
    set lc_d_tlksrq = value((select date(max(ZDTLSJ))+1 day as ksrq 
                        from YYZY.T_YYZY_SJTL_SCPC_RZJL 
                        where pfphdm = v1.pfphdm and date(GXSJ)<IP_PFZXRQ+1 day
                      ),date('1980-01-01'))
    ;
    set lc_d_tljsrq = value((select date(max(ZDTLSJ)) as jsrq 
                        from YYZY.T_YYZY_SJTL_SCPC_RZJL 
                        where pfphdm = v1.pfphdm 
                          and date(GXSJ)=(
                              select date(max(gxsj)) from YYZY.T_YYZY_SJTL_SCPC_RZJL
                              where pfphdm = v1.pfphdm and date(gxsj)<=IP_PFZXRQ+1 day
                            )
                      ),date('1980-01-01'))
    ;
    
    delete from YYZY.T_YYZY_RSCJH_LSB where pfphdm = v1.pfphdm and jhrq>=lc_d_tlksrq;
    insert into YYZY.T_YYZY_RSCJH_LSB(PFPHDM, JHRQ, JHCL, JHPC, BBRQ)
    select v1.pfphdm, riqi as jhrq, 
      value(case when dpcl is null then phscpc else phscpc*dpcl end,0) as jhcl ,
      value(phscpc,0) as jhpc ,
      current_date as bbrq
    from DIM.T_DIM_YYZY_DATE as m
      left join (
          select date(tlsj) as tlrq,pfphdm,phscpc 
          from YYZY.T_YYZY_SJTL_SCPC 
          where pfphdm = v1.pfphdm
        ) as t
        on tlrq = m.riqi
      left join (
          select PFPHDM,DPCL
          from YYZY.T_YYZY_DPCLB
          where (pfphdm, nf*100+yf)in(
              select pfphdm, max(nf*100+yf) 
              from YYZY.T_YYZY_DPCLB group by pfphdm
            )
        ) as d
        on t.pfphdm = d.pfphdm
    where riqi between lc_d_tlksrq and IP_PFZXRQ
      and v1.pfphdm<>16
    ;
    
    lp2:
    for v2 as c2 cursor for 
      select distinct jsdm
      from JYHSF.T_JYHSF_ZSPF_SDB 
      where pfphdm = v1.pfphdm
      union 
      select distinct jsdm 
      from JYHSF.T_JYHSF_ZSPF 
      where pfphdm = v1.pfphdm 
    do 
      if lc_d_tljsrq<lc_d_tlksrq or lc_d_tljsrq is null then --若当天没有新增投料记录
        set lc_n_tlsl = 0;
      else
        --step2:计算投料烟叶数量
        set lc_n_tlsl = value(
            (select sum(tlsl) from YYZY.T_YYZY_TMP_SJTLXS 
              where pfphdm = v1.pfphdm and jsdm = v2.jsdm 
                and rq between lc_d_tlksrq and lc_d_tljsrq 
            ) ,0
          )
        ;
      end if;
      --对一个配方牌号一个角色进行历史扣减
      call YYZY.P_YYZY_LSPF6PHJS(lc_d_tlksrq, lc_d_tljsrq, IP_PFZXRQ, v1.pfphdm, v2.jsdm, lc_n_tlsl);
      
    end for lp2;
    
  end for lp1;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF6(date) IS '6要素历史配方';

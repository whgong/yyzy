/*
DROP TABLE YYZY.T_YYZY_TMP_SJTLXS;
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
drop PROCEDURE YYZY.P_YYZY_LSPF7PHJS; 

SET SCHEMA = ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR; 

CREATE PROCEDURE YYZY.P_YYZY_LSPF7PHJS 
( 
  IN IP_KSRQ DATE, 
  IN IP_JSRQ DATE, 
  IN IP_PFZXRQ DATE, 
  IN IP_PFPHDM INTEGER, 
  IN IP_JSDM INTEGER, 
  IN IP_TLSL DECIMAL(18,6) 
) 
  SPECIFIC PROC_YYZY_LSPF7PHJS
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
  declare lc_c_yypc varchar(4);
  declare lc_i_yynf, lc_i_kclx integer; 
  declare lc_d_ksrq, lc_d_jsrq date; 
  
  declare lc_c_yydm_k varchar(20); 
  declare lc_c_yypc_k varchar(4); 
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
    from JYHSF.T_JYHSF_ZSPF_SDB
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
    from JYHSF.T_JYHSF_ZSPF_LSB 
    where jsrq >= IP_KSRQ-1 day 
      and pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
    order by jsrq desc, ksrq desc, zxsx desc 
    for update of jssyl,jsrq,ksrq 
  do 
    update JYHSF.T_JYHSF_ZSPF_LSB as t 
    set jsrq = IP_KSRQ-1 day, 
      jssyl = (case when yyfpl<lc_n_tlsl then yyfpl else lc_n_tlsl end) 
    where current of c2 
    ;
    update JYHSF.T_JYHSF_ZSPF_LSB
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
    set lc_n_tlsl = v1.tlsl; --获取一天的投料量 
    
    loopw2: --轮询锁定烟叶
    while 1=1 do 
      --获取待扣减烟叶 
      delete from JYHSF.T_JYHSF_ZSPF_SDB --删除异常数据,否则可能出现死循环
      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
        and yyfpl<=0
      ;
      values ('-1',-1,-1,'0','0',0,-1,'-1') 
        into lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, 
          lc_n_yyfpl, lc_i_tdsx, lc_c_yypc_k 
      ; 
      select yydm, yynf, kclx, zlyybj, zpfbj, yyfpl, tdsx, yypc 
          into lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, 
            lc_n_yyfpl, lc_i_tdsx, lc_c_yypc_k 
      from JYHSF.T_JYHSF_ZSPF_SDB 
      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
      order by ksrq, jsrq, tdsx 
      fetch first 1 row only 
      ; 
      
      if lc_c_yydm_k='-1' then --锁定部分已扣减完,使用未锁定部分
        delete from JYHSF.T_JYHSF_ZSPF --删除异常数据,否则可能出现死循环
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
          and yyfpl <= 0
        ;
        select yydm, yynf, kclx, zlyybj, zpfbj, yyfpl, yypc 
            into lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, 
              lc_n_yyfpl, lc_c_yypc_k 
        from JYHSF.T_JYHSF_ZSPF 
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
        order by ksrq, jsrq, tdsx 
        fetch first 1 row only
        ; 
        if lc_c_yydm_k='-1' then --无烟叶可扣减,退出该牌号角色的处理
          leave loopf1; 
        end if;
        
      end if;
      
      --获取历史中的最后一条记录
      values ('-1',-1,-1,date('1980-01-01'),date('1980-01-01'),-1,'-1') 
        into lc_c_yydm, lc_i_yynf, lc_i_kclx, lc_d_ksrq, lc_d_jsrq, lc_n_zxsx, lc_c_yypc
      ;
      select yydm, yynf, kclx, ksrq, jsrq, zxsx, yypc
          into lc_c_yydm, lc_i_yynf, lc_i_kclx, lc_d_ksrq, lc_d_jsrq, lc_n_zxsx, lc_c_yypc
      from JYHSF.T_JYHSF_ZSPF_LSB 
      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
      order by jsrq desc, ksrq desc, zxsx desc 
      fetch first 1 row only 
      ;
      
      if lc_n_tlsl = 0 and lc_c_yydm<>'-1' then --若当天扣减投料量为0
        update JYHSF.T_JYHSF_ZSPF_LSB as t --延长最后一个砖块
        set jsrq = v1.rq, jssyl=0 
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
          and yydm = lc_c_yydm and yynf = lc_i_yynf and yypc = lc_c_yypc 
          and ksrq = lc_d_ksrq and jsrq = lc_d_jsrq 
          and kclx = lc_i_kclx and zxsx = lc_n_zxsx 
        ;
        leave loopw2; --结束该天的数量分配
      elseif lc_n_yyfpl > lc_n_tlsl then --若足够扣减投料量
        if (lc_c_yydm_k = lc_c_yydm and lc_i_yynf_k = lc_i_yynf 
            and lc_i_kclx = lc_i_kclx_k and lc_c_yypc = lc_c_yypc_k 
            ) then -- 若扣减烟叶与历史匹配
          update JYHSF.T_JYHSF_ZSPF_LSB as t --更新已有记录
          set t.yyfpl = t.yyfpl+lc_n_tlsl, t.jsrq = v1.RQ, t.jssyl = lc_n_tlsl
          where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
            and yydm = lc_c_yydm and yypc = lc_c_yypc and yynf = lc_i_yynf 
            and kclx = lc_i_kclx and zxsx = lc_n_zxsx
          ;
        else -- 若未匹配
          insert into JYHSF.T_JYHSF_ZSPF_LSB( --新增记录
            PFPHDM, JSDM, YYDM, YYNF, KSRQ, JSRQ, YYFPL, 
            ZXSX, KSSYL, JSSYL, ZLYYBJ, ZPFBJ,KCLX, YYPC 
          ) 
          values(
            IP_PFPHDM, IP_JSDM, lc_c_yydm_k, lc_i_yynf_k, v1.RQ, v1.RQ, lc_n_tlsl,
            value((select sum(jssyl) from JYHSF.T_JYHSF_ZSPF_LSB 
              where pfphdm=IP_PFPHDM and jsdm=IP_JSDM and jsrq = v1.RQ),0), 
            lc_n_tlsl, lc_n_tlsl, lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_i_kclx_k, lc_c_yypc_k 
          )
          ;
        end if;
        
        update JYHSF.T_JYHSF_ZSPF_SDB --锁定表中扣除已抵扣数量
        set yyfpl = yyfpl - lc_n_tlsl
        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
          and tdsx = lc_i_tdsx
        ;
        
        leave loopw2; --结束该天的数量分配
        
      else --若不够或刚够扣减投料量
        if (lc_c_yydm_k = lc_c_yydm and lc_i_yynf_k = lc_i_yynf 
            and lc_i_kclx = lc_i_kclx_k and lc_c_yypc = lc_c_yypc_k 
            ) then -- 若扣减烟叶与历史匹配
          update JYHSF.T_JYHSF_ZSPF_LSB as t --更新已有记录
          set yyfpl = yyfpl+lc_n_yyfpl, jssyl = lc_n_yyfpl, jsrq = v1.RQ 
          where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
            and yydm = lc_c_yydm and yypc = lc_c_yypc and yynf = lc_i_yynf 
            and kclx = lc_i_kclx and zxsx = lc_n_zxsx
          ;
        else -- 若未匹配
          insert into JYHSF.T_JYHSF_ZSPF_LSB( --新增记录 
            PFPHDM, JSDM, YYDM, YYNF, KSRQ, JSRQ, YYFPL, 
            ZXSX, KSSYL, JSSYL, ZLYYBJ, ZPFBJ,KCLX, YYPC 
          ) 
          values (
            IP_PFPHDM, IP_JSDM, lc_c_yydm_k, lc_i_yynf_k, v1.RQ, v1.RQ, lc_n_yyfpl, 
            value((select sum(jssyl) from JYHSF.T_JYHSF_ZSPF_LSB 
              where pfphdm=IP_PFPHDM and jsdm=IP_JSDM and jsrq = v1.RQ),0), 
            lc_n_yyfpl, lc_n_yyfpl, lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_i_kclx_k, lc_c_yypc_k 
          )
          ; 
        end if; 
        
        delete from JYHSF.T_JYHSF_ZSPF_SDB --删除使用完的烟叶
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

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF7PHJS( 
    date, date, date, integer, integer,  decimal(18,6) 
  ) IS '7要素历史配方 牌号角色'
;

-------------------------------------------------------------------------------------
drop PROCEDURE YYZY.P_YYZY_LSPF7;

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_LSPF7
( 
  IN IP_PFZXRQ DATE
)
  SPECIFIC PROC_YYZY_LSPF7
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
  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
    select pfphdm, jsdm
    from JYHSF.T_JYHSF_ZSPF_SDB
    group by pfphdm, jsdm
    order by pfphdm, jsdm
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
  DECLARE GLOBAL TEMPORARY TABLE t_yyzy_sjtll(
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
  
  open c1;
  lp1:loop
    set SQL_CUR_AT_END = 0;
    fetch c1 into lc_i_pfphdm, lc_i_jsdm;
    if SQL_CUR_AT_END=1 then leave lp1; end if;
    
    --step1:计算ksrq,jsrq
    set lc_d_tlksrq = value((select date(max(ZDTLSJ))+1 day as ksrq 
                        from YYZY.T_YYZY_SJTL_SCPC_RZJL 
                        where pfphdm = lc_i_pfphdm and date(GXSJ)<IP_PFZXRQ+1 day
                      ),date('1980-01-01'))
    ;
    set lc_d_tljsrq = value((select date(max(ZDTLSJ)) as jsrq 
                        from YYZY.T_YYZY_SJTL_SCPC_RZJL 
                        where pfphdm = lc_i_pfphdm 
                          and date(GXSJ)=(
                              select date(max(gxsj)) from YYZY.T_YYZY_SJTL_SCPC_RZJL
                              where pfphdm = lc_i_pfphdm and date(gxsj)<=IP_PFZXRQ+1 day
                            )
                      ),date('1980-01-01'))
    ;
    
    if lc_d_tljsrq<lc_d_tlksrq or lc_d_tljsrq is null then --若当天没有新增投料记录
      set lc_n_tlsl = 0;
    else
      --step2:计算投料烟叶数量
      set lc_n_tlsl = value(
          (select sum(tlsl) 
            from session.t_yyzy_sjtll 
            where pfphdm = lc_i_pfphdm 
              and jsdm = lc_i_jsdm 
              and rq between lc_d_tlksrq and lc_d_tljsrq 
          )
          ,0
        )
      ;
    end if;
    --对一个配方牌号一个角色进行历史扣减
    call YYZY.P_YYZY_LSPF7PHJS(lc_d_tlksrq, lc_d_tljsrq, IP_PFZXRQ, lc_i_pfphdm, lc_i_jsdm, lc_n_tlsl);
    
  end loop lp1;
  close c1;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF7(date) IS '7要素历史配方';

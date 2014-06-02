--drop PROCEDURE YYZY.P_YYZY_LSPF;

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_LSPF
( 
  IN IP_PFZXRQ DATE
)
  SPECIFIC PROC_YYZY_LSPF
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
--    from JYHSF.T_JYHSF_ZSPF_SDB
--    group by pfphdm, jsdm
--    union 
--    select pfphdm, jsdm 
--    from JYHSF.T_JYHSF_ZSPF 
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
--  DECLARE GLOBAL TEMPORARY TABLE t_yyzy_sjtll(
--    rq date,
--    pfphdm integer,
--    jsdm integer,
--    tlsl decimal(18,6)
--  ) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
--  delete from YYZY.T_YYZY_TMP_SJTLXS; 
--  insert into YYZY.T_YYZY_TMP_SJTLXS 
--  select date(tlsj) as tlrq,m.pfphdm,jsdm,sum(phscpc * dpxs) as tlsl 
--  from YYZY.T_YYZY_SJTL_SCPC as m 
--    inner join YYZY.T_YYZY_JSTZ_WHB as j 
--      on m.pfphdm = j.pfphdm 
--      and date(m.tlsj) between j.ksrq and j.jsrq 
--  where j.zybj = '1' 
--    and date(tlsj) >= ( 
--          select date(CSZ) 
--          from YYZY.T_YYZY_STCS 
--          where csmc = 'ZSPFFSQSRQ' 
--          fetch first 1 row only 
--        ) 
--  group by date(tlsj),m.pfphdm,jsdm 
--  ;
  
  ------------------------------------------------------------------
  loopf1: --start of 轮询所有配方(上海地区，非梗)
  for v1 as c1 cursor for
    select pfphdm from JYHSF.T_JYHSF_ZSPF_SDB group by pfphdm
    union 
    select pfphdm from JYHSF.T_JYHSF_ZSPF group by pfphdm
  do --body of 轮询所有配方(上海地区，非梗)
    ---------------------------------------------------------------------------------------
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
    set lc_d_tljsrq = (case when lc_d_tljsrq<lc_d_tlksrq then lc_d_tlksrq else lc_d_tljsrq end);
    set lc_d_tljsrq = (case when lc_d_tljsrq>IP_PFZXRQ then IP_PFZXRQ else lc_d_tljsrq end);
    
    --特殊处理,若牌号长久没有核销数据,只计算最后一天
    if (select count(*) from YYZY.T_YYZY_SJTL_SCPC 
          where pfphdm = v1.pfphdm 
          and date(tlsj) between lc_d_tlksrq and lc_d_tljsrq)=0 
    then 
      set lc_d_tlksrq = IP_PFZXRQ; 
    end if;
    
    -------------------------------------------------------------------------
    loopf2: --start of 轮询投料记录
    for v2 as c2 cursor for
      select RIQI as rq, coalesce(phscpc,0) as pc 
      from DIM.T_DIM_YYZY_DATE as d
      left join (
        select * from YYZY.T_YYZY_SJTL_SCPC
        where pfphdm = v1.pfphdm 
      ) as m on d.riqi = date(m.tlsj)
      where riqi between lc_d_tlksrq and IP_PFZXRQ 
      order by RIQI 
    do --body of 轮询投料记录
      -------------------------------------------------------------
      --历史配方日表数据
      delete from JYHSF.T_JYHSF_ZSPF_LSB_R where pfphdm = v1.pfphdm and syrq>=v2.rq; 
      delete from YYZY.T_YYZY_ZXPF_LSB_R where pfphdm = v1.pfphdm and syrq>=v2.rq; 
      delete from YYZY.T_YYZY_RSCJH_LSB where pfphdm = v1.pfphdm and jhrq>=v2.rq; 
      call YYZY.P_YYZY_LSPF_JSBH(v1.pfphdm, v2.rq, v2.pc); 
    end for loopf2 --end of 轮询投料记录
    ; 
  end for loopf1 --end of 轮询所有配方(上海地区，非梗)
  ; 
  ---------------------------------------------------------------------------------
  --日数据合并为时间段
  call YYZY.P_YYZY_LSPF_SJDHB; 
  ---------------------------------------------------------------------------------
  --删除日表中多余数据(不是端点且数量为0)
  delete from JYHSF.T_JYHSF_ZSPF_LSB_R --删除
  where (pfphdm, jsdm, syrq) not in ( --不是端点
        select pfphdm, jsdm, ksrq from JYHSF.T_JYHSF_ZSPF_LSB 
        union 
        select pfphdm, jsdm, jsrq from JYHSF.T_JYHSF_ZSPF_LSB
      ) 
    and YYSYL = 0 --数量为0
  ;
  delete from YYZY.T_YYZY_ZXPF_LSB_R --删除
  where (pfphdm, jsdm, syrq)not in ( --不是端点
        select pfphdm, jsdm, ksrq from YYZY.T_YYZY_ZXPF_LSB 
        union 
        select pfphdm, jsdm, jsrq from YYZY.T_YYZY_ZXPF_LSB
      )
    and yysyl=0 --数量为0
  ; 
  ---------------------------------------------------------------------------------
  --end of LB_MAIN
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF(date) IS '历史配方数据核销'; 

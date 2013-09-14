--drop PROCEDURE YYZY.P_YYZY_LSPF6PHJS;
--
--SET SCHEMA = ETLUSR;
--SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;
--
--CREATE PROCEDURE YYZY.P_YYZY_LSPF6PHJS
--( 
--  IN IP_KSRQ DATE,
--  IN IP_JSRQ DATE,
--  IN IP_PFPHDM INTEGER,
--  IN IP_JSDM INTEGER,
--  IN IP_TLSL DECIMAL(18,6)
--)
--  SPECIFIC PROC_YYZY_LSPF6PHJS
--  LANGUAGE SQL
--  NOT DETERMINISTIC
--  NO EXTERNAL ACTION
--  MODIFIES SQL DATA
--  CALLED ON NULL INPUT
--LB_MAIN:
--BEGIN ATOMIC
--  /* DECLARE SYSTEM VARIABLES */
--  DECLARE SQLSTATE CHAR(5); 
--  DECLARE SQLCODE INTEGER; 
--  DECLARE V_SQLSTATE CHAR(5); 
--  DECLARE I_SQLCODE INTEGER; 
--  DECLARE SQL_CUR_AT_END INTEGER; 
--  DECLARE SQL_STMT VARCHAR(2000); 
--  /* DECLARE USER-DEFINED VARIABLES */ 
--  declare lc_i_pfphdm, lc_i_jsdm integer;
--  declare lc_n_yhyl, lc_n_tlsl,lc_n_yyfpl decimal(18,6);
--  declare lc_i_tdsx integer;
--
--  /* DECLARE STATIC CURSOR */
----  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
----    select pfphdm, jsdm
----    from YYZY.T_YYZY_ZXPF_WHB_SD
----    WHERE PFPHDM IN(select distinct pfphdm from JYHSF.T_JYHSF_ZSPF)
----    group by pfphdm, jsdm
----    order by pfphdm, jsdm
----  ;
--  DECLARE C2 CURSOR /*WITH RETURN*/ FOR
--    select tdsx, yyfpl
--    from YYZY.T_YYZY_ZXPF_SDB
--    where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
--    order by tdsx
--  ;
--  
--  /* DECLARE DYNAMIC CURSOR */
--  -- DECLARE C2 CURSOR FOR S2;
--  /* DECLARE EXCEPTION HANDLE */
----  DECLARE UNDO HANDLER FOR SQLEXCEPTION
----  BEGIN 
----    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
----    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
----      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
----      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
----    ; 
----  END; 
--  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
--  /* DECLARE TEMPORARY TABLE */
--  
--  /* SQL PROCEDURE BODY */
----  insert into DEBUG.T_DEBUG_MSG(msg)values('input params:IP_KSRQ='||char(IP_KSRQ)||';IP_JSRQ='||char(IP_JSRQ)||';');
--  set lc_n_tlsl = IP_TLSL;
--  open c2;
--  lp2:loop
--    set SQL_CUR_AT_END = 0;
--    fetch c2 into lc_i_tdsx, lc_n_yyfpl; --��������sdb�е���Ҷ������
--    if SQL_CUR_AT_END=1 then leave lp2; end if;
----    set lc_n_yhyl = lc_n_yhyl + lc_n_yyfpl;
--    if lc_n_yyfpl>IP_TLSL then --���㹻�ۼ�Ͷ����
--      merge into YYZY.T_YYZY_ZXPF_LSB as t
--      using (
--        select PFPHDM, JSDM, YYDM, YYNF, KSRQ, JSRQ, YYFPL, SDBH, 
--            ZXSX, TDSX, KSSYL, JSSYL, ZLYYBJ, ZPFBJ, FJCHSX, FJCHXX, 
--            KCLX, BBRQ, LOAD_TIME
--        from YYZY.T_YYZY_ZXPF_SDB
--        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
--          and tdsx = lc_i_tdsx
--          and (ksrq<=IP_JSRQ and IP_KSRQ<=jsrq)
--      ) as s 
--        on t.pfphdm = s.pfphdm and t.jsdm =s.jsdm
--          and t.yydm = s.yydm and t.yynf = s.yynf 
--          and t.kclx = s.kclx and (t.jsrq >= s.ksrq - 1 day)
--      when matched then
--        update set t.yyfpl = t.yyfpl+lc_n_tlsl, t.jssyl = s.kssyl, t.jsrq = IP_JSRQ
--      when not matched then
--        insert values(s.PFPHDM, s.JSDM, s.YYDM, s.YYNF, 
--                    (case when s.ksrq<=IP_JSRQ then s.ksrq else IP_JSRQ end), 
--                    IP_JSRQ, lc_n_tlsl, s.ZXSX, 
--                    s.KSSYL, s.KSSYL, s.ZLYYBJ, s.ZPFBJ, s.KCLX)
--      ;
--      update YYZY.T_YYZY_ZXPF_SDB
--      set yyfpl = yyfpl - lc_n_tlsl
--      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
--        and tdsx = lc_i_tdsx
--      ;
--      
--      leave lp2;
--    else --��������չ��ۼ�Ͷ����
--      merge into YYZY.T_YYZY_ZXPF_LSB as t
--      using (
--        select PFPHDM, JSDM, YYDM, YYNF, KSRQ, JSRQ, YYFPL, SDBH, 
--            ZXSX, TDSX, KSSYL, JSSYL, ZLYYBJ, ZPFBJ, FJCHSX, FJCHXX, 
--            KCLX, BBRQ, LOAD_TIME
--        from YYZY.T_YYZY_ZXPF_SDB
--        where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
--          and tdsx = lc_i_tdsx
--          and (ksrq<=IP_JSRQ and IP_KSRQ<=jsrq)
--      ) as s 
--        on t.pfphdm = s.pfphdm and t.jsdm =s.jsdm
--          and t.yydm = s.yydm and t.yynf = s.yynf 
--          and t.kclx = s.kclx and (t.jsrq >= s.ksrq - 1 day)
--      when matched then
--        update set t.yyfpl = t.yyfpl+s.yyfpl, t.jssyl = s.kssyl, 
--                  t.jsrq = (case when s.jsrq<=IP_JSRQ then s.jsrq else IP_JSRQ end)
--      when not matched then
--        insert values(s.PFPHDM, s.JSDM, s.YYDM, s.YYNF, 
--                  (case when s.ksrq<=IP_JSRQ then s.ksrq else IP_JSRQ end), 
--                  (case when s.jsrq<=IP_JSRQ then s.jsrq else IP_JSRQ end), s.yyfpl, s.ZXSX, 
--                  s.KSSYL, s.KSSYL, s.ZLYYBJ, s.ZPFBJ, s.KCLX)
--      ;
--      delete from YYZY.T_YYZY_ZXPF_SDB 
--      where pfphdm = IP_PFPHDM and jsdm = IP_JSDM
--        and tdsx = lc_i_tdsx
--      ;
--      set lc_n_tlsl = lc_n_tlsl -lc_n_yyfpl;
--      
--    end if;
--    
--  end loop lp2; 
--  close c2; 
--  
--END LB_MAIN;
--
--COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF6PHJS( date,date, integer, integer,  decimal(18,6) ) IS '6Ҫ����ʷ�䷽ �ƺŽ�ɫ';
--
--GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_LSPF6PHJS (date,date, integer, integer,  decimal(18,6)) TO USER APPUSR;

-------------------------------------------------------------------------------------
drop PROCEDURE YYZY.P_YYZY_LSPF6;

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_LSPF6
( 
--  IN IP_KSRQ DATE,
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
  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
    select pfphdm, jsdm
    from YYZY.T_YYZY_ZXPF_SDB
    WHERE PFPHDM IN (select distinct pfphdm from JYHSF.T_JYHSF_ZSPF_SDB)
    group by pfphdm, jsdm
    order by pfphdm, jsdm
  ;
--  DECLARE C2 CURSOR /*WITH RETURN*/ FOR
--    select tdsx, yyfpl
--    from YYZY.T_YYZY_ZXPF_WHB_SD
--    where pfphdm = lc_i_pfphdm and jsdm = lc_i_jsdm
--    order by tdsx
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
  DECLARE GLOBAL TEMPORARY TABLE t_yyzy_sjtll
  (
    rq date,
    pfphdm integer,
    jsdm integer,
    tlsl decimal(18,6)
  ) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  delete from session.t_yyzy_sjtll;
  insert into session.t_yyzy_sjtll
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
    
    --step1:����ksrq,jsrq
    set lc_d_tlksrq = (select date(max(ZDTLSJ))+1 day as ksrq 
                        from YYZY.T_YYZY_SJTL_SCPC_RZJL 
                        where pfphdm = lc_i_pfphdm and date(GXSJ)<IP_PFZXRQ
                      )
    ;
    set lc_d_tljsrq = (select date(max(ZDTLSJ)) as jsrq 
                        from YYZY.T_YYZY_SJTL_SCPC_RZJL 
                        where pfphdm = lc_i_pfphdm and date(GXSJ)=IP_PFZXRQ
                      )
    ;
--    set lc_d_tljsrq = IP_PFZXRQ;
    if lc_d_tljsrq<lc_d_tlksrq or lc_d_tljsrq is null then --������û������Ͷ�ϼ�¼
--      ITERATE lp1; --�������䷽�ƺŵĴ���
      set lc_n_tlsl = 0;
    else
      --step2:����Ͷ����Ҷ����
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
--    if lc_n_tlsl = 0 then --��Ͷ����Ҷ����Ϊ0
--      ITERATE lp1; --�������䷽�ƺš���ɫ�Ĵ���
--    end if;
--    set lc_d_tljsrq = IP_PFZXRQ;
--    if lc_d_tljsrq<lc_d_tlksrq or lc_d_tljsrq is null then --������û������Ͷ�ϼ�¼
--      set lc_n_tlsl = 0;
--      ITERATE lp1; --�������䷽�ƺŵĴ���
--    end if;
    call YYZY.P_YYZY_LSPF6PHJS(lc_d_tlksrq, IP_PFZXRQ, lc_i_pfphdm, lc_i_jsdm, lc_n_tlsl);
    
  end loop lp1; 
  close c1; 
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF6(date) IS '6Ҫ����ʷ�䷽';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_LSPF6 (date) TO USER APPUSR;

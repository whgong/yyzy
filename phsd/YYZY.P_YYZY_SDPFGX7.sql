SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

/*******************************************************************************/
--����������ʹ�õ�����ʱ��
/*
drop table yyzy.t_yyzy_tmp_sdpfyyfpgz;
create table yyzy.t_yyzy_tmp_sdpfyyfpgz
(
  pfphdm integer not null,
  jsdm integer not null,
  yydm varchar(20) not null,
  yynf integer not null,
  kclx integer,
  yypc varchar(10),
  tdsx integer not null,
  YYFPL decimal(18,6),
  zlyybj char(1),
  zpfbj char(1),
  fjchsx integer,
  fjchxx integer,
  bbrq date,
  bjid integer
)
in ts_reg_16K;
;

drop table JYHSF.T_JYHSF_TMP_ZSPF_SDB;
create table JYHSF.T_JYHSF_TMP_ZSPF_SDB like JYHSF.T_JYHSF_ZSPF_SDB in ts_reg_16K;
*/

/*******************************************************************************/
--���µ�����ɫ�������䷽
drop PROCEDURE YYZY.P_YYZY_SDPFGX7DPH;

CREATE PROCEDURE YYZY.P_YYZY_SDPFGX7DPH
( 
  IN  ip_pfphdm INTEGER,
  IN ip_jsdm integer
--  OUT OP_V_ERR_MSG VARCHAR(1000) 
)
  SPECIFIC PROC_YYZY_SDPFGX7DPH
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
  declare flg integer;
  /* DECLARE USER-DEFINED VARIABLES */ 
  -- DECLARE V_SEPARATOR VARCHAR(50) DEFAULT ','; 
  declare lv_i_tdsx integer;
  declare lv_n_yyfpl, lv_n_yhyl, lv_n_thyl,lv_n_thyl_f decimal(18,6); 
  declare lv_d_ksrq, lv_d_jsrq date;
  declare lv_d_pfksrq, lv_d_pfjsrq date;
  declare lv_n_dyl,lv_n_zxsx,lv_n_kssyl,lv_n_jssyl decimal(18,6); 
  /* DECLARE STATIC CURSOR */
  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
    select tdsx,yyfpl
    from YYZY.T_YYZY_TMP_SDPFYYFPGZ 
    where pfphdm = ip_pfphdm and jsdm=ip_jsdm
    order by tdsx
  ;
  DECLARE C2 CURSOR /*WITH RETURN*/ FOR
    select ksrq, jsrq, hl_d
    from yyzy.t_yyzy_tmp_yyxhtjb
    where pfphdm = ip_pfphdm and jsdm=ip_jsdm
    order by ksrq,jsrq
  ;
  /* DECLARE DYNAMIC CURSOR */
  -- DECLARE C2 CURSOR FOR S2;
  /* DECLARE EXCEPTION HANDLE */
--  DECLARE exit HANDLER FOR SQLEXCEPTION set flg = 1;
--  BEGIN 
--    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
--    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
--      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
--      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
--    ; 
--  END; 
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  -- DECLARE GLOBAL TEMPORARY TABLE BBDM_GROUP
  -- (
  --   BBDM INTEGER
  -- ) with replace on commit preserve rows not logged; 
  
  
  /* SQL PROCEDURE BODY */
--  select * 
--  from yyzy.t_yyzy_tmp_yyxhtjb
--  where pfphdm = ip_pfphdm and jsdm = ip_jsdm
--  ;
  set lv_n_jssyl = 0;
  set lv_n_dyl = 0;
  set lv_n_thyl = 0;
  set lv_d_pfjsrq = (select min(ksrq) from yyzy.t_yyzy_tmp_yyxhtjb where pfphdm = ip_pfphdm and jsdm=ip_jsdm);
  open c1;
  open c2;
  lp1:loop
    set SQL_CUR_AT_END = 0;
    fetch c1 into lv_i_tdsx,lv_n_yyfpl;
    if SQL_CUR_AT_END = 1 then LEAVE lp1; end if;
    
    set lv_n_yhyl = lv_n_dyl;
    set lv_d_pfksrq = 
      --case when lv_n_jssyl=0 or lv_n_jssyl=lv_n_thyl then lv_d_pfjsrq+1 day else lv_d_pfjsrq end 
      case when lv_n_jssyl <>0 and (select sum(jssyl) from JYHSF.T_JYHSF_TMP_ZSPF_SDB where pfphdm = ip_pfphdm and jsdm=ip_jsdm and jsrq = lv_d_pfjsrq)=lv_n_thyl then lv_d_pfjsrq+1 day else lv_d_pfjsrq end 
    ; --����һש������ʹ���꣬��ʼ����Ϊ��һש���������+1�죬����Ϊ��һש���������
    set lv_n_zxsx = 
      value((select sum(jssyl) 
      from JYHSF.T_JYHSF_TMP_ZSPF_SDB 
      where pfphdm = ip_pfphdm and jsdm=ip_jsdm 
        and jsrq=lv_d_pfksrq),0)
    ; --zxsxΪ��ש�鿪ʼʹ�������ڣ��ڸ�ש��֮ǰ���õ���Ҷ����
    set lv_n_thyl_f = ( 
        select hl_d from yyzy.t_yyzy_tmp_yyxhtjb 
        where pfphdm = ip_pfphdm and jsdm=ip_jsdm 
        and lv_d_pfksrq between ksrq and jsrq
      )
    ;--ש���һ���������ڵ��պ�����
    set lv_n_kssyl = 
      case when lv_n_yyfpl<lv_n_thyl_f-lv_n_zxsx then lv_n_yyfpl else lv_n_thyl_f-lv_n_zxsx end
    ;-- ��ʼʹ���� = ����Ҷ��һ��ĺ�������
    while lv_n_yhyl < lv_n_yyfpl do
      fetch c2 into lv_d_ksrq, lv_d_jsrq,lv_n_thyl;
      if SQL_CUR_AT_END = 1 then LEAVE lp1; end if;--�쳣����:���ƻ���С�ڷ�����,һ�㲻���ܳ���
      set lv_n_yhyl = lv_n_yhyl + lv_n_thyl * (days(lv_d_jsrq)- days(lv_d_ksrq) + 1 );
      
    end while;
    
    set lv_n_dyl = lv_n_yhyl - lv_n_yyfpl;

    if lv_n_yyfpl=0 then 
      set lv_d_jsrq = lv_d_pfksrq;
    end if;
    set lv_d_pfjsrq = lv_d_jsrq - (case when lv_n_thyl=0 then 0 else int(lv_n_dyl/lv_n_thyl) end) day;
--    set lv_n_yhyl_p = lv_n_dyl;
    set lv_n_jssyl = 
      lv_n_thyl 
      - (case when lv_n_thyl=0 then 0 else mod(int(lv_n_dyl) ,int(lv_n_thyl)) end) 
      - value((select sum(jssyl) from JYHSF.T_JYHSF_TMP_ZSPF_SDB where pfphdm = ip_pfphdm and jsdm=ip_jsdm and jsrq=lv_d_pfjsrq ),0)
--      case when lv_n_yyfpl<lv_n_thyl-lv_n_zxsx then lv_n_yyfpl else lv_n_thyl - (case when lv_n_thyl=0 then 0 else mod(int(lv_n_dyl) ,int(lv_n_thyl)) end) end 
    ; --��ש��ĺ�������<һ����ʣ���ʹ����������ʹ�����͵�����Ҷ�������������պ�����-ȡ��(δ������)
    
    
    insert into JYHSF.T_JYHSF_TMP_ZSPF_SDB(
      PFPHDM, JSDM, YYDM, YYNF, KSRQ, JSRQ, YYFPL, SDBH, 
      ZXSX, TDSX, KSSYL, JSSYL, ZLYYBJ, ZPFBJ, FJCHSX, FJCHXX, 
      KCLX, BBRQ, LOAD_TIME, YYPC
    )
    select PFPHDM, JSDM, YYDM, YYNF, lv_d_pfksrq, lv_d_pfjsrq, 
      YYFPL, 0 as SDBH, lv_n_zxsx, TDSX, lv_n_kssyl, lv_n_jssyl, ZLYYBJ,
      ZPFBJ, FJCHSX, FJCHXX, KCLX, BBRQ, current_timestamp as LOAD_TIME, YYPC
    from YYZY.T_YYZY_TMP_SDPFYYFPGZ 
    where pfphdm = ip_pfphdm and jsdm=ip_jsdm
      and tdsx = lv_i_tdsx
    ;
    
  end loop lp1;
  close c1;
  close c2;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_SDPFGX7DPH( INTEGER,integer ) IS '7Ҫ�������䷽���� ���ƺŽ�ɫ';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SDPFGX7DPH (INTEGER, integer) TO USER APPUSR;



/*******************************************************************************/
drop PROCEDURE YYZY.P_YYZY_SDPFGX7;
CREATE PROCEDURE YYZY.P_YYZY_SDPFGX7
( 
--  IN  IP_I_NF INTEGER,
--  OUT OP_V_ERR_MSG VARCHAR(1000) 
)
  SPECIFIC PROC_YYZY_SDPFGX7
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
  -- DECLARE V_SEPARATOR VARCHAR(50) DEFAULT ','; 
  declare v_i_pfphdm,v_i_jsdm integer;
  /* DECLARE STATIC CURSOR */
  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
    select distinct PFPHDM, jsdm
    from JYHSF.T_JYHSF_ZSPF_SDB
    order by pfphdm,jsdm
  ;
  /* DECLARE DYNAMIC CURSOR */
  -- DECLARE C2 CURSOR FOR S2;
  /* DECLARE EXCEPTION HANDLE */
/*  DECLARE UNDO HANDLER FOR SQLEXCEPTION
  BEGIN 
    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
    ; 
  END;*/
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  -- DECLARE GLOBAL TEMPORARY TABLE BBDM_GROUP
  -- (BBDM INTEGER) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  --��ø��ƺŵ�ÿ�����������
  delete from yyzy.t_yyzy_tmp_yyxhtjb;
  insert into yyzy.t_yyzy_tmp_yyxhtjb(pfphdm,jsdm,ksrq,jsrq,hl_d)
  select pfphdm,JSDM,
    case 
      when ad1 < bd1 then bd1 
      else ad1 
    end KSRQ,
    case 
      when ad2 > bd2 then bd2 else ad2 
    end JSRQ,xhyzl 
  from ( 
    select a.pfphdm,JSDM, a.ksrq ad1 ,a.jsrq ad2,
        b.KSRQ bd1 ,b.jsrq bd2,DPXS ,b.JHPC_AVG, 
        case 
          when a.pfphdm =16 then jhcl_avg*DPXS/100 
          else jhpc_avg * DPXS 
        end xhyzl
    from (
      select pfphdm,JSDM,KSRQ,JSRQ,DPXS 
      from YYZY.T_YYZY_JSTZ_WHB 
      where zybj='1' 
    ) a 
    inner join YYZY.T_YYZY_RSCJHB_WHB as b 
      on a.ksrq <=b.jsrq 
      and b.ksrq<=a.jsrq 
      and a.pfphdm =b.pfphdm 
  ) as t 
  order by pfphdm,JSDM,(case when ad1<bd1 then bd1 else ad1 end)
  ; 
  
  --��ø��䷽������Ҷ���������˳��
  delete from YYZY.T_YYZY_TMP_SDPFYYFPGZ;
  insert into YYZY.T_YYZY_TMP_SDPFYYFPGZ
  (
    PFPHDM, JSDM, YYDM, YYNF, KCLX, YYPC, TDSX, YYFPL,
    zlyybj, zpfbj, fjchsx, fjchxx, bbrq
  )
  select PFPHDM, JSDM, YYDM, YYNF, KCLX, YYPC, 
    rownumber()over(partition by pfphdm, jsdm order by KSRQ, JSRQ, zxsx) as tdsx, 
    YYFPL, zlyybj, zpfbj, fjchsx, fjchxx, bbrq
  from JYHSF.T_JYHSF_ZSPF_SDB
  order by pfphdm, jsdm,ksrq,jsrq
  ;
  
  delete from JYHSF.T_JYHSF_TMP_ZSPF_SDB;
  open c1;
lp1:
  loop
    set SQL_CUR_AT_END = 0;
    fetch c1 into v_i_pfphdm,v_i_jsdm;
    if SQL_CUR_AT_END = 1 then LEAVE lp1; end if;
    call YYZY.P_YYZY_SDPFGX7DPH(v_i_pfphdm, v_i_jsdm);
  end loop lp1;
  
  close c1;
  
  delete from JYHSF.T_JYHSF_ZSPF_SDB;
  insert into JYHSF.T_JYHSF_ZSPF_SDB
  select * from JYHSF.T_JYHSF_TMP_ZSPF_SDB;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_SDPFGX7() IS '7Ҫ�������䷽����';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SDPFGX7 () TO USER APPUSR;

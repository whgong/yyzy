SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

/*******************************************************************************/
--创建过程中使用到的临时表
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

drop table YYZY.T_YYZY_TMP_ZXPF_SDB;
create table YYZY.T_YYZY_TMP_ZXPF_SDB like YYZY.T_YYZY_ZXPF_SDB in ts_reg_16K;
*/

/*******************************************************************************/
--更新单个角色的锁定配方
drop PROCEDURE YYZY.P_YYZY_SDPFGX6DPH;

CREATE PROCEDURE YYZY.P_YYZY_SDPFGX6DPH
( 
  IN  ip_pfphdm INTEGER,
  IN ip_jsdm integer
--  OUT OP_V_ERR_MSG VARCHAR(1000) 
)
  SPECIFIC PROC_YYZY_SDPFGX6DPH
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
--  insert into DEBUG.T_DEBUG_MSG(msg)values('参数:ip_pfphdm='||char(ip_pfphdm)||';ip_jsdm='||char(ip_jsdm)||';');
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
--    insert into DEBUG.T_DEBUG_MSG(msg)values('lp1:lv_i_tdsx='||char(lv_i_tdsx)||';lv_n_yyfpl='||char(lv_n_yyfpl)||';');
    
    set lv_n_yhyl = lv_n_dyl;
    set lv_d_pfksrq = 
      --case when lv_n_jssyl<>0 and lv_n_jssyl=lv_n_thyl then lv_d_pfjsrq+1 day else lv_d_pfjsrq end 、
      case when lv_n_jssyl <>0 and (select sum(jssyl) from YYZY.T_YYZY_TMP_ZXPF_SDB where pfphdm = ip_pfphdm and jsdm=ip_jsdm and jsrq = lv_d_pfjsrq)=lv_n_thyl then lv_d_pfjsrq+1 day else lv_d_pfjsrq end 
    ; --若上一砖块正好使用完，开始日期为上一砖块结束日期+1天，否则为上一砖块结束日期
    set lv_n_zxsx = 
      value((select sum(jssyl) 
      from YYZY.T_YYZY_TMP_ZXPF_SDB 
      where pfphdm = ip_pfphdm and jsdm=ip_jsdm 
        and jsrq=lv_d_pfksrq),0)
    ; --zxsx为该砖块开始使用那天内，在该砖块之前耗用的烟叶数量
--    insert into DEBUG.T_DEBUG_MSG(msg)values('flg1');
    set lv_n_thyl_f = ( 
        select hl_d from yyzy.t_yyzy_tmp_yyxhtjb 
        where pfphdm = ip_pfphdm and jsdm=ip_jsdm 
        and lv_d_pfksrq between ksrq and jsrq
      )
    ;--砖块第一天所处日期的日耗用量
--    insert into DEBUG.T_DEBUG_MSG(msg)values('flg2');
    set lv_n_kssyl = 
      case when lv_n_yyfpl<lv_n_thyl_f-lv_n_zxsx then lv_n_yyfpl else lv_n_thyl_f-lv_n_zxsx end
    ;-- 开始使用量 = 该烟叶第一天的耗用数量
--    insert into DEBUG.T_DEBUG_MSG(msg)values('flg3:lv_n_yhyl='||char(lv_n_yhyl)||';lv_n_yyfpl='||char(lv_n_yyfpl));
    while lv_n_yhyl < lv_n_yyfpl do
      fetch c2 into lv_d_ksrq, lv_d_jsrq,lv_n_thyl;
--      insert into DEBUG.T_DEBUG_MSG(msg)values('flg4');
      if SQL_CUR_AT_END = 1 then LEAVE lp1; end if;--异常处理:若计划量小于分配量,一般不可能出现
--      insert into DEBUG.T_DEBUG_MSG(msg)values('flg5');
      set lv_n_yhyl = lv_n_yhyl + lv_n_thyl * (days(lv_d_jsrq)- days(lv_d_ksrq) + 1 );
      
    end while;
--    insert into DEBUG.T_DEBUG_MSG(msg)values('flg6');
    
    set lv_n_dyl = lv_n_yhyl - lv_n_yyfpl;
--    insert into DEBUG.T_DEBUG_MSG(msg)values('flg7:lv_n_yyfpl='||char(lv_n_yyfpl)||';lv_n_thyl='||char(lv_n_thyl)||';lv_n_zxsx='||char(lv_n_zxsx)||';lv_n_dyl='||char(lv_n_dyl));
--    set lv_n_jssyl = 
--      case when lv_n_yyfpl<lv_n_thyl-lv_n_zxsx then lv_n_yyfpl else lv_n_thyl - (case when lv_n_thyl=0 then 0 else mod(int(lv_n_dyl) ,int(lv_n_thyl)) end) end 
--    ; --若砖块的耗用数量<一天内剩余的使用量，结束使用量就等于烟叶分配量，否者日耗用量-取余(未分配量)
--    insert into DEBUG.T_DEBUG_MSG(msg)values('flg8:lv_d_jsrq='||value(char(lv_d_jsrq),'')||';lv_n_thyl='||char(lv_n_thyl)||';lv_n_dyl='||char(lv_n_dyl));
    
    if lv_n_yyfpl=0 then 
      set lv_d_jsrq = lv_d_pfksrq;
    end if;
    set lv_d_pfjsrq = lv_d_jsrq - (case when lv_n_thyl=0 then 0 else int(lv_n_dyl/lv_n_thyl) end) day;
--    set lv_n_yhyl_p = lv_n_dyl;
    set lv_n_jssyl = 
      lv_n_thyl 
      - (case when lv_n_thyl=0 then 0 else mod(int(lv_n_dyl) ,int(lv_n_thyl)) end) 
      - value((select sum(jssyl) from YYZY.T_YYZY_TMP_ZXPF_SDB where pfphdm = ip_pfphdm and jsdm=ip_jsdm and jsrq=lv_d_pfjsrq ),0)
--      case when lv_n_yyfpl<lv_n_thyl-lv_n_zxsx then lv_n_yyfpl else lv_n_thyl - (case when lv_n_thyl=0 then 0 else mod(int(lv_n_dyl) ,int(lv_n_thyl)) end) end 
    ; --若砖块的耗用数量<一天内剩余的使用量，结束使用量就等于烟叶分配量，否者日耗用量-取余(未分配量)
    
--    insert into DEBUG.T_DEBUG_MSG(msg)values('数据入库');
    insert into YYZY.T_YYZY_TMP_ZXPF_SDB(
      PFPHDM, JSDM, YYDM, YYNF, KSRQ, JSRQ, YYFPL, SDBH, 
      ZXSX, TDSX, KSSYL, JSSYL, ZLYYBJ, ZPFBJ, FJCHSX, FJCHXX, 
      KCLX, BBRQ, LOAD_TIME
    )
    select PFPHDM, JSDM, YYDM, YYNF, lv_d_pfksrq, lv_d_pfjsrq, 
      YYFPL, 0 as SDBH, lv_n_zxsx, TDSX, lv_n_kssyl, lv_n_jssyl, ZLYYBJ,
      ZPFBJ, FJCHSX, FJCHXX, KCLX, BBRQ, current_timestamp as LOAD_TIME
    from YYZY.T_YYZY_TMP_SDPFYYFPGZ 
    where pfphdm = ip_pfphdm and jsdm=ip_jsdm
      and tdsx = lv_i_tdsx
    ;
    
  end loop lp1;
  close c1;
  close c2;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_SDPFGX6DPH( INTEGER,integer ) IS '6要素锁定配方更新 单牌号角色';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SDPFGX6DPH (INTEGER, integer) TO USER APPUSR;



/*******************************************************************************/
drop PROCEDURE YYZY.P_YYZY_SDPFGX6;
CREATE PROCEDURE YYZY.P_YYZY_SDPFGX6
( 
--  IN  IP_I_NF INTEGER,
--  OUT OP_V_ERR_MSG VARCHAR(1000) 
)
  SPECIFIC PROC_YYZY_SDPFGX6
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
    from YYZY.T_YYZY_ZXPF_SDB
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
  --获得各牌号的每日所需耗用量
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
  
  --获得各配方锁定烟叶数量和替代顺序
  delete from YYZY.T_YYZY_TMP_SDPFYYFPGZ;
  insert into YYZY.T_YYZY_TMP_SDPFYYFPGZ
  (
    PFPHDM, JSDM, YYDM, YYNF, KCLX, TDSX, YYFPL,
    zlyybj, zpfbj, fjchsx, fjchxx, bbrq
  )
  select PFPHDM, JSDM, YYDM, YYNF, KCLX, 
    rownumber()over(partition by pfphdm, jsdm order by KSRQ, JSRQ, zxsx) as tdsx, 
    YYFPL, zlyybj, zpfbj, fjchsx, fjchxx, bbrq
  from YYZY.T_YYZY_ZXPF_SDB
  order by pfphdm, jsdm,ksrq,jsrq
  ;
  
  delete from YYZY.T_YYZY_TMP_ZXPF_SDB;
  open c1;
lp1:
  loop
    set SQL_CUR_AT_END = 0;
    fetch c1 into v_i_pfphdm,v_i_jsdm;
    if SQL_CUR_AT_END = 1 then LEAVE lp1; end if;
    call YYZY.P_YYZY_SDPFGX6DPH(v_i_pfphdm, v_i_jsdm);
  end loop lp1;
  
  close c1;
  
  delete from YYZY.T_YYZY_ZXPF_SDB;
  insert into YYZY.T_YYZY_ZXPF_SDB
  select * from YYZY.T_YYZY_TMP_ZXPF_SDB;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_SDPFGX6() IS '6要素锁定配方更新';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SDPFGX6 () TO USER APPUSR;

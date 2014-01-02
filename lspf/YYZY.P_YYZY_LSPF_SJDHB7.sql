--drop PROCEDURE YYZY.P_YYZY_LSPF_SJDHB7; 

SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

/* 
--过程中需要使用到的数组类型定义
--drop type yyzy.int_array;
--drop type yyzy.double_array;
--drop type yyzy.date_array;
--drop type yyzy.ts_array;
--drop type yyzy.char1_array;
--drop type yyzy.vchar10_array;
--drop type yyzy.vchar20_array;

create type int_array as integer array[];
create type double_array as double array[];
create type date_array as date array[];
create type ts_array as timestamp array[];
create type char1_array as char(1) array[];
create type vchar10_array as varchar(10) array[];
create type vchar20_array as varchar(20) array[];
*/

CREATE PROCEDURE YYZY.P_YYZY_LSPF_SJDHB7 
( 
  IN IP_I_PFPHDM INTEGER
)
  SPECIFIC PROC_YYZY_LSPF_SJDHB7
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
  declare lc_ar_PFPHDM, lc_ar_JSDM, lc_ar_YYNF, lc_ar_KCLX  int_array; 
  declare lc_ar_ZLYYBJ, lc_ar_ZPFBJ  char1_array; 
  declare lc_ar_YYDM  vchar20_array; 
  declare lc_ar_KSRQ, lc_ar_JSRQ  date_array;
  declare lc_ar_YYPC  vchar10_array; 
  declare lc_ar_YYFPL, lc_ar_ZXSX, lc_ar_KSSYL, lc_ar_JSSYL  double_array;
  declare lc_i_rn bigint; 
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
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  -- DECLARE GLOBAL TEMPORARY TABLE BBDM_GROUP
  -- (
  --   BBDM INTEGER
  -- ) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  set lc_i_rn = 1; --设定下表初始值
  ------------------------------------------------------------------------------------
  loopf1: --start of 轮询全部历史配方日数据
  for v1 as c1 cursor for
    select PFPHDM, JSDM, YYDM, YYNF, SYRQ, YYSYL, ZXSX, ZLYYBJ, 
      ZPFBJ, KCLX, YYPC 
    from JYHSF.T_JYHSF_ZSPF_LSB_R 
    where pfphdm = IP_I_PFPHDM 
    order by PFPHDM, JSDM, SYRQ, ZXSX 
  do --body of 轮询全部历史配方日数据
    if lc_i_rn = 1 then --是第一条
      goto lb_new; 
    else --不是第一条
      if lc_ar_PFPHDM[lc_i_rn-1] = v1.pfphdm and lc_ar_JSDM[lc_i_rn-1] = v1.jsdm 
          and lc_ar_YYDM[lc_i_rn-1] = v1.yydm and lc_ar_YYNF[lc_i_rn-1] = v1.yynf 
          and lc_ar_KCLX[lc_i_rn-1] = v1.kclx and lc_ar_YYPC[lc_i_rn-1] = v1.yypc 
          --与上一条配方角色烟叶相同
      then 
        goto lb_update; 
      else --与上一条配方角色烟叶不同
        goto lb_new; 
      end if; 
    end if; 
    ----------------------------------------------------------------------------------
    lb_new: --新增记录 
      --配方角色烟叶信息
      set lc_ar_PFPHDM[lc_i_rn] = v1.pfphdm; set lc_ar_JSDM[lc_i_rn] = v1.jsdm; 
      set lc_ar_YYDM[lc_i_rn] = v1.yydm; set lc_ar_YYNF[lc_i_rn] = v1.YYNF; 
      set lc_ar_KCLX[lc_i_rn] = v1.kclx; set lc_ar_YYPC[lc_i_rn] = v1.yypc; 
      set lc_ar_ZLYYBJ[lc_i_rn] = v1.ZLYYBJ; set lc_ar_ZPFBJ[lc_i_rn] = v1.ZPFBJ; 
      --ksrq、kssyl、zxsx 
      set lc_ar_KSRQ[lc_i_rn] = v1.syrq; set lc_ar_YYFPL[lc_i_rn] = v1.yysyl; 
      set lc_ar_ZXSX[lc_i_rn] = v1.zxsx; set lc_ar_KSSYL[lc_i_rn] = v1.yysyl;
      --jsrq、jssyl
      set lc_ar_JSRQ[lc_i_rn] = v1.syrq; set lc_ar_JSSYL[lc_i_rn] = v1.yysyl; 
      --下标+1
      set lc_i_rn = lc_i_rn + 1; 
      iterate loopf1; --取下一条
    -----------------------------------------------------------------------------------
    lb_update: --更新已有记录 
      --jsrq、jssyl计算
      set lc_ar_JSRQ[lc_i_rn-1] = v1.syrq; set lc_ar_JSSYL[lc_i_rn-1] = v1.yysyl; 
      --YYFPL累加
      set lc_ar_YYFPL[lc_i_rn-1] = lc_ar_YYFPL[lc_i_rn-1] + v1.yysyl; 
      iterate loopf1; --取下一条
  end for loopf1; -- end of 轮询全部历史配方日数据
  
  --数据入库
  delete from JYHSF.T_JYHSF_ZSPF_LSB where pfphdm = IP_I_PFPHDM; 
  insert into JYHSF.T_JYHSF_ZSPF_LSB(
      PFPHDM, JSDM, YYDM, YYNF, KSRQ, JSRQ, YYFPL, ZXSX, 
      KSSYL, JSSYL, ZLYYBJ, ZPFBJ, KCLX, BBRQ, LOAD_TIME, YYPC
    ) 
  select pfphdm, jsdm, yydm, yynf, ksrq, jsrq, yyfpl, zxsx, 
    kssyl, jssyl, zlyybj, zpfbj, kclx, current_date, current_timestamp, yypc 
  from 
    unnest ( 
      lc_ar_PFPHDM, lc_ar_JSDM, lc_ar_YYDM, lc_ar_YYNF, 
      lc_ar_KSRQ, lc_ar_JSRQ, lc_ar_YYFPL, lc_ar_ZXSX, 
      lc_ar_KSSYL, lc_ar_JSSYL, lc_ar_ZLYYBJ, lc_ar_ZPFBJ, 
      lc_ar_KCLX, lc_ar_YYPC 
    ) as t(pfphdm, jsdm, yydm, yynf, ksrq, jsrq, 
          yyfpl, zxsx, kssyl, jssyl, zlyybj, zpfbj, kclx, yypc) 
  ; 
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF_SJDHB7 (INTEGER) IS '历史配方时间段合并 7要素';

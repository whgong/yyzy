drop PROCEDURE YYZY.P_YYZY_SJTL_SYPCCL; 

SET SCHEMA = ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR; 

CREATE PROCEDURE YYZY.P_YYZY_SJTL_SYPCCL 
( 
) 
  SPECIFIC PROC_YYZY_SJTL_SYPCCL 
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
  declare lc_i_YCLPC integer;

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
  loop1:
  for v1 as c1 cursor for -- 逐条处理"剩余批次"记录
    select YWQSRQ, PFPHDM, ZYBJ, SYPC, YCLPC, GXSJ, GXJL 
    from YYZY.T_YYZY_SJTL_SYPCL 
    where zybj = '1' 
      and SYPC > YCLPC 
    order by YWQSRQ, PFPHDM 
    for update of ZYBJ, YCLPC, GXSJ, GXJL 
  do
    set lc_i_YCLPC = v1.YCLPC; 
    loop2: 
    for v2 as c2 cursor for -- 逐条处理"实际投料"记录
      select tlsj, phscpc 
      from YYZY.T_YYZY_SJTL_SCPC 
      where date(tlsj)>=v1.ywqsrq 
        and pfphdm = v1.pfphdm 
        and phscpc<>0 
      order by tlsj desc 
      for update of phscpc 
    do
      if v2.phscpc >= (v1.SYPC - lc_i_YCLPC) then -- 当天投料批次 >= 实际剩余批次 : "已抵扣完" 
        update YYZY.T_YYZY_SJTL_SCPC 
        set phscpc = phscpc - (v1.SYPC - lc_i_YCLPC) 
        where current of c2; 
        
        update YYZY.T_YYZY_SJTL_SYPCL 
        set zybj='0', YCLPC = YCLPC + (v1.SYPC - lc_i_YCLPC), 
          gxsj = current_timestamp, 
          gxjl = trim(gxjl)||trim(char(current_date))||':-'||char(v1.SYPC - lc_i_YCLPC)||';' 
        where current of c1; 
        
        --退出该条"剩余批次"记录的处理
        leave loop2; 
        
      else -- 当天投料批次 < 实际剩余批次 : "未抵扣完" 
        update YYZY.T_YYZY_SJTL_SYPCL 
        set YCLPC = YCLPC + v2.phscpc, 
          gxsj = current_timestamp, 
          gxjl = trim(gxjl)||trim(char(current_date))||':-'||char(v2.phscpc)||';' 
        where current of c1; 
        
        set lc_i_YCLPC = lc_i_YCLPC + v2.phscpc; 
        
        update YYZY.T_YYZY_SJTL_SCPC 
        set phscpc = phscpc - v2.phscpc 
        where current of c2; 
        
      end if; 
      
    end for loop2; 
    
  end for loop1; 
  
END LB_MAIN; 

COMMENT ON PROCEDURE YYZY.P_YYZY_SJTL_SYPCCL ( ) IS '剩余批次处理';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SJTL_SYPCCL ( ) TO USER APPUSR; 

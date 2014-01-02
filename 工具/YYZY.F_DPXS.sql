--DROP FUNCTION YYZY.F_DPXS; 

SET SCHEMA ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR; 

CREATE FUNCTION YYZY.F_DPXS 
( 
  ip_pfphdm integer, 
  ip_jsdm integer, 
  ip_rq date 
) 
  RETURNS INTEGER 
  SPECIFIC YYZY.F_DPXS 
  LANGUAGE SQL 
  NOT DETERMINISTIC 
  READS SQL DATA 
  STATIC DISPATCH 
  CALLED ON NULL INPUT 
  NO EXTERNAL ACTION 
  INHERIT SPECIAL REGISTERS 
  RETURN 
    values coalesce(
      (select dpxs from YYZY.T_YYZY_JSTZ_WHB
        where pfphdm = IP_PFPHDM 
          and jsdm = IP_JSDM and zybj = '1'
          and ip_rq between ksrq and jsrq 
        order by ksrq, jsrq 
        fetch first 1 row only 
      )
      ,0
    )
  ;

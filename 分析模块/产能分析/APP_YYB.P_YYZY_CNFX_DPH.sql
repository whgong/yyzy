SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_CNFX_DPH (
    IN ID_KSRQ    DATE,
    IN ID_JSRQ    DATE,
    IN II_PFPHDM    INTEGER,
    IN II_FXBJ    INTEGER )
  SPECIFIC SQL100210162127700
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN 
    --定义系统变量 
    DECLARE SQLSTATE CHAR(5);  
    DECLARE SQLCODE INTEGER; 
    DECLARE ERR_MSG VARCHAR(1000); 
    declare V_MESSAGE varchar(1000); 
    --自定义变量
    declare Ui_not_found integer default 1;
    DECLARE Uv_stmt VARCHAR(8000) DEFAULT '';
    declare Ud_ksrq date;
    DECLARE Ui_pfphdm,Ui_jsdm,Ui_yydm,Ui_yynf,Ui_xhyzl,Ui_bs integer;
    declare Ud_syksrq,Ud_syjsrq,Ud_jsksrq,Ud_jsjsrq date;
    --定义静态游标

    --定义动态游标
--    DECLARE c2 CURSOR FOR s1;
    --定义临时表
--    DECLARE GLOBAL TEMPORARY TABLE TEMP(
--      PFPHDM INTEGER, 
--      JSDM INTEGER, 
--      JHRQ DATE,
--      SDL INTEGER 
--    ) with replace on commit preserve rows NOT LOGGED;
    --定义游标句柄
    declare continue handler for not found 
      begin
        set Ui_not_found = 1;
      end; 
      --定义异常处理
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    SET ERR_MSG = '系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';  ';
    
    --正文
    IF II_PFPHDM NOT IN(15) THEN
            BEGIN
                IF YYZY.F_YYZY_PFSFHF(ID_KSRQ,ID_JSRQ,II_PFPHDM)>0 THEN
                    SET V_MESSAGE='此牌号数据有问题';
                    RETURN -1;
                END IF;
            END;
        END IF;
    
    --delete from YYZY.T_YYZY_TMP_CNFX;
    insert into YYZY.T_YYZY_TMP_CNFX(pfphdm,jsdm,yydm,yynf,jhxs,ksrq,jsrq,fxbj)
    select PFPHDM, JSDM, YYDM, YYNF, floor(sum(XHYZL)) as jhxs, min(PFRQ) as ksrq,max(PFRQ) as ksrq,II_FXBJ as fxbj 
    from YYZY.V_YYZY_ZXPFMX_NEW
    where xhyzl>=0 
      and pfphdm=II_pfphdm 
      and pfrq between ID_KSRQ and ID_JSRQ 
    group by pfphdm,jsdm,yydm,yynf; 
    
END;

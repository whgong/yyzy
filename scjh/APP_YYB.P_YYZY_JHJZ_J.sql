SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHJZ_J ( )
  SPECIFIC SQL100319160947400
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
--存储过程 
  BEGIN 
    /*2010-3-19 13:08
    **龚玮慧
    **季度生产计划加载
    **/
    --定义系统变量 
    DECLARE SQLSTATE CHAR(5); 
    DECLARE SQLCODE INTEGER;  
    
    --定义 
    DECLARE STMT VARCHAR(4000);
    DECLARE STMT2 VARCHAR(4000); 
    DECLARE STMT3 VARCHAR(4000);
    DECLARE TYPE CHAR(1); 
    DECLARE COUNT INTEGER DEFAULT 0;
    DECLARE ISEXIST INTEGER DEFAULT 0;
    DECLARE i ,I_LSBH,max_bbh,month_1,month_2 INTEGER DEFAULT 0;
    DECLARE STRCNT VARCHAR(100) DEFAULT '';
     DECLARE ERR_MSG VARCHAR(1000) DEFAULT ''; 
--    DECLARE RUNSTATUS INTEGER;
    DECLARE AT_END SMALLINT DEFAULT 0;

    --定义动态游标 
    DECLARE c1 CURSOR FOR s1;
    DECLARE c2 CURSOR FOR s2;
    
    --定义异常处理
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
      SET ERR_MSG = '系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';  '; 
    --插入初始记录
    insert into  YYZY.T_YYZY_JZ_RZ  (BZ, MBB, JZSJ, SFCG) 
    values('','YYZY.T_YYZY_JSCJH',current timestamp,0); 
    select MAX(LSBH) INTO I_LSBH 
    FROM YYZY.T_YYZY_JZ_RZ 
    WHERE MBB='YYZY.T_YYZY_JSCJH'; 
    
    SELECT COUNT(1) INTO I 
    FROM ( 
      SELECT date(T.D_CREATETIME) as BBRQ 
      FROM HDS_CXQJ.N_CXQJ_O_PRODPLAN T 
      EXCEPT 
      SELECT BBRQ 
      FROM YYZY.T_YYZY_JSCJH 
    ) AA 
    WHERE BBRQ IS NOT NULL;
    
    IF I=0 THEN 
      RETURN; 
    END IF;
    
    delete from YYZY.T_YYZY_TMP_JSCJH where jhnf>=year(current date);
    insert into YYZY.T_YYZY_TMP_JSCJH (JHNF,JHYF,JHJD,YHBS,CJDM,JHCL,ZYBJ,
        BBH,BBRQ,KSRQ, JSRQ,BRIEFNM,PZMC)
    with jjh_ywxt as ( 
      SELECT Y.PLANID, Y.TOBACCOID, int(Y.CMONTH) as cmonth, int(Y.CYEAR) as cyear, Y.QUANTITY,
          Y.C_CREATETIME, Y.REGIONID,y.N_VERSION,date(y.D_CREATETIME) as bbrq
      FROM HDS_CXQJ.N_CXQJ_O_REGION_QRPLAN AS Y 
      where y.C_CREATETIME is not null 
    ),
    gc as ( 
      select regionid,c.factoryid,brief as gcmc 
      from HDS_CXQJ.N_CXQJ_O_REGIONINFO as a 
      left join HDS_CXQJ.N_CXQJ_O_WORKSHOPINFO as b 
        on a.workshopid=b.workshopid 
      left join HDS_CXQJ.N_CXQJ_O_FACTORYINFO as c 
        on b.factoryid=c.factoryid 
    )
    select cyear as jhnf,cmonth as jhyf,floor((cmonth-1)/3)+1 as jhjd,
        formulaid as yhbs,cast(substr(c.factoryid, 13, 1) as integer) as cjdm,
        QUANTITY as jhcl,'1' as zybj,N_VERSION as bbh,bbrq,'1980-01-01' as ksrq,'3000-12-31' as jsrq,
        standardnm as briefnm,standardnm as pzmc
    from jjh_ywxt as a 
    left join HDS_TF01.N_TF01_TCM21 as b
      on a.TOBACCOID=b.standardid
    left join gc as c 
      on c.regionid=a.regionid
    where cyear>=year(current date); 
    
    
    SELECT COUNT(1) INTO I FROM YYZY.T_YYZY_TMP_JSCJH ;
    IF I=0 THEN 
      RETURN 0; 
    END IF;
    
    delete from YYZY.T_YYZY_JSCJH where jhnf>=year(current date);
    insert into YYZY.T_YYZY_JSCJH (JSCJHDM, JHNF, JHYF, JHJD, YHBS, CJDM,
        JHCL, ZYBJ, BBH, BBRQ, KSRQ, JSRQ,BRIEFNM,PZMC,PZDM,PPDM)
    select JSCJHDM, JHNF, JHYF, JHJD, YHBS, CJDM,   JHCL, ZYBJ, 
        BBH, BBRQ, KSRQ, JSRQ,BRIEFNM,PZMC,PZDM,PPDM 
    from YYZY.T_YYZY_TMP_jSCJH
    where jhnf>=year(current date) AND CJDM IS NOT NULL
    ;
    
    UPDATE YYZY.T_YYZY_JZ_RZ 
    SET BZ='当日加载成功',sfcg=1 
    WHERE LSBH=I_LSBH;
    
    return 1;
  END;
SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_CNFX_MAIN ( IN IP_I_FXBJ INTEGER )
  SPECIFIC SQL100210162128000
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
  --定义
  DECLARE DDATE DATE ; 
  DECLARE i_pfphdm INTEGER DEFAULT 0;
   DECLARE ERR_MSG,V_MESSAGE VARCHAR(1000) DEFAULT '';
  DECLARE i_not_found integer DEFAULT 0;
        
  DECLARE c1 CURSOR with hold WITH RETURN FOR 
    select PFPHDM
    from YYZY.T_YYZY_DPSX 
    where (pfphdm,bbrq) in (
      select pfphdm,max(bbrq) 
      from YYZY.T_YYZY_DPSX 
      group by pfphdm
    )
  ;  
  DECLARE c2 CURSOR with hold WITH RETURN FOR 
    select PFPHDM 
    from YYZY.T_YYZY_DPSX 
    where (pfphdm,bbrq)in(
      select pfphdm,max(bbrq) 
      from YYZY.T_YYZY_DPSX 
      group by pfphdm
    )
  ;
      
  declare continue handler for not found
  begin
    set i_not_found = 1;
  end; 
    
  --定义异常处理
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
  SET ERR_MSG = '系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';';
    
  DECLARE GLOBAL TEMPORARY TABLE TmpA (
    pfphdm integer,
    jsdm integer,
    yydm varchar(36),
    yynf integer,
    ksrq date,
    jsrq date, 
    xhyzl integer,
    jsrq1 date
  ) with replace on commit preserve rows NOT LOGGED;
      
  delete from YYZY.T_YYZY_TMP_CNFX ;
  
  /* 依次计算每个牌号 */
  case IP_I_FXBJ
  when 0 then
    set i_not_found=0; 
    OPEN c1;
    loop_ml: loop
      FETCH c1 INTO i_pfphdm;
      if i_not_found=1 then 
        leave loop_ml;
      end if;
      
      --从生产计划的开始年初至结束的年份末 
      call APP_YYB.P_YYZY_CNFX_DPH ((current date)-(dayofyear(current date)-1) day,(current date)+ 6 year-dayofyear(current date) day,i_pfphdm,0);
      
      set V_MESSAGE=V_MESSAGE||' '||char(DDATE);
      
      update YYZY.T_YYZY_tmp_CNFX a set ppdm=(select PPDM from YYZY.V_YYZY_PFPPDY where pfphdm=a.pfphdm);
      
      SET i_not_found=0; 
    end loop  loop_ml;
    close c1;  
  
  when 1 then
    set i_not_found=0; 
    OPEN c2;
    loop_ml1: loop
      FETCH c2 INTO i_pfphdm;
      if i_not_found=1 then
        leave loop_ml1;
      end if;
      
      call APP_YYB.P_YYZY_CNFX_DPH ((current date)-(day(current date) - 1) day,(current date)+ 6 year-dayofyear(current date) day,i_pfphdm,1);
      
      set V_MESSAGE=V_MESSAGE||' '||char(DDATE);
      
      update YYZY.T_YYZY_tmp_CNFX a set ppdm=(select PPDM from YYZY.V_YYZY_PFPPDY where pfphdm=a.pfphdm);
      
      SET i_not_found=0;
    end loop  loop_ml1;
    close c2;  
    
  when 2 then 
    set i_not_found=0; 
    OPEN c1;
    loop_ml2: loop
      FETCH c1 INTO i_pfphdm;
      if i_not_found=1 then
        leave loop_ml2;
      end if;
    
      -- 
      call APP_YYB.P_YYZY_CNFX_DPH ((current date)-(dayofyear(current date)-2) day+6 month ,(current date)+ 6 year-dayofyear(current date) day,i_pfphdm,0);
    
      set V_MESSAGE=V_MESSAGE||' '||char(DDATE);
    
      update YYZY.T_YYZY_tmp_CNFX a set ppdm=(select PPDM from YYZY.V_YYZY_PFPPDY where pfphdm=a.pfphdm);
    
      SET i_not_found=0; 
    end loop  loop_ml2;
    close c1;  
  
  else 
    return;
  end case;

/* 龚玮慧2010-09-15修改，去除物品名特殊处理 */
/*
  --无品名的特殊处理

   insert into session.TmpA (pfphdm,jsdm,yydm,yynf,ksrq,jsrq,xhyzl,jsrq1)
     with tmp as (
     select * 
   from YYZY.T_YYZY_TMP_CNFX 
   where (pfphdm,jsdm,jsrq) in(
                                 select pfphdm,jsdm,max(jsrq) 
                 from YYZY.T_YYZY_TMP_CNFX  
                 group by jsdm,pfphdm
                )
    ),tmp_jg1 as (
  select PFPHDM,B.RIQI AS JHRQ,JHCL_AVG AS JHCL,JHPC_AVG AS rscpc
    from YYZY.T_YYZY_RSCJHB_WHB A,DIM.T_DIM_YYZY_DATE AS B
    WHERE B.RIQI BETWEEN A.KSRQ AND A.JSRQ
    union all  
    select PFPHDM,JHRQ,JHCL,JHPC as rscpc
    from YYZY.T_YYZY_RSCJH_LSB
    ),tmp_jg2 as (
    select PPDM,PFPHDM,JSDM,a.YYDM,yymc,YYNF,JHXS,a.KSRQ,a.JSRQ,FXBJ 
  from tmp a,(
              select YYDM,YYMC,YYCDDM,YYDJDM,YYKBDM,YYLBDM
              from DIM.T_DIM_YYZY_YYZDB where jsrq>current date
          ) b 
    where  yymc like '%无品名%' and a.yydm=b.yydm 
    order by jsdm,a.ksrq  
   )
    select a.pfphdm,jsdm,yydm,yynf,ksrq,jsrq,
     (
      select sum(rscpc) 
    from tmp_jg1 b 
    where a.pfphdm=b.pfphdm and b.jhrq>a.ksrq
     ) xhyzl1,
     (
      select max(jhrq) 
    from tmp_jg1 b 
    where a.pfphdm=b.pfphdm 
     ) jsrq1 
    from tmp_jg2 a;  

    update YYZY.T_YYZY_tmp_CNFX a 
  set (jsrq,jhxs)=(
                   select jsrq1,xhyzl 
           from session.TmpA 
           where pfphdm=a.pfphdm 
           and jsdm=a.jsdm 
           and a.yydm=yydm 
           and yynf=a.yynf 
           and ksrq=a.ksrq 
           and jsrq=a.jsrq
           )
    where (pfphdm,jsdm,yydm,yynf,ksrq,jsrq) in (
        select pfphdm,jsdm,yydm,yynf,ksrq,jsrq 
       from session.TmpA
  );
*/

  delete from  YYZY.T_YYZY_CNFX where fxbj=IP_I_FXBJ;
  insert into YYZY.T_YYZY_CNFX 
  select * 
  from YYZY.T_YYZY_tmp_CNFX
/*
  2010-12-02 修改 过滤砖墙图上不存在的牌号
*/  
  where 1=1 
    and pfphdm in (
      select PFPHDM  
      from YYZY.T_YYZY_ZXPF_WHB
    union all
    select pfphdm from YYZY.T_YYZY_ZXPF_LSB WHERE JSRQ>CURRENT DATE
    )

  ;
  
  --循环调用所有牌号  起始日期为年初2号，分析标记0
  --循环调用所有牌号  起始日期为本月1号 若一月1号，则加一天  ，分析标记1
  
  return 0;  

END;

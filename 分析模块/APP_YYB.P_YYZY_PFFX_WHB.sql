SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_PFFX_WHB (
    IN SYRQ    DATE,
    OUT SM    VARCHAR(1000) )
  SPECIFIC SQL100114163918200
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN
  /* MDL:配方分析 
   * TGT:YYZY.T_YYZY_PFFX; 
   * SRC:YYZY.T_YYZY_ZXPF_LSB,YYZY.T_YYZY_ZXPF_WHB; 
   * RFC: 
   * ATH:Rake.Gong 
   * UPD:2010-1-14 16:37 
   */ 
    --定义系统变量 
    DECLARE SQLSTATE CHAR(5);
    DECLARE SQLCODE INTEGER; 
    
    --定义自定义变量
    DECLARE ERR_MSG VARCHAR(1000) DEFAULT ''; 
    declare ddate date default '2007-01-01'; 
    declare var_stmt varchar(8000) default '';
    declare i_not_found integer default 0;
    declare var_pfphdm, var_jsdm, var_yynf, var_zxsx integer; 
    declare var_yydm varchar(50); 
    declare var_ksrq,var_jsrq date; 
    
    --定义游标
    --历史配方游标
    DECLARE c1 CURSOR FOR 
    select pfphdm,jsdm,yydm,yynf,ksrq,jsrq,zxsx
    from YYZY.T_YYZY_ZXPF_LSB 
    where ksrq<=SYRQ and ltrim(rtrim(yydm))<>'0' --取yydm为0的
    order by pfphdm,jsdm,ksrq
    ;
    
    --执行配方游标
    declare c2 cursor for
    select PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,ZXSX  
    from YYZY.T_YYZY_ZXPF_WHB
    where ltrim(rtrim(yydm))<>'0' /*tdsx=1 and*/ --取yydm不为0的,tdsx为1的
    order by pfphdm,jsdm,ksrq
    ;
    
    --定义异常处理
    declare continue handler for not found 
    begin 
      set i_not_found = 1; 
    end; 
    
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
      SET ERR_MSG = '系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';  '; 
    
    
    Set ERR_MSG = '';
    
    --定义临时表
    declare global temporary table tmp_zxpf ( 
      PFPHDM integer, 
      JSDM integer, 
      YYDM varchar(36), 
      YYNF integer, 
      KSRQ date, 
      JSRQ date, 
      ZXSX integer 
    ) with replace on commit preserve rows NOT LOGGED; 
    
    
    --初始化加载(插入历史配方数据),判断条件YYZY.T_YYZY_PFFX是否为空表
    if (select value(count(*),0) from YYZY.T_YYZY_PFFX)=0 then 
      set i_not_found=0;
      open c1;
      bh1:loop
        --遍历历史配方表
        fetch c1 into var_pfphdm,var_jsdm,var_yydm,var_yynf,var_ksrq,var_jsrq,var_zxsx ;
        if i_not_found=1 then 
          leave bh1;
        end if;
        --若满足条件(on...)则更新其结束时间，若不满足则插入数据
        merge into session.tmp_zxpf as t 
        using (
          select * from (values(var_pfphdm,var_jsdm,var_yydm,var_yynf,var_ksrq,var_jsrq,var_zxsx)) as a
        ) as s 
        on (t.pfphdm,t.jsdm,t.yydm,t.jsrq+1 day)=(var_pfphdm,var_jsdm,var_yydm,var_ksrq) 
          or (t.pfphdm,t.jsdm,t.yydm,t.jsrq)=(var_pfphdm,var_jsdm,var_yydm,var_ksrq) 
        when matched then 
          update set (t.jsrq)=(var_jsrq) 
        when not matched then 
          insert(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,zxsx)
          values(var_pfphdm,var_jsdm,var_yydm,var_yynf,var_ksrq,var_jsrq,var_zxsx)
        ;
      end loop;
      close c1; 
      
      delete from session.tmp_zxpf where rtrim(ltrim(yydm))='0';
      --遍历DDATE，每次+1月
      while year(DDATE)*100+month(ddate) <= year(syrq)*100+month(syrq) do 
        insert into YYZY.T_YYZY_PFFX(PFPHDM,JSDM,YYDM,PFNF,PFYF)
        with tmp as (
          select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,a.jsrq,a.zxsx 
          from session.tmp_zxpf a 
          where ddate-(day(ddate)-1) day between a.ksrq and a.jsrq 
        )
        select PFPHDM,JSDM,YYDM,year(DDATE) as PFNF,month(DDATE) as PFYF 
        from tmp; 
        SET DDATE=DDATE + 1 month; 
      end while; 

--    else 
--      insert into session.tmp_zxpf(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,zxsx) 
--    select PFPHDM,JSDM,YYDM,YYNF,KSRQ,max(JSRQ) as jsrq,ZXSX 
--    from YYZY.T_YYZY_ZXPF_LSB 
--    where year(jsrq)*100+month(jsrq)>=year(SYRQ)*100+month(SYRQ) 
--    group by PFPHDM,JSDM,YYDM,YYNF,KSRQ,ZXSX; 
    end if;
    
    delete from session.tmp_zxpf;
    
    insert into session.tmp_zxpf(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,zxsx) 
    select PFPHDM,JSDM,YYDM,YYNF,KSRQ,max(JSRQ) as jsrq,ZXSX 
    from YYZY.T_YYZY_ZXPF_LSB 
    where year(jsrq)*100+month(jsrq)>=year(SYRQ)*100+month(SYRQ) 
    group by PFPHDM,JSDM,YYDM,YYNF,KSRQ,ZXSX; 
    
    --历史配方数据进临时表

--       insert into session.tmp_zxpf(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,zxsx) 
--       select PFPHDM,JSDM,YYDM,YYNF,KSRQ,/*max(JSRQ) as*/jsrq,ZXSX 
--       from YYZY.T_YYZY_ZXPF_LSB 
--       where jsrq<=SYRQ 
      --group by PFPHDM,JSDM,YYDM,YYNF,KSRQ,ZXSX; 
    
    --现在配方数据进临时表
    set i_not_found=0;
    open c2;
    bh2:loop
      fetch c2 into var_pfphdm,var_jsdm,var_yydm,var_yynf,var_ksrq,var_jsrq,var_zxsx;
      if i_not_found=1 then 
        leave bh2;
      end if;
      
      merge into session.tmp_zxpf as e
      using (
        select * 
        from (values(var_pfphdm,var_jsdm,var_yydm,var_yynf,var_ksrq,var_jsrq,var_zxsx)) as a
      ) as s
      on (e.PFPHDM,e.jsdm,e.yydm,e.jsrq+1 day)=(var_pfphdm,var_jsdm,var_yydm,var_ksrq) 
        or (e.pfphdm,e.jsdm,e.yydm,e.jsrq)=(var_pfphdm,var_jsdm,var_yydm,var_ksrq) 
      when matched then 
        update set (e.jsrq) = (var_jsrq) 
      when not matched then 
        insert(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,zxsx) 
        values (var_pfphdm,var_jsdm,var_yydm,var_yynf,var_ksrq,var_jsrq,var_zxsx); 
    end loop;
    close c2;
     
    delete from session.tmp_zxpf 
    where rtrim(ltrim(yydm))='0'; 
    
    --历史配方数据
    delete from YYZY.T_YYZY_PFFX 
    where pfnf*100 + pfyf>=year(syrq)*100+month(syrq); 
      
    --pffx执行开始日期
    set ddate=syrq;
   
    WHILE year(DDATE)*100+month(DDATE) <= year(syrq + 5 year - month(syrq) month)*100+month(syrq+ 5 year - month(syrq) month) DO 
      INSERT INTO YYZY.T_YYZY_PFFX(PFPHDM,JSDM,YYDM,PFNF,PFYF) 
      with tmp as (
        select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,a.jsrq,a.zxsx 
        from session.tmp_zxpf a 
        where ddate-(day(ddate)-1) day between a.ksrq and a.jsrq 
      )
      select PFPHDM,JSDM,yydm,year(DDATE) as PFNF,month(DDATE) as PFYF 
      from tmp as a 
      --where (pfphdm,jsdm,yydma,year(DDATE),month(DDATE)) not in (select pfphdm,jsdm,yydm,pfnf,pfyf from YYZY.T_YYZY_PFFX)
      ; 
      SET DDATE=DDATE+1 month; 
    END WHILE; 
    
    delete from YYZY.T_YYZY_PFFX 
    where lsbh in ( 
      select lsbh 
      from ( 
        select max(lsbh) as lsbh,pfphdm,jsdm,yydm,pfnf,pfyf 
        from YYZY.T_YYZY_PFFX 
        group by pfphdm,jsdm,yydm,pfnf,pfyf 
        having count(*)>1 
      ) as t 
    ); 

       IF ERR_MSG <>'' then 
       set sm = '插入历史配方表替代信息出错'||err_msg; 
       return -1;
    end if;
    set err_msg='';

END;

COMMENT ON PROCEDURE APP_YYB.P_YYZY_PFFX_WHB( DATE, VARCHAR(1000) ) IS '生成配方数据';

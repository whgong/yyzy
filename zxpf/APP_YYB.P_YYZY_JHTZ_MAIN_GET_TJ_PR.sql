SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_TJ_PR ( )
  SPECIFIC SQL091215171652200
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN 

  -- 定义系统变量 
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER;
  
  -- 定义存储变量
  declare v_ksrq,v_jsrq date;
  declare v_sdl,v_kssyl,v_jssyl,v_hl_d decimal(10,2);
  declare v_pfphdm,v_jsdm,i_not_found integer;
  
  -- 定义静态游标c1 
  declare c1 cursor for 
    select PFPHDM,JSDM,SDL,KSSYL,JSSYL,HL_D,SDKSRQ,SDJSRQ+1 day as sdjsrq
    from YYZY.T_YYZY_TMP_YYSDB
    where sdfs in('3','5')
    order by pfphdm,jsdm,sdksrq
  ;
  
  declare continue handler for not found set i_not_found = 1; 
  
  declare global temporary table yyxhtjb 
    like yyzy.t_yyzy_tmp_yyxhtjb 
    with replace on commit preserve rows NOT LOGGED
  ; 
  
  
  open c1;
  loop_ml: loop
  
    set i_not_found=0; 
    fetch c1 into v_pfphdm,v_jsdm,v_sdl,v_kssyl,v_jssyl,v_hl_d,v_ksrq,v_jsrq;
    
    if i_not_found=1 then 
      leave loop_ml;
    end if;
    
    insert into session.yyxhtjb(pfphdm,jsdm,ksrq,jsrq,hl_d)
    -- 更新开始日期
    select pfphdm,jsdm,ksrq,v_ksrq-1 day as jsrq,hl_d 
    from yyzy.t_yyzy_tmp_yyxhtjb 
    where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
      and ksrq<v_ksrq 
      and jsrq>=v_ksrq 
    ;
    
    -- 更新结束日期 处理结束使用量
    if v_jssyl<>v_hl_d then 
      insert into session.yyxhtjb(pfphdm,jsdm,ksrq,jsrq,hl_d)
      select 
        pfphdm,jsdm,v_jsrq as ksrq,v_jsrq as jsrq, hl_d-value(v_jssyl,0) as hl_d 
      from yyzy.t_yyzy_tmp_yyxhtjb
      where (pfphdm,jsdm)=(v_pfphdm,v_jsdm) 
        and ksrq<=v_jsrq and jsrq>=v_jsrq 
      ;
    end if;
    
    -- 更新结束日期 普通处理
    insert into session.yyxhtjb(pfphdm,jsdm,ksrq,jsrq,hl_d)
    select 
      pfphdm,jsdm,
      case when v_jssyl=v_hl_d then v_jsrq else v_jsrq+1 day end as ksrq, 
      jsrq, hl_d 
    from yyzy.t_yyzy_tmp_yyxhtjb 
    where (pfphdm,jsdm)=(v_pfphdm,v_jsdm) 
      and ksrq<=v_jsrq and jsrq>v_jsrq 
    ; 
    
    -- 删除抢用烟叶
    delete from yyzy.t_yyzy_tmp_yyxhtjb 
    where (pfphdm,jsdm,ksrq,jsrq)in(
      select pfphdm,jsdm,ksrq,jsrq
      from yyzy.t_yyzy_tmp_yyxhtjb
      where (pfphdm,jsdm)=(v_pfphdm,v_jsdm) 
        and (ksrq<=v_jsrq and jsrq>=v_ksrq) 
    )
    ;
    
    -- 更新耗量点
    -- insert into yyzy.t_yyzy_tmp_yyxhtjb(pfphdm,jsdm,ksrq,jsrq,hl_d)
    -- values(v_pfphdm,v_jsdm,v_ksrq,v_jsrq,0.00)
    -- ; 
    
    insert into yyzy.t_yyzy_tmp_yyxhtjb(pfphdm,jsdm,ksrq,jsrq,hl_d)
    select pfphdm,jsdm,ksrq,jsrq,hl_d 
    from session.yyxhtjb;
    
  end loop  loop_ml;
  close c1; 
  
  delete from yyzy.t_yyzy_tmp_yysdtjb;
  insert into yyzy.t_yyzy_tmp_yysdtjb
  select pfphdm,jsdm,ksrq,jsrq,hl_d 
  from yyzy.t_yyzy_tmp_yyxhtjb
  where (pfphdm,jsdm) in (
      select pfphdm,jsdm 
      from YYZY.T_YYZY_TMP_YYSDB
      where sdfs in('3','5')
    )
  ;

END;

SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD_PR (
    IN V_PFPHDM	INTEGER,
    IN V_JSDM	INTEGER,
    IN V_KSRQ	DATE,
    IN V_SDL	DECIMAL(10,2),
    IN V_SDBH	INTEGER )
  SPECIFIC SQL090913170508000
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
begin 
  --定义变量
  declare i_not_found integer;
  declare i_ksrq,i_jsrq,mark_date date;
  declare i_hl_d,i_hl_l,js_ycl decimal(10,2);
  declare js_ljl decimal(10,2) default 0.00;
  
  --定义静态游标 
  declare c1 cursor for 
  select ksrq,jsrq,hl_d,hl_l
  from(
    select v_ksrq as ksrq,jsrq,hl_d,hl_d*(days(jsrq)-days(v_ksrq)+1) as hl_l
    from yyzy.t_yyzy_tmp_yyxhtjb
    where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
      and jsrq>=v_ksrq 
      AND KSRQ<=v_ksrq 
    union all 
    select ksrq,jsrq,hl_d,hl_d*(days(jsrq)-days(ksrq)+1) as hl_l
    from yyzy.t_yyzy_tmp_yyxhtjb
    where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
      and KSRQ>v_ksrq
  ) tjb 
  order by ksrq,jsrq; 
  
  declare continue handler for not found
  begin 
    set i_not_found = 1;
  end; 
  
  set i_not_found=0; 
  open c1;
  loop_ml: loop
    fetch c1 into i_ksrq,i_jsrq,i_hl_d,i_hl_l; 
    if i_not_found=1 then
      leave loop_ml;
    end if; 
    set JS_LJL=JS_LJL+I_HL_L;
    case 
      when V_SDL>JS_LJL then 
        set i_not_found=0; 
      else
        set JS_YCL=JS_LJL-V_SDL;
        
        set MARK_DATE=I_JSRQ;
        
        while js_ycl>0.00 do 
          set js_ycl=js_ycl-i_hl_d; 
          set mark_date=mark_date- 1 day;
        end while;
        
        case 
          when js_ycl<0 then 
            update yyzy.t_yyzy_tmp_yysdb 
            set (sdl,hl_d,jssyl,sdjsrq)=(V_SDL,i_hl_d,abs(js_ycl),mark_date)
            where sdbh=v_sdbh; 
          else
            update yyzy.t_yyzy_tmp_yysdb
            set (sdl,hl_d,jssyl,sdjsrq)=(V_SDL,i_hl_d,i_hl_d,mark_date)
            where sdbh=v_sdbh; 
        end case;
        
        --退出循环
        leave loop_ml; 
      end case;
   end loop  loop_ml;
   close c1;
end;

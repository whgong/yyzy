SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_PR_SD1 (
    IN S_YYDM    VARCHAR(50),
    IN S_YYNF    INTEGER,
    IN S_SYSX    INTEGER,
    IN S_KCLX    INTEGER,
    IN V_SDFS    CHARACTER(1) )
  SPECIFIC SQL091018102526400
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
--存储过程 
  BEGIN   
    --定义系统变量   
    declare v_sdksrq,v_sdjsrq,v_jsrq date;
    declare v_pfphdm,v_jsdm,v_count,v_sdbh,i_not_found integer;
    /*采用游标方式主要是为了判断同一个烟叶存在多个结束时间锁定*/    
    declare c1 cursor for 
    select pfphdm,jsdm,max(jsrq)+1 day as sd_ksrq 
    from yyzy.t_yyzy_tmp_zxpfb_whb
    where (pfphdm,jsdm) in(
    select pfphdm,jsdm 
    from yyzy.t_yyzy_tmp_yyfpgzb_all
    where(yydm,yynf,sysx,kclx)=(s_yydm,s_yynf,s_sysx,s_kclx)
    and sdfs=V_SDFS
    )
    group by pfphdm,jsdm
    order by sd_ksrq;
    
    declare continue handler for not found
               begin
                 set i_not_found = 1;
            end; 
    set i_not_found=0; 
            open c1;
                 loop_ml: loop
                         fetch c1 into v_pfphdm,v_jsdm,v_sdksrq;
                         if i_not_found=1 then  --两种情况：1 角色已遍历完，
                                                --2 角色不存在，即它是一个新的角色
                            --取得锁定结束时间
                            select max(sdjsrq),sdbh into V_SDJSRQ,V_SDBH
                             from YYZY.T_YYZY_TMP_YYSDB
                             where sdfs=V_SDFS 
                             and sdbh in (select sdbh from yyzy.t_yyzy_tmp_yyfpgzb_all where(yydm,yynf,sysx,kclx)=(s_yydm,s_yynf,s_sysx,s_kclx))
                             group by sdbh;                                                
                            --根据库存进行锁定量得计算
                             call APP_YYB.P_YYZY_JHTZ_MAIN_GET_QB(S_YYDM,S_YYNF,S_SYSX,S_KCLX,V_SDFS,V_SDJSRQ);
                            select max(jsrq) into v_jsrq 
                            from yyzy.t_yyzy_tmp_yyjqb;                                
                             if v_jsrq>=V_SDJSRQ then                                      
                               update YYZY.T_YYZY_TMP_YYFPGZB_ALL
                               set sdfs='0'
                               where sdbh=v_sdbh;                          
                              end if;                                   
                            leave loop_ml;                                      
                         end if;
                         --取得锁定结束时间
                         select sdjsrq,sdbh into v_sdjsrq,v_sdbh
                         from YYZY.T_YYZY_TMP_YYSDB
                         where sdfs=v_sdfs 
                         and sdbh=(select sdbh from yyzy.t_yyzy_tmp_yyfpgzb_all where(pfphdm,jsdm,yydm,yynf,sysx,kclx)=(v_pfphdm,v_jsdm,s_yydm,s_yynf,s_sysx,s_kclx));    
                         --根据库存进行锁定量得计算
                         call APP_YYB.P_YYZY_JHTZ_MAIN_GET_QB(S_YYDM,S_YYNF,S_SYSX,S_KCLX,V_SDFS,V_SDJSRQ);
                         select jsrq into v_jsrq 
                         from yyzy.t_yyzy_tmp_yyjqb;
                         if v_jsrq>=v_sdjsrq then                                      
                            update YYZY.T_YYZY_TMP_YYFPGZB_ALL
                            set sdfs='0'
                            where sdbh=v_sdbh;                          
                          end if;
                         leave loop_ml;                         
--                          set i_not_found=0;                                   
                 end loop  loop_ml;
            close c1;                
    end;

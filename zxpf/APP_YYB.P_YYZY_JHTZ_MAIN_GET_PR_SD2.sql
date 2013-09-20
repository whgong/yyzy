SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_PR_SD2 (
    IN S_YYDM    VARCHAR(50),
    IN S_YYNF    INTEGER,
    IN S_SYSX    INTEGER,
    IN S_KCLX    INTEGER,
    IN D_END_DATE    DATE,
    IN V_SDFS    CHARACTER(1),
    INOUT N_SYL    DECIMAL(10,2) )
  SPECIFIC SQL091215171818900
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
--存储过程 
  BEGIN   
    --定义系统变量 
    DECLARE SQLSTATE CHAR(5);  
    DECLARE SQLCODE INTEGER;
    declare v_pfphdm,v_jsdm,v_sdbh,i_not_found integer;
    declare V_SDL,J_ZHL,V_JSSYL,V_ZXL DECIMAL(10,2);
    --量锁定游标C2
    declare c2 cursor for
    select pfphdm,jsdm,max(jsrq)+1 day as SDL_KSRQ  
    from yyzy.t_yyzy_tmp_zxpfb_whb
    where (pfphdm,jsdm) in(
    select pfphdm,jsdm 
    from yyzy.t_yyzy_tmp_yyfpgzb_all
    where(yydm,yynf,sysx,kclx)=(s_yydm,s_yynf,s_sysx,S_KCLX)
    and sdfs=V_SDFS
    )
    group by pfphdm,jsdm
    order by SDL_KSRQ;
    
    declare continue handler for not found
               begin
                 set i_not_found = 1;
            end;     
    set i_not_found=0; 
    open c2;
         loop_ml: loop
                  fetch c2 into v_pfphdm,v_jsdm;
                  if i_not_found=1 then  --两种情况：1 角色已遍历完，
                                         --2 角色不存在，即它是一个新的角色 
                     --取得锁定结束时间
                     select sdl,sdbh into V_SDL,v_sdbh
                     from YYZY.T_YYZY_TMP_YYSDB
                     where sdfs=v_sdfs 
                     and sdbh in (
                                  select sdbh 
                                  from yyzy.t_yyzy_tmp_yyfpgzb_all 
                                  where(yydm,yynf,sysx,kclx)=(s_yydm,s_yynf,s_sysx,s_kclx)
                                  )
                     group by sdbh,sdl;    
                     case
                         when N_SYL<=V_SDL then
                              set V_ZXL=N_SYL;
                              set N_SYL=0.00; 
                         else 
                              set V_ZXL=V_SDL;
                              SET N_SYL=N_SYL-V_SDL;
                     end case; 
                     while V_ZXL>0.05 do
                           delete from yyzy.t_yyzy_tmp_yyjqb;      
                           --取时间段 得到权比 
                           call APP_YYB.P_YYZY_JHTZ_MAIN_GET_QB(S_YYDM,S_YYNF,S_SYSX,S_KCLX,V_SDFS,D_END_DATE);
                           select sum((days(jsrq)-days(ksrq)+1)*hl_d) into J_ZHL 
                           from yyzy.t_yyzy_tmp_yyjqb;        
                           select value(sum(jssyl),0.00) into V_JSSYL from YYZY.T_YYZY_TMP_JSSYL;
                           set V_ZXL=V_ZXL+V_JSSYL; 
                           --获得执行配方
                           call APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF(V_ZXL,J_ZHL,'2',D_END_DATE); 
                     end while; 
                     update yyzy.t_yyzy_tmp_yyfpgzb_all
                        set sysx=sysx-1
                        where (pfphdm,jsdm) in(
                                               select pfphdm,jsdm from yyzy.t_yyzy_tmp_yyfpgzb_all
                                               where (yydm,yynf,sysx,sdfs,kclx)=(S_yydm,S_yynf,S_sysx,v_sdfs,s_kclx)
                                            );
                     delete from yyzy.t_yyzy_tmp_yyfpgzb_all
                       where sysx=0;
                     delete from YYZY.T_YYZY_TMP_YYSDB
                     where sdbh=v_sdbh;
                     leave loop_ml;        
                    end if;     
                    --取得锁定结束时间
                    select sdl,sdbh into V_SDL,v_sdbh
                    from YYZY.T_YYZY_TMP_YYSDB
                    where sdfs=v_sdfs 
                    and sdbh in (
                                 select sdbh 
                                 from yyzy.t_yyzy_tmp_yyfpgzb_all 
                                 where(pfphdm,jsdm,yydm,yynf,sysx,kclx)=(v_pfphdm,v_jsdm,s_yydm,s_yynf,s_sysx,s_kclx)
                                 )
                    group by sdbh,sdl; 
                    case
                         when N_SYL<=V_SDL then
                              set V_ZXL=N_SYL;
                              set N_SYL=0.00;
                         else 
                              set V_ZXL=V_SDL;
                              SET N_SYL=N_SYL-V_SDL;
                    end case; 
                    while V_ZXL>0.05 do     
                          delete from yyzy.t_yyzy_tmp_yyjqb; 
                          --取时间段 得到权比 
                           call APP_YYB.P_YYZY_JHTZ_MAIN_GET_QB(S_YYDM,S_YYNF,S_SYSX,S_KCLX,V_SDFS,D_END_DATE);
                           select sum((days(jsrq)-days(ksrq)+1)*hl_d) into J_ZHL 
                           from yyzy.t_yyzy_tmp_yyjqb;
                          select value(sum(jssyl),0.00) into V_JSSYL from YYZY.T_YYZY_TMP_JSSYL;
                          set V_ZXL=V_ZXL+V_JSSYL; 
                           --获得执行配方
                           call APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF(V_ZXL,J_ZHL,'2',D_END_DATE); 
                     end while;    
                     update yyzy.t_yyzy_tmp_yyfpgzb_all
                        set sysx=sysx-1
                        where (pfphdm,jsdm)=(v_pfphdm,v_jsdm);
                     delete from yyzy.t_yyzy_tmp_yyfpgzb_all
                       where sysx=0;
                     delete from YYZY.T_YYZY_TMP_YYSDB  
                     where sdbh=v_sdbh ; 
                     set i_not_found=0; 
         end loop  loop_ml;
    close c2;
  end;

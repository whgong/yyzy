SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_PR (
    IN S_YYDM  VARCHAR(50),
    IN S_YYNF  INTEGER,
    IN S_SYSX  INTEGER,
    IN S_KCLX  INTEGER,
    IN D_END_DATE  DATE,
    IN S_SDFS  CHARACTER(1),
    INOUT N_SYL  DECIMAL(10,2) )
  SPECIFIC SQL091017141818700
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
--存储过程 
  BEGIN   
    --定义系统变量 
  --定义
    case
        when S_SDFS='1' then
           --这个烟叶是结束时间锁定(继续分配) 
             call  APP_YYB.P_YYZY_JHTZ_MAIN_GET_PR_SD1(S_YYDM,S_YYNF,S_SYSX,S_KCLX,S_SDFS);   
        when S_SDFS='2' then  
           --这个烟叶是量锁定    
             call APP_YYB.P_YYZY_JHTZ_MAIN_GET_PR_SD2(S_YYDM,S_YYNF,S_SYSX,S_KCLX,D_END_DATE,S_SDFS,N_SYL);  
         case
           when N_SYL=0.00 then
              set N_SYL=9999999.99;
           else
                set N_SYL=N_SYL; 
           end case; 
         delete from yyzy.t_yyzy_tmp_yyjqb; 
      when S_SDFS='6' then
           --这个烟叶是结束时间锁定(停止分配) 
             call APP_YYB.P_YYZY_JHTZ_MAIN_GET_PR_SD6(S_YYDM,S_YYNF,S_SYSX,S_KCLX,S_SDFS); 
         else
           --一般情况
         call APP_YYB.P_YYZY_JHTZ_MAIN_GET_QB(S_YYDM,S_YYNF,S_SYSX,S_KCLX,'0',D_END_DATE);
    end case; 
  END;

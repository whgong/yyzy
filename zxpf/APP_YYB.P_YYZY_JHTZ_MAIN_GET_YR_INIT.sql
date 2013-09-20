SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR_INIT ( OUT OUTPUT_MSG VARCHAR(1000) )
  SPECIFIC SQL100731145825000
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN 
    DECLARE SQLSTATE CHAR(5); 
    DECLARE SQLCODE INTEGER; 
    declare D_START_DATE,D_END_DATE date; 
    --定义
    declare exit handler for sqlexception 
    begin 
      set OUTPUT_MSG='加载失败:sqlcode '||rtrim(char(SQLCODE))||',sqlstate '||rtrim(SQLSTATE);
      rollback;
    end; 
    
    set OUTPUT_MSG=''; 
    
    select min(ksrq),max(jsrq) into D_START_DATE,D_END_DATE from yyzy.t_yyzy_rscjhb_whb; 
    
    /*删除已经添加的采购计划*/
    delete from yyzy.t_yyzy_zxpf_whb where yynf>=year(current date); 
    
    CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR(D_START_DATE,D_END_DATE,1); 
    
    set OUTPUT_MSG='加载成功';
    
  END;

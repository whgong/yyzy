SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD3 (
    IN V_PFPHDM	INTEGER,
    IN V_JSDM	INTEGER,
    IN V_YYDM	VARCHAR(50),
    IN V_YYNF	INTEGER,
    IN V_KCLX	INTEGER,
    IN V_KSRQ	DATE,
    IN V_JSRQ	DATE,
    IN V_SDBH	INTEGER,
    IN V_KCL	DECIMAL(10,2),
    IN V_SDFS	CHARACTER(1),
    IN D_START_DATE	DATE,
    IN D_END_DATE	DATE )
  SPECIFIC SQL090927105750900
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
    declare flag integer;
    
    --定义中间变量
    declare n_sdl,v_hl_d decimal(10,2);
    case
      when V_KSRQ>=D_START_DATE and v_jsrq>=D_START_DATE then 
        set flag=1; 
      when V_KSRQ<D_START_DATE and v_jsrq>=D_START_DATE then 
        update yyzy.t_yyzy_tmp_yysdb 
        set sdksrq=D_START_DATE 
        where sdbh=v_sdbh; 
        
        set v_ksrq=D_START_DATE; 
        set flag=1; 
      else 
        --锁定时间段失效
        delete from yyzy.t_yyzy_tmp_yysdb 
        where sdbh=v_sdbh; 
        
        set flag=0; 
        return; 
    end case; 
    
    case 
      when flag=1 then 
        select sum(hl_l) into N_SDL 
        from (
          select value(hl_d*(days(case when jsrq>v_jsrq then v_jsrq else jsrq end)-days(v_ksrq)+1),0.00) as hl_l
          from yyzy.t_yyzy_tmp_yyxhtjb
          where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
            and jsrq>=v_ksrq AND KSRQ<=v_ksrq
          union all
          select value(sum(hl_d*(days(jsrq)-days(ksrq)+1)),0.00) as hl_l
          from yyzy.t_yyzy_tmp_yyxhtjb 
          where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
            and jsrq<v_jsrq AND KSRQ>v_ksrq
          union all 
          select value(hl_d*(days(v_jsrq)-days(ksrq)+1),0.00) as hl_l
          from yyzy.t_yyzy_tmp_yyxhtjb
          where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
            and jsrq>=v_jsrq AND KSRQ<=v_jsrq and ksrq>v_ksrq
        ) as tjb;
        
        case 
          when V_KCL>=N_SDL then --库存足够分配
            update YYZY.T_YYZY_TMP_YYKC
            set yykcjs=v_kcl-N_SDL
            where (yydm,yynf,KCLX)=(v_yydm,v_yynf,V_KCLX);
            --更新库存为0的烟叶
            update YYZY.T_YYZY_TMP_YYKC
            set yykcjs=9999999.99
            where floor(yykcjs)=0;
            --得到结束使用量(jssyl)
            select hl_d into v_hl_d 
            from yyzy.t_yyzy_tmp_yyxhtjb
            where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
              and v_jsrq between ksrq and jsrq;
            --添加锁定量
            update YYZY.T_YYZY_TMP_YYSDB
            set (jssyl,hl_d,sdl)=(v_hl_d,v_hl_d,N_SDL)
            where (sdbh,sdfs)=(v_sdbh,v_sdfs);
          else 
            --得到结束时间，更新锁定的结束时间
            CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD_PR(V_PFPHDM,V_JSDM,V_KSRQ,V_KCL,V_SDBH);
        end case; 
      else
        return ; 
  end case; 
END;

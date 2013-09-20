SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD5 (
    IN V_PFPHDM	INTEGER,
    IN V_JSDM	INTEGER,
    IN V_YYDM	VARCHAR(50),
    IN V_YYNF	INTEGER,
    IN V_KCLX	INTEGER,
    IN V_KSRQ	DATE,
    IN V_SDBH	INTEGER,
    IN V_SDL	DECIMAL(10,2),
    IN V_KCL	DECIMAL(10,2),
    IN V_SDFS	CHARACTER(1),
    IN D_START_DATE	DATE,
    IN D_END_DATE	DATE )
  SPECIFIC SQL091019100140200
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
  --定义中间变量
  declare n_sdl decimal(10,2);
  declare flag integer;
  declare v_yyfpl_o decimal(10,2);
  case
    when v_ksrq>=D_START_DATE and v_ksrq<=D_END_DATE then set flag=1; 
    when v_ksrq<D_START_DATE then 
      --计算锁定量已经被分配部分
      select sum(jhpc*dpxs) as yyfpl into v_yyfpl_o
      from (
        select a.pfphdm,jsdm,jhrq,dpxs,jhpc
        from (
          select a.pfphdm,jhrq,jhcl*1.00/dpcl as jhpc 
          from ( 
            select pfphdm,jhrq,jhcl
            from (
              select PFPHDM, riqi as jhrq, JHCL_AVG as jhcl
              from YYZY.T_YYZY_RSCJHB_WHB AS A
              inner join DIM.T_DIM_YYZY_DATE AS D
                ON D.RiqI BETWEEN A.KSRQ AND A.JSRQ
              union all 
              select PFPHDM, JHRQ, JHCL
              from YYZY.T_YYZY_RSCJH_LSB
            ) as a
            where jhrq between v_ksrq and D_START_DATE-1 day
          ) as a
          inner join (
            select pfphdm,dpcl
            from yyzy.t_yyzy_dpclb
            where (pfphdm,nf*100+yf)in(
              select pfphdm,max(nf*100+yf)
              from yyzy.t_yyzy_dpclb
              group by pfphdm
            )
          ) as b on a.pfphdm=b.pfphdm
        ) as a 
        inner join YYZY.T_YYZY_JSTZ_WHB as b 
          on a.pfphdm=b.pfphdm 
          and a.jhrq between b.ksrq and b.jsrq
        where b.zybj='1'
        ) as jh_sjd 
      where pfphdm=v_pfphdm and jsdm=v_jsdm
      ;
      set v_ksrq=D_START_DATE; 
      --更新锁定数据
      update yyzy.t_yyzy_tmp_yysdb 
      set sdksrq=D_START_DATE 
      where (sdbh,sdfs)=(v_sdbh,v_sdfs); 
      
      set v_sdl=v_sdl-value(v_yyfpl_o,0);
      set flag=1;
    else 
      --此锁定无效
      delete from yyzy.t_yyzy_tmp_yysdb
      where sdbh=v_sdbh;
      set flag=0;
      return; 
  end case; 
  
  case 
    when flag=1 then 
      select value(sum(hl_l),0.00) into N_SDL 
      from(
        select value(hl_d*(days(jsrq)-days(v_ksrq)+1),0.00) as hl_l 
        from yyzy.t_yyzy_tmp_yyxhtjb 
        where (pfphdm,jsdm)=(v_pfphdm,v_jsdm) 
          and jsrq>=v_ksrq AND KSRQ<=v_ksrq
        union all 
        select value(sum(hl_d*(days(jsrq)-days(ksrq)+1)),0.00) as hl_l
        from yyzy.t_yyzy_tmp_yyxhtjb
        where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
          and KSRQ>v_ksrq 
      ) tjb; 
      
      case 
        when v_kcl>v_sdl and v_sdl<=n_sdl then 
          --更新库存
           update YYZY.T_YYZY_TMP_YYKC 
           set yykcjs=v_kcl-v_sdl 
           where (yydm,yynf,KCLX)=(v_yydm,v_yynf,V_KCLX); 
           --得到结束时间 
           CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD_PR(V_PFPHDM,V_JSDM,V_KSRQ,V_SDL,V_SDBH);
        when v_kcl<=v_sdl and v_kcl<=n_sdl then 
           --更新库存
           update YYZY.T_YYZY_TMP_YYKC
           set yykcjs=9999999.99
           where (yydm,yynf,KCLX)=(v_yydm,v_yynf,V_KCLX); 
           CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD_PR(V_PFPHDM,V_JSDM,V_KSRQ,v_kcl,V_SDBH);
          else 
           --更新库存
           update YYZY.T_YYZY_TMP_YYKC
           set yykcjs=v_kcl-n_sdl
           where (yydm,yynf,KCLX)=(v_yydm,v_yynf,V_KCLX);
           update YYZY.T_YYZY_TMP_YYKC
           set yykcjs=9999999.99
           where floor(yykcjs)=0;  
           --更新锁定数据 
            CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD_PR(V_PFPHDM,V_JSDM,V_KSRQ,n_sdl,V_SDBH); 
      end case;           
    else     
            return;
     end case; 
END;

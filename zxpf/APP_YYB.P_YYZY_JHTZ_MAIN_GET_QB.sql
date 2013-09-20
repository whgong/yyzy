SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_QB (
    IN S_YYDM    VARCHAR(50),
    IN S_YYNF    INTEGER,
    IN S_SYSX    INTEGER,
    IN S_KCLX    INTEGER,
    IN V_SDFS    CHARACTER(1),
    IN V_SDJSRQ    DATE )
  SPECIFIC SQL090924094903300
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
    --定义
    --得到权比    
    insert into yyzy.t_yyzy_tmp_yyjqb(pfphdm,jsdm,yydm,yynf,zlyybj,zpfbj,ksrq,jsrq,sdfs,sdbh,hl_d,jq_qz,kclx) 
    with tmp1 as(
    --取配对烟叶代码对应的时间段
    select a.pfphdm,a.jsdm,b.yydm,b.yynf,b.zlyybj,b.zpfbj,min(a.ksrq) as ksrq,min(a.jsrq) as jsrq,b.sdfs,b.sdbh,a.hl_d,b.kclx
    from yyzy.t_yyzy_tmp_yyxhtjb a,(
                                    select pfphdm,jsdm,yydm,yynf,zlyybj,zpfbj,sdfs,sdbh,
                                            (case 
                                               when s_sysx=0 then
                                                    0
                                               else
                                                    sysx
                                           end) as sysx,kclx 
                                    from yyzy.t_yyzy_tmp_yyfpgzb_all 
                                    ) b
    where (a.pfphdm,a.jsdm,b.yydm,b.yynf,b.kclx,b.sdfs,b.sysx)=(b.pfphdm,b.jsdm,s_yydm,s_yynf,s_kclx,v_sdfs,s_sysx)
    and ksrq<=v_sdjsrq 
    group by a.pfphdm,a.jsdm,b.yydm,b.yynf,b.zlyybj,b.zpfbj,a.hl_d,b.sdfs,b.sdbh,b.kclx  
    ),tmp_min_ksrq as(
    select min(ksrq) as min_ksrq
    from tmp1
    ),tmp_min_jsrq as(
    select min(jsrq) as min_jsrq from ( 
    select ksrq - 1 day as jsrq from tmp1
    where ksrq not in(select min_ksrq from tmp_min_ksrq)
    union 
    select case
               when jsrq<=v_sdjsrq then 
                   jsrq
                else
                    v_sdjsrq
           end as jsrq from tmp1) rq
    ),tmp3 as(
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,b.min_ksrq as ksrq,c.min_jsrq as jsrq,a.sdfs,a.sdbh,a.hl_d,a.kclx
    from tmp1 a,tmp_min_ksrq b,tmp_min_jsrq c
    where a.ksrq<=c.min_jsrq 
    ),tmp4 as(
    select case
               when sum(hl_d)=0 then 
                    1.00000
               else sum(hl_d) 
          end as jq_qm
    from tmp3
    )
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,a.ksrq,a.jsrq,a.sdfs,a.sdbh,a.hl_d,round(a.hl_d/jq_qm,11) as jq_qz,a.kclx 
    from tmp3 a,tmp4 b
    where 1=1;
    
    --取得ksrq 使用掉得烟叶
    delete from YYZY.T_YYZY_TMP_JSSYL; 
    insert into YYZY.T_YYZY_TMP_JSSYL(pfphdm,jsdm,jssyl,jsrq)
    select a.pfphdm,a.jsdm,value(sum(a.jssyl),0.00),a.jsrq 
    from yyzy.t_yyzy_tmp_zxpfb_whb a,yyzy.t_yyzy_tmp_yyjqb b
    where (a.pfphdm,a.jsdm,a.jsrq)=(b.pfphdm,b.jsdm,b.ksrq)
    group by a.pfphdm,a.jsdm,a.jsrq;    
END;

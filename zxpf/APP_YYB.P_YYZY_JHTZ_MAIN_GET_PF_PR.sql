SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF_PR (
    IN D_START_DATE    DATE,
    IN D_END_DATE    DATE )
  SPECIFIC SQL091215171506900
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
--存储过程 
  BEGIN   
    --定义存储变量
    declare v_yydm varchar(50);
    declare v_ksrq,v_jsrq date;
    declare v_zlyybj,v_zpfbj character(1);
    declare v_sdl,v_kssyl,v_jssyl,v_hl_d decimal(10,2);
    declare v_pfphdm,v_jsdm,v_yynf,v_sdbh,v_kclx,i_not_found integer;

    --定义静态游标c1
    declare c1 cursor for
    select pfphdm,jsdm,yydm,yynf,kclx,sdbh,sdl,kssyl,jssyl,hl_d,sdksrq,sdjsrq,zlyybj,zpfbj
    from yyzy.t_yyzy_tmp_yysdb
    where sdfs in('3','5')
    order by pfphdm,jsdm,sdksrq;
    
    declare continue handler for not found
              begin
                set i_not_found = 1;
            end;         
    --还原yydm<'0' 烟叶为0烟叶
    update yyzy.t_yyzy_tmp_zxpfb_whb 
    set yydm='0' 
    where yydm<'0';    
    --yyfpl=-1.00处理
    update yyzy.t_yyzy_tmp_zxpfb_whb
    set(kssyl,jssyl)=(-1.00,-1.00)
    where yyfpl=-1.00;
    --特殊处理
    delete from yyzy.t_yyzy_tmp_zxpfb_whb 
    where (ksrq,ksrq,yyfpl)=(jsrq,d_end_date,0.00);
    --写入TDSX
    update yyzy.t_yyzy_tmp_zxpfb_whb a
    set(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,sdbh,zxsx,tdsx)=(
         select pfphdm,jsdm,yydm,yynf,ksrq,jsrq,sdbh,zxsx,tdsx from(
               select pfphdm,jsdm,yydm,yynf,ksrq,jsrq,sdbh,zxsx,rownumber() over(partition by pfphdm,jsdm order by ksrq,jsrq,tdsx,zxsx) as tdsx 
               from YYZY.T_YYZY_TMP_ZXPFB_WHB
               ) b  
         where (a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,a.jsrq,a.sdbh,a.zxsx)=(b.pfphdm,b.jsdm,b.yydm,b.yynf,b.ksrq,b.jsrq,b.sdbh,b.zxsx) 
    );
    --还原锁定方式'4'，即开始时间锁定
    update YYZY.T_YYZY_TMP_ZXPFB_WHB a
    set sdbh=(
    select b.sdbh from yyzy.t_yyzy_tmp_yyfpgzb b
    where (a.pfphdm,a.jsdm,a.yydm,a.yynf,b.sdfs,a.kclx)=(b.pfphdm,b.jsdm,b.yydm,b.yynf,'4',b.kclx)
    )
    where (pfphdm,jsdm,yydm,yynf,kclx) 
    in(
       select pfphdm,jsdm,yydm,yynf,kclx 
       from YYZY.T_YYZY_TMP_YYSDB
       where sdfs='4'
      );     
    set i_not_found=0; 
    open c1;
     loop_ml: loop
       fetch c1 into v_pfphdm,v_jsdm,v_yydm,v_yynf,v_kclx,v_sdbh,v_sdl,v_kssyl,v_jssyl,v_hl_d,v_ksrq,v_jsrq,v_zlyybj,v_zpfbj;
       if i_not_found=1   then
          leave loop_ml;
       end if;
       case
           when v_hl_d>v_jssyl then
                set v_jsrq=v_jsrq+1 day;
           else
                set v_jsrq=v_jsrq;
       end case;                 
       insert into yyzy.t_yyzy_tmp_zxpfb_whb(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,yyfpl,sdbh,zxsx,tdsx,kssyl,jssyl,zlyybj,zpfbj,bbrq,kclx)
       with tmp as(
       --更新开始日期
       select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,v_ksrq-1 day as jsrq,a.sdbh,a.zxsx,a.tdsx,a.kssyl,b.hl_d as jssyl,a.zlyybj,a.zpfbj,a.bbrq,a.kclx
       from yyzy.t_yyzy_tmp_zxpfb_whb a,yyzy.t_yyzy_tmp_yysdtjb b
       where (a.pfphdm,a.jsdm)=(b.pfphdm,b.jsdm) and (a.pfphdm,a.jsdm)=(v_pfphdm,v_jsdm)
       and a.ksrq<=v_ksrq and a.jsrq>=v_jsrq and b.jsrq=v_ksrq-1 day),tmp2 as(
       select pfphdm,jsdm,sum(yyfpl) as yyfpl from(
         select a.pfphdm,a.jsdm,(days(a.jsrq)-days(b.ksrq)+1)*a.hl_d as yyfpl
            from yyzy.t_yyzy_tmp_yysdtjb a,tmp b
            where (a.pfphdm,a.jsdm)=(b.pfphdm,b.jsdm)
            and b.ksrq between a.ksrq and a.jsrq
            union all       
            select a.pfphdm,a.jsdm,sum((days(a.jsrq)-days(a.ksrq)+1)*a.hl_d) as yyfpl 
            from yyzy.t_yyzy_tmp_yysdtjb a,tmp b
            where (a.pfphdm,a.jsdm)=(b.pfphdm,b.jsdm)
            and a.ksrq>b.ksrq and a.jsrq<=b.jsrq
            group by a.pfphdm,a.jsdm
         ) as fpl
       group by pfphdm,jsdm     
       ),tmp3 as(
       select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,a.ksrq,a.jsrq,b.yyfpl,a.sdbh,a.zxsx,a.tdsx,a.kssyl,jssyl,a.zlyybj,a.zpfbj,a.bbrq
       from tmp a,tmp2 b
       where(a.pfphdm,a.jsdm)=(b.pfphdm,b.jsdm)    
       ),tmp4 as(
       --求结束使用量
       select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,
         case
             when v_hl_d>v_jssyl then
                  v_jsrq
             else
                  v_jsrq+1 day
         end as ksrq,a.jsrq,a.yyfpl-b.yyfpl as yyfpl,a.sdbh,
         case
             when v_hl_d>v_jssyl then
                  v_jssyl
             else
                  0.00
         end as zxsx,a.tdsx,
         case
             when v_hl_d-v_jssyl>0 then
                  v_hl_d-v_jssyl
             else
                  v_hl_d 
         end as kssyl,a.jssyl,a.zlyybj,a.zpfbj,a.bbrq 
       from yyzy.t_yyzy_tmp_zxpfb_whb a,tmp3 b
       where (a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx)=(b.pfphdm,b.jsdm,b.yydm,b.yynf,b.kclx)
       and a.jsrq>b.jsrq
       )
       select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,a.jsrq,a.yyfpl,a.sdbh,a.zxsx,a.tdsx,a.kssyl,jssyl,a.zlyybj,a.zpfbj,a.bbrq,a.kclx
       from tmp3 a
       union all
       select v_pfphdm,v_jsdm,v_yydm,v_yynf,v_ksrq,v_jsrq,v_sdl,v_sdbh,0.00 as zxsx,-1 as tdsx,v_kssyl,v_jssyl,v_zlyybj,v_zpfbj,current date as bbrq,v_kclx
       from sysibm.sysdummy1
       union all
       select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,a.jsrq,yyfpl,a.sdbh,a.zxsx,a.tdsx,a.kssyl,jssyl,a.zlyybj,a.zpfbj,a.bbrq,a.kclx
       from tmp4 a;  
       delete from yyzy.t_yyzy_tmp_zxpfb_whb
       where (pfphdm,jsdm)=(v_pfphdm,v_jsdm)
       and ksrq<v_ksrq and jsrq>v_jsrq;   
    set i_not_found=0; 
    end loop  loop_ml;
    close c1;
    --处理D_END_DATE+1 day 烟叶    
    --处理烟叶的醇化周期
    delete from yyzy.t_yyzy_zxpf_whb;
    insert into yyzy.t_yyzy_zxpf_whb(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,yyfpl,sdbh,zxsx,tdsx,kssyl,jssyl,zlyybj,zpfbj,fjchsx,fjchxx,bbrq,kclx)
    with chzq as (
      select yydm,yynf,fjchsx,fjchxx
      from (
        select yydm,yynf,fjchsx,fjchxx,
            rownumber()over(partition by yydm,yynf order by bbrq desc,bbh desc,FJCHSX DESC,FJCHXX) as xh
        from yyzy.t_yyzy_chzq
        where current date between ksrq and jsrq and zybj='1'
          and yydm<>'0' and yynf<>0
      ) as t
      where xh=1
    )
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,a.jsrq,a.yyfpl,a.sdbh,a.zxsx,tdsx,a.kssyl,a.jssyl,a.zlyybj,a.zpfbj,value(fjchsx,0),value(fjchxx,0),D_START_DATE,a.kclx
    from YYZY.T_YYZY_TMP_ZXPFB_WHB a
    left join chzq as b 
      on (a.yydm,a.yynf)=(b.yydm,b.yynf)
    ;      
  END;

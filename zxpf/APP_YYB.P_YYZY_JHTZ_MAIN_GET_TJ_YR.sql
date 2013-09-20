SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_TJ_YR (
    IN D_START_DATE	DATE,
    IN D_END_DATE	DATE )
  SPECIFIC SQL091204164114600
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
--开始存储过程
  BEGIN 
    /*定义变量*/
    declare S_SDFS CHARACTER(1); 
    declare S_yydm varchar(50); 
    declare flag,judge_flag integer default 1; 
    declare s_yynf,s_sysx,v_count,s_kclx,i_not_found integer;
    declare N_SDL decimal(10,2) default 0.00;
    declare S_KCJS,N_SYL,J_ZHL decimal(10,2);
    
    --定义静态游标c2 
    declare c2 cursor for 
      select yydm,yynf,yykcjs,sysx,kclx
      from YYZY.T_YYZY_TMP_YYKC_NEW
      order by yydm,yynf
    ;
    
    declare continue handler for not found 
    begin 
      set i_not_found = 1;
    end; 
    
    /*循环分配烟叶*/
    set flag=1;
    while flag=1 do 
      --更新zxsx
      update yyzy.t_yyzy_tmp_yyfpgzb_all
      set sysx=sysx-1 
      where (pfphdm,jsdm)in(
        select pfphdm,jsdm 
        from yyzy.t_yyzy_tmp_yyfpgzb_all 
        where (yydm,yynf,kclx) in(
          select yydm,yynf,kclx 
          from YYZY.T_YYZY_TMP_YYKC_NEW
        )
      ); 
      --删除耗尽的烟叶 
      delete from YYZY.T_YYZY_TMP_YYFPGZB_ALL where sysx=0; 
      --清空库存 
      delete from YYZY.T_YYZY_TMP_YYKC_NEW; 
      --取得库存量 
      call APP_YYB.P_YYZY_JHTZ_MAIN_GET_KC(judge_flag); 
      case 
        when judge_flag=0 then 
          --为了获得空白砖块 
          insert into yyzy.t_yyzy_tmp_yykc_new(yydm,yynf,yykcjs,sysx,kclx)
          values ('0',0,8888888.88,0,1); 
        else 
          set flag=1; 
      end case; 
      open c2; 
      loop_m2: loop 
        set i_not_found=0; 
        fetch c2 into S_yydm,S_yynf,S_kcjs,S_sysx,s_kclx;
        if i_not_found=1 then 
          leave loop_m2; 
        end if; 
        delete from yyzy.t_yyzy_tmp_yyjqb; 
        set N_SYL=S_kcjs; 
        while N_SYL>0.05 do 
          --取时间段 得到权比
          case 
            when N_SYL=9999999.99 then 
              insert into yyzy.t_yyzy_tmp_zxpfb_whb(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,yyfpl,sdbh,zxsx,kssyl,jssyl,zlyybj,zpfbj,kclx)
              with tmp as(
                select a.pfphdm,a.jsdm,a.yydm,a.yynf,max(b.jsrq) as ksrq,max(b.jsrq) as jsrq,-1.00 as yyfpl,a.sdbh,0.00 as kssyl,0.00 as jssyl,a.zlyybj,a.zpfbj,a.kclx
                from yyzy.t_yyzy_tmp_yyfpgzb_all a,yyzy.t_yyzy_tmp_zxpfb_whb b 
                where (a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx)=(b.pfphdm,b.jsdm,s_yydm,s_yynf,s_kclx)
                  and not ((b.yydm,b.yynf,b.kclx)=(s_yydm,s_yynf,s_kclx)) 
                group by a.pfphdm,a.jsdm,a.yydm,a.yynf,a.sdbh,a.zlyybj,a.zpfbj,a.kclx
              )
              ,tmp2 as( 
                select a.pfphdm,a.jsdm,sum(a.jssyl) as zxsx 
                from yyzy.t_yyzy_tmp_zxpfb_whb a,tmp b 
                where (a.pfphdm,a.jsdm,a.jsrq)=(b.pfphdm,b.jsdm,b.jsrq) 
                group by a.pfphdm,a.jsdm
              ) 
              select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.ksrq,a.jsrq,yyfpl,a.sdbh,b.zxsx,a.kssyl,a.jssyl,a.zlyybj,a.zpfbj,a.kclx
              from tmp a,tmp2 b 
              where(a.pfphdm,a.jsdm)=(b.pfphdm,b.jsdm)
              ; 
              set N_SYL=0.00; 
            else 
              set N_SDL=N_SYL; 
              select max(sdfs) into S_SDFS 
              from yyzy.t_yyzy_tmp_yyfpgzb_all
              where (yydm,yynf,kclx)=(s_yydm,s_yynf,s_kclx); 
              case 
                when S_SDFS>='0'then 
                  call APP_YYB.P_YYZY_JHTZ_MAIN_GET_PR(S_YYDM,S_YYNF,S_SYSX,s_kclx,D_END_DATE,S_SDFS,N_SDL); 
                  select count(1) into v_count from yyzy.t_yyzy_tmp_yyjqb; 
                  if v_count>0 then 
                    set N_SYL=N_SDL; 
                    select sum((days(jsrq)-days(ksrq)+1)*hl_d) into J_ZHL 
                    from yyzy.t_yyzy_tmp_yyjqb; 
                    
                    set N_SYL=N_SYL+(select value(sum(jssyl),0.00) from YYZY.T_YYZY_TMP_JSSYL);
                    --获得执行配方 
                    call APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF(N_SYL,J_ZHL,S_SDFS,D_END_DATE); 
                  else 
                    case 
                      when N_SDL=9999999.99 then 
                        set N_SYL=N_SDL; 
                      else 
                        set N_SYL=0.00; 
                    end case; 
                  end if; 
                else 
                  set N_SYL=0.00; 
            end case; 
          end case; 
        end while; 
        --更新zxsx
        if S_yydm='0' then 
          delete from yyzy.t_yyzy_tmp_yyfpgzb_all; 
          leave loop_m2; 
        else 
          set i_not_found=0; 
        end if; 
      end loop loop_m2; 
      close c2; 
      select count(1) into v_count from yyzy.t_yyzy_tmp_yyfpgzb_all; 
      if v_count<1 then 
        set flag=0; 
      else 
        set flag=judge_flag; 
      end if; 
    end while;
    --配方最后数据处理
    call APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF_PR(D_START_DATE,D_END_DATE);
    
END;

GRANT EXECUTE ON PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_TJ_YR( DATE, DATE ) TO USER DB2INST2 WITH GRANT OPTION;


SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD_ALL (
    IN D_START_DATE	DATE,
    IN D_END_DATE	DATE )
  SPECIFIC SQL091207134529600
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
    --定义存储变量 
    declare v_yydm varchar(50); 
    declare v_sdfs CHARACTER(1); 
    declare v_ksrq,v_jsrq date; 
    declare v_sdl,v_kcl decimal(10,2); 
    declare v_pfphdm,v_jsdm,v_yynf,v_kclx,v_sdbh,i_not_found,v_count integer;
    
    --定义静态游标c1
    declare c1 cursor for
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,a.sdbh,b.sdfs,b.sdl,b.sdksrq,b.sdjsrq
    from yyzy.t_yyzy_zxpf_whb as a,yyzy.t_yyzy_tmp_yysdb as b
    where a.sdbh=b.sdbh and a.yydm>'0'
      and a.jsrq>=D_START_DATE 
      and sdfs not in('2') 
    order by a.ksrq,a.jsrq; 
    
    declare continue handler for not found
    begin 
      set i_not_found = 1;
    end; 
    
    /*锁定脚本*/
    --确保yyzy.t_yyzy_tmp_yysdb表中的数据不变
    --确保锁定空白砖块锁定可以以有效的形式展示，而不与因为烟叶不足弥补的空白烟叶混淆
    delete from yyzy.t_yyzy_tmp_yysdb;
    insert into yyzy.t_yyzy_tmp_yysdb(pfphdm,jsdm,sdbh,yydm,yynf,kclx,sdl,sdksrq,sdjsrq,sdfs,zlyybj,zpfbj) 
    select b.pfphdm,b.jsdm,A.sdbh,B.YYDM,B.YYNF,b.kclx, 
        A.sdl,A.sdksrq,A.sdjsrq,A.sdfs,b.zlyybj,b.zpfbj 
    from yyzy.t_yyzy_yysdb as A,yyzy.t_yyzy_tmp_yyfpgzb_all as B 
    WHERE A.SDBH=B.SDBH 
    and yydm<>'0'
    group by b.pfphdm,b.jsdm,A.sdbh,B.YYDM,B.YYNF,b.kclx, 
        A.sdl,A.sdksrq,A.sdjsrq,A.sdfs,b.zlyybj,b.zpfbj 
    union all 
    --空白烟叶的特殊处理
    select b.pfphdm,b.jsdm,A.sdbh,'-'||char(rownumber()over()),
        0,b.kclx,A.sdl,A.sdksrq,A.sdjsrq,A.sdfs,b.zlyybj,b.zpfbj
    from yyzy.t_yyzy_yysdb as A,yyzy.t_yyzy_tmp_yyfpgzb_all as B 
    WHERE A.SDBH=B.SDBH and yydm='0' 
    group by b.pfphdm,b.jsdm,A.sdbh,A.sdl,b.kclx, 
        A.sdksrq,A.sdjsrq,A.sdfs,b.zlyybj,b.zpfbj 
    ;
    
    --更新空白砖块量锁定烟叶代码
    update yyzy.t_yyzy_tmp_yyfpgzb_all e 
    set yydm=( 
      select yydm 
      from yyzy.t_yyzy_tmp_yysdb m 
      where yydm<'0' 
        and e.sdbh=m.sdbh 
    ) 
    where e.sdbh in( 
      select sdbh 
      from yyzy.t_yyzy_tmp_yysdb 
      where yydm<'0' and sdfs='2' 
    ); 
    
    --处理锁定量 
    set i_not_found=0; 
    open c1;
      loop_ml: loop
        fetch c1 into v_pfphdm,v_jsdm,v_yydm,v_yynf,v_kclx,v_sdbh,v_sdfs,v_sdl,v_ksrq,v_jsrq;
        if i_not_found=1 then 
           leave loop_ml; 
        end if; 
        --得到相应的库存量
        select yykcjs,count(1) into v_kcl,v_count 
        from YYZY.T_YYZY_TMP_YYKC 
        where (yydm,yynf,kclx)=(v_yydm,v_yynf,v_kclx)
        group by yykcjs;
        case 
          when v_count>0 then 
            case 
              --结束时间锁定
              when v_sdfs='1' then 
                case --结束时间锁定结束,锁定的烟叶按正常的烟叶分配
                  when v_jsrq<D_START_DATE then 
                    update yyzy.t_yyzy_tmp_yyfpgzb_all 
                    set (sdfs,sdbh)=('0',0) 
                    where sdbh=v_sdbh;
                  else 
                    set i_not_found=0;
                end case; 
              --结束时间锁定
              --开始时间+结束时间锁定 
              when v_sdfs='3' then 
                CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD3(
                    V_PFPHDM,V_JSDM,V_YYDM,V_YYNF,V_KCLX,
                    V_KSRQ,V_JSRQ,V_SDBH,V_KCL,V_SDFS,
                    D_START_DATE,D_END_DATE
                  );
                --开始时间锁定，需更新时间为当前天，如果在zxpf中找不到这个记录，需要清空
              when v_sdfs='4' then 
                case 
                  when v_ksrq<D_START_DATE then 
                    update yyzy.t_yyzy_tmp_yysdb 
                    set sdksrq=D_START_DATE
                    where sdbh=v_sdbh;
                  when  v_ksrq>D_END_DATE then 
                    delete from yyzy.t_yyzy_tmp_yysdb 
                    where sdbh=v_sdbh;
                  else 
                   set i_not_found=0;
                end case;
                --开始时间+量锁定
              when v_sdfs='5' then 
                CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD5(
                    V_PFPHDM,V_JSDM,V_YYDM,V_YYNF,V_KCLX,
                    V_KSRQ,V_SDBH,V_SDL,V_KCL,V_SDFS,
                    D_START_DATE,D_END_DATE
                  );
              else 
                set i_not_found=0;
            end case;
          else 
            delete from yyzy.t_yyzy_tmp_yysdb 
             where (sdbh,sdfs)=(v_sdbh,v_sdfs); 
        end case; 
        set i_not_found=0; 
      end loop  loop_ml;
    close c1;
    
    --更新kssyl 
    update yyzy.t_yyzy_tmp_yysdb m 
    set kssyl=(
      case 
        when sdksrq<=sdjsrq then ( 
            select hl_d 
            from yyzy.t_yyzy_tmp_yyxhtjb as e 
            where (m.pfphdm,m.jsdm)=(e.pfphdm,e.jsdm) 
              and m.sdksrq between e.ksrq and e.jsrq 
            group by hl_d 
          ) 
        else 
          jssyl 
      end) 
    where sdfs in('3','5') 
    ; 
    
    --处理TJB
    call APP_YYB.P_YYZY_JHTZ_MAIN_GET_TJ_PR();
    
    delete from yyzy.t_yyzy_tmp_yyfpgzb_all
    where sdfs in('3','5'); 
    
    --增加空白砖块量锁定库存
    insert into YYZY.T_YYZY_TMP_YYKC(yydm,yynf,yykcjs,kclx)
    select yydm,yynf,sdl,kclx 
    from yyzy.t_yyzy_tmp_yysdb 
    where sdfs='2' and yydm<'0'; 
    
    delete from YYZY.T_YYZY_TMP_YYFPGZB_ALL a 
    where (a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,a.sdbh,a.sysx)in(
      select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,a.sdbh,min(a.sysx)
      from YYZY.T_YYZY_TMP_YYFPGZB_ALL a 
      group by a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,a.sdbh
      having(count(*))>1
    );
    
    --更新SYSX
    update YYZY.T_YYZY_TMP_YYFPGZB_ALL a 
    set(a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,a.sdbh,a.sysx)=(
      select b.pfphdm,b.jsdm,b.yydm,b.yynf,b.kclx,b.sdbh,b.sysx 
      from (
        select b.pfphdm,b.jsdm,b.yydm,b.yynf,b.kclx,b.sdbh,
            rownumber()over(partition by pfphdm,jsdm order by sysx) as sysx
        from (
          select b.pfphdm,b.jsdm,b.yydm,b.yynf,b.kclx,b.sdbh,max(b.sysx) as sysx 
          from YYZY.T_YYZY_TMP_YYFPGZB_ALL b
          group by b.pfphdm,b.jsdm,b.yydm,b.yynf,b.sdbh,b.kclx
        ) b 
      ) b
      where (a.pfphdm,a.jsdm,a.yydm,a.yynf,a.kclx,a.sdbh)=
          (b.pfphdm,b.jsdm,b.yydm,b.yynf,b.kclx,b.sdbh) 
    ); 
  END;

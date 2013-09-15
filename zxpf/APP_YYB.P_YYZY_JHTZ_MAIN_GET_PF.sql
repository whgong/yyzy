
drop PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF;
SET SCHEMA ETLUSR;

SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF
(
  INOUT N_SYL DECIMAL(10, 2), 
  IN J_ZHL DECIMAL(10, 2), 
  IN S_SDFS CHARACTER(1), 
  IN D_END_DATE DATE
) 
  SPECIFIC APP_YYB.SQL090923132133600
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
  
  begin 
    declare e_rq,lc_d_ksrq date;
    declare lc_n_hld, lc_n_jssyl decimal(18,6);
    declare tflg integer;
--    declare debug_yydm varchar(20);
--    declare debug_ksrq,debug_jsrq date;
    
--    set debug_yydm = (select yydm from yyzy.t_yyzy_tmp_yyjqb fetch first 1 row only);
--    set debug_ksrq = (select ksrq from yyzy.t_yyzy_tmp_yyjqb fetch first 1 row only);
--    set debug_jsrq = (select jsrq from yyzy.t_yyzy_tmp_yyjqb fetch first 1 row only);
--    insert into debug.t_debug_JHTZ_MAIN_GET_PF(N_SYL,J_ZHL,S_SDFS,D_END_DATE,yydm,ksrq,jsrq)
--    values (
--        N_SYL,J_ZHL,S_SDFS,D_END_DATE,
--        debug_yydm, debug_ksrq, debug_jsrq
--      )
--    ;
--    insert into debug.t_debug_yyjqb(
--      PFPHDM, JSDM, YYDM, YYNF, ZLYYBJ, ZPFBJ, KSRQ, 
--      JSRQ, SDFS, SDBH, HL_D, JQ_QZ, KCLX
--    )
--    select PFPHDM, JSDM, YYDM, YYNF, ZLYYBJ, ZPFBJ, KSRQ, JSRQ, 
--      SDFS, SDBH, HL_D, JQ_QZ, KCLX
--    from YYZY.T_YYZY_TMP_YYJQB
--    ;
--    
--    insert into debug.t_debug_JSSYL(PFPHDM, JSDM, JSSYL, JSRQ)
--    select PFPHDM, JSDM, JSSYL, JSRQ
--    from YYZY.T_YYZY_TMP_JSSYL
--    ;
    
    case
      when N_SYL>=J_ZHL then --当库存烟叶足够时
        merge into yyzy.t_yyzy_tmp_zxpfb_whb as e
          using(
              select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,a.ksrq,a.jsrq,
                case when a.ksrq=b.jsrq and a.jsrq=b.jsrq then a.hl_d-VALUE(B.JSSYL,0.00) else a.hl_d end as jssyl,
                a.hl_d,(hl_d-VALUE(B.JSSYL,0.00)) as kssyl,a.kclx,
                ((days(a.jsrq)-days(a.ksrq)+1)*a.hl_d-VALUE(B.JSSYL,0.00)) as hl_l,a.sdbh
              from yyzy.t_yyzy_tmp_yyjqb a
                left join YYZY.T_YYZY_TMP_JSSYL b
                  on(a.pfphdm,a.jsdm,a.ksrq)=(b.pfphdm,b.jsdm,b.jsrq)
            ) as m  
            on (e.pfphdm,e.jsdm,e.yydm,e.yynf,e.kclx,e.sdbh,e.jsrq)=(m.pfphdm,m.jsdm,m.yydm,m.yynf,m.kclx,m.sdbh,m.ksrq-1 day)
          when matched then
            update set (e.yyfpl,e.jsrq,jssyl) = (e.yyfpl+hl_l,m.jsrq,m.hl_d)
          when not matched then
            insert (pfphdm,jsdm,yydm,yynf,ksrq,jsrq,yyfpl,sdbh,zxsx,kssyl,jssyl,zlyybj,zpfbj,kclx)
            values (m.pfphdm,m.jsdm,m.yydm,m.yynf,m.ksrq,m.jsrq,m.hl_l,m.sdbh,
              value((select sum(jssyl) from yyzy.t_yyzy_tmp_zxpfb_whb where (pfphdm,jsdm,jsrq)=(m.pfphdm,m.jsdm,m.ksrq)),0.00),
              m.kssyl,m.jssyl,m.zlyybj,m.zpfbj,m.kclx
            )
        ; 
        --更新结束日期.
        select min(jsrq) into e_rq from yyzy.t_yyzy_tmp_yyjqb;

        delete from yyzy.t_yyzy_tmp_yyxhtjb 
        where (pfphdm,jsdm,jsrq)in(select pfphdm,jsdm,jsrq from yyzy.t_yyzy_tmp_yyjqb group by pfphdm,jsdm,jsrq);

        update yyzy.t_yyzy_tmp_yyxhtjb
        set ksrq=e_rq + 1 day
        where (pfphdm,jsdm) in(select pfphdm,jsdm from yyzy.t_yyzy_tmp_yyjqb group by pfphdm,jsdm)
          and ksrq<=e_rq and jsrq>e_rq
        ;
        
        
        
        --得到烟叶的剩余量
        case
          when e_rq=D_END_DATE then 
            case 
              when S_SDFS='1'and(N_SYL-J_ZHL<1) then
                set N_SYL=9999999.99;
              when S_SDFS='1'and (N_SYL-J_ZHL>=1) then
                set N_SYL=N_SYL-J_ZHL;
              else
                set N_SYL=0.00; 
            end case;
          else
            set N_SYL=N_SYL-J_ZHL;
        end case;
        delete from yyzy.t_yyzy_tmp_yyjqb;
      else --库存不够
        --取可支撑的结束时间 
        select min(ksrq),sum(hl_d), sum(value(jssyl,0)) into lc_d_ksrq,lc_n_hld,lc_n_jssyl
        from yyzy.t_yyzy_tmp_yyjqb as m
          left join YYZY.T_YYZY_TMP_JSSYL as j
            on m.pfphdm = j.pfphdm and m.jsdm = j.jsdm and j.jsrq = m.ksrq
        ;
        
        set N_SYL = N_SYL - lc_n_jssyl; --退回外层特殊处理而增加的数量
        
        set e_rq  = NULL;
        set e_rq = lc_d_ksrq + int((N_SYL-(lc_n_hld - lc_n_jssyl))/lc_n_hld) day;
        
--                    if debug_yydm='09KY21189120' then
--                      insert into DEBUG.T_DEBUG_MSG(msg)
--                      values('e_rq='||char(e_rq))
--                      ;
--                    end if;
        
/*      
        select min(ksrq+ceil(jq_qz*N_SYL/hl_d) day - 1 day) into e_rq 
        from yyzy.t_yyzy_tmp_yyjqb 
        where hl_d>0.00;
*/
        set tflg = 0; --判断能否支撑1天
        if lc_n_hld - lc_n_jssyl >= N_SYL then 
          set tflg=1;
        end if;

        if tflg = 0 then
          merge into yyzy.t_yyzy_tmp_zxpfb_whb as e
            using (
                select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,
                  a.ksrq,e_rq as jsrq,a.hl_d,a.sdbh, 
                  (days(e_rq)-days(ksrq)+1)*hl_d - value(b.jssyl,0.00) as yyfpl,
  --                (a.jq_qz*N_SYL-value(b.jssyl,0.00)) as yyfpl,
                  case 
                    when a.hl_d>=a.jq_qz*N_SYL then (days(e_rq)-days(ksrq)+1)*hl_d - value(b.jssyl,0.00)--a.jq_qz*N_SYL-value(b.jssyl,0.00)
                    else a.hl_d-value(b.jssyl,0.00) 
                  end as kssyl,
                  hl_d as jssyl,
--                  case 
--                    when a.hl_d>=a.jq_qz*N_SYL then (days(e_rq)-days(ksrq))*hl_d - value(b.jssyl,0.00)--a.jq_qz*N_SYL-value(b.jssyl,0.00)
--                    else (a.jq_qz*N_SYL-(days(e_rq)-days(a.ksrq))*a.hl_d)
--                  end as jssyl,
                  a.kclx
                from yyzy.t_yyzy_tmp_yyjqb a
                left join YYZY.T_YYZY_TMP_JSSYL b
                  on(a.pfphdm,a.jsdm,a.ksrq)=(b.pfphdm,b.jsdm,b.jsrq)
              ) as m
                on (e.pfphdm,e.jsdm,e.yydm,e.yynf,e.kclx,e.sdbh,e.jsrq)=(m.pfphdm,m.jsdm,m.yydm,m.yynf,m.kclx,m.sdbh,m.ksrq-1 day)
            when matched then 
              update set (e.yyfpl,e.jsrq,jssyl) = (e.yyfpl+m.yyfpl,m.jsrq,m.jssyl)
            when not matched then 
              insert (pfphdm,jsdm,yydm,yynf,ksrq,jsrq,yyfpl,sdbh,zxsx,kssyl,jssyl,zlyybj,zpfbj,kclx)
              values (m.pfphdm,m.jsdm,m.yydm,m.yynf,m.ksrq,m.jsrq,m.yyfpl,m.sdbh,
                    value((select sum(jssyl) from yyzy.t_yyzy_tmp_zxpfb_whb where (pfphdm,jsdm,jsrq)=(m.pfphdm,m.jsdm,m.ksrq)),0.00),
                    m.kssyl,m.jssyl,m.zlyybj,m.zpfbj,m.kclx)
          ;

          set N_SYL = N_SYL 
                  + value((select sum(jssyl) from YYZY.T_YYZY_TMP_JSSYL),0) 
                  - value((select sum((days(e_rq) - days(ksrq)+1)*hl_d) from yyzy.t_yyzy_tmp_yyjqb where hl_d>0.00),0);
          update YYZY.T_YYZY_TMP_JSSYL set jssyl = 0;

--          if debug_yydm='09KY21189120' then
--            insert into DEBUG.T_DEBUG_MSG(msg)
--            values('fzpd-tflg;e_rq='||char(e_rq)||';N_SYL='||char(n_syl))
--            ;
--          end if;
        end if;
        /*处理整数包*/
        lp1:
        for v1 as 
            select m.PFPHDM, m.JSDM, m.YYDM, m.YYNF, m.ZLYYBJ, 
              m.ZPFBJ, m.KSRQ, m.JSRQ, m.SDFS, m.SDBH, m.HL_D, 
              m.JQ_QZ, m.KCLX, value(b.jssyl,0) as jssyl 
            from yyzy.t_yyzy_tmp_yyjqb as m
              left join YYZY.T_YYZY_TMP_JSSYL b
                on(m.pfphdm,m.jsdm,m.ksrq)=(b.pfphdm,b.jsdm,b.jsrq)
            order by hl_d desc, pfphdm desc, jsdm desc
          do 
            if n_syl>v1.hl_d - v1.jssyl then
              merge into yyzy.t_yyzy_tmp_zxpfb_whb as t
                using (
                  values(v1.pfphdm, v1.jsdm, v1.yydm, v1.yynf, v1.kclx, v1.hl_d, v1.ksrq, v1.jsrq)
                ) as s(pfphdm, jsdm, yydm, yynf, kclx, hl_d,ksrq,jsrq) 
                  on (t.pfphdm, t.jsdm, t.yydm, t.yynf, t.kclx) = (s.pfphdm, s.jsdm, s.yydm, s.yynf, s.kclx)
                when matched then
                  update set yyfpl = yyfpl+v1.hl_d - v1.jssyl, jssyl = v1.hl_d - v1.jssyl, 
                            jsrq = jsrq+1 day --(case when tflg = 1 then jsrq else jsrq+1 day end) 
                when not matched then
                  insert(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,yyfpl,sdbh,zxsx,kssyl,jssyl,zlyybj,zpfbj,kclx)
                  values(
                    v1.PFPHDM, v1.JSDM, v1.YYDM, v1.YYNF, 
                    case when tflg = 1 then e_rq else e_rq + 1 day end, 
                    case when tflg = 1 then e_rq else e_rq + 1 day end, 
                    v1.hl_d - v1.jssyl, v1.SDBH,
                    value((select sum(jssyl) from yyzy.t_yyzy_tmp_zxpfb_whb where (pfphdm,jsdm,jsrq)=(v1.pfphdm,v1.jsdm,e_rq)),0),
                    v1.hl_d - v1.jssyl,v1.hl_d - v1.jssyl,
                    v1.ZLYYBJ, v1.ZPFBJ, v1.KCLX
                  )
              ;
              set N_SYL = N_SYL - (v1.hl_d - v1.jssyl);
--              update yyzy.t_yyzy_tmp_zxpfb_whb 
--              set yyfpl = yyfpl+v2.hl_d, jssyl = 0, jsrq = jsrq+1 day
--              where (pfphdm, jsdm, yydm, yynf, kclx) = (v1.pfphdm, v1.jsdm, v1.yydm, v1.yynf, v1.kclx)
--              ;
--                    if debug_yydm='09KY21189120' then
--                      insert into DEBUG.T_DEBUG_MSG(msg)
--                      values('fzpd-lp1-1;N_SYL='||char(n_syl))
--                      ;
--                    end if;
            else --不够分配
              merge into yyzy.t_yyzy_tmp_zxpfb_whb as t
                using (
                  values(v1.pfphdm, v1.jsdm, v1.yydm, v1.yynf, v1.kclx, v1.hl_d, v1.ksrq, v1.jsrq)
                ) as s(pfphdm, jsdm, yydm, yynf, kclx, hl_d,ksrq,jsrq) 
                  on (t.pfphdm, t.jsdm, t.yydm, t.yynf, t.kclx) = (s.pfphdm, s.jsdm, s.yydm, s.yynf, s.kclx)
                when matched then
                  update set yyfpl = yyfpl+int(N_SYL), jssyl = int(N_SYL), 
                            jsrq = jsrq+1 day--(case when tflg = 1 then jsrq else jsrq+1 day end) 
                when not matched then
                  insert(pfphdm,jsdm,yydm,yynf,ksrq,jsrq,yyfpl,sdbh,zxsx,kssyl,jssyl,zlyybj,zpfbj,kclx)
                  values(
                    v1.PFPHDM, v1.JSDM, v1.YYDM, v1.YYNF, 
                    case when tflg = 1 then e_rq else e_rq + 1 day end, 
                    case when tflg = 1 then e_rq else e_rq + 1 day end, 
                    int(N_SYL), v1.SDBH,
                    value((select sum(jssyl) from yyzy.t_yyzy_tmp_zxpfb_whb where (pfphdm,jsdm,jsrq)=(v1.pfphdm,v1.jsdm,e_rq)),0),
                    int(N_SYL),int(N_SYL), v1.ZLYYBJ, v1.ZPFBJ, v1.KCLX
                  )
              ;
--              update yyzy.t_yyzy_tmp_zxpfb_whb 
--              set yyfpl = yyfpl+v1.hl_d, jssyl = int(n_syl)
--              where (pfphdm, jsdm, yydm, yynf, kclx) = (v1.pfphdm, v1.jsdm, v1.yydm, v1.yynf, v1.kclx)
--              ;
--                    if debug_yydm='09KY21189120' then
--                      insert into DEBUG.T_DEBUG_MSG(msg)
--                      values('fzpd-lp1-2;N_SYL='||char(n_syl))
--                      ;
--                    end if;
              leave lp1;
            end if;
            
        end for lp1;
/*        
        update yyzy.t_yyzy_tmp_yyxhtjb 
        set ksrq=e_rq
        where (pfphdm,jsdm) in(select pfphdm,jsdm from yyzy.t_yyzy_tmp_yyjqb group by pfphdm,jsdm)
          and e_rq>ksrq and jsrq>=e_rq
        ;
*/

--                    if debug_yydm='09KY21189120' then
--                      insert into debug.T_debug_TMP_ZXPFB_WHB1
--                      select * from yyzy.t_yyzy_tmp_zxpfb_whb
--                      where pfphdm = 8 and jsdm = 6
--                      ;
--                    end if;

        update yyzy.t_yyzy_tmp_yyxhtjb as t
        set ksrq = (
                    select 
                      case 
                        when (m.ksrq<=c.jsrq and c.ksrq<=m.jsrq) and m.jssyl = c.hl_d then m.jsrq + 1 day 
                        when not(m.ksrq<=c.jsrq and c.ksrq<=m.jsrq) then c.ksrq
                        else m.jsrq 
                      end
                    from yyzy.t_yyzy_tmp_zxpfb_whb as m
                      inner join yyzy.t_yyzy_tmp_yyjqb as c
                        on m.pfphdm = c.pfphdm and m.jsdm = c.jsdm
                        --and (m.ksrq<=c.jsrq and c.ksrq<=m.jsrq)
                    where m.pfphdm = t.pfphdm and m.jsdm = t.jsdm 
                    order by m.jsrq desc 
                    fetch first 1 row only
                  )
        where (t.pfphdm, t.jsdm, t.ksrq)in (
            select pfphdm, jsdm, min(ksrq) as ksrq
            from yyzy.t_yyzy_tmp_yyxhtjb 
            group by pfphdm, jsdm
          )
          and (t.pfphdm, t.jsdm)in (
            select pfphdm, jsdm
            from yyzy.t_yyzy_tmp_yyjqb
          )
        ;
        delete from yyzy.t_yyzy_tmp_yyxhtjb where jsrq<ksrq;
        
        set N_SYL=9999999.99;
        update yyzy.t_yyzy_tmp_yyfpgzb_all
        set sysx=sysx-1
        where (pfphdm,jsdm)in(
            select pfphdm,jsdm 
            from yyzy.t_yyzy_tmp_yyjqb 
            group by pfphdm,jsdm
          )
        ;
        delete from yyzy.t_yyzy_tmp_yyfpgzb_all
        where sysx=0;
    end case; 
  end;

GRANT EXECUTE ON PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_PF
 (DECIMAL(10, 2), 
  DECIMAL(10, 2), 
  CHARACTER(1), 
  DATE
 ) 
  TO USER DB2INST2 WITH GRANT OPTION;


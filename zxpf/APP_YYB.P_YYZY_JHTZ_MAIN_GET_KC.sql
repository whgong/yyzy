SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_KC ( OUT JUDGE_FLAG INTEGER )
  SPECIFIC SQL091215171148900
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
begin 
    declare global temporary table temp_kc(
      yydm varchar(36),
      yynf integer,
      YYKCJS decimal(10,2),
      sysx integer,
      kclx integer
    )with replace on commit preserve rows NOT LOGGED;
    
    insert into session.temp_kc(yydm,yynf,yykcjs,sysx,kclx)
    select a.yydm,a.yynf,
        value(b.yykcjs,9999999.99),a.sysx,a.kclx 
    from ( 
      select sysx,yydm,yynf,kclx 
      from yyzy.t_yyzy_tmp_yyfpgzb_all 
      group by sysx,yydm,yynf,kclx
    ) a 
    left join yyzy.t_yyzy_tmp_yykc b
      on (a.yydm,a.yynf,a.kclx)=(b.yydm,b.yynf,b.kclx)
    where a.yydm<>'0';
    
    insert into yyzy.t_yyzy_tmp_yykc_new(yydm,yynf,yykcjs,sysx,kclx)
    with tmp_count as(select yydm,yynf,kclx,count(1) as count
       from session.temp_kc
      group by yydm,yynf,kclx)
      select a.yydm,a.yynf,a.yykcjs,a.sysx,a.kclx from session.temp_kc a
      where (a.yydm,a.yynf,a.kclx) in (select yydm,yynf,kclx from tmp_count b where count=1)
      and a.sysx=1; 
    
    case 
        when (select count(1) from yyzy.t_yyzy_tmp_yykc_new)>0  then
          set JUDGE_FLAG=1;
        else 
            set JUDGE_FLAG=0; 
    end case;
 end;

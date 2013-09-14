
DROP SPECIFIC PROCEDURE "APP_YYB"."SQL130520170855400";

SET SCHEMA KSUSR   ;

SET CURRENT PATH = "SYSIBM","SYSFUN","SYSPROC","SYSIBMADM","KSUSR";

CREATE PROCEDURE "APP_YYB"."P_YYZY_JHJZ_R"
 (IN "START_DATE" DATE, 
  OUT "MSG" VARCHAR(1000)
 ) 
  SPECIFIC "APP_YYB"."SQL130520170855400"
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
  --�洢����   
begin  
  /*����*/ 
  --����ϵͳ���� 1
  DECLARE SQLSTATE CHAR(5);  
  DECLARE SQLCODE INTEGER; 
  --������� 
  declare min_yr,max_yr,yr_2,yr_1,mn_1,mn_2,v_count,i_not_found integer; 
  declare v_rq,v_ksrq,v_jsrq,v_jsrq_2,judge_time date; 
  declare v_jhnf integer;
  declare err_msg varchar(1000) default ''; 
  declare new_ksrq date; 
  declare new_jsrq date;
  
  
  --�������
  declare EXE_SQL           varchar(20000);     --��̬SQL
  declare v_gengpeng_jm     varchar(8000); 	    --�������
  declare v_gengpeng_gs     varchar(8000);		--������㹫ʽ
  declare v_gengpeng_cx     varchar(8000);		--��������ѯ
  declare v_gengpeng_jm_p   varchar(8000); 	    --�������_��
  declare v_gengpeng_gs_p   varchar(8000);		--������㹫ʽ_��
  declare v_gengpeng_cx_p   varchar(8000);		--��������ѯ_��
  
    
  --������α�
  DECLARE c0 CURSOR for s0;
  
  
  --���徲̬�α� 
  declare c1 cursor for 
    select rq 
    from 
      (
        select RIQI as rq
        from DIM.T_DIM_YYZY_DATE
        where riqi between START_DATE and judge_time- 1 day
        union ALL
        select ksrq as rq
        from yyzy.t_yyzy_tmp_rscjhb_whb
        group by ksrq
        union 
        select jsrq as rq
        from yyzy.t_yyzy_tmp_rscjhb_whb
        group by jsrq
      ) date
    order by rq;
    
  declare c2 cursor for 
    select ksrq,min(jsrq) from yyzy.t_yyzy_tmp_rscjhb_whb 
    where ksrq>=judge_time 
    group by ksrq 
    order by ksrq;
      
  declare c3 cursor for
    select distinct jhnf
    from YYZY.T_YYZY_SCJH_ZZL
    where 1=1
      and jhnf>=year(new_ksrq) 
      and jhnf<=year(new_jsrq) 
    order by jhnf
  ;

  declare continue handler for not found set i_not_found = 1; 
  
  --�����쳣���� 
  declare exit handler for sqlexception 
  begin 
    set MSG = 'ϵͳ����sqlcode='||rtrim(char(sqlcode))||',sqlstate='||sqlstate||';  '; 
  end; 
  
  --������ʱ��
  declare global temporary table tmp2( 
    pfphdm integer,
    jhrq date,
    jhcl_avg decimal(18,2),
    jhpc_avg decimal(18,2)
  ) with replace on commit preserve rows not logged; 

  declare global temporary table tmp3 like yyzy.t_yyzy_tmp_rscjhb_whb 
  with replace on commit preserve rows not logged;
  
  /*����*/ 
  --set new_ksrq = START_DATE - (dayofyear(START_DATE)-1) day + 3 year;
	SELECT DATE(RTRIM(CHAR(MAX(JHNF)+1))||'-01-01') INTO NEW_KSRQ 
  FROM YYZY.T_YYZY_SNSCJH_ZXB
  ;
  set new_jsrq = START_DATE - (dayofyear(START_DATE)) day + 10 year;
    
    delete from yyzy.t_yyzy_tmp_rscjhb_whb; 
    delete from yyzy.t_yyzy_rscjhb_whb;  
    delete from session.tmp2;
    delete from session.tmp3;
	
	--���� YYZY.T_YYZY_SNSCJH_ZXB ������ƻ���������ݣ�������̱��� 20110630
	INSERT INTO YYZY.T_YYZY_SNSCJH_ZXB_SC(PFPHDM, PZMC, SCCJ, JHNF, JHCL, BBH, SCSJ)
	SELECT PFPHDM, PZMC, SCCJ, JHNF, JHCL, BBH,CURRENT TIMESTAMP AS SCSJ
		   FROM YYZY.T_YYZY_SNSCJH_ZXB WHERE JHNF>=YEAR(CURRENT DATE)+3;
	DELETE FROM YYZY.T_YYZY_SNSCJH_ZXB WHERE JHNF>=YEAR(CURRENT DATE)+3;
    
  --д���µ��յļƻ� 
  insert into session.tmp2(pfphdm,jhrq,jhcl_avg) 
  with scjh as
  ( 
    select date(substr(a.c_date, 1, 4)||'-'||substr(a.c_date, 5, 2)||'-'||substr(a.c_date, 7, 2)) as jhrq,
      a.quantity as jhcl,a.regionid,a.tobaccoid,c.factoryid,
      substr(a.tobaccoid,17,2) as yhbs,substr(d.factoryid, 13, 1) as cjdm
      from hds_cxqj.n_cxqj_o_prodplanhist_n as a 
      left join hds_cxqj.n_cxqj_o_regioninfo b 
      on a.regionid = b.regionid 
      left join hds_cxqj.n_cxqj_o_workshopinfo c 
      on b.workshopid = c.workshopid 
      left join hds_cxqj.n_cxqj_o_factoryinfo d 
      on c.factoryid = d.factoryid
    where (d_createtime,c_date) in(
        select MAX(d_createtime),c_date 
        from hds_cxqj.n_cxqj_o_prodplanhist_n 
        where int(c_date)>=year(START_DATE)*10000+month(START_DATE)*100+day(START_DATE) 
        group by c_date 
      ) 
  ) 
    select f.pfphdm,jhrq, sum(a.jhcl) as jhcl_avg
    from scjh as a 
	  left join (
    /*
      2012-06-07������޸ģ��䷽�ϲ��߼�
      ��YYZY.T_YYZY_YS_PH27��������ӳ����򣬿��Ժϲ��䷽
    */
	    select yhbs_src, cjdm_src, YHBS, CJDM 
      from YYZY.T_YYZY_YS_PH27  
      where zybj='1'
        and yhbs_src=yhbs
	    and cjdm_src<>cjdm	  
        and (yhbs_src, cjdm_src,bbrq)in(
										select yhbs_src, cjdm_src,max(bbrq)
										  from YYZY.T_YYZY_YS_PH27
                                         where yhbs_src=yhbs
	                                       and cjdm_src<>cjdm
                                      group by yhbs_src, cjdm_src
                                        )
	  ) as ys on (ys.yhbs_src, ys.cjdm_src)=(a.yhbs,a.cjdm)
    left join (
      select pfphdm,yhbs,sccjdm 
	    from dim.t_dim_yyzy_pfph 
      where jsrq>current date and ksrq <current date 
    ) as f 
      on value(ys.yhbs,a.yhbs) = f.yhbs 
      and cast(value(ys.cjdm,a.cjdm) as integer) = f.sccjdm 
      and f.sccjdm in (0,1,2) 
    group by jhrq,f.pfphdm
    ;

    --���⴦�����µ���1��1�յļƻ�Ϊ0 
    update session.tmp2 
    set jhcl_avg=0.00
    where jhrq=start_date - dayofyear(start_date) day + 1 day;

	
	
    --��ÿ�ռƻ����ж�����(������ڵ��¸��µ�1��) 
    SELECT max(jhrq)+ 1 month- day(max(jhrq)+ 1 month) day+1 day into judge_time 
    from session.tmp2; 
    
    set yr_1=year(judge_time-1 day); 		--yr_1 �µ��ռƻ������� 
    select min(jhnf) into yr_2 				--yr_2 ��ƻ���� 
    from YYZY.T_YYZY_SNSCJH_ZXB 
    where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB)
	  AND jhnf>=(select year(max(jhrq)) from session.tmp2);
    
    set mn_1=month(judge_time-1 day); 		--mn_1�µ��ռƻ�����·� 
    select value(max(jhyf),0) into mn_2 				--mn_2�����¼ƻ�����·� 
    from YYZY.T_YYZY_JSCJH 
    where (jhnf,jhyf,bbh)in( 
      select jhnf,jhyf,max(bbh) 
      from YYZY.T_YYZY_JSCJH 
      group by jhnf,jhyf 
    ) and jhnf=(select year(max(jhrq)) from session.tmp2)
    ; 
	
	
	
    if yr_1=yr_2 then --��ƻ����¼ƻ�Ϊͬһ�� 
      if mn_1<12 then --�¼ƻ�Ϊ������ĩ,��Ҫ������ƻ�ʣ���� 
        if mn_1<mn_2 then --�¼ƻ����²��������ȼƻ�����Ҫ���㼾��ʣ��ƻ� 
          --����ʣ��ƻ�
          insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg) 
          with jdjh_tmp as (
            select pfphdm,date(rtrim(char(jhnf))||'-'||rtrim(char(jhyf))||'-01') as ksrq, 
                date(rtrim(char(jhnf))||'-'||rtrim(char(jhyf))||'-01')+1 month-1 day as jsrq, 
                sum(JHCL) as jhcl 
            from YYZY.T_YYZY_JSCJH as a 
    /*
      2012-06-07������޸ģ��䷽�ϲ��߼�
      ��YYZY.T_YYZY_YS_PH27��������ӳ����򣬿��Ժϲ��䷽
	  
      2013-04-07�޸ģ��������Һϲ����ƺŲ��ϲ�	
	*/
            left join (
              select yhbs_src, cjdm_src, YHBS, CJDM 
              from YYZY.T_YYZY_YS_PH27  
              where zybj='1' 
				and yhbs_src=yhbs
	            and cjdm_src<>cjdm
                and (yhbs_src, cjdm_src,bbrq)in(
												select yhbs_src, cjdm_src,max(bbrq)
									 			  from YYZY.T_YYZY_YS_PH27
			                                     where yhbs_src=yhbs 
	                                               and cjdm_src<>cjdm
                                                 group by yhbs_src, cjdm_src
                                                )
            ) as py on a.yhbs = py.yhbs_src and a.cjdm = int(py.cjdm_src)
            left join DIM.T_DIM_YYZY_PFPH as b 
              on value(py.yhbs,a.yhbs) = b.yhbs
              and value(int(py.cjdm),a.cjdm) = b.sccjdm 
              and b.jsrq>start_date 
            where (jhnf,jhyf,bbh)in(select jhnf,jhyf,max(bbh) from YYZY.T_YYZY_JSCJH group by jhnf,jhyf) 
              and zybj='1' 
              and jhnf=yr_1 
              and pfphdm is not null 
              and jhyf>mn_1 
            group by pfphdm,JHNF, JHYF
          )
          select pfphdm,ksrq,jsrq,jhcl/(days(jsrq)-days(ksrq)+1) as jhcl 
          from jdjh_tmp; 
        end if; 
		
		 
		
        --�¼ƻ������뼾�ȼƻ�ͬ��������Ҫ���㼾��ʣ��ƻ�
        insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg)
        with pfph as ( 
          select distinct PFPHDM 
          from DIM.T_DIM_YYZY_PFPH 
          where jsrq>start_date
        ),
        njh as ( 
          select PFPHDM, JHNF, sum(JHCL) as jhcl
          from YYZY.T_YYZY_SNSCJH_ZXB 
          where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB) 
            and jhnf=(select year(max(jhrq)) from session.tmp2)
            and pfphdm is not null
          group by pfphdm,jhnf 
        ),
        jjh as (
          select  pfphdm,JHNF,sum(JHCL) as jhcl
          from YYZY.T_YYZY_JSCJH as a 
    /*
      2012-06-07������޸ģ��䷽�ϲ��߼�
      ��YYZY.T_YYZY_YS_PH27��������ӳ����򣬿��Ժϲ��䷽
    */
          left join (
            select yhbs_src, cjdm_src, YHBS, CJDM 
            from YYZY.T_YYZY_YS_PH27  
            where zybj='1' 
			  and yhbs_src=yhbs
	          and cjdm_src<>cjdm
              and (yhbs_src, cjdm_src,bbrq)in(
												select yhbs_src, cjdm_src,max(bbrq)
                                                  from YYZY.T_YYZY_YS_PH27
		                                         where yhbs_src=yhbs
	                                               and cjdm_src<>cjdm
                                              group by yhbs_src, cjdm_src
                                              )
          ) as py on a.yhbs = py.yhbs_src and a.cjdm = int(py.cjdm_src)
          left join DIM.T_DIM_YYZY_PFPH as b 
            on value(py.yhbs,a.yhbs)=b.yhbs
            and value(int(py.cjdm),a.cjdm)=b.sccjdm 
            and b.jsrq>start_date 
          where (jhnf,jhjd,jhyf,bbh)in(
              select jhnf,jhjd,jhyf,max(bbh) 
              from YYZY.T_YYZY_JSCJH 
              group by jhnf,jhjd,jhyf
            ) 
            and zybj='1' 
            and jhnf=(select YEAR(max(jhrq)) from session.tmp2)
            and pfphdm is not null 
          group by pfphdm,JHNF
        )
        ,jjh_add as (
          select pfphdm,jhnf,sum(jhcl) as jhcl
          from (
            select pfphdm,jhnf,jhcl
            from jjh
			/*
			2013-01-11�޸ģ�����δ���ؼ��������ƻ�����ȼƻ���ȥ��ʵ�����µ��յ������ƻ�������¶������ƻ���Ҫ������ʷ���ݣ�
			*/			
			union all
            select pfphdm,year(jhrq) as jhnf,jhcl
              from YYZY.T_YYZY_RSCJH_LSB
			 where jhrq<START_DATE 
			   and year(jhrq)>=year(START_DATE)
			Union all
            select pfphdm,year(jhrq) as jhnf,jhcl_avg as jhcl
            from session.tmp2
            --where month(jhrq)>6  --2013-01-11 ע�� ������δ���ؼ��������ƻ�����˲����µ��յ������ƻ�������
          ) as a
          group by pfphdm,jhnf
        )
        ,cl_dnjy as (
          select a.pfphdm,value(b.jhnf,c.jhnf) as jhnf,
              value(b.jhcl,0) as cl_njh,value(c.jhcl,0) as cl_jjh,
              value(b.jhcl,0)-value(c.jhcl,0) as cl_jy
          from pfph as a 
          left join njh as b
            on a.pfphdm=b.pfphdm 
          left join jjh_add as c 
            on a.pfphdm=c.pfphdm
		   and b.jhnf=c.jhnf
          where b.jhnf is not null or c.jhnf is not null
        ),
        cl_ksjs as (
          select pfphdm,date(rtrim(char(jhnf))||'-'||rtrim(char(case when mn_1>mn_2 then mn_1+1 else mn_2+1 end))||'-01') as ksrq,
              date(rtrim(char(jhnf))||'-12-31') as jsrq,cl_jy as jhcl 
          from cl_dnjy
         where date(rtrim(char(jhnf))||'-'||rtrim(char(case when mn_1>mn_2 then mn_1+1 else mn_2+1 end))||'-01')<date(rtrim(char(jhnf))||'-12-31')
        )
        select pfphdm,ksrq,jsrq,jhcl*1.0000/(days(jsrq)-days(ksrq)+1) as jhcl_avg
        from cl_ksjs;

	  
	 end if;
      

	  
	  
      --�¼ƻ�����ĩ,����Ҫ������ƻ�ʣ�� 
      insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg) 
      with mhnjh_tmp as ( 
        select PFPHDM,rtrim(char(jhnf))||'-01-01' as ksrq, 
            rtrim(char(jhnf))||'-12-31' as jsrq,sum(JHCL) as jhcl 
        from YYZY.T_YYZY_SNSCJH_ZXB 
        where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB) 
          AND jhnf in (yr_1+1,yr_1+2) 
          and pfphdm is not null 
        group by PFPHDM,JHNF 
      )
      select pfphdm,ksrq,jsrq,(jhcl*1.00000)/(days(jsrq)-days(ksrq)+1) as jhcl_avg
      from mhnjh_tmp; 
    end if; 
	
	  


	
    --��ƻ����<�¼ƻ����
	
	
	
		
		
    if yr_2<yr_1 then 
      

	  
	  delete from session.tmp3 where ksrq>=date(rtrim(char(yr_1))||'-01-01');
      insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg)
      with mhnjh_tmp as ( 
        select PFPHDM, rtrim(char(jhnf))||'-01-01' as ksrq, 
            rtrim(char(jhnf))||'-12-31' as jsrq, sum(JHCL) as jhcl
        from YYZY.T_YYZY_SNSCJH_ZXB
        where pfphdm is not null
          and jhnf in (yr_1,yr_1+1,yr_1+2) 
        group by PFPHDM,JHNF
      ) 
      select a.pfphdm,rtrim(char(yr_1))||'-'||rtrim(char(mn_1+1))||'-01' as ksrq,
          jsrq,((a.jhcl-value(b.jhcl,0))*1.00000)/(days(jsrq)-days(rtrim(char(yr_1))||'-'||rtrim(char(mn_1+1))||'-01')+1) as jhcl_avg 
      from mhnjh_tmp as a
	      left join (select pfphdm,sum(jhcl_avg) jhcl,year(jhrq) as jhnf 
		   		from session.tmp2 group by pfphdm,year(jhrq)
				)as b on a.pfphdm=b.pfphdm and year(ksrq)=b.jhnf
      where year(ksrq)=yr_1 
      union all 
      select pfphdm,ksrq,jsrq,(jhcl*1.00000)/(days(jsrq)-days(ksrq)+1) as jhcl_avg 
      from mhnjh_tmp 
      where year(ksrq)>yr_1; 
	  

	  
    end if; 
    
	

	
	 
	
    --�������ݣ�������ʱ��yyzy.t_yyzy_tmp_rscjhb_whb
    insert into yyzy.t_yyzy_tmp_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg) 
    select pfphdm,ksrq,jsrq,jhcl_avg 
    from session.tmp3; 
    
    --��ӹ��޷�ȡ���µ��յļƻ�Ĭ��Ϊÿ��ƽ̯
    delete from yyzy.t_yyzy_tmp_rscjhb_whb 
    where pfphdm in ( 
      select PFPHDM 
      from DIM.T_DIM_YYZY_PFPH 
      where sccjdm in (4,5,6,7) and jsrq>=start_date 
    );
	
	--2013.4.10�ģ�������ӹ�ά������
    insert into yyzy.t_yyzy_tmp_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg)
    WITH PPGG AS (
  select PPGGID, 
	     JHNF, 
		 CJMC, 
		 CJDM, 
		 PPMC, 
		 PPDM, 
		 YHBS, 
		 JYGG, 
		 CZR , 
		 BBH, 
		 BBRQ
    from DIM.T_DIM_YYZY_WJGPPGG
  WHERE (JHNF,CJDM,PPDM,YHBS,JYGG,BBH) IN (SELECT JHNF,CJDM,PPDM,YHBS,JYGG,MAX(BBH) FROM DIM.T_DIM_YYZY_WJGPPGG GROUP BY JHNF,CJDM,PPDM,YHBS,JYGG)
  )
,YSCJH_WJG_TMP AS (
    select PPGGID, 
		   JHNF, 
		   JHYF, 
		   JHCL, 
		   ZSBJ, 
		   CZR, 
		   BBH, 
		   BBRQ
      from YYZY.T_YYZY_YSCJH_WJG
     WHERE (PPGGID,JHNF,JHYF,BBH) IN (SELECT PPGGID,JHNF,JHYF,MAX(BBH) FROM YYZY.T_YYZY_YSCJH_WJG GROUP BY PPGGID,JHNF,JHYF)
)
,YSCJH_WJG AS (
  select C.PFPHDM,
  		 B.JHNF,
  		 B.JHYF,
		 DATE(RTRIM(CHAR(B.JHNF))||'-'||RTRIM(CHAR(B.JHYF))||'-01') + 1 MONTH - 1 DAY as JHRQ,
		 A.CJDM,
		 A.YHBS,
		 SUM(B.JHCL) AS JHCL
	FROM PPGG AS A,
		 YSCJH_WJG_TMP AS B,
		 DIM.T_DIM_YYZY_PFPH AS C
   WHERE A.PPGGID=B.PPGGID
     AND int(A.CJDM)=int(C.SCCJDM)
	 AND A.YHBS=C.YHBS
GROUP BY C.PFPHDM,B.JHNF,B.JHYF,A.CJDM,A.YHBS
),
 snjh_wjg as ( 
      select PFPHDM, PZMC, SCCJ, JHNF, JHCL, BBH
      from YYZY.T_YYZY_SNSCJH_ZXB
      where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB)
        and pfphdm in (select pfphdm from DIM.T_DIM_YYZY_PFPH where sccjdm in (4,5,6,7)) 
    )
    ,lsjh as (
      select PFPHDM, nf as jhnf ,scsl as jhcl 
      from (
        select NF, PFPHDM, SCSL
        from YYZY.V_YYZY_PHCL
        where nf=year(start_date)
      ) as a
      where pfphdm in (
          select pfphdm
          from DIM.T_DIM_YYZY_PFPH
          where sccjdm in (4,5,6,7)
            and jsrq>start_date 
        ) 
    )
    ,snjh_ksjs as (
	  select a.pfphdm,date(rtrim(char(a.jhnf))||'-01-01')+(case when a.jhnf=year(start_date) then (dayofyear(start_date)-1) else 0 end) day as ksrq,
          	 date(rtrim(char(a.jhnf))||'-12-31') as jsrq,
          	 case when sum(a.jhcl-value(b.jhcl,0))>0 then sum(a.jhcl-value(b.jhcl,0)) else 0 end as jhcl
      from snjh_wjg as a
      left join lsjh as b
        on a.pfphdm=b.pfphdm and a.jhnf=b.jhnf 
      group by a.pfphdm,a.jhnf
),
    snjh_avg as ( 
    	select B.pfphdm,
		   A.RIQI AS KSRQ,
		   A.RIQI AS JSRQ,		   
		   B.jhcl*1.0000/(days(B.jsrq)-days(B.ksrq)+1)+VALUE(C.JHCL,0) as jhcl_avg 
	  from DIM.T_DIM_YYZY_DATE AS A
	  LEFT JOIN snjh_ksjs AS B 
	    ON A.RIQI BETWEEN B.KSRQ AND B.JSRQ
	  LEFT JOIN YSCJH_WJG AS C
	    ON A.RIQI=C.JHRQ
	   AND B.PFPHDM=C.PFPHDM
	 WHERE A.RIQI BETWEEN B.KSRQ AND B.JSRQ 
    )
,PX AS(
    select pfphdm,ksrq,jsrq,jhcl_avg
    from snjh_avg 
    where jsrq<current date+1 year-dayofyear(current date +1 year) day+1 day 
    union all
    select pfphdm,ksrq,jsrq,jhcl_avg
    from snjh_avg 
    where ksrq>=current date+1 year-dayofyear(current date +1 year) day+1 day)
	--��������
,PX1 AS(
	SELECT 
	PFPHDM,
	KSRQ,
	JSRQ,
	JHCL_AVG,
	ROWNUMBER()OVER(PARTITION BY PFPHDM ORDER BY JSRQ) AS 
	PX FROM PX)
,SJHB(PFPHDM,KSRQ,JSRQ,JHCL_AVG,PX,ZH) AS(
SELECT 
	PFPHDM,
	KSRQ,
	JSRQ,
	JHCL_AVG,
	PX,
	1 AS ZH 
	FROM PX1 WHERE PX = 1
UNION ALL
SELECT 
	A.PFPHDM,
	A.KSRQ,
	A.JSRQ,
	A.JHCL_AVG,
	A.PX,
	CASE 
		WHEN (A.PFPHDM,A.JSRQ,A.JHCL_AVG)=(B.PFPHDM,B.JSRQ+1 DAY,B.JHCL_AVG) 
			THEN B.ZH
		ELSE A.PX END AS ZH
	FROM PX1 A,SJHB B
	WHERE A.PX=B.PX+1 AND A.PFPHDM = B.PFPHDM
)
,SJHB1 AS(
SELECT PFPHDM,
	MIN(JSRQ) AS KSRQ,
	MAX(JSRQ) AS JSRQ,
	JHCL_AVG, 
	ZH
	FROM SJHB 
	GROUP BY PFPHDM,JHCL_AVG,ZH)
SELECT PFPHDM,KSRQ,JSRQ,JHCL_AVG FROM SJHB1
	;
    
    
	
	
    --��������
    update yyzy.t_yyzy_tmp_rscjhb_whb e 
    set jhpc_avg=( 
      select round(a.jhcl_avg/b.dpcl,2) as jhpc_avg
      from yyzy.t_yyzy_tmp_rscjhb_whb a 
      left join (
                  select pfphdm,
				  		 dpcl 
        		    from yyzy.t_yyzy_dpclb 
        		   where (pfphdm,nf*100+yf)in(select pfphdm,max(nf*100+yf) 
          from yyzy.t_yyzy_dpclb 
          group by pfphdm
        )
      ) b
        on a.pfphdm=b.pfphdm
      where (e.pfphdm,e.ksrq,e.jsrq)=(a.pfphdm,a.ksrq,a.jsrq)
    );
    
	
	
    --��������
    update session.tmp2 e 
    set jhpc_avg=( 
      select round(a.jhcl_avg/b.dpcl,2) as jhpc_avg
      from session.tmp2 a 
      left join (
        select pfphdm,dpcl 
        from yyzy.t_yyzy_dpclb 
        where (pfphdm,nf*100+yf)in( 
          select pfphdm,max(nf*100+yf) 
          from yyzy.t_yyzy_dpclb 
          group by pfphdm
        )
      ) b
        on a.pfphdm=b.pfphdm
      where (e.pfphdm,e.jhrq)=(a.pfphdm,a.jhrq)
    );
    
	


--����̬���������ֵ
SET v_gengpeng_jm='';				
SET v_gengpeng_gs='';
SET v_gengpeng_cx='';


SELECT REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, ','||A.GENGPENG_JM))),'</A>',''),'<A>','') as GENGPENG_JM,
	   REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, ','||A.GENGPENG_GS))),'</A>',''),'<A>','') as GENGPENG_GS,
	   REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, 'UNION ALL '||A.GENGPENG_CX))),'</A>',''),'<A>','') as GENGPENG_CX 
	   INTO v_gengpeng_jm,v_gengpeng_gs,v_gengpeng_cx
  FROM 
  (
SELECT 'cast(decrypt_char(B.'||A.NAME||',yyzy.f_yyzy_myjm()) as decimal(10, 2)) as '||A.NAME AS GENGPENG_JM,
	   case when A.name like 'GENG%' OR A.NAME LIKE 'geng%' then 'ceil(sum(xhyzl*1.000 / yansi * '||A.NAME||'/0.96)*4) as '||A.NAME
	   		when A.name like 'PENG%' OR A.NAME like 'peng%'then 'ceil(sum(xhyzl*1.000 / yansi * '||A.NAME||')/0.95) as '||A.NAME
	   end AS GENGPENG_GS,
	   ' SELECT '||RTRIM(CHAR(B.PFPHDM))||' AS PFPHDM,SCRQ,'||A.NAME||' from tmp1 ' AS GENGPENG_CX 
	  FROM sysibm.syscolumns as a,YYZY.T_YYZY_PFPH_CFG as b 
	 WHERE A.NAME=B.YSJGLM
	   AND TBNAME='T_DIM_YYZY_YSJGB_KZB' 
	   AND TBCREATOR='DIM'
	   AND (NAME LIKE 'GENG%' OR NAME LIKE 'PENG%')  
	   AND NAME NOT IN ('PFPHDM', 'BBH', 'ZDMC', 'ZDZ', 'ZDPFPH', 'TJRQ')
	) AS A;
	
	
    --���������ƻ��м��빣��˿ 
    set i_not_found=0; 
    open c1;
      loop_ml: loop 
        fetch c1 into v_rq;
        if i_not_found=1   then
          leave loop_ml;
        end if;


SET EXE_SQL='
        insert into session.tmp2(pfphdm,jhrq,jhcl_avg)
        with xhyzl(pfphdm,jsdm,scrq,xhyzl) as(
          select x.pfphdm,y.jsdm,date('''||rtrim(char(v_rq))||''') as JHRQ,x.jhpc_avg * y.pfbs xhyzl
          from (
            select pfphdm,ksrq,jhcl_avg,jhpc_avg 
            from yyzy.t_yyzy_tmp_rscjhb_whb 
            where date('''||rtrim(char(v_rq))||''') between ksrq and jsrq
            UNION all
            select pfphdm,jhrq as ksrq,jhcl_avg,jhpc_avg
            from session.tmp2 
            where jhrq=date('''||rtrim(char(v_rq))||''')
          ) x,
          (
            select pfphdm,jsdm,ksrq,jsrq,dpxs as pfbs 
            from yyzy.t_yyzy_jstz_whb 
            where zybj=''1''
          ) y
          where x.pfphdm = y.pfphdm and x.ksrq between y.ksrq and y.jsrq 
        ),
        tmp1 as ( 
          select scrq,ceil(sum(xhyzl*1.000/yansi * gengsi/0.96)*4) as ghyxsl,
		      --ceil(sum(xhyzl*1.000/yansi * gengs5/0.96)*4) as g5hyxsl, 
              ceil(sum(xhyzl*1.000 / yansi * pengsi1)/0.95) as pengsi1, 
              ceil(sum(xhyzl*1.000 / yansi * pengsi2)/0.95) as pengsi2
			  '||v_gengpeng_gs||'
          from xhyzl a,
          (
            select 
			    a.pfphdm,
			    cast(decrypt_char(a.yansi, yyzy.f_yyzy_myjm()) as decimal(10, 2)) as yansi,
                cast(decrypt_char(a.gengsi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gengsi,
				--cast(decrypt_char(a.gengs5,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gengs5,
                cast(decrypt_char(a.pengsi1,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi1,
                cast(decrypt_char(a.pengsi2,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi2
				'||v_gengpeng_jm||'
            from dim.t_dim_yyzy_ysjgb as a
			left join (SELECT * FROM DIM.T_DIM_YYZY_YSJGB_KZB AS T WHERE (T.pfphdm,T.BBH) in (select pfphdm,max(bbh) from DIM.T_DIM_YYZY_YSJGB_KZB group by pfphdm)) AS B
			  on a.pfphdm=b.pfphdm
            where a.jsrq> date('''||rtrim(char(start_date))||''')
              and (a.pfphdm,a.bbh) in (select pfphdm,max(bbh) from dim.t_dim_yyzy_ysjgb group by pfphdm)
          ) c
          where a.pfphdm = c.pfphdm 
          group by scrq
        )
        select 16 as pfphdm,scrq,ghyxsl from tmp1 
        union all 
		--select 63 as pfphdm,scrq,g5hyxsl from tmp1 
        --union all 
        select 23 as pfphdm,scrq,pengsi1 from tmp1 
        union all 
        select 24 as pfphdm,scrq,pengsi2 from tmp1
		'||v_gengpeng_cx||'
     '; 
		
    
	   PREPARE s0 FROM EXE_SQL;
	   EXECUTE s0;	
	   
	   
        set i_not_found=0; 
      end loop loop_ml;
    close c1; 
    
	  	
    --�õ�һ��������ϵ��ֵ
    insert into yyzy.t_yyzy_tmp_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg)
    with temp as(
      select riqi as jhrq,'x' as gl 
      from dim.t_dim_yyzy_date 
      where riqi between START_DATE and judge_time-1 day 
    ),
    pfph as (
      select pfphdm 
      from session.tmp2 
      union all
      select pfphdm 
      from yyzy.t_yyzy_tmp_rscjhb_whb 
    ),
    temp1 as( 
      select distinct pfphdm,'x' as gl 
      from pfph  
      where pfphdm not in (
        select pfphdm
        from DIM.T_DIM_YYZY_PFPH
        where sccjdm in (4,5,6,7)
      )
--      where jhrq<judge_time
      group by pfphdm
    ),
    gl as(
      select pfphdm,a.gl,b.jhrq 
      from temp1 a
      left join temp b
        on a.gl=b.gl 
    ),
    result_1 as(
      select a.pfphdm,a.jhrq,coalesce(b.jhcl_avg,0) as jhcl_avg
      from gl a 
      left join session.tmp2 b
        on a.pfphdm=b.pfphdm
        and a.jhrq=b.jhrq
    ),
    result_2 as( 
      select pfphdm,jhrq,jhcl_avg,rownumber()over(partition by pfphdm order by jhrq) as xh
      from result_1
    ),
    result_3(pfphdm,jhrq,jhcl_avg,xh,xh_1) as(
      select pfphdm,jhrq,jhcl_avg,xh,1 as xh_1 
      from  result_2 A
      where A.XH=1
      union all
      select a.PFPHDM,A.JHRQ,a.jhcl_avg,A.XH,
          (case 
            when (a.PFPHDM,A.JHRQ,a.jhcl_avg)=(B.PFPHDM,B.JHRQ+1 day,b.jhcl_avg)
              then b.xh_1 
            else a.xh 
          end) as xh_1 
      from result_2 a,result_3 b 
      WHERE A.XH=b.XH+1 
        and a.pfphdm=b.pfphdm
    ),
    result_4 as(
      select a.pfphdm,min(jhrq) as ksrq,max(jhrq) as jsrq,jhcl_avg as jhcl_avg
      from result_3 a 
      group by a.pfphdm,jhcl_avg,xh_1
      union 
      select a.pfphdm,min(jhrq) as ksrq,max(jhrq) as jsrq,jhcl_avg as jhcl_avg
      from session.tmp2 a 
      where jhrq>=judge_time 
      group by a.pfphdm,jhcl_avg,year(jhrq)
    )
    select a.pfphdm,a.ksrq,a.jsrq,a.jhcl_avg,round(a.jhcl_avg/b.dpcl,2) 
    from result_4 a 
    left join (
      select pfphdm,dpcl
      from yyzy.t_yyzy_dpclb 
      where (pfphdm,nf*100+yf)in(
          select pfphdm,max(nf*100+yf)
          from yyzy.t_yyzy_dpclb 
          group by pfphdm
        ) 
      ) b 
      on a.pfphdm=b.pfphdm;
    
    insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg) 
    select pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg 
    from yyzy.t_yyzy_tmp_rscjhb_whb;
    
	/*2012-04-06�޸�bug*/
    /* 
      �ж�ʱ��εĲ�ȫ�㷨
     */
	 set v_ksrq = judge_time;
     select max(jsrq) into v_jsrq from yyzy.t_yyzy_tmp_rscjhb_whb;
	 
     insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg) 
     with pfphlb as (
         select distinct pfphdm
         from (
           select pfphdm
           from yyzy.t_yyzy_tmp_rscjhb_whb
           union all
           select pfphdm
           from session.tmp2 
         ) as a
         where pfphdm not in (
           select pfphdm
           from dim.t_dim_yyzy_pfph
           where sccjdm in (4,5,6,7)
         )
       )
       , bq(pfphdm,ksrq,jsrq) as ( 
		select pfphdm, 
		  (lag(jsrq + 1 day,1,cast(null as date))over(partition by pfphdm order by ksrq,jsrq)) as ksrq,
		  ksrq - 1 day as jsrq
		from yyzy.t_yyzy_tmp_rscjhb_whb
		where jsrq>=v_ksrq and ksrq<=v_jsrq
		union all
		--s
		select pfphdm, 
		  v_ksrq as ksrq,
		  min(ksrq - 1 day) jsrq
		from yyzy.t_yyzy_tmp_rscjhb_whb
		where jsrq>=v_ksrq and ksrq<=v_jsrq
		group by pfphdm
		union all
		--e
		select pfphdm, 
		  max(jsrq+1 day) ksrq,
		  v_jsrq as jsrq
		from yyzy.t_yyzy_tmp_rscjhb_whb
		where jsrq>=v_ksrq and ksrq<=v_jsrq
		group by pfphdm
	)
	select pfphlb.pfphdm, value(ksrq,v_ksrq), value(jsrq,v_jsrq), 0, 0
	from pfphlb left join bq on pfphlb.pfphdm = bq.pfphdm
	where ksrq is not null and  jsrq is not null
	  and jsrq>=ksrq
	;
/*	 
    set i_not_found=0; 
    open c2; 
    loop_mt: loop 
       fetch c2 into v_ksrq,v_jsrq;
       if i_not_found=1   then
          leave loop_mt;
       end if;
       --�鹹�����գ���������ʽ��yyzy.t_yyzy_rscjhb_whb
       insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg) 
       with tmp as (
         select distinct pfphdm
         from (
           select pfphdm
           from yyzy.t_yyzy_tmp_rscjhb_whb
           union all
           select pfphdm
           from session.tmp2 
         ) as a
         where pfphdm not in (
           select pfphdm
           from dim.t_dim_yyzy_pfph
           where sccjdm in (4,5,6,7)
         )
       )
       select a.pfphdm,v_ksrq as ksrq,v_jsrq as jsrq,0.00 as jhcl_avg,0.00 as jhpc_avg 
       from tmp a 
       where (pfphdm,year(v_ksrq)*100+month(v_ksrq))not in( 
           select pfphdm,year(ksrq)*100+month(ksrq) 
           from yyzy.t_yyzy_rscjhb_whb 
           where jsrq>=judge_time 
         ) 
       group by a.pfphdm,v_ksrq,v_jsrq; 
       
       set i_not_found=0; 
       
    end loop  loop_mt;
    close c2; 
*/
	/* upon 2012-04-06�޸�bug*/
    
  /*����ǰ̨ά��ÿ������������ʣ�����������*/
  open c3;
  cl_zzl:loop
    set i_not_found=0;
    fetch c3 into v_jhnf;
    if i_not_found=1 then
      leave cl_zzl;
    end if;
    
    set new_ksrq = date(rtrim(char(v_jhnf))||'-01-01');
    
    insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg)
    select 
      a.pfphdm,new_ksrq as ksrq,
      (new_ksrq+1 year -1 day) as jsrq,
      value(scjh,0)/(days(new_ksrq+1 year -1 day)-days(new_ksrq)+1) as jhcl_avg,
      0 as jhpc_avg
    from 
      yyzy.t_yyzy_rscjhb_whb as a
      left join 
      (
        select pfphdm,scjh
        from YYZY.T_YYZY_SCJH_ZZL
        where 1=1
          and (pfphdm,jhnf,bbh)in(
            select pfphdm,jhnf,max(bbh)
            from YYZY.T_YYZY_SCJH_ZZL
            where jhnf=v_jhnf 
            group by pfphdm,jhnf
          )
      ) as b
        on a.pfphdm = b.pfphdm
    where 1=1
      and ( (new_ksrq-1 year) between ksrq and jsrq)
      and a.pfphdm not in (16,23,24,63)
    ;
    
	/*  --2012��11��28���޸�,���˵������̳���������˿����.��˿���ݲ���,�����һ��SQL
    insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg)
    with xhyzl(pfphdm,jsdm,ksrq,jsrq,xhyzl) as 
    (
      select 
        x.pfphdm,y.jsdm,
        new_ksrq as ksrq ,
        (new_ksrq+1 year -1 day)as jsrq,
        (x.jhcl_avg/dpcl) * y.pfbs as xhyzl
      from 
        (
          select pfphdm,ksrq,jhcl_avg,jhpc_avg 
          from yyzy.t_yyzy_rscjhb_whb 
          where ksrq=new_ksrq
        ) as x,
        (
          select pfphdm,jsdm,ksrq,jsrq,dpxs as pfbs 
          from yyzy.t_yyzy_jstz_whb 
          where zybj='1'
        ) as y,
        (
          select pfphdm,dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in(
            select pfphdm,max(nf*100+yf) 
            from yyzy.t_yyzy_dpclb 
            group by pfphdm
          )
        ) as d
      where x.pfphdm = y.pfphdm 
        and x.ksrq between y.ksrq and y.jsrq 
        and x.pfphdm = d.pfphdm
    )
    ,
    tmp1 as ( 
      select 
        ksrq,jsrq,
        ceil(sum(xhyzl*1.000/yansi * gengsi/0.96)*4) as ghyxsl, 
        ceil(sum(xhyzl*1.000 / yansi * pengsi1)/0.95) as pengsi1, 
        ceil(sum(xhyzl*1.000 / yansi * pengsi2)/0.95) as pengsi2 
      from 
        xhyzl a,
        (
          select 
            pfphdm,
            cast(decrypt_char(yansi, yyzy.f_yyzy_myjm()) as decimal(10, 2)) as yansi,
            cast(decrypt_char(gengsi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gengsi,
            cast(decrypt_char(pengsi1,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi1,
            cast(decrypt_char(pengsi2,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi2
          from 
            dim.t_dim_yyzy_ysjgb 
          where jsrq>current date 
            and (pfphdm,bbh) in (select pfphdm,max(bbh) from dim.t_dim_yyzy_ysjgb group by pfphdm)
        ) c
      where a.pfphdm = c.pfphdm 
      group by ksrq,jsrq
    )
    select 16 as pfphdm,ksrq,jsrq,ghyxsl from tmp1 
    union all 
    select 23 as pfphdm,ksrq,jsrq,pengsi1 from tmp1 
    union all 
    select 24 as pfphdm,ksrq,jsrq,pengsi2 from tmp1
    ;
    */
	/* 2013-05-15�޸�,���öνű��޸�Ϊ��̬SQL,�����һ��SQL
	insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg)
	with gsxhyzl(pfphdm,jsdm,ksrq,jsrq,gsxhyzl) as   --��˿����
    (
      select 
        x.pfphdm,y.jsdm,
        new_ksrq as ksrq ,
        (new_ksrq+1 year -1 day)as jsrq,
        (x.jhcl_avg/dpcl) * y.pfbs as gsxhyzl
      from 
        (
          select pfphdm,ksrq,jhcl_avg,jhpc_avg 
          from yyzy.t_yyzy_rscjhb_whb 
          where ksrq=new_ksrq
        ) as x,
        (
          select pfphdm,jsdm,ksrq,jsrq,dpxs as pfbs 
          from yyzy.t_yyzy_jstz_whb 
          where zybj='1'
        ) as y,
        (
          select pfphdm,dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in(
            select pfphdm,max(nf*100+yf) 
            from yyzy.t_yyzy_dpclb 
            group by pfphdm
          )
        ) as d
      where x.pfphdm = y.pfphdm 
        and x.ksrq between y.ksrq and y.jsrq 
        and x.pfphdm = d.pfphdm
    ),
    tmp1 as ( 
      select 
        ksrq,jsrq,
        ceil(sum(gsxhyzl*1.000/yansi * gengsi/0.96)*4) as ghyxsl,
		ceil(sum(gsxhyzl*1.000/yansi * gengs5/0.96)*4) as g5hyxsl
      from 
        gsxhyzl a,
        (
          select 
            pfphdm,
            cast(decrypt_char(yansi, yyzy.f_yyzy_myjm()) as decimal(10, 2)) as yansi,
            cast(decrypt_char(gengsi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gengsi,
			cast(decrypt_char(gengs5,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gengs5
          from 
            dim.t_dim_yyzy_ysjgb 
          where jsrq>current date 
            and (pfphdm,bbh) in (select pfphdm,max(bbh) from dim.t_dim_yyzy_ysjgb group by pfphdm)
        ) c
      where a.pfphdm = c.pfphdm 
      group by ksrq,jsrq
    ),psxhyzl(pfphdm,jsdm,ksrq,jsrq,psxhyzl) as   --��˿����,���������̳��ƺ�
    (
      select 
        x.pfphdm,y.jsdm,
        new_ksrq as ksrq ,
        (new_ksrq+1 year -1 day)as jsrq,
        (x.jhcl_avg/dpcl) * y.pfbs as psxhyzl
      from 
        (
          select pfphdm,ksrq,jhcl_avg,jhpc_avg 
          from yyzy.t_yyzy_rscjhb_whb 
          where ksrq=new_ksrq
		    and pfphdm not in (select PFPHDM from DIM.T_DIM_YYZY_PFPH where sccjdm=4 and jsrq>current date)  --���������̳��ƺ�
        ) as x,
        (
          select pfphdm,jsdm,ksrq,jsrq,dpxs as pfbs 
          from yyzy.t_yyzy_jstz_whb 
          where zybj='1'
		    and pfphdm not in (select PFPHDM from DIM.T_DIM_YYZY_PFPH where sccjdm=4 and jsrq>current date)  --���������̳��ƺ�
        ) as y,
        (
          select pfphdm,dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in(select pfphdm,max(nf*100+yf) from yyzy.t_yyzy_dpclb group by pfphdm)
		    and pfphdm not in (select PFPHDM from DIM.T_DIM_YYZY_PFPH where sccjdm=4 and jsrq>current date)  --���������̳��ƺ�
        ) as d
      where x.pfphdm = y.pfphdm 
        and x.ksrq between y.ksrq and y.jsrq 
        and x.pfphdm = d.pfphdm
    ),
    tmp2 as ( 
      select 
        ksrq,jsrq,
        ceil(sum(psxhyzl*1.000 / yansi * pengsi1)/0.95) as pengsi1, 
        ceil(sum(psxhyzl*1.000 / yansi * pengsi2)/0.95) as pengsi2 
      from 
        psxhyzl a,
        (
          select 
            pfphdm,
            cast(decrypt_char(yansi, yyzy.f_yyzy_myjm()) as decimal(10, 2)) as yansi,
            cast(decrypt_char(pengsi1,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi1,
            cast(decrypt_char(pengsi2,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi2
          from 
            dim.t_dim_yyzy_ysjgb 
          where jsrq>current date 
            and (pfphdm,bbh) in (select pfphdm,max(bbh) from dim.t_dim_yyzy_ysjgb group by pfphdm)
        ) c
      where a.pfphdm = c.pfphdm 
      group by ksrq,jsrq
    )
    select 16 as pfphdm,ksrq,jsrq,ghyxsl from tmp1 
    union all 
	select 63 as pfphdm,ksrq,jsrq,g5hyxsl from tmp1 
    union all 
    select 23 as pfphdm,ksrq,jsrq,pengsi1 from tmp2
    union all 
    select 24 as pfphdm,ksrq,jsrq,pengsi2 from tmp2
	;
	*/
	
	
--����̬���������ֵ
SET v_gengpeng_jm='';				
SET v_gengpeng_gs='';
SET v_gengpeng_cx='';
SET v_gengpeng_jm_p='';				
SET v_gengpeng_gs_p='';
SET v_gengpeng_cx_p='';


SELECT REPLACE(A.GENG_JM,'<A/>','') AS GENG_JM,
	   REPLACE(A.PENG_JM,'<A/>','') AS PENG_JM,
	   REPLACE(A.GENG_GS,'<A/>','') AS GENG_GS,
	   REPLACE(A.PENG_GS,'<A/>','') AS PENG_GS,
	   REPLACE(A.GENG_CX,'<A/>','') AS GENG_CX,
	   REPLACE(A.PENG_CX,'<A/>','') AS PENG_CX
	   INTO v_gengpeng_jm,v_gengpeng_jm_p,v_gengpeng_gs,v_gengpeng_gs_p,v_gengpeng_cx,v_gengpeng_cx_p
  FROM (
SELECT REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, ','||A.GENG_JM))),'</A>',''),'<A>','') as GENG_JM,
	   REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, ','||A.PENG_JM))),'</A>',''),'<A>','') as PENG_JM,
	   REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, ','||A.GENG_GS))),'</A>',''),'<A>','') as GENG_GS,
	   REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, ','||A.PENG_GS))),'</A>',''),'<A>','') as PENG_GS,
	   REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, 'UNION ALL '||A.GENG_CX))),'</A>',''),'<A>','') as GENG_CX,
	   REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, 'UNION ALL '||A.PENG_CX))),'</A>',''),'<A>','') as PENG_CX
  FROM 
  (
SELECT case when A.name like 'GENG%' OR A.NAME LIKE 'geng%' then 'cast(decrypt_char(B.'||A.NAME||',yyzy.f_yyzy_myjm()) as decimal(10, 2)) as '||A.NAME 
       end AS GENG_JM,
	   
	   case when A.name like 'PENG%' OR A.NAME LIKE 'peng%' then 'cast(decrypt_char(B.'||A.NAME||',yyzy.f_yyzy_myjm()) as decimal(10, 2)) as '||A.NAME 
       end AS PENG_JM,
	   
	   case when A.name like 'GENG%' OR A.NAME LIKE 'geng%' then 'ceil(sum(gsxhyzl*1.000 / yansi * '||A.NAME||'/0.96)*4) as '||A.NAME
	   end AS GENG_GS,
	   
	   case when A.name like 'PENG%' OR A.NAME like 'peng%'then 'ceil(sum(psxhyzl*1.000 / yansi * '||A.NAME||')/0.95) as '||A.NAME
	   end AS PENG_GS,
	   
	   case when A.name like 'GENG%' OR A.NAME LIKE 'geng%' then ' SELECT '||RTRIM(CHAR(B.PFPHDM))||' AS PFPHDM,KSRQ,JSRQ,'||A.NAME||' from tmp1 ' 
	   end AS GENG_CX,
	   
	   case when A.name like 'PENG%' OR A.NAME LIKE 'peng%' then ' SELECT '||RTRIM(CHAR(B.PFPHDM))||' AS PFPHDM,KSRQ,JSRQ,'||A.NAME||' from tmp2 ' 
	   end AS PENG_CX 
	   
	  FROM sysibm.syscolumns as a,YYZY.T_YYZY_PFPH_CFG as b 
	 WHERE A.NAME=B.YSJGLM
	   AND TBNAME='T_DIM_YYZY_YSJGB_KZB' 
	   AND TBCREATOR='DIM'
	   AND (NAME LIKE 'GENG%' OR NAME LIKE 'PENG%')  
	   AND NAME NOT IN ('PFPHDM', 'BBH', 'ZDMC', 'ZDZ', 'ZDPFPH', 'TJRQ')
	) AS A
	) AS A
	;
	

SET EXE_SQL='	
	insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg)
	with gsxhyzl(pfphdm,jsdm,ksrq,jsrq,gsxhyzl) as   --��˿����
    (
      select 
        x.pfphdm,y.jsdm,
		date('''||rtrim(char(new_ksrq))||''') as ksrq ,
        (date('''||rtrim(char(new_ksrq))||''')+1 year -1 day)as jsrq,
        (x.jhcl_avg/dpcl) * y.pfbs as gsxhyzl
      from 
        (
          select pfphdm,ksrq,jhcl_avg,jhpc_avg 
          from yyzy.t_yyzy_rscjhb_whb 
          where ksrq=date('''||rtrim(char(new_ksrq))||''')
        ) as x,
        (
          select pfphdm,jsdm,ksrq,jsrq,dpxs as pfbs 
          from yyzy.t_yyzy_jstz_whb 
          where zybj=''1''
        ) as y,
        (
          select pfphdm,dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in(
            select pfphdm,max(nf*100+yf) 
            from yyzy.t_yyzy_dpclb 
            group by pfphdm
          )
        ) as d
      where x.pfphdm = y.pfphdm 
        and x.ksrq between y.ksrq and y.jsrq 
        and x.pfphdm = d.pfphdm
    ),
    tmp1 as ( 
      select 
        ksrq,jsrq,
        ceil(sum(gsxhyzl*1.000/yansi * gengsi/0.96)*4) as ghyxsl
		'||v_gengpeng_gs||'
      from 
        gsxhyzl a,
        (
          select 
            a.pfphdm,
            cast(decrypt_char(a.yansi, yyzy.f_yyzy_myjm()) as decimal(10, 2)) as yansi,
            cast(decrypt_char(a.gengsi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gengsi
			'||v_gengpeng_jm||'
          from dim.t_dim_yyzy_ysjgb as a
		  left join (SELECT * FROM DIM.T_DIM_YYZY_YSJGB_KZB AS T WHERE (T.pfphdm,T.BBH) in (select pfphdm,max(bbh) from DIM.T_DIM_YYZY_YSJGB_KZB group by pfphdm)) AS B
			  on a.pfphdm=b.pfphdm
          where jsrq>current date 
            and (a.pfphdm,a.bbh) in (select pfphdm,max(bbh) from dim.t_dim_yyzy_ysjgb group by pfphdm)
        ) c
      where a.pfphdm = c.pfphdm 
      group by ksrq,jsrq
    ),psxhyzl(pfphdm,jsdm,ksrq,jsrq,psxhyzl) as   --��˿����,���������̳��ƺ�
    (
      select 
        x.pfphdm,y.jsdm,
        date('''||rtrim(char(new_ksrq))||''') as ksrq ,
        (date('''||rtrim(char(new_ksrq))||''')+1 year -1 day)as jsrq,
        (x.jhcl_avg/dpcl) * y.pfbs as psxhyzl
      from 
        (
          select pfphdm,ksrq,jhcl_avg,jhpc_avg 
          from yyzy.t_yyzy_rscjhb_whb 
          where ksrq=date('''||rtrim(char(new_ksrq))||''')
		    and pfphdm not in (select PFPHDM from DIM.T_DIM_YYZY_PFPH where sccjdm=4 and jsrq>current date)  --���������̳��ƺ�
        ) as x,
        (
          select pfphdm,jsdm,ksrq,jsrq,dpxs as pfbs 
          from yyzy.t_yyzy_jstz_whb 
          where zybj=''1''
		    and pfphdm not in (select PFPHDM from DIM.T_DIM_YYZY_PFPH where sccjdm=4 and jsrq>current date)  --���������̳��ƺ�
        ) as y,
        (
          select pfphdm,dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in(select pfphdm,max(nf*100+yf) from yyzy.t_yyzy_dpclb group by pfphdm)
		    and pfphdm not in (select PFPHDM from DIM.T_DIM_YYZY_PFPH where sccjdm=4 and jsrq>current date)  --���������̳��ƺ�
        ) as d
      where x.pfphdm = y.pfphdm 
        and x.ksrq between y.ksrq and y.jsrq 
        and x.pfphdm = d.pfphdm
    ),
    tmp2 as ( 
      select 
        ksrq,jsrq,
        ceil(sum(psxhyzl*1.000 / yansi * pengsi1)/0.95) as pengsi1, 
        ceil(sum(psxhyzl*1.000 / yansi * pengsi2)/0.95) as pengsi2 
		'||v_gengpeng_gs_p||'
      from 
        psxhyzl a,
        (
          select 
            a.pfphdm,
            cast(decrypt_char(a.yansi, yyzy.f_yyzy_myjm()) as decimal(10, 2)) as yansi,
            cast(decrypt_char(a.pengsi1,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi1,
            cast(decrypt_char(a.pengsi2,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as pengsi2
			'||v_gengpeng_jm_p||'
          from dim.t_dim_yyzy_ysjgb as a
		  left join (SELECT * FROM DIM.T_DIM_YYZY_YSJGB_KZB AS T WHERE (T.pfphdm,T.BBH) in (select pfphdm,max(bbh) from DIM.T_DIM_YYZY_YSJGB_KZB group by pfphdm)) AS B
			  on a.pfphdm=b.pfphdm
          where a.jsrq>current date 
            and (a.pfphdm,a.bbh) in (select pfphdm,max(bbh) from dim.t_dim_yyzy_ysjgb group by pfphdm)
        ) c
      where a.pfphdm = c.pfphdm 
      group by ksrq,jsrq
    )
    select 16 as pfphdm,ksrq,jsrq,ghyxsl from tmp1 
    union all 
    select 23 as pfphdm,ksrq,jsrq,pengsi1 from tmp2
    union all 
    select 24 as pfphdm,ksrq,jsrq,pengsi2 from tmp2
	'||v_gengpeng_cx||'
	'||v_gengpeng_cx_p||'
	';

	  PREPARE s0 FROM EXE_SQL;
	  EXECUTE s0;	
	
  end loop;
  close c3;
  
  /*
    insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg)
    select pfphdm,new_ksrq as ksrq,new_jsrq as jsrq,jhcl_avg,jhpc_avg
    from yyzy.t_yyzy_rscjhb_whb 
    where new_ksrq- 1 year between ksrq and jsrq;
  */
    
    -- 2013-09-10 ������������ӹ����Ϻ���˿���������ƻ�
    call YYZY.P_YYZY_RSCJH_WJGPHZS(START_DATE);
    
    update yyzy.t_yyzy_rscjhb_whb
    set jhcl_avg=0.00
    where jhcl_avg<0;
    
    update yyzy.t_yyzy_rscjhb_whb
    set ksrq=START_DATE
    where ksrq<START_DATE;
    
    update yyzy.t_yyzy_rscjhb_whb
    set bbrq=(select MAX(date(d_createtime)) from hds_cxqj.n_cxqj_o_prodplanhist_n);
    
    --��������
    update yyzy.t_yyzy_rscjhb_whb e 
    set jhpc_avg=( 
      select round(a.jhcl_avg/b.dpcl,2) as jhpc_avg
      from yyzy.t_yyzy_rscjhb_whb a 
      left join (
        select pfphdm,dpcl 
        from yyzy.t_yyzy_dpclb 
        where (pfphdm,nf*100+yf)in( 
          select pfphdm,max(nf*100+yf) 
          from yyzy.t_yyzy_dpclb 
          group by pfphdm
        )
      ) b
        on a.pfphdm=b.pfphdm
      where (e.pfphdm,e.ksrq,e.jsrq)=(a.pfphdm,a.ksrq,a.jsrq)
    );
    
    --�����β���ȷ
    update yyzy.t_yyzy_rscjhb_whb 
    set jhpc_avg=cast(null as decimal(10,2)) 
    where pfphdm IN (select PFPHDM from YYZY.T_YYZY_PFPH_CFG WHERE GPBJ='1' AND JSBJ='0'); 
	
    
end;

GRANT EXECUTE ON PROCEDURE "APP_YYB"."P_YYZY_JHJZ_R"
 (DATE, 
  VARCHAR(1000)
 ) 
  TO USER "ETLUSR" WITH GRANT OPTION;


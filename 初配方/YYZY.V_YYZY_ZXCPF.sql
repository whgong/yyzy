SET SCHEMA = YYZY;

SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,KSUSR;

CREATE VIEW YYZY.V_YYZY_ZXCPF
    ( YYDM, YYMC, YYNF, YYKCJS, KCLX )
AS
with zxcpf as ( 
  select z.ZXCPFZBDM, z.PYDM, value(z.pfnf,year(current date)) as PFNF, z.YYLXDM ,c.BYDM, c.BYBL, c.BLSX, c.BLXX, c.YYHYL 
  from ( 
  select * 
  from YYZY.T_YYZY_ZXCPFZB 
  where (pfnf,pydm,bbh)in ( 
  select pfnf,PYDM,max(bbh) 
  from YYZY.T_YYZY_ZXCPFZB group by pfnf,PYDM ) and zybj='1' ) as z left join YYZY.T_YYZY_ZXCPFCB as c on z.ZXCPFZBDM=c.ZXCPFZBDM 
  where yyhyl>0 ) ,bysl_cg as ( 
  select pydm as yydm,pfnf as yynf,sum(yyhyl)*50/200 as cgkcsl,yydjmc,yycdmc, dcdmc,yykbmc,yylbmc 
  from zxcpf as a inner join YYZY.T_YYZY_YYZDBMX as b on a.pydm=b.yydm and a.pfnf=b.yynf group by pydm,pfnf,yydjmc,yycdmc,dcdmc,yykbmc,yylbmc ) ,tcg_sl as ( 
  select a.yydm,a.yynf,a.cgkcsl ,a.yycdmc,a.yydjmc,a.yylbmc,a.yykbmc 
  from bysl_cg as a 
  where a.yydjmc not like '%挑%' 
  union all 
  select b.yydm,a.yynf,a.cgkcsl*0.35 as cgkcsl ,b.yycdmc,b.yydjmc,b.yylbmc, b.yykbmc 
  from bysl_cg as a inner join YYZY.T_YYZY_YYZDBMX as b on a.dcdmc=b.dcdmc and b.dcdmc=b.yycdmc and b.yydjmc='CG' and b.yylbmc='其它' and b.yykbmc='长烟梗' and a.yynf=b.yynf 
  union all 
  select b.yydm,a.yynf,a.cgkcsl ,b.yycdmc,b.yydjmc,b.yylbmc,b.yykbmc 
  from bysl_cg as a inner join YYZY.T_YYZY_YYZDBMX as b on a.yycdmc=b.yycdmc and a.yylbmc=b.yylbmc and b.yydjmc='C4F(次)' and b.yykbmc='单打片烟' and a.yynf=b.yynf 
  where a.yydjmc like '%挑%' 
  union all 
  select yydm,yynf,cgkcsl ,yycdmc,yydjmc,yylbmc,yykbmc 
  from bysl_cg 
  where yydjmc like '%挑%' ) ,cgkc as( 
  SELECT value(b.yydm,a.yydm) as yydm,value(b.yymc,'') as yymc,a.yynf, integer ( case when b.yylbmc='烤烟' and b.yydjmc like '%挑%' then cgkcsl*DY when b.yylbmc='烤烟' and b.yydjmc like '%选%' then cgkcsl*XY when b.yylbmc='烤烟' and b.yydjmc like '%次%' then cgkcsl*CY when b.yylbmc='烤烟' then cgkcsl*NY else cgkcsl 
  end ) as yykcjs,3 as kclx 
  from ( 
  select yydm,yynf,sum(cgkcsl) as cgkcsl 
  from tcg_sl 
  where cgkcsl>0 group by yydm,yynf ) as a left join YYZY.T_YYZY_YYZDBMX as b on a.yydm=b.YYDM and a.yynf=b.yynf inner join DIM.T_DIM_YYZY_PYBYGX as c on 1=1 ) 
  select yydm,yymc,yynf,yykcjs,kclx 
  from cgkc
;
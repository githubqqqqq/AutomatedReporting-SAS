libname EDWSMDM odbc dsn='EDW_SM_DM2' schema = dbo;
libname EDW odbc dsn='EDW' schema = dbo; 
libname biadm odbc dsn='BIA_DM' schema=dbo; 
libname media odbc dsn='CEPPROD' schema=dbo; 
libname ODSWAPRS ORACLE user=PPENG pw=happy11 path=ODSWAPR_S schema=ODSMGR;
libname PFS_DM odbc dsn='PowerFds' schema=dbo; 

options compress=yes; 

proc sql;
connect to odbc as db ( dsn="BIA_DM" ) ;
execute (


SELECT [period], [OppSFid],[GroupNewId],[Brandprofiledimkey],[BannerID],[CreatedDate],[ActivityId],
[InternationalFlag], [Level], [College_Name], [Program_Name] ,[SQ_flag],[Channel], 
[region_name], [Country_Name], [State], [EA_Manager],[EA_Location],[EA_name],
[Cohort_Month], [cohort_month_id], 
case when matched_start_date='6/27/16' and sq_flag='Q' then 
(select distinct start_date_group
                from [Walden_Pipeline_Data]
                where [matched_start_date]='6/6/16') 
when matched_start_date='6/27/16' and sq_flag='S' then 
(select distinct start_date_group
                from [Walden_Pipeline_Data]
                where [matched_start_date]='7/3/17') 
when matched_start_date='6/26/17'  then 
(select distinct start_date_group
                from [Walden_Pipeline_Data]
                where [matched_start_date]='6/12/17') 
else start_date_group end as start_date_group,
[matched_start_date],
[calendar_year_id], [last_stage],[last_disposition],
[App_7day],[App_30Day],[App_90Day],[Closed_7Day],[Closed_30Day],[Closed_90Day],[IsTempo],
case 
  when ([last_stage]='Pre-enroll') OR
([last_stage]='Student' AND ([last_disposition] not in ('Withdrawn','Inactive'))) then 'Pre-Enroll'
                when [last_stage]='Applicant' AND [last_disposition]='Admitted' then 'Admitted'
                when [last_stage]='Applicant' AND [last_disposition]='Admissions Review in Progress'  then 'Applicant (Completed)'
                when [last_stage]='Applicant' AND [last_disposition]='Complete - EA Ready for Review' then 'Applicant (Submitted)'
                when ([last_stage]='Applicant' AND ([last_disposition]='New' OR 
              [last_disposition]='.' OR 
                                                  [last_disposition]='None' OR 
                                                  [last_disposition]='New – No Outreach' OR
                                                  [last_disposition]='New - No Outreach' OR
                                                  [last_disposition]='Uncontacted')) then 'Applicant (New/Uncontacted)'
                when ([last_stage]='Applicant' AND 
              ([last_disposition]='Withdrawn' OR 
                                                  [last_disposition]='In Process' )) then 'Applicant (Incomplete)'
                when  [last_stage]='Active' OR [last_stage]='Qualified'  then 'Active/Qualified'
                when  [last_stage]='Open' then 'Open'
                when  [last_stage]='Closed Lost' then 'Closed'
    when  [last_stage]='Applicant' AND [last_disposition]='App Denied'  then 'Closed'
                else 'Other' end as Stage,
1 AS [Ct]
into #pipeline
FROM [Walden_Pipeline_Data]
WHERE [last_stage] in ('Pre-enroll','Student','Applicant','Qualified','Active','Open','Closed Lost')



) by db ;

create table pipeline as 
      select * from connection to db ( select * from #pipeline) ;
disconnect from db ;
quit;

/*proc freq data=pipeline;table period* matched_start_date/missing list;run;*/

proc freq data=pipeline;table last_disposition/missing;run;

proc freq data=pipeline;table start_date_group*matched_start_date/list missing;run;
 
data Walden_Pipeline_Data;
set pipeline ;
opps=1;
if Last_Stage in ('Pre-enroll','Student') then preEnrolled=1; else preEnrolled=0;
if Last_Stage in ('Applicant') and last_disposition='Admissions Review in Progress' then CompletedApp=1; else CompletedApp=0;
if Last_Stage in ('Applicant') and last_disposition='In Process' then InCompletedApp=1; else InCompletedApp=0;
where find(start_date_group,'Future')>0 and matched_start_date>0 and
internationalflag=0 and istempo=0 and last_stage not in ('Other') and college_name  in ('COEL','COHS','COMT','CSBS','CUGS');
run;/*matched start date only works when the user select specific start date*/

proc freq data=Walden_Pipeline_Data;table start_date_group*matched_start_date/list missing;run;


ods html close;
ods html style=statistical;
proc freq data=Walden_Pipeline_Data;table period* matched_start_date/missing list;run;
proc freq data=Walden_Pipeline_Data;table period period* preEnrolled   period* CompletedApp  period* InCompletedApp  last_stage College_Name/missing list;run;
 

/*
data Walden_Pipeline_Data1;
set Walden_Pipeline_Data(obs=10000);
run;

data Walden_Pipeline_Data2;
set Walden_Pipeline_Data(obs=10000); where period^='Current';
run;

data Walden_Pipeline_Data3;
set Walden_Pipeline_Data1 Walden_Pipeline_Data2;
if Last_Stage='Pre-enroll' then preEnrolled=1; else preEnrolled=0;
run;
*/

%let fixedVars=InternationalFlag,start_date_group;
%let fixedVarList=InternationalFlag start_date_group;
%let varlist2='channel' 'program_name' 'state' ;
%let varlist=channel program_name state;
%let inputdata=Walden_Pipeline_Data;
%let kpiList=opps CompletedApp InCompletedApp preEnrolled;
%let total=3;
%let alt=LY;

%inc '\\loe.corp\ex\RNA\RNA\Baltimore\Qi\Automated Insights\Code\Version 3.0\macro - 1. dimensions fixed KPI.sas';
%inc '\\loe.corp\ex\RNA\RNA\Baltimore\Qi\Automated Insights\Code\Version 3.0\macro - 2. combos - changing KPI.sas';

%dimensions_KPI;

libname BIADM odbc dsn="BIA_DM2" schema=dbo ;
proc sql;
	connect to odbc (dsn='BIA_DM');
	EXEC (if object_id('bi_analytics_dm.dbo.WaldenPipelineAutomatedInsightV3', 'U') is not null drop table bi_analytics_dm.dbo.WaldenPipelineAutomatedInsightV3) by odbc;
	disconnect from odbc;

quit;

*a. conversion rate;
data biadm.WaldenPipelineAutomatedInsightV3;
	set all_combinations_kpi;
run;




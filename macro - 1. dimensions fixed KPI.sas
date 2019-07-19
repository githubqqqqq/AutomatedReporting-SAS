
%macro dimensions(kpi);

********************take all input variables out***********************************;
%let counter=1;
%do %while(%scan(&varlist,&counter,%str( )) ne %str() );
  %let var&counter = %scan(&varlist, &counter,%str( ));
  %let counter=%eval(&counter + 1);
%end;
%let counter=%eval(&counter - 1);
%put %%%%%%%%%%%% &counter variables detected;

********************take all input variables out FIXED***********************************;
%let counterF=1;
%do %while(%scan(&fixedVarList,&counterF,%str( )) ne %str() );
  %let FixedVar&counterF = %scan(&fixedVarList, &counterF,%str( ));
  %let counterF=%eval(&counterF + 1);
%end;
%let counterF=%eval(&counterF - 1);
%put %%%%%%%%%%%% &counterF variables detected and &fixedvar1;

********************impute missing values***********************************;
data &inputdata._1;
set &inputdata.;
%do i=1 %to &counter;
if strip(&&var&i)='' then  &&var&i='---MISSING---';
%end;
%do i=1 %to &counterF;
if strip(&&FixedVar&i)='' then  &&FixedVar&i='---MISSING---';
%end;
run;

*******************Total Observation***************************************;
proc sql noprint;
select count(*) into : Total_CY from  &inputdata._1 where period='Current';
quit;
proc sql noprint;
select sum(&kpi.) into : KPI_CY from  &inputdata._1 where period='Current';
quit;
********************generate all combinations***********************************;
%do i=1 %to &counter;
	data combinations&i;
	   array x[&counter] $100 (&varlist2);
	   n=dim(x);
	   k=&i.;
	   ncomb=comb(n,k);
	   do j=1 to ncomb;
	      call allcomb(j, k, of x[*]);
		  %do j=1 %to &i.;
		      z&j. =  x&j. ;
		  %end;
		  output;
	   end;
	run;
%end;
********************combine all combinations***********************************;
data combinations;
set %do i=1 %to &counter; combinations&i. %end;;
run;

proc datasets lib=work; delete %do i=1 %to &counter; combinations&i. %end ;
quit;

********************dedup in case of dups***********************************;
proc sql;
create table combinations_nodup as
select distinct %do i=1 %to &counter; z&i., %end; k from combinations;
quit;


******************Calculate Overall***********************;
		
			proc sql;
			create table AllDimensionsCY as
			select &fixedVars, count(*) as total, sum(&kpi) as KPI,
			(calculated KPI)*1.00/calculated total   as pctKPI,calculated total/&total_cy. as pctV_Total format=percent9., calculated KPI/&KPI_CY. as PctV_KPI format=percent9.
			from &inputdata._1 where period='Current'
			group by  &fixedVars 
			;
			quit;

			proc sql;
			create table AllDimensionsLY as
			select &fixedVars ,count(*) as total_&alt., sum(&kpi) as KPI_&alt.,
			(calculated  KPI_&alt.)*1.00/calculated total_&alt.   as pctKPI_&alt.
			,calculated total_&alt./&total_cy. as pctV_Total_&alt. format=percent9., calculated KPI_&alt./&KPI_CY. as PctV_KPI_&alt. format=percent9.
			from &inputdata._1 where period^='Current'
			group by  &fixedVars 
			;
			quit;


				proc sql;
				create table KPI_combL as
				select  "0" as NumDimension, 'Overall' as Combo, *,case when  pctKPI is null then 0-pctKPI_&alt.  when  pctKPI_&alt.  is null then  pctKPI-0 else  pctKPI-pctKPI_&alt. end as Diff_pctKPI,
				case when pctKPI is null then  ( 0-pctKPI_&alt. )/(pctKPI_&alt.) 
					 when pctKPI_&alt. is null then ( pctKPI-0)/(0.000001)  
					 else ( pctKPI-pctKPI_&alt. )/(pctKPI_&alt.+0.000001) 
				end as pct_Diff_pctKPI  format=percent9.,
				case when KPI is null then  ( 0-KPI_&alt.) 
					 when KPI_&alt. is null then KPI  
					 else KPI-KPI_&alt.
				end as Diff_KPI ,
				case when KPI_&alt. is null then 100 else calculated Diff_KPI/KPI_&alt. end as Diff_KPI_Pct
				from AllDimensionsCY a left join AllDimensionsLY b on 1=1 %do j=1 %to &CounterF; and a.&&fixedvar&j=b.&&fixedvar&j %end;;
				quit;

				proc sql;
				create table KPI_combR as
				select  "0" as NumDimension, 'Overall' as Combo, *,case when  pctKPI is null then 0-pctKPI_&alt.  when  pctKPI_&alt.  is null then  pctKPI-0 else  pctKPI-pctKPI_&alt. end as Diff_pctKPI,
				case when pctKPI is null then  ( 0-pctKPI_&alt. )/(pctKPI_&alt.) 
					 when pctKPI_&alt. is null then ( pctKPI-0)/(0.000001)  
					 else ( pctKPI-pctKPI_&alt. )/(pctKPI_&alt.+0.000001) 
				end as pct_Diff_pctKPI  format=percent9.,
				case when KPI is null then  ( 0-KPI_&alt.) 
					 when KPI_&alt. is null then KPI  
					 else KPI-KPI_&alt.
				end as Diff_KPI ,
				case when KPI_&alt. is null then 100 else calculated Diff_KPI/KPI_&alt. end as Diff_KPI_Pct
				from AllDimensionsLY a left join AllDimensionsCY b on 1=1 %do j=1 %to &CounterF; and a.&&fixedvar&j=b.&&fixedvar&j %end;
				where b.&fixedvar1 is null;  
				quit;

			data KPI_Comb0;
			set KPI_combL(in=a) KPI_combR(in=b);
			run;



*******************create macro variables for different combinations*********************************;
data combinations_nodup;
set combinations_nodup;
n=_n_;
run;
proc sql noprint;
select max(n) into : num_comb from combinations_nodup;/*this is the number of combinations*/
quit;

%do i=1 %to &num_comb; /*&num_comb; i is just a sequency number*/
		%put %%%%%%%%%%%% &i.;
			proc sql noprint;
				select k into : num_vars from combinations_nodup where n=&i; /*count number of vars in the comb*/
			quit;
			%put %%%%%%%%%%%% &num_vars;
			%if &num_vars=1 %then %do;
					data temp; 
					set combinations_nodup; where n=&i;
					length combo combo_alt $200;
					combo=strip(z1);
					combo_alt=compbl(strip(z1) || "_&alt." );
					run;

			%end;
			%else %do;
					data temp; 
					set combinations_nodup; where n=&i;
					length combo $200;
					combo=compbl(strip(z1)  %do j=2 %to &num_vars; || "," || strip(z&j) %end; );
					combo_alt=compbl(strip(z1)|| "_&alt."    %do j=2 %to &num_vars; || "," || strip(z&j) || "_&alt."  %end; );
					run;
			%end;

			%do j=1 %to &num_vars;
						proc sql noprint;
						select strip(z&j) into : z&j from temp; /*create individual variable*/
						quit;
			%end;
						
							proc sql noprint;
							select strip(combo) into : comb&i /*create list of variables*/
							from temp;
							quit;


							proc sql noprint;
							select strip(combo_alt) into : combo_alt&i/*create list of variables in alternative names*/
							from temp;
							quit;
			%put %%%%%%%%%%%% &&comb&i. &&combo_alt&i. &z1 ;


*************************Calculate by dimension**************************;


			proc sql;
			create table AllDimensionsCY as
			select &fixedVars, &&comb&i. ,count(*) as total, sum(&kpi) as KPI,
			(calculated KPI)*1.00/calculated total   as pctKPI,calculated total/&total_cy. as pctV_Total format=percent9., calculated KPI/&KPI_CY. as PctV_KPI format=percent9.
			from &inputdata._1 where period='Current'
			group by  &fixedVars, &&comb&i.
			;
			quit;

			proc sql;
			create table AllDimensionsLY as
			select &fixedVars, &&comb&i. ,count(*) as total_&alt., sum(&kpi) as KPI_&alt.,
			(calculated  KPI_&alt.)*1.00/calculated total_&alt.   as pctKPI_&alt.
			,calculated total_&alt./&total_cy. as pctV_Total_&alt. format=percent9., calculated KPI_&alt./&KPI_CY. as PctV_KPI_&alt. format=percent9.
			from &inputdata._1 where period^='Current'
			group by  &fixedVars,  &&comb&i. 
			;
			quit;

			%if &num_vars=1 %then %do;
				proc sql;
				create table KPI_combL as
				select "&num_vars  " as NumDimension, strip("By: &&comb&i") as Combo, 
				*,case when  pctKPI is null then 0-pctKPI_&alt.  when  pctKPI_&alt.  is null then  pctKPI-0 else  pctKPI-pctKPI_&alt. end as Diff_pctKPI,
				case when pctKPI is null then  ( 0-pctKPI_&alt. )/(pctKPI_&alt.) 
					 when pctKPI_&alt. is null then ( pctKPI-0)/(0.000001)  
					 else ( pctKPI-pctKPI_&alt. )/(pctKPI_&alt.+0.000001) 
				end as pct_Diff_pctKPI  format=percent9.,
				case when KPI is null then  ( 0-KPI_&alt.) 
					 when KPI_&alt. is null then KPI  
					 else KPI-KPI_&alt.
				end as Diff_KPI ,
				case when KPI_&alt. is null then 100 else calculated Diff_KPI/KPI_&alt. end as Diff_KPI_Pct

				from AllDimensionsCY a left join AllDimensionsLY b on   a.&z1=b.&z1 %do j=1 %to &CounterF; and a.&&fixedvar&j=b.&&fixedvar&j %end;;  
				quit;
			%end;
			%else %do;
				proc sql;
				create table KPI_combL as
				select  "&num_vars  " as NumDimension, strip("By: &&comb&i") as Combo, *,case when  pctKPI is null then 0-pctKPI_&alt.  when  pctKPI_&alt.  is null then  pctKPI-0 else  pctKPI-pctKPI_&alt. end as Diff_pctKPI,
				case when pctKPI is null then  ( 0-pctKPI_&alt. )/(pctKPI_&alt.) 
					 when pctKPI_&alt. is null then ( pctKPI-0)/(0.000001)  
					 else ( pctKPI-pctKPI_&alt. )/(pctKPI_&alt.+0.000001) 
				end as pct_Diff_pctKPI  format=percent9.,
				case when KPI is null then  ( 0-KPI_&alt.) 
					 when KPI_&alt. is null then KPI  
					 else KPI-KPI_&alt.
				end as Diff_KPI ,
				case when KPI_&alt. is null then 100 else calculated Diff_KPI/KPI_&alt. end as Diff_KPI_Pct
				from AllDimensionsCY a left join AllDimensionsLY b on a.&z1=b.&z1  %do j=1 %to &num_vars.; 	and a.&&z&j=b.&&z&j  %end; 
                     %do j=1 %to &CounterF; and a.&&fixedvar&j=b.&&fixedvar&j %end;;
				quit;
			%end;

			%if &num_vars=1 %then %do;
				proc sql;
				create table KPI_combR as
				select  "&num_vars  " as NumDimension, strip("By: &&comb&i") as Combo, *,case when  pctKPI is null then 0-pctKPI_&alt.  when  pctKPI_&alt.  is null then  pctKPI-0 else  pctKPI-pctKPI_&alt. end as Diff_pctKPI,
				case when pctKPI is null then  ( 0-pctKPI_&alt. )/(pctKPI_&alt.) 
					 when pctKPI_&alt. is null then ( pctKPI-0)/(0.000001)  
					 else ( pctKPI-pctKPI_&alt. )/(pctKPI_&alt.+0.000001) 
				end as pct_Diff_pctKPI  format=percent9.,
				case when KPI is null then  ( 0-KPI_&alt.) 
					 when KPI_&alt. is null then KPI  
					 else KPI-KPI_&alt.
				end as Diff_KPI ,
				case when KPI_&alt. is null then 100 else calculated Diff_KPI/KPI_&alt. end as Diff_KPI_Pct
				from AllDimensionsLY a left join AllDimensionsCY b on a.&z1=b.&z1 %do j=1 %to &CounterF; and a.&&fixedvar&j=b.&&fixedvar&j %end;
				where b.&z1 is null;  
				quit;
			%end;
			%else %do;
				proc sql;
				create table KPI_combR as
				select  "&num_vars  " as NumDimension, strip("By: &&comb&i") as Combo, *,case when  pctKPI is null then 0-pctKPI_&alt.  when  pctKPI_&alt.  is null then  pctKPI-0 else  pctKPI-pctKPI_&alt. end as Diff_pctKPI,
				case when pctKPI is null then  ( 0-pctKPI_&alt. )/(pctKPI_&alt.) 
					 when pctKPI_&alt. is null then ( pctKPI-0)/(0.000001)  
					 else ( pctKPI-pctKPI_&alt. )/(pctKPI_&alt.+0.000001) 
				end as pct_Diff_pctKPI  format=percent9.,
				case when KPI is null then  ( 0-KPI_&alt.) 
					 when KPI_&alt. is null then KPI  
					 else KPI-KPI_&alt.
				end as Diff_KPI ,
				case when KPI_&alt. is null then 100 else calculated Diff_KPI/KPI_&alt. end as Diff_KPI_Pct
				from AllDimensionsLY a left join AllDimensionsCY b on a.&z1=b.&z1  %do j=1 %to &num_vars.;  and a.&&z&j=b.&&z&j  %end;
                  %do j=1 %to &CounterF; and a.&&fixedvar&j=b.&&fixedvar&j %end;
				where b.&z1 is null;
				quit;
			%end;

			data KPI_Comb&i.;
			set KPI_combL(in=a) KPI_combR(in=b);
			run;
/*
			proc sort data=KPI_Comb&i. nodupkey;
			by _all_;
			run;
*/
	

****************************************Calculate percentage of contribution*************************;


					proc sql;
					select sum(Diff_KPI) into: total_positive
					from KPI_Comb&i. where Diff_KPI>0;
					quit;

					proc sql;
					select sum(Diff_KPI) into: total_negative
					from KPI_Comb&i. where Diff_KPI<0;
					quit;

					data KPI_Comb&i.;
					set KPI_Comb&i.;
					length contribution $15;
					if Diff_KPI>0 then do;
						pct_KPI_Diff_pos=Diff_KPI/&total_positive.;
						contribution='positive';
					end;
					else if Diff_KPI<0 then do;
						pct_KPI_Diff_pos=Diff_KPI/&total_negative.;
						contribution='negative';

					end;
					format pct_KPI_Diff_pos percent9.2;
					run; 



****************************************Combine datasets*******************************************;

		proc sort data=KPI_Comb&i.; by  Diff_KPI ;run;


%end;

data all_combinations;
length combo $200;
set %do i=1 %to  &num_comb; KPI_comb&i. %end; KPI_Comb0;
run;

proc sort data=all_combinations;by Diff_KPI ;run;
data all_combinations_&kpi.;
length KPI_Name $20;
set all_combinations;
by Diff_KPI ;
N=_N_;
KPI_Name="&kpi.";

run;


**********************************create combinations******************************************************;
 			proc datasets lib=work noprint; delete AllDimensionsLY AllDimensionsCY KPI_combL KPI_combR temp  ;
			quit;


%mend;

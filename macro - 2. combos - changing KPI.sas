

%macro dimensions_KPI;

proc datasets;delete all_combinations_kpi noprint;quit;

********************take all input variables out***********************************;
%let KPI_counter=1;
%do %while(%scan(&kpiList,&KPI_counter,%str( )) ne %str() );
  %let KPIvar&KPI_counter = %scan(&kpiList, &KPI_counter,%str( ));
  %put ################################################## &&KPIvar&KPI_counter ;
  proc datasets;delete all_combinations_&&KPIvar&KPI_counter;quit;

    %dimensions( &&KPIvar&KPI_counter );

     proc append data=all_combinations_&&KPIvar&KPI_counter base= all_combinations_KPI force;quit;

	%let KPI_counter=%eval(&KPI_counter + 1);
%end;

%let counter=%eval(&KPI_counter - 1);
%put %%%%%%%%%%%% &KPI_counter KPI variables detected;



%mend; 


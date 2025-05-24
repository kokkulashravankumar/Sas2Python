* PROGRAM: CLEANUP.SAS;
* DESCRIPTION: CLEANS UP ANY DATASETS IN THE WORK DIRECTORY;

%macro cleanup;
proc sql;
   create table members as
          select memname
          from dictionary.tables
          where libname='WORK' and memtype = 'DATA';
quit;
run;

data _null_;
call symput('numds',trim(left(put(numds,2.))));
stop;
set members nobs=numds;
stop;
run;

%if "&numds" = "0" %then %do;
%end;
%else %do;
proc datasets library=work kill;
quit;
%end;
run;
%mend;

%cleanup;
run;

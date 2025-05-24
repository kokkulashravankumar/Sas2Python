/***
$Header: /SAS/1 - Development/MACLIB/define_variables.sas 15    7/07/11 10:38p Cwang $
***/
 
/* cw #210588 #214630 #226194: remove first parameter  tablex. keep the second one: infile */
%macro define_variables(infile);
/* Description:  Generate program statements from MCORCAT Table Mapping                  */
%global svclinesf claimsf numflds dateflds has_prvtaxcod;
%let has_prvtaxcod=0;
 
/* cw #210588 #214630 #226194: remove some fields that are not needed to improve process  */
%let hldfld=	clamno
				lineno
				revsto
				revind
				svcstat
				membno
				svccod
				svctyp		
				aprectyp
				apsubsys
				svcdat
				enddat
				aptrndat			
				formcd
				revcod
				ipopind
				caretype
				alwamt
				compno
				medcty
				revsfr
				unitct
				prod prvorg provno
extclm  disdat firstdos lastdos  admdat patnum
primdiag diagd2 diagd3 diagd4 diagd5 diagd6 diagd7 diagd8 diagd9 diag10 diag11 diag12 diag13 diag14 diag15 				
			  ;
 
/* Capure common variables used in SVCLINES & CLAIMS*/
data hldfld;
length varname $40;
%let j=1;
%do %until(%scan(&hldfld,&j)=);
%let varname=%scan(&hldfld,&j);
varname="&varname";
output;
%let j=%eval(&j + 1);
%end;
keep varname;
run;
 
 
/* cw #210588 #214630 #226194: expand error check for all fields ----- start  */
/* Capure all field name in mcormapping.varname */
data tb_varname;	
	length name nameout $40.;
	set &infile;
	if varname ne ' ' then do;
				name = varname;									
				select;
					when (kindex(name,'(') > 0 and kindex(name,',')= 0) do;
						x = kindex(name,'(') + 1;	
						y = kindex(name,')');
						nameout = klowcase(ksubstr(name,x, y-x ));
						output;
					end;
	
					when (kindex(name,'(') > 0 and kindex(name,',') > 0) do; 	
					    j = kindex(name,'(') + 1;	
						i = j;
						
						do until(ksubstr(name,i,1)=')');
						   if ksubstr(name,i,1) = ',' then do;
			                   nameout = klowcase(ksubstr(name,j, i-j ));
							   if klength(nameout) < 3 then delete;
							   output ;
							   j = i + 1;
						   end;
						   i+1;
		                end;
						nameout = klowcase(ksubstr(name,j, i-j));
						output;
						
		        	end;											
							
		    		otherwise do;
						nameout = klowcase(name);
						if klength(nameout) < 3 then delete;
	                	output ;	
					end;
	            end;
end;
 
run;
 
/* Capure all field name in mcormapping.varname2 */
data tb_varname2;	
	length name nameout $40.;
	set &infile;
	if varname2 ne ' ' then do;
				name = varname2;									
				select;
					when (kindex(name,'(') > 0 and kindex(name,',')= 0) do;
						x = kindex(name,'(') + 1;	
						y = kindex(name,')');
						nameout = klowcase(ksubstr(name,x, y-x ));
						output;
					end;
	
					when (kindex(name,'(') > 0 and kindex(name,',') > 0) do; 	
					    j = kindex(name,'(') + 1;	
						i = j;
						
						do until(ksubstr(name,i,1)=')');
						   if ksubstr(name,i,1) = ',' then do;
			                   nameout = klowcase(ksubstr(name,j, i-j ));
							   if klength(nameout) < 3 then delete;
							   output ;
							   j = i + 1;
						   end;
						   i+1;
		                end;
						nameout = klowcase(ksubstr(name,j, i-j));
						output;
						
		        	end;											
							
		    		otherwise do;
						nameout = klowcase(name);
						if klength(nameout) < 3 then delete;
	                	output ;	
					end;
	            end;
end;
																		
			
run;
/* cw #210588 #214630 #226194: expand error check for all fields ----- end */
 
data fldmcor(rename=(nameout=varname));
set tb_varname tb_varname2;
if nameout in ('prvtaxcod') then do;
	call symput('has_prvtaxcod','1');
end;
if nameout in ('prvtaxcod' 'nysmeds1' 'nysmeds2' 'nysmeds3' 'nysmeds4' 'nysmeds5' ' ') then delete;
keep nameout;
run;
 
proc sort data=fldmcor nodupkey;					
	by varname;										
run;												
 
/* Combine all variable names captured*/
data totfields;
set hldfld fldmcor;				/* KS - 202643 - 06282010 *//* cw #210588 #214630 #226194*/
varname = klowcase(varname);
rename varname=name;
run;
 
/* Dedupe all variable names captured*/
proc sort data=totfields nodupkey;
by name;
run;
 
/* Capure all variables used in SVCLINES*/
proc contents data=&libn1..svclines out=svclines noprint;
run;
 
/* Capure all variables used in CLAIMS*/
proc contents data=&libn1..claims out=claims noprint;
run;
 
/* Determine what varibles will be selected in SVCLINES */
proc sql;
create table svclinesf as
select distinct name
from totfields
where kupcase(name)  in
(select distinct kupcase(name)
from  svclines);
quit;	
 
/* Determine what varibles ARE NOT in SVCLINES */
proc sql;
create table svclinesn as
select distinct name
from totfields
where kupcase(name) not in
(select distinct kupcase(name)
from  svclines);
quit;	
 
/* Determine what varibles will be selected in CLAIMS */
proc sql;
create table claimsf as
select distinct name
from svclinesn
where kupcase(name)  in
(select distinct kupcase(name)
from  claims);
quit;
 
/*PREPARE the variable names to be selected for SVCLINES */
data temp;
		set svclinesf;
		length testvalue $ 25;
		testvalue=kcompress('a.'||name||',');
		run;
 
/*CREATE the variable name list to be selected for SVCLINES */
proc sql noprint;
	select testvalue into :svclinesf separated by ' ' from temp;
quit;
 
/*PREPARE the variable names to be selected for CLAIMS */
data temp;
		set claimsf;
		length testvalue $ 25;
		testvalue=kcompress('b.'||name||',');
run;
 
/*CREATE the variable name list to be selected for CLAIMS */
proc sql noprint;
	select testvalue into :claimsf separated by ' ' from temp;
quit;
 
/* Capture all DATE FIELDS from SVCLINES & CLAIMS */
data dateflds;
	set svclines claims;
	if format = 'DATE'; /*SAS2AWS2: Updated 'MMDDYY' as 'DATE'*/
	keep name;
run;
 
/* DEDUPE the DATE FIELDS */
proc sort data=dateflds nodupkey;
by name;
run;
 
/* PREPARE the DATE FIELDS */
data dateflds;
		set dateflds;
		length testvalue $ 25;
		testvalue=compress("'"||kupcase(name)||"'");
run;
 
/*CREATE the variable name list to be selected for DATE FIELDS */
proc sql noprint;
	select testvalue into :dateflds separated by ' ' from dateflds;
quit;
 
/* Capture all NUMERIC FIELDS from SVCLINES & CLAIMS */
data numflds;
	set svclines claims;
	if type = 1 and format ne 'DATE'; /*SAS2AWS2: Updated 'MMDDYY' as 'DATE'*/
	keep name;
run;
 
/* DEDUPE the NUMERIC FIELDS */
proc sort data=numflds nodupkey;
by name;
run;
 
/* PREPARE the NUMERIC FIELDS */
data numflds;
		set numflds;
		length testvalue $ 25;
		testvalue=compress("'"||kupcase(name)||"'");
run;
 
/*CREATE the variable name list to be selected for NUMERIC FIELDS */
proc sql noprint;
	select testvalue into :numflds separated by ' ' from numflds;
quit;
%mend ;	


/***       
$Header: $  
***/ 
option nomlogic nomprint nosymbolgen nomacrogen;
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19701 Program Start execution ~~~~~~~~~~~~~~~";
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/HIOSID_Xwalk.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/HIOSID_Xwalk.xlsx"
	out=HIOSID (drop=comments) dbms=xlsx   
	replace;
Run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NDC_SVCCODE_REJECT.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/x12_ctrl_tab.xls &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 


Data _null_;
	 a=sleep(60,1);
run;


proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/x12_ctrl_tab.xls"
	out=x12_ctrl_tab dbms=xls
	replace;sheet='REPORT';  
run;

data _null_; 
  set x12_ctrl_tab(where=(compno="&compno." and lobcod="&LOB."));   
  call symput('x12_ctrl',kcompress(x12_ctrl));
run;

%put &x12_ctrl.;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/ &drv2./Encounters_Reporting/APD/Control_Table/  --recursive --exclude '*' --include '*.sas7bdat'  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

/***********************************************************************************************/
/****					Assigning librefs to macro variables								****/
/***********************************************************************************************/
%let RGA191 = work 	;
%let RGA191_ = RGA191;
%let RGA192 = RGA192;	 
%let RGA193 = RGA193;	
%let R42173 = RGA193;  

/********************************************************************************************/
/****					Dates from frames - Start  						   				 ****/ 
/********************************************************************************************/

Data _null_;
 format today_dt date9.;
 today_dt=today();
 call symput('today_dt',today_dt);
run;

%macro QI_file_arc();
   %global ddir;	 
	%let path1 = &drv72./Encounters_Reporting/APD/Input_File ;

  data _null_;
   length ddir $10.;          
   ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
   call symput('ddir',ddir);					
  run;
               
  %let destpath = &path1./&ddir.;

%mend;
%QI_file_arc();

%macro AssignDt;	 
	
   data _null_;	
   	length run_dttime $ 50.	;
 	p1str    = "'";
    pstr1    = "'d";
    
    fi_ds_dt = put(&today_dt.,yymmddn8.);	   
	r_date=put(&today_dt.,mmddyy10.);
	r_date1=put(&today_dt.,julian6.);
	   
	call symput('fi_ds_dt',kcompress(fi_ds_dt)); 
	call symput('r_date',kcompress(r_date));
	call symput('r_date1',kcompress(r_date1));   
	   
	run_dt = put(&today_dt.,mmddyy10.);
	run_date = put(&today_dt.,date7.);
	run_time = put(time(),time10.); 
	run_dttime = catx('_' ,run_date,run_time);
	
	st_time = put(datetime(),DATETIME20.);
	fi_ds_dt = put(&today_dt.,yymmddn8.);
	fi_ds_dt_T=put(&today_dt.,mmddyy10.);	
	
	call symput('run_date',kcompress(run_date));    
    call symput('run_dttime',kcompress(run_dttime,": "));	
	call symput('st_time',kcompress(st_time));	
	call symput('fi_ds_dt',kcompress(fi_ds_dt));	
	call symput('run_dt',kcompress(run_dt));	
	call symput('fi_ds_dt_T',kcompress(fi_ds_dt_T));
	   
%if &QIfiflag=Y %then %do;
	 svcdat  = put(&svcdat,date9.);	 
	 test=ktrim(p1str)||ktrim(svcdat)||ktrim(pstr1);
	 call symput('svcdat',test);      
%end;
%else %do;
	   startdt = put(&Begf,date9.);
       enddt   = put(&Endf,date9.);
	   svcdat  = put(&svcdat,date9.);   
       
       rptdate1 = ktrim(p1str)||ktrim(startdt)||ktrim(pstr1);        
       rptdate2 = ktrim(p1str)||ktrim(enddt)||ktrim(pstr1);
       
	   call symput('startdate',rptdate1);
       call symput('enddate',rptdate2);
       
       begfdt  = put(&Begf,date9.);
       endfdt  = put(&Endf,date9.);
        
       svcdate1 = ktrim(p1str)||ktrim(begfdt)||ktrim(pstr1);        
       svcdate2 = ktrim(p1str)||ktrim(endfdt)||ktrim(pstr1);	    
	   test=ktrim(p1str)||ktrim(svcdat)||ktrim(pstr1);  

       call symput('begfdt',begfdt);
       call symput('endfdt',endfdt);

	   call symput('begfirstd',svcdate1);
       call symput('endfirstd',svcdate2);
	   call symput('svcdat',test);
%end;
   run;
%mend AssignDt;
%AssignDt;

data _null_;
call symputx('compno', symget('compno'));
run;

/********************************************************************************************/
/****					Dates from frames - End  						   				 ****/ 
/********************************************************************************************/

/* Run id Assignment */


proc sql; 
	select "&r_date1."||put(sum(count(distinct(input(run_id,7.))),1),Z2.)
	into :run_id
	from &R42173..medical_job_mstr where Run_date ="&r_date." ;
quit;

%let run_id=&run_id.;
%let LOB=&lob;
%let LOB_=&lob_;

%put &LOB.;
%put &LOB_;
%put &run_id.;

x "rm -f &drv2./Encounters_Reporting/APD/Current/*.log"; 
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.txt";  
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.xls";
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.xlsx"; 
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.csv"; 
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Edit_Matrix_script_&compno..sas"; 
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Control_Table_script_&compno..sas"; 
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Prof_layout_script_&compno..sas"; 
x "rm -f &drv2./Encounters_Reporting/APD/Temp/INST_layout_script_&compno..sas"; 
x "rm -f &drv2./Encounters_Reporting/APD/Temp/varlist_script_&compno..sas"; 
 
proc printto print = "&drv2./Encounters_Reporting/APD/Current/&lib._837_Control_Report_&Compno._&fi_ds_dt._&run_id..log" new ;
run;
 
proc printto log = "&drv2./Encounters_Reporting/APD/Current/&lib._837_Log_File_&Compno._&fi_ds_dt._&run_id..log" new ;
run;
 

%let log_file =&drv2./Encounters_Reporting/APD/Current/&lib._837_Log_File_&Compno._&fi_ds_dt._&run_id..log;

options obs = max nosource2 nosource nosymbolgen nomlogic nomprint  
							pagesize = max linesize = max missing=" " nodate ;

/* For SHP hiosid mapping*/

%macro hios_id;
%if &Compno.=02 %then %do;
data _null_;
hios_id=kreverse(ksubstr(kreverse(kscan("&fname.",1,'.')),1,5));
call symput('hios_id',hios_id);	
run;
%put &hios_id.;
%end;
Data HIOSID;
	set HIOSID;
	%if &Compno.=02 %then %do;
			where compno="&compno." and lobcod="&LOB." and HIOSID = "&hios_id.";
	%end;
	%else %do;
			where compno="&compno." and lobcod="&LOB.";
	%end;
	call symput('HIOSID',HIOSID);
	call symput('cname',Compname);
	call symput('TPAID',TPAID);
Run;
%let HIOSID=&HIOSID.;
%let cname=&cname.;
%let TPAID=&TPAID.;
%put &HIOSID. &cname. &TPAID.;
%mend hios_id;
%hios_id;

%macro job_mst_update;

data _null_; 
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type. &drv2./Encounters_Reporting/APD/Control_Report/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

Data _null_;
	 a=sleep(60,1);
run;


%if %sysfunc(fileexist("&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type.")) =1 %then %do;  
		
		Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type."
		out=Control_Report dbms=&extn_type. replace;
		run;
		Data Control_Report1(keep=sequence_number Load_flag_new typecode split_status_new treo_flag_new);
				set Control_Report;
				where missing(sequence_number)=0 and missing(typecode)=0;
				rename Load_flag=Load_flag_new
					   split_status=split_status_new
					   treo_flag=treo_flag_new
					   ;			   
		Run;
		Proc sort data=&R42173..medical_job_mstr;by sequence_number typecode;run;
		Proc sort data=Control_Report1;by descending Load_flag_new ;run;
		Proc sort data=Control_Report1 nodupkey;by sequence_number typecode;run;
		Data &R42173..medical_job_mstr;
			merge &R42173..medical_job_mstr(in=_a) Control_Report1(in=_b);
			if _a;
			if _a and _b then  do;
				Load_flag=Load_flag_new;
				split_status=split_status_new;
				 treo_flag=treo_flag_new;/* HS 542635 - kv*/
			end;
			by sequence_number typecode;
			drop Load_flag_new split_status_new treo_flag_new;
		Run;
%end;	
%mend;
%job_mst_update;

/*********************Reading QI file ****************************************/

  Data &RGA191..finclaims_&compno.  ;     
    infile "&drv2./Encounters_Reporting/APD/Import/&fname." truncover delimiter='09'x DSD lrecl=32767
                     firstobs = 2 ;  
	informat clamno 		$20.   
			 lineno 		$05.   
			 clamno_mcd 	$20. 
			 lineno_mcd 	$05.
			 clamno_mcr 	$20. 
			 lineno_mcr 	$05.			 
             claimfrq 		$01.
			 icn 			$22. 
		;
    input    clamno 	$ 
			 lineno		$
			 claimfrq 	$ 
			 icn		$
		;
		
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;
		
			clamno_mcd=''; clamno_mcr=''; lineno_mcd=''; lineno_mcr='';
			mcd_lin_missfl=0; mcr_lin_missfl=0;
	run;

/********************************QI file read ends*****************************/

/********************************************************************************************/
/****					Clearing Current directory 						   				 ****/ 
/********************************************************************************************/
proc datasets lib= &RGA191_. nolist kill;
quit;
run;

/********************************************************************************************/
/****					Updating control table with run id & dummy values initially		 ****/ 
/********************************************************************************************/
Proc Sql;
                insert into &R42173..medical_job_mstr
                values("&run_id.",'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0',"&st_time.",' ',' ','N ',' ',' ',"&fi_ds_dt_T.",' ',' ','N',"&LOB.",'N','   ',0,0,0); /*272515 added cnt variable*/
quit;

/********************************************************************************************/
/****					Layout file Location declaration	 ****/ 
/********************************************************************************************/

filename InpPrg 	"&drv./Programs/";
filename EmOut 		"&drv2./Encounters_Reporting/APD/Temp/Edit_Matrix_script_&Compno..sas" 				;
filename ctrlout 	"&drv2./Encounters_Reporting/APD/Temp/Control_Table_script_&Compno..sas"			;
filename Prof		"&drv2./Encounters_Reporting/APD/Temp/Prof_layout_script_&compno..sas";
filename INST		"&drv2./Encounters_Reporting/APD/Temp/INST_layout_script_&compno..sas";
filename varlist 	"&drv2./Encounters_Reporting/APD/Temp/varlist_script_&compno..sas";

%global ctrl_tab;	

/*******Control Tab Assignment */

%macro ctrl;
%if (&compno=45 and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do;	 
  %if "&x12_ctrl." eq "EIS" %then %do;			
	%let ctrl_tab = R42171_ctrl_tab_eis;
  %end;
  %else %do;
	%let ctrl_tab = R42171_ctrl_tab;		
  %end;
%end;

%else %do;
  %if "&x12_ctrl." eq "EIS" %then %do;			
 	%let ctrl_tab = RGA191_ctrl_tab_eis;
  %end;
  %else %do;
	%let ctrl_tab = RGA191_ctrl_tab;
 %end;
%end;

%put &x12_ctrl. &ctrl_tab.;
%mend;
%ctrl;

%put &rga193.;
%put &ctrl_tab.;

Data _null_;
Inst_sheet = "&ctrl_tab."||"_"||"Inst";
Prof_sheet = "&ctrl_tab."||"_"||"Prof";
call symputx("Inst_sheet",Inst_sheet);
call symputx("Prof_sheet",Prof_sheet);
run;

%put &inst_sheet.;
%put &prof_sheet.;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/apd_adhoc_submission_ctrltab.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

%macro ctrl_input_file(ind,sheet);

proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/apd_adhoc_submission_ctrltab.xlsx"
	out=&RGA191..&ind.837_1_Pass_&compno._&fi_ds_dt._&run_id. dbms=xlsx
	replace; sheet = &sheet.; 
run;
%mend;

%ctrl_input_file(I,&inst_sheet.);
%ctrl_input_file(P,&prof_sheet.); 

data &RGA191..p837_1_Pass_&compno._&fi_ds_dt._&run_id.;
length PR_2330B_ICN_MCD $22.;
set &RGA191..p837_1_Pass_&compno._&fi_ds_dt._&run_id.;
PR_2330B_ICN_MCD = '';
run;

data &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;
length IN_2330B_ICN_MCD  $22.;
set &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;
IN_2330B_ICN_MCD = '';
run;

Data &RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.;
	length encapdfld $50. editchktyp $100. clamno $20. lineno $5. typecode $1. 
		   Recsubind $1.  editcd $5. rundt $10. runid $7. LOB $2. sequence_number $9. 
		   lobcod $6. ;
		   
	encapdfld = ''; editchktyp = ''; clamno = ''; lineno = ''; typecode = '';
	Recsubind = ''; editcd = ''; rundt = '';  runid = ''; LOB = '';
	sequence_number = ''; lobcod = ''; 
	
run;

proc sort data = &RGA191..finclaims_&compno.; by clamno lineno; run;
proc sort data = &RGA191..i837_1_Pass_&compno._&fi_ds_dt._&run_id.; by clamno lineno; run;
proc sort data = &RGA191..p837_1_Pass_&compno._&fi_ds_dt._&run_id.; by clamno lineno; run;

%macro qi_validation(ind);
data &RGA191..&ind.837_1_Pass_&compno._&fi_ds_dt._&run_id.(drop = claimfrq icn)
              &ind._fin_notmatch ; 
              hold_code='';
	merge &RGA191..finclaims_&compno.(in=a)
	      &RGA191..&ind.837_1_Pass_&compno._&fi_ds_dt._&run_id.(in=b);
	by clamno lineno;
	if a and b then output &RGA191..&ind.837_1_Pass_&compno._&fi_ds_dt._&run_id.; 
	else output &ind._fin_notmatch;	
	
run;
%mend;
%qi_validation(i);
%qi_validation(p);

%macro in_f8_icn;
%if "&x12_ctrl." ne "EIS" %then %do;	 
	%if (&compno.=45 and "&LOB." ne "MPPO")  or (&compno.=42 and "&LOB." eq "QHP") %then %do; /*B-248997*/
		
		IN_2330B_ICN     = kcompress(clamno||sequence_number) ;
	%end;
	%else 
	%if &compno.=30 or (&compno.=45  and "&LOB." eq "MPPO") %then %do;		 
		IN_2330B_ICN_MCR = kcompress(clamno||sequence_number) ;		
	%end;
	%else 
	%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;		
		IN_2330B_ICN_MCR = kcompress(clamno||sequence_number) ;		
		IN_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
	%else %if (&compno.=01 and &LOB. =DSNP) %then %do;
		IN_2330B_ICN_MCR = kcompress(IN_2330B_ICN_MCR||sequence_number) ;
		IN_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;	
	%else %do;		
		IN_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
%end;
%mend in_f8_icn;

%macro pr_f8_icn;
%if "&x12_ctrl." ne "EIS" %then %do;	 
	%if (&compno.=45 and "&LOB." ne "MPPO") or (&compno.=42 and "&LOB." eq "QHP") %then %do; 
		 
		PR_2330B_ICN     = kcompress(clamno||sequence_number) ;
	%end;
	%else 
	%if &compno.=30 or (&compno.=45  and "&LOB." eq "MPPO")  %then %do;	
		 
		PR_2330B_ICN_MCR = kcompress(clamno||sequence_number) ;

        PR_2330B_ICN_MCD = '                      ';					
		PR_2330B_ICN1_MCD= '                      ';		
		PR_2320_SGMT_HDR1_MCD='   ';
		PR_2320_SGMT_HDR3_MCD='   ';
		PR_2320_AMT01_D_MCD=' ';
		PR_2330A_SGMT_HDR1_MCD='   ';
		PR_2330A_NM101_MCD='  ';
 		PR_2330A_NM102_MCD=' ';
		PR_2330A_NM108_MI_MCD='  ';
		PR_2330A_SGMT_HDR1A_MCD='  ';
		PR_2330A_SGMT_HDR2_MCD='  ';
		PR_2330B_SGMT_HDR1_MCD='   ';
		PR_2330B_NM101_MCD='  ';
		PR_2330B_NM102_OTPAY_PRV_ENT_MCD=' ';
		PR_2330B_NM108_OTPAY_PRV_Q_MCD='  ';
		PR_2330B_SGMT_HDR2_MCD='  ';
		PR_2330B_SGMT_HDR3_MCD='  ';
		PR_2330B_SGMT_HDR4_MCD='   ';
		PR_2330B_DTP01_MCD='   ';
		PR_2330B_DTP02_MCD='  ';
		PR_2330B_SGMT_HDR6_MCD='   ';
		PR_2330B_F8_MCD='  ';
		PR_2330B_SGMT_HDR7_MCD='   ';
		PR_2330B_BP_MCD='  ';
	%end;
	%else 
	%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;		
		PR_2330B_ICN_MCR = kcompress(clamno||sequence_number) ;		
		PR_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
	%else %if (&compno.=01 and &LOB. =DSNP) %then %do;
		PR_2330B_ICN_MCR = kcompress(PR_2330B_ICN_MCR||sequence_number) ;
		PR_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
	%else %do;		
		PR_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
%end;
%mend pr_f8_icn;


%macro icn_in;	 

Data &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;
      set      &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;                  
	  by clamno  ;
		claimfrq=IN_2300_CLM05_3_FREQ_CDE_1;
			%if &compno=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do ;	 
			   	icn=IN_2330B_ICN1_MCR;		
			%end;
			%else %if (&compno=45 and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do ;	 
			   	icn=IN_2330B_ICN1;		
			%end;
			%else %do;
				icn=IN_2330B_ICN1_MCD;
			%end;
run;
%mend icn_in;
%icn_in;

%macro icn_pr;	 
Data &rga191..p837_1_Pass_&compno._&fi_ds_dt._&run_id.  ;
      set       &rga191..p837_1_Pass_&compno._&fi_ds_dt._&run_id. ;                  
	  by clamno  ;
					claimfrq=PR_2300_CLM05_3_FREQ_CDE;

				%if &compno=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do ;	  
				   	icn= PR_2330B_ICN1_MCR;		
				%end;
				%else %if (&compno=45 and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do ;	 
				   	icn= PR_2330B_ICN1;		
				%end;
				%else %do;
					icn=PR_2330B_ICN1_MCD;
				%end;
run;
%mend icn_pr;
%icn_pr;


Data RGA191_Prof_ctrl_output_layout;
    SET &RGA193..&ctrl_tab. ;
	where kupcase(file_type)='PROF' and kupcase(active)='Y';
run;

Data RGA191_Inst_ctrl_output_layout;
    SET &RGA193..&ctrl_tab. ;
	where file_type='INST' and kupcase(active)='Y' ;
run;
Proc sort data=RGA191_Prof_ctrl_output_layout;
      by loop_seq Var_Seq;
Run;
Proc sort data=RGA191_Inst_ctrl_output_layout;
      by loop_seq Var_Seq;
Run;

%macro layout(name,name2);
%let test=1;
proc sql noprint;
                  select  count (distinct(Loop_Seq) )
                  into :var_count
                  from &name.
                  ;
quit;
      %let  var_count = &var_count;
  %let i = 1 ;
%do %while(&i. <= &var_count.);
%if &i.=1 %then %do;
        data _null_ ;
            file &name2. dropover  mod;
           put "if first.clamno then do;"; 
        Run;
%end;

proc sql noprint;
                  select distinct(Loop_name)
                              into :Loop_name SEPARATED by ' '
                              from &name. where Loop_Seq=&i.;
                              ;
               quit;


%let Loop_name=&Loop_name.;

               %put  &Loop_name.;
                  %if &test.=1 and  &Loop_name.=2400 %then %do;
                       data _null_ ;
                              file &name2. dropover  mod;
                               put "end;"; 
                               put "if first.clamno ^= 1 then do; ";
                        run;
                              %let test=2;
                        %end;


      proc sql noprint;
                  select  count (Loop_Seq) 
                  into :var_count1
                  from &name. where Loop_Seq=&i.
                  ;
    quit;
   data _null_ ;
      file &name2. dropover  mod;   
      put "PUT" ;
      run;
    %let j = 1 ;
%do %while(&j. <= &var_count1.);
              proc sql noprint;
                  select Variable_name
                              into :tt SEPARATED by ' '
                              from &name. where Loop_Seq=&i. and Var_Seq=&j.;
                              ;
               quit;

               data _null_ ;
                  file &name2. dropover  mod;
				  %if "&tt."="PR_2430_SVD01_HMO" %then %do;
				    put "&tt" ; 
				  %end;
				   %else %if  "&tt."="IN_2430_SVD01" %then %do;
				    put "&tt" ;
				  %end;
				  %else %do;
                  	put "&tt" ;
				  %end;
                  run;

   %let j = %eval(&j. + 1);
%end;
     data _null_ ;
      file &name2. dropover  mod;
      put ";" ;
      run;


%let i = %eval(&i. + 1);
%end;
data _null_ ;
      file &name2. dropover  mod;
      put "end;" ;
      run;

%mend;
%layout(RGA191_Prof_ctrl_output_layout,Prof);
%layout(RGA191_Inst_ctrl_output_layout,INST);
%macro varlist ;
proc sql noprint;
			select count(*) 
				into :prof_var_list
				from RGA191_Prof_ctrl_output_layout;	
			%let  prof_var_list = &prof_var_list.;
				select variable_name
				into :prof_var_name1-:prof_var_name&prof_var_list.
				from RGA191_Prof_ctrl_output_layout;			 
quit;

	data _null_ ;
	file varlist dropover mod ;
	%let i = 1 ;
	abc="%"||"let"||" "||"prof_var_list"||"=";
	put  abc;
	%do %while(&i. <= &prof_var_list.);
	put "&&prof_var_name&i." ;
	%let i = %eval(&i. + 1);
	%end;
	put "Recsubind";
	put "clamno";
	put "lineno";
	put "run_dt";
	put "run_id";
	put "typecode";
	put "LOB";
	put "extclm";
	put "sequence_number" ; 
	put "LOBCOD" ; 
	put "rollup_line";
	put ";";
	run;

	proc sql noprint;
			select count(*) 
				into :prof_var_list
				from RGA191_Prof_ctrl_output_layout where segment='CLAIM';	
			%let  prof_var_list = &prof_var_list.;
				select variable_name
				into :prof_var_name1-:prof_var_name&prof_var_list.
				from RGA191_Prof_ctrl_output_layout where segment='CLAIM';			 
   quit;

data _null_ ;
	file varlist dropover mod ;
	%let i = 1 ;
	abc="%"||"let"||" "||"keep_prof_clm_vars"||"=";
	put  abc;
	%do %while(&i. <= &prof_var_list.);
	put "&&prof_var_name&i." ;
	%let i = %eval(&i. + 1);
	%end;
	put "clamno";
	put "lineno";
	put "typecode";
	put "extclm";
	put ";";
	run;

proc sql noprint;
			select count(*) 
				into :prof_var_list
				from RGA191_Prof_ctrl_output_layout where segment='SERVICELINE';	
			%let  prof_var_list = &prof_var_list.;
				select variable_name
				into :prof_var_name1-:prof_var_name&prof_var_list.
				from RGA191_Prof_ctrl_output_layout where segment='SERVICELINE';			 
quit;

data _null_ ;
	file varlist dropover mod ;
	%let i = 1 ;
	abc="%"||"let"||" "||"keep_prof_svc_vars"||"=";
	put  abc;
	%do %while(&i. <= &prof_var_list.);
	put "&&prof_var_name&i." ;
	%let i = %eval(&i. + 1);
	%end;
	put "clamno";
	put "lineno";
	put "Recsubind";
	put "typecode";
	put "rollup_line";
	put ";";
	run;
		proc sql noprint;
			select count(*) 
			into :inst_var_list
			from RGA191_inst_ctrl_output_layout;
			%let  inst_var_list = &inst_var_list.;
			select variable_name
			into :inst_var_name1-:inst_var_name&inst_var_list.
			from RGA191_inst_ctrl_output_layout;			
	quit;
	data _null_ ;
	file varlist dropover  mod;
	%let i = 1 ;
	abc="%"||"let"||" "||"inst_var_list"||"=";
	put  abc;
	%do %while(&i. <= &inst_var_list.);
	put "&&inst_var_name&i." ;
	%let i = %eval(&i. + 1);
	%end;
	put "Recsubind";
	put "clamno";
	put "lineno";
	put "run_dt";
	put "run_id";
	put "typecode";
	put "LOB";
	put "extclm"; 
	put "sequence_number";
	put "LOBCOD";
	put "rollup_line";
	put ";";
	run;

	proc sql noprint;
			select count(*) 
			into :inst_var_list
			from RGA191_inst_ctrl_output_layout where segment='CLAIM' ;
			%let  inst_var_list = &inst_var_list.;
			select variable_name
			into :inst_var_name1-:inst_var_name&inst_var_list.
			from RGA191_inst_ctrl_output_layout where segment='CLAIM';			
	quit;
	data _null_ ;
	file varlist dropover  mod;
	%let i = 1 ;
	abc="%"||"let"||" "||"keep_inst_clm_vars"||"=";
	put  abc;
	%do %while(&i. <= &inst_var_list.);
	put "&&inst_var_name&i." ;
	%let i = %eval(&i. + 1);
	%end;
	put "clamno";
	put "lineno";	
	put "extclm"; 
	put ";";
	run;

	proc sql noprint;
			select count(*) 
			into :inst_var_list
			from RGA191_inst_ctrl_output_layout where segment='SERVICELINE' ;
			%let  inst_var_list = &inst_var_list.;
			select variable_name
			into :inst_var_name1-:inst_var_name&inst_var_list.
			from RGA191_inst_ctrl_output_layout where segment='SERVICELINE';			
	quit;
	data _null_ ;
	file varlist dropover  mod;
	%let i = 1 ;
	abc="%"||"let"||" "||"keep_inst_svc_vars"||"=";
	put  abc;
	%do %while(&i. <= &inst_var_list.);
	put "&&inst_var_name&i." ;
	%let i = %eval(&i. + 1);
	%end;
	put "clamno";
	put "lineno";
	put "Recsubind";
	put "typecode";	
	put "rollup_line";
	put ";";
	run;
%mend varlist  ;
%varlist ;
%include varlist ; 

data _null_;
	length ddir $10.;
    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
    call symput('ddir',ddir);					
run;

Data _null_ ; 
     put "QIfiflag =  &QIfiflag" ;   
     put "run_id   =  &run_id     " ;
	 put "svcdat   =  &svcdat     " ; 
	 put "Compno   =  &Compno     " ;
Run ;

Data temp_job; 
		set &R42173..medical_job_mstr
(drop=output_dataset_name output_file_name sequence_number typecode ) ;
		where run_id = "&run_id.";
run;

/**********split *********************************/

%macro Split_dataset1(i,firstobs,lastobs,dsname,type);

 %global ddir;
	%let path1 = &drv72./Encounters_Reporting/APD/Input_File ;
                data _null_;
                	length ddir $10.;
                    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
                    call symput('ddir',ddir);				
                run;
                               %let destpath = &path1./&ddir.;

	%put &i. &firstobs. &lastobs.;

	data &RGA191..tt;
   	%if &type.=P  %then %do;	
		sq=put(&i.,z2.);	
		call symput ("sq",sq);
	%end;
	%else %if &type.=I  %then %do;
		sq=put(sum(&i.,&p_n.),z2.);
		call symput ("sq",sq);
	%end;	
	run;
	%put &sq.;	
	
	 %if &dsname.=prof837  %then %do;
		data &RGA191..Tempdatset;  		
		set &RGA191..&dsname._ds1;     
		  where  &firstobs. <= t <= &lastobs.  and missing(hold_code)=1 ;
		  sequence_number="&run_id.&sq.";
 			%pr_f8_icn;            
	    run;	
		Data Map_seq_temp;
		length clamno $20.  ; /* DS - Clamno length update*/
				set  &RGA191..&dsname._ds1(keep=clamno lineno t);
			    where  &firstobs. <= t <= &lastobs. ;
				sequence_number="&run_id.&sq.";	
		run;
		Data p_temp;
				s="&&run_id.&sq.";
				Type="P";
			run;
 
		%if &sq. = 01 %then %do;
	    Data &RGA191_..prof837_Pass_&compno._&fi_ds_dt._&run_id.(compress = yes keep=&prof_var_list.);/* HS 542635 - added compress option */
				retain &prof_var_list.; 
				length clamno $20.; 
				set &RGA191..Tempdatset;				
			run;
			Data Map_seq;
				set  Map_seq_temp;			   
			run;
			Data p_list;
				set p_temp;
			run;
			
		%end;
		%else %do;
			Data &RGA191_..prof837_Pass_&compno._&fi_ds_dt._&run_id.(compress = yes keep=&prof_var_list.);/* HS 542635 - added compress option */
				retain &prof_var_list.;  
				length clamno $20.;  
				set &RGA191_..prof837_Pass_&compno._&fi_ds_dt._&run_id. &RGA191..Tempdatset;
			run;
			Data Map_seq;
				set  Map_seq Map_seq_temp;			   
			run;
			Data p_list;
				set p_list p_temp;
			run;
		%end;

		Data &RGA191..prof837_QI;
			retain clamno lineno claimfrq icn ;
			set &RGA191..&dsname._ds1 (keep=clamno lineno claimfrq icn t );			
			where  &firstobs. <= t <= &lastobs.;
			drop t;
		Run;		
		

				data _null_ ;
	         
        	 file "&drv2./Encounters_Reporting/APD/Input_File/QiFile_prof837_Pass_&compno._&fi_ds_dt._&run_id.&sq..txt"   delimiter='09'x  ;
       		 Set &RGA191..prof837_QI ;                                        
			 if _n_ = 1 then do;
			 put "clamno" '09'x
		  	    "lineno" '09'x
				"claimfrq" '09'x
				"icn"
				;
       		end;
    		Put (_all_)(+0);
			;
		run;

data _null_;
cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Input_File/  &destpath./ --recursive --exclude '*' --include 'QiFile_prof837_Pass_*.txt' --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

    %end;
	%else %if &dsname.=inst837  %then %do;
		data &RGA191..Tempdatset;         
		set &RGA191..&dsname._ds1;     
		  where  &firstobs. <= t <= &lastobs.  and  missing(hold_code)=1 ;	
		  sequence_number="&run_id.&sq.";
		  %in_f8_icn;  
	    run;
		Data Map_seq_temp;
		length clamno $20.  ; /* DS - Clamno length update*/
				set  &RGA191..&dsname._ds1(keep=clamno lineno t);
			    where  &firstobs. <= t <= &lastobs. ;
				sequence_number="&run_id.&sq.";	
		run;
		Data Map_seq;
				set  Map_seq Map_seq_temp;			   
		run;
		Data p_temp;
				s="&&run_id.&sq.";
				Type="I";
		run;

		Data p_list;
				set p_list p_temp;
		run;

		%if &i. = 1 %then %do;
	    Data &RGA191_..inst837_Pass_&compno._&fi_ds_dt._&run_id.(compress = yes keep=&inst_var_list.);/* HS 542635 - added compress option */
				retain &inst_var_list.;  
				length clamno $20.;  
				set &RGA191..Tempdatset;
		run;
		%end;
		%else %do;
			Data &RGA191_..inst837_Pass_&compno._&fi_ds_dt._&run_id.(compress = yes keep=&inst_var_list.);/* HS 542635 - added compress option */
				retain &inst_var_list.; 
				length clamno $20.;  
				set &RGA191_..inst837_Pass_&compno._&fi_ds_dt._&run_id. &RGA191..Tempdatset;
			run;
		%end;

		Data &RGA191..inst837_QI;
			retain clamno lineno claimfrq icn ;
			set &RGA191..&dsname._ds1(keep=clamno lineno claimfrq icn t);
			where  &firstobs. <= t <= &lastobs.;	
			Drop t;
		Run;

		data _null_ ;
	         
        	 file "&drv2./Encounters_Reporting/APD/Input_File/QiFile_inst837_Pass_&compno._&fi_ds_dt._&run_id.&sq..txt"    delimiter='09'x  ;
       		 Set &RGA191..inst837_QI ;                                        
			 if _n_ = 1 then do;
			 put "clamno" '09'x
		  	    "lineno" '09'x
				"claimfrq" '09'x
				"icn"
				;
       		end;
    		Put (_all_)(+0);
			;
		run;

data _null_;
cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Input_File/  &destpath./ --recursive --exclude '*' --include 'QiFile_inst837_Pass_*.txt' --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

    %end;  


    %mend;
    
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/record_count.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;    

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/record_count.csv"
	out=record_count dbms=csv
	replace;
Run;

proc sql;
Select lastobs into :lastobs_i_nomas  from record_count where claimtype = 'Inst' and submission_type = 'Clean';
Select lastobs into :lastobs_p_nomas  from record_count where claimtype = 'Prof' and submission_type = 'Clean';
Select lastobs into :lastobs_i7_nomas  from record_count where claimtype = 'Inst' and submission_type = 'Adjustment';
Select lastobs into :lastobs_p7_nomas  from record_count where claimtype = 'Prof' and submission_type = 'Adjustment';
Select lastobs into :lastobs_i8_nomas  from record_count where claimtype = 'Inst' and submission_type = 'Void';
Select lastobs into :lastobs_p8_nomas  from record_count where claimtype = 'Prof' and submission_type = 'Void';
quit;
%put &lastobs_i_nomas. &lastobs_p_nomas. ;
%put &lastobs_i7_nomas. &lastobs_p7_nomas. ;
%put &lastobs_i8_nomas. &lastobs_p8_nomas. ; 

 
 %macro Split_dataset(dsname,dsn1,type);
	%global firstobs lastobs p_n P_n1 claimfrq_sub;
	Proc sort data=&RGA191..&dsn1._1_Pass_&compno._&fi_ds_dt._&run_id.;by clamno lineno;
	run;		
	Data &RGA191..&dsname._ds;
		retain t 0;
		set &RGA191..&dsn1._1_Pass_&compno._&fi_ds_dt._&run_id. ;
		by clamno;
		run_dt = "&run_dt." ;
            run_id = "&run_id." ;
			LOB="&Compno.";
			LOBCOD="&LOB.";			
		if first.clamno then t+1;     
	run;	

	%nobs(&RGA191..&dsname._ds);
	%if &nobs. =0 %then %do;
		%let ds_count=0;
	%end;
	%else %do;
		Proc sql noprint;
			select max(t) into :ds_count from &RGA191..&dsname._ds;
		quit;
	%end;
	
	/* DS - Adding dummy datasets for processing */
	
	Data &dsname._cla_rej;
	set &RGA191..&dsn1._1_Pass_&compno._&fi_ds_dt._&run_id.(obs=0);
	run;
     
	Data &RGA191..&dsname._cla_rej1;
		retain t &ds_count.;
		set &RGA191..&dsname._cla_rej;
		by clamno;
			run_dt = "&run_dt." ;
            run_id = "&run_id." ;
			LOB="&Compno.";
			LOBCOD="&LOB.";
		if first.clamno then t+1;
	run;			
	Data &RGA191..&dsname._ds1;
	set &RGA191..&dsname._ds &RGA191..&dsname._cla_rej1; 
	run;
	%nobs(&RGA191..&dsname._ds1);
	%if &nobs. =0 %then %do;
		%let ds_count=0;
	%end;
	%else %do;
		Proc sql noprint;
			select max(t) into :ds_count from &RGA191..&dsname._ds1;
		quit;
	%end;

/* DS - Adding dummy datasets for processing */

	Data &RGA191..&dsname._hold_Cla_rej;
		set &RGA191..&dsn1._1_Pass_&compno._&fi_ds_dt._&run_id.(obs=0);
	run;
	
    Data &RGA191..&dsname._hold_Cla_rej1;
		retain t &ds_count.;
		set &RGA191..&dsname._hold_Cla_rej;
		by clamno;
			run_dt = "&run_dt." ;
            run_id = "&run_id." ;
			LOB="&Compno.";
			LOBCOD="&LOB.";
		if first.clamno then t+1;
	run;
	
	Data &RGA191..&dsname._ds1;
	set &RGA191..&dsname._ds1 &RGA191..&dsname._hold_Cla_rej1;
	  if claimfrq ne ' ' then do;				
	    call symput('claimfrq_sub', claimfrq);	
	  end;
	run;
 
  %nobs(&RGA191..&dsname._ds1);
	%if &nobs. =0 %then %do;
		%let ds_count=0;
	%end;
	%else %do;
		Proc sql noprint;
			select max(t) into :ds_count from &RGA191..&dsname._ds1;
		quit;
	%end;



	data _null_;
		format n 10.;
		%if &masking.=Y %then %do;
			%if &type.=P %then %do;
				n=ceil(&ds_count/50);
			%end;
			%else %do;
			   	n=ceil(&ds_count/50);
			%end;
        %end;
		%else %do ;
			%if &type.=P %then %do;
				%if &claimfrq_sub. = 8 %then %do;			 
				n=ceil(&ds_count/&lastobs_p8_nomas.);		
				%end; %else
				%if &claimfrq_sub. = 7 %then %do;			 
				n=ceil(&ds_count/&lastobs_p7_nomas.);		
				%end; %else %do;
				n=ceil(&ds_count/&lastobs_p_nomas.); 
				%end;
			%end;
			%else %do;
				%if &claimfrq_sub. = 8 %then %do;			 
				n=ceil(&ds_count/&lastobs_i8_nomas.);		
				%end; %else
				%if &claimfrq_sub. = 7 %then %do;			 
				n=ceil(&ds_count/&lastobs_i7_nomas.);		
				%end; %else %do;
				n=ceil(&ds_count/&lastobs_i_nomas.); 
				%end;
			%end;
		%end;
		if n < 1 then  n=1;
		
		call symput ("n",ktrim(kleft(n)));
	run;

	%put n = &n.;
	%if &type.=P  %then %do;
		%let P_n = &n.;
    %end;	
	%put &P_n.;


	data _null_ ;

		%do i = 1 %to &n.;
			%if &i. = 1 %then 
				%do;
				%let firstobs = 1 ;
				  %If &masking.=Y %then %do;
					%if &type.=P %then %do;
						%let lastobs=50;
					%end;
					%else %do;
						%let lastobs=50;
					%end;
				  %end;
				  %else %do;
				  	%if &type.=P %then %do;
						%if &claimfrq_sub. = 8 %then %do;		 
						%let lastobs=&lastobs_p8_nomas.; 		
						%end; %else
						%if &claimfrq_sub. = 7 %then %do;		 
						%let lastobs=&lastobs_p7_nomas.; 		
						%end; %else %do;
						%let lastobs=&lastobs_p_nomas.;  
						%end;
					%end;
					%else %do;
						%if &claimfrq_sub. = 8 %then %do;		 
						%let lastobs=&lastobs_i8_nomas.;		
						%end; %else
						%if &claimfrq_sub. = 7 %then %do;		 
						%let lastobs=&lastobs_i7_nomas.;		
						%end; %else %do;
						%let lastobs=&lastobs_i_nomas.; 
						%end;
					%end;
				  %end;

				%end;

			%else
				%do;
				%let firstobs = %eval(&lastobs.+ 1);
				 %If &masking.=Y %then %do;
					%if &type.=P %then %do;
						%let lastobs = %eval(&lastobs.+ 50);
					%end;
					%else %do;
						%let lastobs = %eval(&lastobs.+ 50);
					%end;
				%end;
				%else %do;
					%if &type.=P %then %do;
						%if &claimfrq_sub. = 8 %then %do;					 
						%let lastobs = %eval(&lastobs.+&lastobs_p8_nomas.);	
						%end; %else
						%if &claimfrq_sub. = 7 %then %do;					 
						%let lastobs = %eval(&lastobs.+&lastobs_p7_nomas.);	
						%end; %else %do;
						%let lastobs = %eval(&lastobs.+&lastobs_p_nomas.); 
						%end;
					%end;
					%else %do;
						%if &claimfrq_sub. = 8 %then %do;					 
						%let lastobs = %eval(&lastobs.+&lastobs_i8_nomas.); 
						%end; %else
						%if &claimfrq_sub. = 7 %then %do;					/* B-97137 */
						%let lastobs = %eval(&lastobs.+&lastobs_i7_nomas.);	
						%end; %else %do;
						%let lastobs = %eval(&lastobs.+&lastobs_i_nomas.);/* HS 542635 - KV*/
						%end;
					%end;
				%end;

				%end;		     
		%Split_dataset1(&i.,&firstobs.,&lastobs.,&dsname.,&type.); 
		%end;
run;
%mend Split_dataset;

%Split_dataset(prof837,p837,P); 
%Split_dataset(inst837,i837,I); 


 Data  &RGA191..Prof_temp ; 
length orig_clamno $20.; 
 	set  p837_1_Pass_&compno._&fi_ds_dt._&run_id.	; 
	by clamno;	
	orig_clamno = ksubstr(clamno,1,13);
run; 


Proc sort data=&RGA191_..prof837_Pass_&compno._&fi_ds_dt._&run_id. (keep=clamno sequence_number) 
out=prof_seq  nodupkey;by clamno;
Run; 
Proc sort data=&RGA191..Prof_temp ; by orig_clamno;
Run;
Data &RGA191..Prof_temp1(drop = orig_clamno);/* HS - 569650 */
merge &RGA191..Prof_temp(in=_a) prof_seq (in=_b rename = (clamno = orig_clamno));
if _a;
by orig_clamno;
run; 
 
Data  &RGA191..inst_temp; 
length orig_clamno $20.;  
 	set  &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id. ;        
	by clamno;	 
orig_clamno = ksubstr(clamno,1,13);  
Run; 
Proc sort data=&RGA191_..inst837_Pass_&compno._&fi_ds_dt._&run_id. (keep=clamno sequence_number) 
out=inst_seq  nodupkey;by clamno ;
Run; 
Proc sort data=&RGA191..inst_temp ; by orig_clamno;
Run;
Data &RGA191..inst_temp1(drop = orig_clamno);
merge &RGA191..inst_temp(in=_a) inst_seq (in=_b rename = (clamno = orig_clamno));
if _a;
by orig_clamno ;
run; 

%Macro Layout(ds,ind)/minoperator; 

%if &ind.=P %then %do;
	Data P;
	set P_list;
	where Type ='P';
	run;
	%let dsn=P;	
%nobs(P);
%end;

%if &ind.=I %then %do;
	Data I;
	set P_list;
	where Type ='I';
	run;
	%let dsn=I;	
	%nobs(I);
%end;


%if &nobs.= 0 %then %do;

%end;

%else %do;

	%let count=&nobs.;
	%do i = 1 %to &count.;

	Data _null_;
		Set &dsn.;
		if _n_=&i.;
		call symput('sequence_number',s);	
	Run;
    %let sequence_number=&sequence_number.;
	%put &sequence_number.;
	
	Data Temp_&sequence_number.;
		set &RGA191..&ds.;
		where sequence_number="&sequence_number.";
	Run;
	

	%if &ind.=P %then %do;
	%if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do;  
			  
		  	filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %if &compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA") %then %do; 
			  
		  	filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %If &compno.=01 %then %do;
		%If &LOB. =DSNP  %then %do;
		  	filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
		%end;
		%else %do;
			filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
		%end;
	%end;	
	%else %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do; 
		  	filename proout /	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %do;
		  
	  	filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	
	proc sort data = Temp_&sequence_number. out = temp_1_&sequence_number. nodupkey;
	by clamno; 
	run;
	
	 
	Data  Temp_&sequence_number.; 
 	set  temp_1_&sequence_number. 
     	 Temp_&sequence_number.  ; 
	by clamno;
	%pr_f8_icn; 
	
	run;
	
	
		Data _null_;
	     	 file proout dlm = "|" lrecl = 2000 dsd; 
	      	set Temp_&sequence_number.;     	
		  	by clamno ;	
		  	%include Prof;
	    Run;
		 
		
	Data _null_;
	 %if (&compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP")) or (&compno.=01 and &LOB. =DSNP) or
		(&compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA")) or 
		(&compno. eq 45 and "&LOB." eq "MPPO")  %then %do;
		x "iconv -c -f UTF8 -t ASCII &drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt > &drv2./Encounters_Reporting/APD/Current/upd_Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" ;
		x "mv &drv2./Encounters_Reporting/APD/Current/upd_Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt &drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt"; 
	%end;
	%else %do;
	  %If &compno.=01 and "&LOB." ne "DSNP"  %then %do;
		x "iconv -c -f UTF8 -t ASCII &drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt > &drv2./Encounters_Reporting/APD/Current/upd_Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt" ;
		x "mv &drv2./Encounters_Reporting/APD/Current/upd_Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt &drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt"; 
	  %end;
	  %else %do;
		x "iconv -c -f UTF8 -t ASCII &drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt > &drv2./Encounters_Reporting/APD/Current/upd_Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt" ;
		x "mv &drv2./Encounters_Reporting/APD/Current/upd_Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt &drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt"; 
	  %end;
	%end;
   run;
		Data _null_;
			a=sleep(10);
		run;
	    data _null_;
			t1=kcompress(put(time(),time8.),".: ");				
			if klength(t1)=5 then t1=kcompress("0"||t1);
			call symput('t1',t1);
	    Run;
		Data temp_job_&sequence_number.;
			length output_file_name $40. Temp $24.; 
			set temp_job;
			%if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do;
				output_file_name = "Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else %if &compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA") %then %do;/* B-146417 */ 
				output_file_name = "Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else %If &compno.=01 and &LOB. =DSNP  %then %do; 
				output_file_name = "Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;	
			%else %If &compno.=01 and "&LOB." ne "DSNP"  %then %do; 
				output_file_name = "Prof_837_&Compno._&fi_ds_dt._&sequence_number." ; 
			%end;			
			%else %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do; 
				output_file_name = "Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else %do;
				output_file_name = "Prof_837_&Compno._&fi_ds_dt._&sequence_number." ;
			%end;
			output_dataset_name ="Prof837_pass_&Compno._&fi_ds_dt._&run_id." ;

 			%if &compno. eq 42 and "&LOB." eq "EP" %then %do;
			Temp="TR."||"&TPAID."||".837PE.W.";
			%end;
			%else%if &compno. eq 42 and "&LOB." eq "QHP" %then %do;  
			Temp="TR."||"&TPAID."||".837PQ.W.";
			%end;
			%else %if &compno. eq 45 and "&LOB." ne "MPPO" and "&x12_ctrl." ne "EIS" %then %do;  
			Temp="TR."||"&TPAID."||".837PC.W.";
			%end;
			%else %if &compno. eq 20 and "&x12_ctrl." ne "EIS" %then %do;  
			Temp="TR."||"&TPAID."||".837PK.W.";
			%end;
			%else %if &compno. eq 30 or (&compno. eq 45 and "&LOB." eq "MPPO") %then %do;  
				Temp="TR."||"&TPAID."||".837PA.W.";
			%end;
			%else %do;
			Temp="TR."||"&TPAID."||".837PM.W.";
			%end;
			
			 
			%if "&x12_ctrl." ne "EIS" %then %do;
				%if &compno. in(01 02 20 34 42 45) %then %do;				
				temp=tranwrd(temp, "NYE", "Z10090");
				%end;
				%if &compno. in(02) %then %do;				
				temp=tranwrd(temp, "T09004", "Z10090");
				%end;
			%end;
			
			Translated_file_name=kcompress(Temp||put(today(),yymmdd6.)||"&t1.")||".001"; 
			sequence_number="&sequence_number.";
			typecode='P';
			Drop temp;
		run; 
		
		
	  Data &R42173..medical_job_mstr;
		set &R42173..medical_job_mstr  temp_job_&sequence_number.;
	 Run; 
    
   %end;
   
   %else%if &ind.=I %then %do;   	
   	 
	 
   %if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do;
		
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;  /* ALM 7174 - added lob in the name */
	%end;
	%else %if &compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA") %then %do; 
		
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %If &compno.=01 and &LOB. =DSNP  %then %do;
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;	
	%else %If &compno.=01 and "&LOB." ne "DSNP"  %then %do;
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;		
	%else %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do;  
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %do;
		
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	
	proc sort data = Temp_&sequence_number. out = temp_1_&sequence_number. nodupkey;
	by clamno; 
	run;
	
	 
	Data  Temp_&sequence_number.; 
 	set  temp_1_&sequence_number. 
     	 Temp_&sequence_number.  ; 
	by clamno;
	%in_f8_icn;
	
	run;	

		Data _null_;
    	  	file insout dlm = "|" lrecl = 2000 dsd; 
     	 	 set Temp_&sequence_number.;  
		 	 by clamno ;		 	 
    	 	 %include inst;
    	Run;
		 
		Data _null_;
		 %if (&compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP")) or (&compno.=01 and &LOB. =DSNP) or
			 (&compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA")) or 
			 (&compno. eq 45 and "&LOB." eq "MPPO")  %then %do;
			x "iconv -c -f UTF8 -t ASCII &drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt > &drv2./Encounters_Reporting/APD/Current/upd_Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" ;
			x "mv &drv2./Encounters_Reporting/APD/Current/upd_Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt &drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt"; 
		%end;
		 %else %if (&compno.=01 and "&LOB." ne "DSNP") %then %do;
			x "iconv -c -f UTF8 -t ASCII &drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt > &drv2./Encounters_Reporting/APD/Current/upd_Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt" ;
		    x "mv &drv2./Encounters_Reporting/APD/Current/upd_Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt &drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt"; 
		%end;		

		%else %do;
		 x "iconv -c -f UTF8 -t ASCII &drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt > &drv2./Encounters_Reporting/APD/Current/upd_Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt" ;
		 x "mv &drv2./Encounters_Reporting/APD/Current/upd_Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt &drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt"; 
		%end;
	   run;

		Data _null_;
			a=sleep(10);
		run;

		  data _null_;
			t1=kcompress(put(time(),time8.),".: ");			
			if klength(t1)=5 then t1=kcompress("0"||t1);
			call symput('t1',t1);
	     Run;
			Data temp_job_&sequence_number.;
			length output_file_name $40. Temp $24.; 
			set temp_job;
		   %if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do; 
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else  %if &compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA") %then %do;  
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else  %if &compno.=01 and &LOB. =DSNP %then %do;
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;	
			%else  %if &compno.=01 and "&LOB." ne "DSNP" %then %do;
				output_file_name = "Inst_837_&Compno._&fi_ds_dt._&sequence_number." ;
			%end;
			%else  %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do; 
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else %do;
				output_file_name = "Inst_837_&Compno._&fi_ds_dt._&sequence_number." ;
			%end;
			output_dataset_name ="Inst837_pass_&Compno._&fi_ds_dt._&run_id." ;

			%if &compno. eq 42 and "&LOB." eq "EP" %then %do;
				Temp="TR."||"&TPAID."||".837IE.W.";
			%end;
			%else %if &compno. eq 42 and "&LOB." eq "QHP" %then %do;  
				Temp="TR."||"&TPAID."||".837IQ.W.";
			%end;
			%else %if &compno. eq 45  and "&LOB." ne "MPPO" and "&x12_ctrl." ne "EIS" %then %do; 
			Temp="TR."||"&TPAID."||".837IC.W.";
			%end;
			%else %if &compno. eq 20 and "&x12_ctrl." ne "EIS" %then %do;  
				Temp="TR."||"&TPAID."||".837IK.W.";
			%end;
			%else %if &compno. eq 30 or (&compno. eq 45 and "&LOB." eq "MPPO") %then %do;  
				Temp="TR."||"&TPAID."||".837IA.W.";
			%end;
			%else %do;
				Temp="TR."||"&TPAID."||".837IM.W.";
			%end;
			 
			%if "&x12_ctrl." ne "EIS" %then %do;
				%if &compno. in(01 02 20 34 42 45) %then %do;				
				temp=tranwrd(temp, "NYE", "Z10090");
				%end;
				%if &compno. in(02) %then %do;				
				temp=tranwrd(temp, "T09004", "Z10090");
				%end;
			%end;
			Translated_file_name=kcompress(Temp||put(today(),yymmdd6.)||"&t1.")||".001";
		  	sequence_number="&sequence_number.";
			typecode='I';
			Drop temp;
		run;		
		
	  Data &R42173..medical_job_mstr;
		set &R42173..medical_job_mstr  temp_job_&sequence_number.;
	 Run; 
   %end;
	
%end;
%end;

%Mend;
%layout(Prof_temp1,P);
%layout(inst_temp1,I);

/****************************************split over ****************/

Data Map_seq;
length clamno $20.;
set Map_seq(rename=(Clamno=clamno1));
Clamno=kcompress(ksubstr(clamno1,1,13));
run;
Proc sort data=&RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.;by clamno;run;
Proc sort data=Map_seq(keep=clamno sequence_number) out=Map_seq1 nodupkey;by clamno;run;

Data &R42173..medical_job_mstr;
	set &R42173..medical_job_mstr;
	if run_id="&run_id." and sequence_number="" then delete;
Run;

%macro record_cnt(dsn= , recind= , typ= );  

proc sql; 
			create table &typ._rec_count as
				select 'CLAIM' as level,"&dsn." as output_dataset_name,sequence_number,count(*) as cnt from 
						(select distinct clamno,sequence_number from &RGA191_..&dsn. where recsubind=&recind.) group by sequence_number
				union all
				select 'LINELEVEL' as level,"&dsn." as output_dataset_name,sequence_number,count(*) as cnt from 
						(select distinct clamno,lineno,sequence_number from &RGA191_..&dsn. where recsubind=&recind.) group by sequence_number
				union all
				select 'TOTAL' as level,"&dsn." as output_dataset_name,sequence_number,count(*) as cnt from 
						(select clamno,lineno,sequence_number from &RGA191_..&dsn. ) group by sequence_number
				;
				create table &typ._rec_count1 as				
				select  sequence_number,
						output_dataset_name,
						max(claim_cnt) as claim_cnt ,
						max(line_cnt) as line_cnt,
						max(total_cnt) as total_cnt						
				from 	(select sequence_number,output_dataset_name,
								 case when level='CLAIM' then cnt else 0 end as claim_cnt, 
								 case when level='LINELEVEL' then cnt else 0 end as line_cnt,
								 case when level='TOTAL' then cnt else 0 end as total_cnt
						 from  &typ._rec_count) group by sequence_number,output_dataset_name;
		quit;    
		
	
		proc sort data=&R42173..medical_job_mstr;by sequence_number output_dataset_name;run;
		proc sort data=&typ._rec_count1;by sequence_number output_dataset_name;run;		
		
		data &R42173..medical_job_mstr;
		merge &R42173..medical_job_mstr(in=a) &typ._rec_count1(in=b);
		by sequence_number output_dataset_name;
		if a;
		run;

%mend;

%record_cnt(dsn= Inst837_pass_&compno._&fi_ds_dt._&run_id. ,  recind='Y' , typ=inst);
%record_cnt(dsn= Prof837_pass_&compno._&fi_ds_dt._&run_id. ,  recind='Y' , typ=prof);
%record_cnt(dsn= &lib._Rej_837_&compno._&fi_ds_dt._&run_id.,  recind='N' , typ=rej);


%macro mrgqidsnr(finqidsn=,fqidsn=);
 %if (&compno.=01 and &LOB. =DSNP) %then %do;
	proc sql;
		create table &fqidsn as
			select a.*, b.clamno_mcd, b.lineno_mcd, b.clamno_mcr, b.lineno_mcr
					  , b.claimfrq as claimfrq_qi, /*b.icn as icn_qi, */b.extclm as extclm_qi, "&fname." as qi_filename
			from &finqidsn. a
			left join &RGA191..finclaims_&compno. b
			on a.clamno=b.clamno and a.lineno=b.lineno
		;
	quit;
	data &finqidsn.;
		set &fqidsn;
	run;
 %end;
 %else %do;
 	proc sql;/*B-346894*/
		create table &fqidsn as
			select distinct a.*, '' as clamno_mcd, '' as lineno_mcd, '' as clamno_mcr, '' as lineno_mcr
					  , b.claimfrq as claimfrq_qi, /*'' as  icn_qi, */'' as extclm_qi, "&fname." as qi_filename
			from &finqidsn. a
			left join &RGA191..finclaims_&compno. b
			on a.clamno=b.clamno and a.lineno=b.lineno
		;
	quit;
	/*data &finqidsn.;
		set &finqidsn.;
		clamno_mcd=''; lineno_mcd=''; clamno_mcr=''; lineno_mcr=''; claimfrq_qi=''; icn_qi=''; extclm_qi=''; qi_filename='';
	run;*/
	data &finqidsn.;
		set &fqidsn;
	run;
 %end;
%mend mrgqidsnr;
%mrgqidsnr(finqidsn=&RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.,fqidsn=rejfqidsn);


%macro mrgqidsn(finqidsn=,fqidsn=);
 %if (&compno.=01 and &LOB. =DSNP) %then %do;
	proc sql;
		create table &fqidsn as
			select a.*, b.clamno_mcd, b.lineno_mcd, b.clamno_mcr, b.lineno_mcr
					  , b.claimfrq as claimfrq_qi, /*b.icn as icn_qi, */b.extclm as extclm_qi, "&fname." as qi_filename
			from &finqidsn. a
			left join &RGA191..finclaims_&compno. b
			on a.clamno=b.clamno and a.lineno=b.lineno
		;
	quit;
	data &finqidsn.;
		set &fqidsn;
	run;
 %end;
 %else %do;
 	proc sql;/*B-346894*/
		create table &fqidsn as
			select distinct a.*, '' as clamno_mcd, '' as lineno_mcd, '' as clamno_mcr, '' as lineno_mcr
					  , '' as claimfrq_qi, /*'' as  icn_qi, */'' as extclm_qi, "&fname." as qi_filename
			from &finqidsn. a
			left join &RGA191..finclaims_&compno. b
			on a.clamno=b.clamno and a.lineno=b.lineno
		;
	quit;
	/*data &finqidsn.;
		set &finqidsn.;
		clamno_mcd=''; lineno_mcd=''; clamno_mcr=''; lineno_mcr=''; claimfrq_qi=''; icn_qi=''; extclm_qi=''; qi_filename='';
	run;*/
	data &finqidsn.;
		set &fqidsn;
	run;
 %end;
%mend mrgqidsn;
%mrgqidsn(finqidsn=&RGA191_..Inst837_pass_&compno._&fi_ds_dt._&run_id.,fqidsn=instfqidsn);
%mrgqidsn(finqidsn=&RGA191_..Prof837_pass_&compno._&fi_ds_dt._&run_id.,fqidsn=proffqidsn);

proc sql;
update &R42173..medical_job_mstr
set Cmsmap_flag = '1'
where run_id = "&run_id."
;
quit;

/**************************RGA19701 BLOCK STARTS ***************/

data _null_;
  length end_dttime $ 30. ;
  end_date = put(today(),date7.);  
  end_time = kcompress(put(time(),time10.),':');  
  end_dttime = catx('_' ,end_date,end_time);
  call symput('end_dttime',kcompress(end_dttime));
  ed_time = put(datetime(),DATETIME20.);  
  call symput('ed_time',kcompress(ed_time));

proc sql;
update &R42173..medical_job_mstr
set 
	end_time	 =   "&ed_time." ,	
	compno       =    "&Compno."      
where run_id = "&run_id."
;
quit;

Data Report(keep=run_date run_id sequence_number  Compno output_file_name output_dataset_name Load_flag typecode split_status LOBCOD Translated_file_name treo_flag claim_cnt line_cnt total_cnt);
		retain run_date run_id sequence_number  Compno LOBCOD typecode output_file_name output_dataset_name  Load_flag  split_status treo_flag   Translated_file_name claim_cnt line_cnt total_cnt; /*272515 added cnt variable*/
	Set &R42173..medical_job_mstr;	 
	where missing(sequence_number)=0;
Run;
Proc sort data=Report;by descending run_id ;
Run;

data Report(drop=run_id_julian two_old two_old1_n run_id_n two_old1);  
set Report;
attrib run_id_julian format=date9. two_old format=date9.;
run_id_julian=input(kstrip(ksubstr(run_id,1,5)),julian6.);
two_old=intnx('year',today(),-2,'same');
two_old1=put(two_old,julian6.); 
run_id_n = input(ksubstr(run_id,1,5), 8.);
two_old1_n = input(two_old1, 8.);
if run_id_n gt two_old1_n then output;
run;

%if &extn_type.=xls %then %do;  
PROC EXPORT DATA= Report	 
 OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type."
 DBMS= &extn_type. REPLACE;sheet='REPORT';
RUN;
%end;

PROC EXPORT DATA= Report	 
 OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report"
 DBMS=xlsx REPLACE;sheet='REPORT';
RUN;

%macro handle_error;

data logfile;
  length Message $ 250;
  infile "&drv2./Encounters_Reporting/APD/Current/&lib._837_Log_File_&Compno._&fi_ds_dt._&run_id..log" truncover;
  input Message $ 1-250;
LineNum = _n_;
  if ((Message =: "WARNING")	 
      
      and (kindex(Message,"Your system is scheduled to expire") = 0)      
      and (kindex(Message,"The Base Product product with which") = 0)      
      and (kindex(Message,"expire within ") = 0)      
      and (kindex(Message,"will be expiring soon") = 0)	  
	  and (kindex(Message,"The SAS/CONNECT product with which")=0))    
    or ((Message =: "ERROR") and (kindex(message,"The setting of the SYSPRINT ")=0))									
	or (kindex(Message,"unin") ~= 0) ;
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;
run;

data logfile;
set logfile;
if (kindex(message,"Variable mcor2")~=0) or (kindex(message,"Variable col2")~=0) then delete;
run;
%mend handle_error;

/***********************************************/

%macro data_archive;


 x "rm -f &drv2./Encounters_Reporting/APD/Temp/Edit_Matrix_script_&compno..sas"; 
 x "rm -f &drv2./Encounters_Reporting/APD/Temp/Control_Table_script_&compno..sas"; 
 x "rm -f &drv2./Encounters_Reporting/APD/Temp/Prof_layout_script_&compno..sas"; 
 x "rm -f &drv2./Encounters_Reporting/APD/Temp/INST_layout_script_&compno..sas"; 
 x "rm -f &drv2./Encounters_Reporting/APD/Temp/varlist_script_&compno..sas"; 
 
%let path1 = &drv72./Encounters_Reporting/APD/Archive ;
%let dirid2=%sysfunc(dopen(&path1));

data _null_;
length ddir $10.;                    
ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
call symput('ddir',ddir);
run;

%let destpath = &path1./&ddir;              
   				
data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &destpath. --recursive --exclude '*' --include '*.*'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837p/In/To_Onprem --recursive --exclude '*' --include 'Prof_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837p/In/&ddir. --recursive --exclude '*' --include 'Prof_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837i/In/To_Onprem --recursive --exclude '*' --include 'Inst_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;   		

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837i/In/&ddir. --recursive --exclude '*' --include 'Inst_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;		
			

%if &extn_type.=xls %then %do; 
				 
	PROC EXPORT DATA= Report	 
	 OUTFILE= "&drv2./Encounters_Reporting/APD/Current/Medical_Job_Mstr_Report_&run_id..&extn_type."
	 DBMS=&extn_type. REPLACE;sheet='REPORT';
	RUN;
	
%end;
				
	PROC EXPORT DATA= Report	 
	 OUTFILE= "&drv2./Encounters_Reporting/APD/Current/Medical_Job_Mstr_Report_&run_id."
	 DBMS=xlsx REPLACE;sheet='REPORT';
	RUN;			
				 		
data _null_; 
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/Medical_Job_Mstr_Report_&run_id..&extn_type. &drv72./Encounters_Reporting/APD/Archive/&ddir./ --sse --acl bucket-owner-full-control";
call system(cmd);
run;
   
 
Data temp;		
 length File_name1 $270.;
 file_name=kscan("&fname.",1,'.');
 file_name1=ktrim(file_name)||kcompress("_")||kcompress("&run_id.");
 call symput("fname",File_name);	
 call symput("fname1",File_name1);
Run;	

%LET fname=&fname.;
%LET fname1=&fname1.;
 
%handle_error;	
%nobs(LOGFILE); 
%let nobs1 =&nobs.;
%if &nobs1. = 0 %then %do;

Data mail_file;			
 status = 'PROCESSED';		
Run;

%let path1 = &drv72./Encounters_Reporting/APD/Import/Archive;
%let dirid2=%sysfunc(dopen(&path1));

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 
	
	%if %sysfunc(fileexist("&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv")) =0 %then %do;
	
	filename fileref "&drv2./Encounters_Reporting/APD/Import/&fname..TXT";
	data filename_count;
			infile fileref truncover obs=1;                                                                                                       
			retain filename start_time end_time diff_minutes filesize Recordcount;
			format start_time end_time DATETIME20. filesize $20.;
			length filename $100. filesize $20. Recordcount 8.; 
			fid=fopen('fileref');                                                                                                                 
			filename="&fname1.";
			start_time="&start_time.";
		    end_time = datetime();
			diff_minutes = intck('minute',start_time,end_time);			
			filesize=kcompress(round((finfo(fid,'File Size (bytes)'))/1024)||' KB');
			Recordcount="&recordcount.";                                                                                                    
			drop fid;
		run;
PROC EXPORT DATA= filename_count				            
 OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv"
 dbms=csv replace;
RUN;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv72./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv72./Encounters_Reporting/APD/Archive/&ddir./  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 			
%end;
%else %do;
	
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 		

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv"
 out=filename_count dbms=csv replace;
run;
		data filename_count;
				retain filename start_time end_time diff_minutes filesize Recordcount;
				format start_time end_time DATETIME20. filesize $20.;
				length filename $100. filesize $20. Recordcount 8. ;
				set filename_count;
		run;
		
		filename fileref "&drv2./Encounters_Reporting/APD/Import/&fname..TXT";
		data filename_count1;
			infile fileref truncover obs=1;                                                                                                       
			retain filename start_time end_time diff_minutes filesize Recordcount;
			format start_time end_time DATETIME20. filesize $20.;
			length filename $100. filesize $20. Recordcount 8.; 
			fid=fopen('fileref');                                                                                                                 
			filename="&fname1.";
			start_time="&start_time.";
		    end_time = datetime();
			diff_minutes = intck('minute',start_time,end_time);			
			filesize=kcompress(round((finfo(fid,'File Size (bytes)'))/1024)||' KB');
			Recordcount="&recordcount.";                                                                                                    
			drop fid;
		run;
		proc append base=filename_count data=filename_count1 force;
		run;

PROC EXPORT DATA= filename_count				            
 OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv"
 dbms = csv replace;
RUN;
		
data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv72./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv72./Encounters_Reporting/APD/Archive/&ddir./  --sse --acl bucket-owner-full-control";
call system(cmd);
run;
		
%end;


         data _null_;
         cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Import/&fname..TXT &drv72./Encounters_Reporting/APD/Import/Archive/&ddir./&fname1..TXT --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;

		data _null_;
         cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Import/&fname..TXT &drv72./Encounters_Reporting/APD/Import/Archive/To_Onprem/&fname1..TXT --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;	

         run;	
       %put &fname..TXT was  moved to Archive folder ;
	  %send_mail;
%end;
%else %do ;
	data mail_file;				  		
			status = 'ERROR';
		RUN;

		
		%let path1 = &drv72./Encounters_Reporting/APD/Import/Error ;
				%let dirid2=%sysfunc(dopen(&path1));


                data _null_;
                length ddir $10.;
                    
                    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
                               call symput('ddir',ddir);
                run;

               
                %let destpath = &path1./&ddir;



			data _null_;
     				cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Import/&fname..TXT  &drv72./Encounters_Reporting/APD/Import/Error/To_Onprem/&fname1..TXT --sse --acl bucket-owner-full-control";
     				call system(cmd);
    	 run;
     
			data _null_;
     			cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Import/&fname..TXT  &drv72./Encounters_Reporting/APD/Import/Error/&ddir./&fname1..TXT --sse --acl bucket-owner-full-control";
     			call system(cmd);
     run;
    
     data _null_;
			     cmd1=" aws s3 cp &drv2./Encounters_Reporting/APD/Import/  &drv72./Encounters_Reporting/APD/Import/Error/To_Onprem/ --recursive --exclude '*' --include '*.TXT' --sse --acl bucket-owner-full-control";
			     call system(cmd1);
			   run; 
             
     data _null_;
			     cmd1=" aws s3 mv &drv2./Encounters_Reporting/APD/Import/  &drv72./Encounters_Reporting/APD/Import/Error/&ddir./ --recursive --exclude '*' --include '*.TXT' --sse --acl bucket-owner-full-control";
			     call system(cmd1);
			   run;
			   
					   
%put "Proces was aborted due to errors for &fname..TXT";
				%let nobs1 = 0;
			%send_mail; 
%end;	
		
				
%mend data_archive;
%data_archive; 


data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type. &drv72./Encounters_Reporting/APD/Control_Report/ --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.xlsx &drv72./Encounters_Reporting/APD/Control_Report/ --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/medical_job_mstr.sas7bdat &drv72./Encounters_Reporting/APD/Control_Table/ --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/medical_job_mstr.sas7bdat &drv72./Encounters_Reporting/APD/Archive/&ddir./medical_job_mstr_&run_id..sas7bdat --sse --acl bucket-owner-full-control";
call system(cmd);
run;

proc printto ;
run;
%put ;

data _null_;
put " ~~~~~~~~~~~~~~~ RGA19701 Program End execution ~~~~~~~~~~~~~~~";
run;
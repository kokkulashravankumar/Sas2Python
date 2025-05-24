

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19101 Program Start execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

/* [B-222732] - [SD] - Historical flag - control table is added here */
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/hist_flag.xls &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/socsec_compress.xls &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Bilcod.xls &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/APDenceditmatrix.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NDC_SVCCODE.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/HIOSID_Xwalk.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NDC_SVCCODE_REJECT.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/DIAG_PROCCODE_REJECT.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NUBC_VALUE_REJECT.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Revcod_analysis.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/SVCTYP_HG_Transportation_XWALK.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/SVCTYP_HG_XWALK.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/PAS_Code.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NDC_Unit_of_measurment.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/diag_xwalk.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/CAS_XWALK.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/lob_code.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Patient_status_code.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/USA_States_List.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/x12_ctrl_tab.xls &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/value_code.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Rate_Code.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Outpatient_Rate_Code.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/COSProvider_Specialty_codes_mapping.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

/* 261072 - commeted the logic and introduced new control file */
/*
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/HCPCS_Codes_JCode.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;
*/

/* 261072 - Introduced new control file */

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NDC_Proc_Code_Inclusion.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

/* B-263565 - Control file to refer the NPI city value*/

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NPI_City.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

Data _null_;
	 a=sleep(60,1);
run;

proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/hist_flag.xls"
	out=hist_ctrl_flag dbms=xls
	replace;
run;
proc print data = hist_ctrl_flag; run;
data _null_; 
  set hist_ctrl_flag;   
  call symput('hist_ctrl_flag',kcompress(flag));
run;

proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/socsec_compress.xls"
	out=socsec_compress dbms=xls
	replace;
run;

proc sql ;
select "'"||kstrip(socsec_ctrl)||"'" into :socsec_blnk_out separated by ","  from socsec_compress;
quit;

/* [B-222732] - [SD] - Historical flag - control table ends here */

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/ &drv2./Encounters_Reporting/APD/Control_Table/  --recursive --exclude '*' --include '*.sas7bdat'  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

%let RGA193 = RGA193 	;
%let R42173=/*R42173*/ RGA193;/* HS 542635 - kv*/
Data _null_;
	format today_dt date9.;
	today_dt=today();
	call symput('today_dt',today_dt);
Run;

data _null_;
call symputx('compno', symget('compno'));
run; /* Please Cheack*/

/* %syslput today_dt=&today_dt.; */
/* %syslput fname=&fname.; */
/* %syslput flag=&flag.;  HS 569650 - kv */


data TT;
	fi_ds_dt = put(&today_dt.,yymmddn8.);
	 /* SAS2AWS2: ReplacedFunctionCompress */
	call symput('fi_ds_dt',kcompress(fi_ds_dt)); 
	r_date=put(&today_dt.,mmddyy10.);
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('r_date',kcompress(r_date));
	r_date1=put(&today_dt.,julian6.);
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('r_date1',kcompress(r_date1));
run;

%macro job_mst_update_new;

data _null_;/*B-244088 extension change*/
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type. &drv2./Encounters_Reporting/APD/Control_Report/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

Data _null_;
	 a=sleep(60,1);
run;

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
%if %sysfunc(fileexist("&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type.")) =0 %then %do; /*B-244088 extension change*/
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Report/R42171_Job_Mstr_Report.xls"
		out=Job_Mstr_Report dbms=xls replace;
		run;
		
		data Report;
		retain run_date	run_id	sequence_number	Compno	LOBCOD	typecode	output_file_name	
		output_dataset_name	Load_flag	split_status	treo_flag	Translated_file_name claim_cnt line_cnt total_cnt ; /*272515 added cnt variable*/

		length LOBCOD $3  treo_flag $1.	Translated_file_name $40.;
		set Job_Mstr_Report;
		treo_flag	= '';
		Translated_file_name = ' ';
		LOBCOD = ' ';
		run;
		%if &extension.=xls %then %do; /*B-244088 extension change*/
		PROC EXPORT DATA= Report	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type."
		           DBMS=&extn_type. REPLACE;sheet='REPORT';
		RUN;	 
	%end;
		PROC EXPORT DATA= Report	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report"
		           DBMS=xlsx REPLACE;sheet='REPORT';
		RUN;	
		
%end;	
%else %do;
%end;
%if %sysfunc(exist(&R42173..medical_job_mstr)) = 0 %then %do;
    Data &R42173..medical_job_mstr;
	retain run_id	holdcd_flag	editmatrix_flag	Cmsmap_flag	claim_count	svc_count	ClmY_SvcY_Count	Inst_Inp_Count	
	Inst_Out_Count	Prof_Count	rec_encY2	edit_pass	prof_clm_count	prof_svc_count	inst_clm_count	inst_svc_count
	start_time	end_time	Compno	Load_flag	output_dataset_name	output_file_name	run_date	sequence_number	
	typecode	split_status LOBCOD  treo_flag 	Translated_file_name claim_cnt line_cnt total_cnt; /*272515 added cnt variable*/

	length LOBCOD $3  treo_flag $1.	Translated_file_name $40.;
			set &R42173..R42171_job_mstr;
			treo_flag	= '';
			Translated_file_name = ' ';
			lobcod= '';
	Run;
	
%end;
%else %do;

%end;
%mend;
%job_mst_update_new;


proc sql;
/*     select "&r_date1."||put(sum(count(distinct(run_id)),1),Z2.) */
	select "&r_date1."||put(sum(count(distinct(input(run_id,7.))),1),Z2.)
	into :run_id
	from &R42173..medical_job_mstr where Run_date ="&r_date." ;
quit;
%let run_id=&run_id.;
/* %syslput run_id=&run_id; */
%icd10mapset(RGA13801,1); 
%let icd10cd1=&icd10cd1.;
/* %syslput icd10cd1=&icd10cd1; */
%icd10mapset(RGA13801,2); 
%let icd10cd2=&icd10cd2.;
/* %syslput icd10cd2=&icd10cd2; */
%let LOB=&lob;
%let LOB_=&lob_;
/* %syslput LOB=&LOB; */
%put &LOB.;
%put &LOB_;
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.log"; 
/* B-96285 changed APD to lib macro */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
proc printto print = "&drv2./Encounters_Reporting/APD/Current/&lib._837_Control_Report_&Compno._&fi_ds_dt._&run_id..log" new ;
run;
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
proc printto log = "&drv2./Encounters_Reporting/APD/Current/&lib._837_Log_File_&Compno._&fi_ds_dt._&run_id..log" new ;
run;
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */

%let log_file =&drv2./Encounters_Reporting/APD/Current/&lib._837_Log_File_&Compno._&fi_ds_dt._&run_id..log;

options obs = max nosource2 nosource nosymbolgen nomlogic nomprint  
							pagesize = max linesize = max missing=" " nodate ;
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.txt";  
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.xls";
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.xlsx";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Current/*.csv";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Edit_Matrix_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Control_Table_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Prof_layout_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/INST_layout_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/varlist_script_&compno..sas";
/* %syslput Begf=&Begf; */
/* %syslput Endf=&Endf; */
/* %syslput svcdat=&svcdat; */
/* %syslput compno=&compno.; */
/* %syslput QIfiflag=&QIfiflag; */
%let libn=&libn1.;
/* %syslput libn=&libn.; */
/* %syslput libn2=&libn2.; */
/* %syslput lib=&lib.;	/* B-96285 */
/* %syslput  temp_ds_del_flag=&temp_ds_del_flag.; */
/* %syslput KK=&KK.;  CR# CHG0038264 - added */

%macro QI_file_arc();
   %global ddir;
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	%let path1 = &drv72./Encounters_Reporting/APD/Input_File ;
/* 	%let dirid2=%sysfunc(dopen(&path1)); */
                data _null_;
                length ddir $10.;
                    /* SAS2AWS2: ReplacedFunctionTranslate */
                    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
                    call symput('ddir',ddir);					
                run;

                /* SAS2AWS2: ReplacedSlash */
                %let destpath = &path1./&ddir.;


/*                 %let dirid2=%sysfunc(dopen(&destpath)); */
/*                 data _null_ ; */
/*                                SAS2AWS2: ReplacedSlash-UpdatedOSCommands */
/*                                commd="mkdir &path1./&ddir"; */
/*                                call system(commd); */
/*                 run; */
/*                 %let rc=%sysfunc(dclose(&dirid2)); */

%mend;
%QI_file_arc();
/* %syslput ddir=&ddir.; */

/*CR# CHG0038264 - added macro and if , else condition  */
%macro test;
%if &KK. = 1 %then %do;
%let keepv_providernpi= ENTITYTYP NPI  PROVFSTNAM PROVLSTNAM  PROVMIDNAM NPIDEACTDATE PROVENUMDATE NPIREACTDATE	/* [B-335998] Added*/			
						HLTHPROVTAXOCODE1 PROVFIRPRACADDR PROVSECPRACADDR PROVPRACCITY 
						PROVPRACSTATE 	PROVPRACPOSTAL  PORGNAM EIN HLTHPROVTAXOCODE2
						HLTHPROVTAXOCODE3 HLTHPROVTAXOCODE4 HLTHPROVTAXOCODE5;/*B-289556 Added*/
						
Data /*providernpi_&compno.*/ providernpi(rename=(npi=npiid PROVPRACPOSTAL1 =PROVPRACPOSTAL));/*CR# CHG0038264 - dataset name changed */
	Length First_name Middle_name $35. Last_name $60.;	/* B-146417 Last name $35. */
	format First_name Middle_name $35. Last_name $60.;	/* B-146417 Last name $35. */
	set &libn2..providernpi(keep=&keepv_providernpi.);
	if ENTITYTYP=1 then do;
		First_name=PROVFSTNAM;
		Last_name=PROVLSTNAM; 
		Middle_name=PROVMIDNAM;
	end;
	else if ENTITYTYP=2 then do;
		First_name='';
		Last_name=PORGNAM;
		Middle_name='';
	end;
    /* SAS2AWS2: ReplacedFunctionCompress */
    EIN=kcompress(EIN,"<UNAVAIL>");
	*PROVPRACPOSTAL1= compress(PROVPRACPOSTAL,"@:*`~#-= ");/*CR# CHG0034118 - added equal sign in compress function*/
	/* CR# CHG0038449 - Added compress function for zip code to keep only digits */
	/* SAS2AWS2: ReplacedFunctionCompress */
	PROVPRACPOSTAL1=kcompress(PROVPRACPOSTAL,'','kd');
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranslate */
	PROVPRACCITY=compbl(kcompress(ktranslate(PROVPRACCITY,' ','-'),"","ka"));		/* B-108297 city alpha */
	where missing(npi) = 0 ;
	Drop PROVFSTNAM  PROVLSTNAM PROVMIDNAM PROVPRACPOSTAL ;
run;
%end;
%mend test;
%test;
/*CR# CHG0038264 - added macro and if , else condition  ---> end */

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile = "&drv2./Encounters_Reporting/APD/Control_Table/Bilcod.xls" DBMS = xls
	OUT = cntrl_Bilcod replace;	/* B-92443 added import */
Run;


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/APDenceditmatrix.xlsx"
		out=APD_enceditmatrix dbms=xlsx replace;
run;

%macro encedit;        /*[B-315447] - DS - Updates*/ 
 %global elig_chk;	
 
		proc sql;
		create table APD_enceditmatrix_tmp as
		select distinct	encapdfld, description, editlogic, dwhfld, dwhtable, editcode, enctypcode,
			edittyp, missflag, valflag, Dtrgeflag, Numflag, Othcheckflag,COStype	/* keep COStype as blank */
		from APD_enceditmatrix	  
		where &LOB_. = 'Y';  
		quit;
		
		data APD_enceditmatrix; set APD_enceditmatrix_tmp;				
			  if ( (&compno=45)  or (&compno=30) or (&compno=42 and "&LOB." eq "QHP") )  then do;
				COStype = '  ';		
					if editcode='99942' then call symput('elig_chk', 'Y');
				end;
		run;	
%mend;
%encedit;


/* %syslput elig_chk=&elig_chk.; */

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NDC_SVCCODE.xlsx"
	out=NDC_SVCCODE dbms=xlsx
	replace;
Run;

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/HIOSID_Xwalk.xlsx"
	out=HIOSID (drop=comments) dbms=xlsx  /*B-274606*/
	replace;
Run;


/* 541126 - kv added logic to read NDC_SVCCODE_reject control file */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NDC_SVCCODE_REJECT.xlsx"
	out=NDC_SVCCODE_reject dbms=xlsx
	replace;
Run;


/* 569650 - kv added logic to read DIAG_PROCCODE_reject control file */
 /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
 Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/DIAG_PROCCODE_REJECT.xlsx"
	out=DIAG_PROCCODE_REJECT dbms=xlsx
	replace;
Run;
/* HS# 565133 -  added logic to read new control tables  ---> start */


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NUBC_VALUE_REJECT.xlsx"
	out=NUBC_VALUE_REJECT dbms=xlsx 
	replace;sheet=data;
Run;



/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Revcod_analysis.xlsx"
	out=revcod_analysis dbms=xlsx
	replace;sheet=data;
Run;


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/SVCTYP_HG_Transportation_XWALK.xlsx"
	out=SVCTYP_HG_Transportation_XWALK dbms=xlsx
	replace;sheet=data;
Run;

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/SVCTYP_HG_XWALK.xlsx"
	out=SVCTYP_HG_XWALK dbms=xlsx
	replace;sheet=data;
Run;

data SVCTYP_HG_XWALK;
length clamno $20. Rollup_lineno lineno $05.;
set SVCTYP_HG_XWALK;
run;
/* HS# 565133 - added logic to read new control tables --> end */

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/PAS_Code.xlsx"
	out=PAS_code dbms=xlsx
	replace; sheet=data;
Run; /* B-168796 */	

/* CR# CHG0031701 - kv added logic to read NDC_Unit_of_measurment control file */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NDC_Unit_of_measurment.xlsx"
	out=NDC_Unit_of_measurment dbms=xlsx
	replace;
Run;

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/diag_xwalk.xlsx"
	out=diag_xwalk dbms=xlsx
	replace;						/* B-117000 import diag */
run;

/* VVKR Code Start -- B-75041 */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/CAS_XWALK.xlsx"
	out=CAS_XWALK dbms=xlsx
	replace;
run;
/* VVKR Code End -- B-75041 */
/* Rsubmit;  */
/* ALM# 7174 -  added logic to read lob_code control file */

/* VVKR Code Start -- B-75041 */
/* proc upload data=CAS_XWALK; */
/* run; */
/* VVKR Code End -- B-75041 */
 
data lob_code;
	Length lobcode $4. descrp $4.; 
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	infile "&drv2./Encounters_Reporting/APD/Control_Table/lob_code.csv" recfm=v dlm=',' firstobs = 2 ;
	input lobcode descrp;				   

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

run;

proc sql;
/* SAS2AWS2: ReplacedFunctionStrip */
select "'"||kstrip(lobcode)||"'" into :lobcode separated by ","  from lob_code where descrp eq "&lob." /*'EP'*/; /* B-96285 include QHP */
quit;


/* B-108297 Patient status code reject */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Patient_status_code.xlsx"
		out=Patient_status_code dbms=xlsx replace;
Run;
Proc sort data=Patient_status_code nodupkey; by distat; run;

proc sql ;
/* SAS2AWS2: ReplacedFunctionStrip */
select "'"||kstrip(distat)||"'" into :distat_reject separated by ","  from Patient_status_code;
quit;


/* B-108297 state check */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/USA_States_List.xlsx"
	out=USA_States_List dbms=xlsx
	replace;
run;
Proc sort data=USA_States_List(keep=Abbreviation) nodupkey; by Abbreviation; run;

proc sql ;
/* SAS2AWS2: ReplacedFunctionStrip */
select "'"||kstrip(Abbreviation)||"'" into :US_States separated by ","  from USA_States_List;
quit;



/* B-121242 OSDS */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/x12_ctrl_tab.xls"
	out=x12_ctrl_tab dbms=xls
	replace;sheet='REPORT'; /*B-248997*/
run;

data _null_; 
  set x12_ctrl_tab(where=(compno="&compno." and lobcod="&LOB.")); /* B-146417 */
  /* SAS2AWS2: ReplacedFunctionCompress */
  call symput('x12_ctrl',kcompress(x12_ctrl));
run;

/* Proc upload data=HIOSID;run; */
/*ALM# 8377 kv - added logic to map HIOS ID for shpcaid ---> START */
%macro hios_id;
%if &Compno.=02 %then %do;
data _null_;
/* SAS2AWS2: ReplacedFunctionReverse-ReplacedFunctionScan-ReplacedFunctionSubstr */
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
/*ALM# 8377 kv - added logic to map HIOS ID for shpcaid ---> END */


data valcode;
  length valcode $2;
  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
  infile "&drv2./Encounters_Reporting/APD/Control_Table/value_code.csv" dsd dlm=',';
  input valcode $;

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

run;
Proc sql;
	/* SAS2AWS2: ReplacedFunctionCompress */
	select kcompress('"' || valcode || '"')
		into :vcode  separated by ','
		from valcode;
Quit;

/* Proc upload data=NDC_SVCCODE; */
/* Run; */
Data NDC_SVCCODE;
set NDC_SVCCODE;
/* SAS2AWS2: ReplacedFunctionCompress */
NDC_Svccod1=kcompress("'"||NDC_Svccod||"'");
run;
Proc sql noprint;
Select NDC_Svccod1 into :NDC_Svccod SEPARATED
 by ',' from NDC_SVCCODE;
quit;
%put &NDC_Svccod.;
/* 541126 - kv added logic to convert NDCSVC codes available in NDC_SVCCODE_reject control file into macro variable -- start */
/* Proc upload data=NDC_SVCCODE_reject; */
/* Run; */
Data NDC_SVCCODE_reject;
set NDC_SVCCODE_reject;
/* SAS2AWS2: ReplacedFunctionCompress */
NDCSvccod1=kcompress("'"||NDCSvccod||"'");
run;
Proc sql noprint;
Select NDCSvccod1 into :NDCSvccodrej SEPARATED
 by ',' from NDC_SVCCODE_reject;
quit;

%put &NDCSvccodrej.;
/* 569650 - kv added logic to convert diag & proc codes available in DIAG_PROCCODE_REJECT control file into macro variable -- end */
/* Proc upload data=DIAG_PROCCODE_REJECT; */
/* Run; */
Data DIAG_PROCCODE_REJECT;
set DIAG_PROCCODE_REJECT;
/* SAS2AWS2: ReplacedFunctionCompress */
diagcode1=kcompress("'"||diagcode||"'");
/* SAS2AWS2: ReplacedFunctionCompress */
proccode1=kcompress("'"||proccode||"'");
run;
Proc sql noprint;
Select diagcode1 into :icd10cd1 SEPARATED
 by ',' from DIAG_PROCCODE_REJECT;
 Select proccode1 into :icd10cd2 SEPARATED
 by ',' from DIAG_PROCCODE_REJECT;
quit;

%put &icd10cd1.  &icd10cd2. ;
/* CR# CHG0031701 - kv added logic to read NDC_Unit_of_measurment control file */
/* Proc upload data=NDC_Unit_of_measurment; */
/* Run; */
Data NDC_Unit_of_measurment;
set NDC_Unit_of_measurment;
/* SAS2AWS2: ReplacedFunctionCompress */
NDCUOM1=kcompress("'"||NDCUOM||"'");
run;
Proc sql noprint;
Select NDCUOM1 into :NDC_UOM SEPARATED
 by ',' from NDC_Unit_of_measurment;
quit;

%put &NDC_UOM.;

/* CR# CHG0033169 - added logic to read rate code map control file */ 
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Rate_Code.xlsx"
	out=Rate_Code dbms=xlsx
	replace;
Run;

Proc sql;
  	select count(*)
   	into :Rate_code_total
	from Rate_code ;
quit;
%let  Rate_code_total			= 	&Rate_code_total			;


proc sql noprint ;
    select Rate_Code,
		   New_Rate_Code
    into :Rate_Code1-:Rate_Code&Rate_code_total.,
		 :New_Rate_Code1-:New_Rate_Code&Rate_code_total.		 
    from Rate_code; 
Quit;	


/* B-97137 add Outpatient Rate Code start */ 
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Outpatient_Rate_Code.csv"
	out=Outpatient_Rate_Code dbms=CSV
	replace;
Run;

Proc sql;
  	select count(*)
   	into :OP_Rate_code_total
	from Outpatient_Rate_code ;
quit;
%let  OP_Rate_code_total			= 	&OP_Rate_code_total			;


proc sql noprint ;
    select valcd01, valcd02, valcd03, valcd04, valcd05, valcd06, valcd07, valcd08, valcd09, valcd10, valcd11, valcd12,
			vala01, vala02, vala03, vala04, vala05, vala06, vala07, vala08, vala09, vala10, vala11, vala12
    into :valcd01OP1-:valcd01OP&OP_Rate_code_total.,
		 :valcd02OP1-:valcd02OP&OP_Rate_code_total.,
		 :valcd03OP1-:valcd03OP&OP_Rate_code_total., 
		 :valcd04OP1-:valcd04OP&OP_Rate_code_total.,
		 :valcd05OP1-:valcd05OP&OP_Rate_code_total., 
		 :valcd06OP1-:valcd06OP&OP_Rate_code_total.,
		 :valcd07OP1-:valcd07OP&OP_Rate_code_total., 
		 :valcd08OP1-:valcd08OP&OP_Rate_code_total.,
		 :valcd09OP1-:valcd09OP&OP_Rate_code_total., 
		 :valcd10OP1-:valcd10OP&OP_Rate_code_total., 
		 :valcd11OP1-:valcd11OP&OP_Rate_code_total., 
 		 :valcd12OP1-:valcd12OP&OP_Rate_code_total.,	 
		 :vala01OP1-:vala01OP&OP_Rate_code_total.,
		 :vala02OP1-:vala02OP&OP_Rate_code_total.,
		 :vala03OP1-:vala03OP&OP_Rate_code_total., 
		 :vala04OP1-:vala04OP&OP_Rate_code_total.,
		 :vala05OP1-:vala05OP&OP_Rate_code_total., 
		 :vala06OP1-:vala06OP&OP_Rate_code_total.,
		 :vala07OP1-:vala07OP&OP_Rate_code_total., 
		 :vala08OP1-:vala08OP&OP_Rate_code_total.,
		 :vala09OP1-:vala09OP&OP_Rate_code_total., 
		 :vala10OP1-:vala10OP&OP_Rate_code_total., 
		 :vala11OP1-:vala11OP&OP_Rate_code_total., 
 		 :vala12OP1-:vala12OP&OP_Rate_code_total.		 
    from Outpatient_Rate_code; 
Quit;

/* B-97137 add Outpatient Rate Code end */ 

/* New change CR# S-22225 - added logic to read COSProvider_Specialty_codes_mapping control file-Start Here */ 

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/COSProvider_Specialty_codes_mapping.xlsx"
	out=COSProvider_Specialty_codes dbms=xlsx
	replace;
Run;

Proc sql;
  	select count(*)
   	into :COSProvider_code_total
	from COSProvider_Specialty_codes ;
quit;
%let  COSProvider_code_total= &COSProvider_code_total			;
proc sql  ;
    select Valcode,
		   Valamount,
		   COS,
		   prvspcd
    into :Valcode1-:Valcode&COSProvider_code_total.,
		 :Valamount1-:Valamount&COSProvider_code_total.,
		 :COS1-:COS&COSProvider_code_total.,
		 :prvspcd1-:prvspcd&COSProvider_code_total.

    from COSProvider_Specialty_codes; 
Quit;

/* B68148 */

/* 261072 - commeted the logic and introduced new control file */
/*

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/HCPCS_Codes_JCode.xlsx"
	out=HCPCS_Codes_JCode_Rejection dbms=xlsx
	replace;
Run;

proc sql ;
select "'"||kstrip(HCPCS_PROC_CD)||"'" into :J_HCPCS_PROC_CD separated by ","  from HCPCS_Codes_JCode_Rejection ;
quit;
%put &J_HCPCS_PROC_CD.;
*/

/* B-261072 - introduced new control file */

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NDC_Proc_Code_Inclusion.xlsx"
		out=NDCSVC_PROCCOD_Rejection dbms=xlsx replace; sheet ='Proccod_List';
run;

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NDC_Proc_Code_Inclusion.xlsx"
		out=NDCSVC_BILLCOD_Rejection dbms=xlsx replace; sheet ='Bilcod_List';
run;

/* B-263565 - Control file to refer the NPI city value*/

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NPI_City.xlsx"
		out=NPI_City dbms=xlsx replace; sheet ='NPI_City';
run;


proc sql ;
select "'"||kstrip(PROC_CD)||"'" into :NDC_PROC_CD separated by ","  from NDCSVC_PROCCOD_Rejection ;
quit;
/* %put &NDC_PROC_CD; */

proc sql ;
select "'"||kstrip(Bill_Code)||"'" into :NDC_BILL_CD separated by ","  from NDCSVC_BILLCOD_Rejection ;
quit;
%put &NDC_BILL_CD;

/* commenting to check empty file */
/*
proc sql ;
select "'"||kstrip(NPI_Ctrl)||"'" into :NPI_Ctrl separated by ","  from NPI_City ;
quit;
%put &NPI_Ctrl;
*/



/* B68148 */

/* New change CR# S-22225 - added logic to read COSProvider_Specialty_codes_mapping control file-Ends Here */ 


/* HS# 565133 - kv added control tables */
/* Proc upload data=NUBC_VALUE_REJECT; */
/* Run; */
/* Proc upload data=revcod_analysis; */
/* Run; */
/*  */
/* Proc upload data=SVCTYP_HG_Transportation_XWALK; */
/* Run; */
/* Proc upload data=SVCTYP_HG_XWALK; */
/* Run; */
/*  */
/* Proc upload data=PAS_Code; */
/* Run; /* B-168796 */
/*  */
/* Proc upload data=diag_xwalk; */
/* Run; /* B-117000 import diag */

data TT;
	length run_dttime $ 50. 
	;
	run_dt = put(&today_dt.,mmddyy10.);
	run_date = put(&today_dt.,date7.);
	run_time = put(time(),time10.); 
	/* SAS2AWS2: ReplacedFunctionCatx */
	run_dttime = catx('_' ,run_date,run_time);

	st_time = put(datetime(),DATETIME20.);
	fi_ds_dt = put(&today_dt.,yymmddn8.);
	fi_ds_dt_T=put(&today_dt.,mmddyy10.);

	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('run_date',kcompress(run_date));
    /* SAS2AWS2: ReplacedFunctionCompress */
    call symput('run_dttime',kcompress(run_dttime,": "));
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('st_time',kcompress(st_time));
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('fi_ds_dt',kcompress(fi_ds_dt));
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('run_dt',kcompress(run_dt));
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('fi_ds_dt_T',kcompress(fi_ds_dt_T));
 run;
/*CR# CHG0038264 - added macro and if , else condition  ---> start */
/* Proc upload data=providernpi_&compno.;*//*CR# CHG0038264 - commented */
/*run;*/
 %macro test;
%if &KK. = 1 %then %do;
/* Proc upload data=providernpi; */
/* run; */
data providernpi_&compno.;
set providernpi;
run; 
%end;
%else %do;
data providernpi_&compno.;
set providernpi;
run; 
%end;
%mend test;
%test;

/* B-263565 - Control file to refer the NPI city value*/

data providernpi_&compno.;
set providernpi_&compno.;
PROVPRACCITY = kcompress(PROVPRACCITY ,"@:*`~#") ;
run;  

proc sort data = NPI_City out=npi_Ctrl1(rename=(NPI_Ctrl=NPIID)); by NPI_Ctrl; run;
proc sort data = providernpi_&compno.; by NPIID; run;

data providernpi_&compno.(DROP = NPI_Ctrl_City);
merge providernpi_&compno.(in=a) npi_Ctrl1(in=b);
if a ;
if a and b then do;	 
if missing(PROVPRACCITY) =0 and (klength(PROVPRACCITY)<2 or klength(PROVPRACCITY)>30) then
PROVPRACCITY = NPI_Ctrl_City;
end;
by NPIID;
run;


	
/*CR# CHG0038264 - added macro and if , else condition  ---> end */

/* Proc upload data=cntrl_Bilcod ;	/* B-92443 */
/* Run; */
/*  */
/* Proc upload data=APD_enceditmatrix ; */
/* Run; */

options obs = max nosource2 nosource nosymbolgen nomlogic nomprint 
							pagesize = max linesize = max missing=" " nodate ;
/***********************************************************************************************/
/****					Assigning librefs to macro variables								****/
/***********************************************************************************************/
%let RGA191 = work 	;
%let RGA191_ = RGA191;
%let RGA192 = RGA192;	 
%let RGA193 = RGA193;	
%let R42173=/*R42173*/ RGA193;/* HS 542635 - kv*/

/********************************************************************************************/
/****					Dates from frames - Start  						   				 ****/ 
/********************************************************************************************/
%macro AssignDt;
  
   data _null_;	
 p1str    = "'";
       pstr1    = "'d";	
%if &QIfiflag=Y %then %do;
	 svcdat  = put(&svcdat,date9.);
	 /* SAS2AWS2: ReplacedFunctionTrim */
	 test=ktrim(p1str)||ktrim(svcdat)||ktrim(pstr1);
	 call symput('svcdat',test);      
%end;
%else %do;
	 startdt = put(&Begf,date9.);
       enddt   = put(&Endf,date9.);
	   svcdat  = put(&svcdat,date9.);     
       /* SAS2AWS2: ReplacedFunctionTrim */
       rptdate1 = ktrim(p1str)||ktrim(startdt)||ktrim(pstr1);
       /* SAS2AWS2: ReplacedFunctionTrim */
       rptdate2 = ktrim(p1str)||ktrim(enddt)||ktrim(pstr1);
	   call symput('startdate',rptdate1);
       call symput('enddate',rptdate2);
       begfdt  = put(&Begf,date9.);
       endfdt  = put(&Endf,date9.);
       /* SAS2AWS2: ReplacedFunctionTrim */
       svcdate1 = ktrim(p1str)||ktrim(begfdt)||ktrim(pstr1);
       /* SAS2AWS2: ReplacedFunctionTrim */
       svcdate2 = ktrim(p1str)||ktrim(endfdt)||ktrim(pstr1);
	   /* SAS2AWS2: ReplacedFunctionTrim */
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

/********************************************************************************************/
/****					Dates from frames - End  						   				 ****/ 
/********************************************************************************************/

/********************************************************************************************/
/****					Getting the run id from control table - Starts	   				 ****/ 
/********************************************************************************************/
%macro job_mst_update;

data _null_;/*B-244088 extension change*/
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type. &drv2./Encounters_Reporting/APD/Control_Report/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

Data _null_;
	 a=sleep(30,1);
run;

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
%if %sysfunc(fileexist("&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type.")) =1 %then %do; /*B-244088 extension change*/

		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
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

/********************************************************************************************/
/****					Assigning process start time					   				 ****/ 
/********************************************************************************************/
/********************************************************************************************/
/****					Getting the run id from control table - Ends	   				 ****/ 
/********************************************************************************************/
/* SAS2AWS2: ReplacedSlash */
filename InpPrg 	"&drv./Programs/";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
filename EmOut 		"&drv2./Encounters_Reporting/APD/Temp/Edit_Matrix_script_&Compno..sas" 				;
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
filename ctrlout 	"&drv2./Encounters_Reporting/APD/Temp/Control_Table_script_&Compno..sas"			;
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
filename Prof		"&drv2./Encounters_Reporting/APD/Temp/Prof_layout_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
filename INST		"&drv2./Encounters_Reporting/APD/Temp/INST_layout_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
filename varlist 	"&drv2./Encounters_Reporting/APD/Temp/varlist_script_&compno..sas";
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
/****                                Writing out Control Report and Log file      ****/ 
/********************************************************************************************/

/********************************************************************************************/
/****     Calling Main Programs                                                          ****/ 
/********************************************************************************************/
%if &compno.=01 and &LOB. = DSNP %then %do;  /*B-315473*/
%include InpPrg(RGA19601.sas) ;
%end;
%include InpPrg(RGA19201.sas) ;
%include InpPrg(RGA19301.sas) ;
%include InpPrg(RGA19401.sas) ;


DATA STATS;
   SET STAT;
   BI_TOTAL = BI_HIT + BI_NOHIT ;
   BI_RATE = BI_HIT/BI_TOTAL ;

   OP_TOTAL = OP_HIT + OP_NOHIT ;
   OP_RATE = OP_HIT/OP_TOTAL ;

   MEM_TOTAL = MEM_HIT + MEM_NOHIT ;
   MEM_RATE = MEM_HIT/MEM_TOTAL ;
 
   BI_providernpi_TOTAL = BI_providernpi_HIT + BI_providernpi_NOHIT  ;
   BI_providernpi_RATE = BI_providernpi_HIT / BI_providernpi_TOTAL ;

   AT_providernpi_TOTAL = AT_providernpi_HIT + AT_providernpi_NOHIT  ;
   AT_providernpi_RATE = AT_providernpi_HIT / AT_providernpi_TOTAL ;   

   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_HIT',kright(put(BI_HIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_NOHIT',kright(put(BI_NOHIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_TOTAL',kright(put(BI_TOTAL,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_RATE',kright(put(BI_RATE,PERCENT8.2)));

   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_providernpi_HIT',kright(put(BI_providernpi_HIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_providernpi_NOHIT ',kright(put(BI_providernpi_NOHIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_providernpi_Total ',kright(put(BI_providernpi_Total,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('BI_providernpi_rate',kright(put(BI_providernpi_rate,PERCENT8.2)));

   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('OP_HIT',kright(put(OP_HIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('OP_NOHIT',kright(put(OP_NOHIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('OP_TOTAL',kright(put(OP_TOTAL,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('OP_RATE',kright(put(OP_RATE,PERCENT8.2)));

   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('AT_providernpi_HIT',kright(put(AT_providernpi_HIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('AT_providernpi_NOHIT ',kright(put(AT_providernpi_NOHIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('AT_providernpi_Total ',kright(put(AT_providernpi_Total,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('AT_providernpi_rate',kright(put(AT_providernpi_rate,PERCENT8.2)));

   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('MEM_HIT',kright(put(MEM_HIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('MEM_NOHIT',kright(put(MEM_NOHIT,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('MEM_TOTAL',kright(put(MEM_TOTAL,COMMA10.)));
   /* SAS2AWS2: ReplacedFunctionRight */
   call symput('MEM_RATE',kright(put(MEM_RATE,PERCENT8.2)));

RUN ;

Data _null_;
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('QI_count',kcompress(&QI_count.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('QI_and_SV_count',kcompress(&QI_and_SV_count.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('QI_count_clm',kcompress(&QI_count_clm.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('QI_and_clm_count',kcompress(&QI_and_clm_count.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('ClmY_SvcY_Count1',kcompress(&ClmY_SvcY_Count1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('II_Count2_1',kcompress(&II_Count2_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('IO_Count2_1',kcompress(&IO_Count2_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('P_Count2_1',kcompress(&P_Count2_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('Rej_Enc_N_IO_P_1',kcompress(&Rej_Enc_N_IO_P_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('Hold_Code_Pass_1',kcompress(&Hold_Code_Pass_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('rec_encY2_1',kcompress(&rec_encY2_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('rec_encY2_1',kcompress(&rec_encY2_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('edit_pass_1',kcompress(&edit_pass_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('ClmTot_Fail_1',kcompress(&ClmTot_Fail_1.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('prof_clm_count_p',kcompress(&prof_clm_count_p.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('inst_clm_count_p',kcompress(&inst_clm_count_p.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('inst_I_clm_count_P',kcompress(&inst_I_clm_count_P.));
   /* SAS2AWS2: ReplacedFunctionCompress */
   call symput('inst_O_clm_count_P',kcompress(&inst_O_clm_count_P.));
   call symput ('Reject_DG_I', kcompress(&Reject_DG_I.));
   call symput('Bil_cod_Rej_count',kcompress(&Bil_cod_Rej_count.));
run;

%Macro Control_Report();

%if &QIfiflag=Y %then %do;
data _null_;
  length end_dttime $ 30. ;
  end_date = put(today(),date7.);
  /* SAS2AWS2: ReplacedFunctionCompress */
  end_time = kcompress(put(time(),time10.),':');
  /* SAS2AWS2: ReplacedFunctionCatx */
  end_dttime = catx('_' ,end_date,end_time);
  /* SAS2AWS2: ReplacedFunctionCompress */
  call symput('end_dttime',kcompress(end_dttime));
  ed_time = put(datetime(),DATETIME20.);
  /* SAS2AWS2: ReplacedFunctionCompress */
  call symput('ed_time',kcompress(ed_time));


  file print;

  title ;
  put ;
  put @ 20              "APD 837 Encounter Extract Control Report";
  put @ 20              "***********************************************************";
  put ;
  
  put @ 20              "Service Line level internal Reject and output count Details";
  put @ 20              "***********************************************************";
  put ;
  
  put ;
  put  @ 5  "01. Input QI Records........................................................:" @ 85 "&QI_count."  ;
  put  @ 5  "01A. QI File Duplicate Record at Claims and Service line level[Rejected]...:" @ 85 "&QI_duplicate." ; 
  put  @ 5  "02. QI File Match with Service Line Table Records...........................:" @ 85 "&QI_and_SV_count." ;
  put  @ 5  "03. QI File Not Match with Service Line Table Records[Rejected].............:" @ 85 "&QI_not_SV_count." ;
  put ;
 
  put  @ 5  "05.BILCOD reject[Rejected]..................................................:" @ 85 "&Bil_cod_Rej_count." ;
  put  @ 5  "05A. Service lines Records with NO  Claims Records [Rejected]................:" @ 85 "&ClmN_SvcY_Count." ;
  put  @ 5  "06. Claims with Service lines Records after merge............................:" @ 85 "&ClmY_SvcY_Count." ;
  put ;       
  put;
  put  @ 5  "07. Institutional Inpatient Service Line Records(I) ........................:" @ 85 "&II_Count2." ;
  put  @ 5  "08. Institutional Outpatient Service Line Records(O) .......................:" @ 85 "&IO_Count2." ;
  put  @ 5  "09. Professional Service Line Records(P) ...................................:" @ 85 "&P_Count2." ;
  put ;       
  put  @ 5  "10. Claims with Service lines Records (IO & P with HoldFlag = N)[Rejected]..:" @ 85 "&Rej_Enc_N_IO_P." ;
  put  @ 5  "11. Claims Records Passed after Hold Code Process ..........................:" @ 85 "&Hold_Code_Pass." ;
  
  put ;
  put  @ 5  "12. Claims with Svc Records passed Hold Code after provider/member merge....:" @ 85 "&rec_encY2." ;

  put ;
  put  @ 5  "12A. Reject_DG_I service line level count(Rejected]..........................:" @ 85 "&Reject_DG_I." ; 

  put ;
  put  @ 5  "13. Claims with Svc Records Passed Edit Matrix..............................:" @ 85 "&edit_pass." ;
  put  @ 5  "14. Claims with Svc Records Failed Edit Matrix..............................:" @ 85 "&edit_fail." ;
  
  put ;
  put  @ 5  "15. Total Svc Records Failed [01A. + 4. + 5. +5A. + 10. + 12A. +14]..........:" @ 85 "&ClmTot_Fail." ;
  put ;
  
  
  put  @ 5  "16. Professional Service Line Records (2400 + ).............................:" @ 85 "&prof_svc_count_p." ;
  put  @ 5  "16A. Dme Service Line Records (2400 + ).....................................:" @ 85 "&Dme_svc_count_p." ;
  put ;
  put  @ 5  "17. Institutional Service Line Records (2400 + )............................:" @ 85 "&inst_svc_count_p." ;
  put ;
  put  @ 5  "18. Institutional Service Line Records Inpatient(2400 + )...................:" @ 85 "&inst_I_SVC_count_P." ;
  put  @ 5  "19. Institutional Service Line Records Outpatient(2400 + )..................:" @ 85 "&inst_O_SVC_count_P." ;
  put ;


  put @ 20             "Claims level internal Reject and output count Details";
  put @ 20              "********************************************************";
  put ; 

  put ;
  put  @ 5  "01. Input QI Records........................................................:" @ 85 "&QI_count_clm."  ;
  put  @ 5  "02. After Match with Service Line Table Claims records.......................:" @ 85 "&QI_and_clm_count." ;
  put ;
 
  put  @ 5  "03A. Bilcode [Rejected].....................................................:" @ 85 "&Bil_cod_Rej_count1." ;
  put  @ 5  "04. Service lines Records with NO  Claims Records [Rejected]................:" @ 85 "&ClmN_SvcY_Count1." ;
  put  @ 5  "05. Claims with Service lines Records after merge...........................:" @ 85 "&ClmY_SvcY_Count1." ;
  put ;       
  put;
  put  @ 5  "06. Institutional Inpatient Service Line Records(I) ........................:" @ 85 "&II_Count2_1." ;
  put  @ 5  "07. Institutional Outpatient Service Line Records(O) .......................:" @ 85 "&IO_Count2_1." ;
  put  @ 5  "08. Professional Service Line Records(P) ...................................:" @ 85 "&P_Count2_1." ;
  put ;       
  put  @ 5  "09. Claims with Service lines Records (IO & P with HoldFlag = N)[Rejected]..:" @ 85 "&Rej_Enc_N_IO_P_1." ;
  put  @ 5  "10. Claims Records Passed after Hold Code Process ..........................:" @ 85 "&ttt." ;

  
  put ;
  put  @ 5  "11. Claims with Svc Records passed Hold Code after provider/member merge....:" @ 85 "&rec_encY2_1." ;

  put ;
  put  @ 5  "11A. Reject_DG_I Claims level count(Rejected]................................:" @ 85 "&Reject_DG_I_2." ; 

  put ;
  put  @ 5  "12. Claims Failed Edit Matrix...............................................:" @ 85 "&edit_fail_1." ;
  put  @ 5  "13. Claims Passed Edit Matrix...............................................:" @ 85 "&edit_pass_1." ;
  
  
  put ;
  put  @ 5  "14. Total Claims Records Failed [03. + 03A. + 04  + 09. + 11A. + 12].......:" @ 85 "&ClmTot_Fail_1." ;
  put ;
  
  put  @ 5  "15. Professional Claims Records.............................................:" @ 85 "&prof_clm_count_p." ;
  put  @ 5  "15A. Dme Claims Records.....................................................:" @ 85 "&Dme_clm_count_p." ;
  put ;
  put  @ 5  "16. Institutional Claims Records............................................:" @ 85 "&inst_clm_count_p." ;
  put ;
  put  @ 5  "17. Institutional Claims Records Inpatient(2400 + )..........................:" @ 85 "&inst_I_clm_count_P." ;
  put  @ 5  "18. Institutional Claims Records Outpatient(2400 + ).........................:" @ 85 "&inst_O_clm_count_P." ;
  put ;

  put  @ 5  "*****************************************************************************";
  put  @ 5  "** NOTE: Provider / Member table merge is done after Hold Code Pass and    **";
  put  @ 5  "**      before Edit Matrix Process                                         **";
  put  @ 5  "*****************************************************************************";

  put ;
  put  @ 5  "Operating Physician [Claims.oprnpi = Providernpi.NPIID]" ;
  put  @ 5  "------------------------------------------------------" ;
  put  @ 5  "01. Total Records...........................................................:" @ 85 "&OP_TOTAL." ;
  put  @ 5  "02. Operating Physician Matching Records....................................:" @ 85 "&OP_HIT." ;
  put  @ 5  "03. Operating Physician Non-Matching Records................................:" @ 85 "&OP_NOHIT." ;
  put  @ 5  "04. Percentage of Operating Physician Matched Records.......................:" @ 85 "&OP_RATE." ;
  put ;

  put ;
	put  @ 5  "Billing Provider  [Claims.blpnpi = Providernpi.NPIID]" ;
	put  @ 5  "-----------------------------------------------------------------" ;
	put  @ 5  "01. Total Records...........................................................:" @ 85 "&BI_providernpi_Total." ;
	put  @ 5  "02. Billing Provider Matching Records.......................................:" @ 85 "&BI_providernpi_HIT." ;
	put  @ 5  "03. Billing Provider Non-Matching Records...................................:" @ 85 "&BI_providernpi_NOHIT." ;
	put  @ 5  "04. Percentage of Billing Provider Matched Records..........................:" @ 85 "&BI_providernpi_rate." ;
	put ;

  put ;
  put  @ 5  "Attending Provider [Claims.atpnpi = Providernpi.NPIID]" ;
  put  @ 5  "-----------------------------------------------------------------" ;
  put  @ 5  "01. Total Records...........................................................:" @ 85 "&AT_providernpi_Total." ;
  put  @ 5  "02. Attending Provider Matching Records.....................................:" @ 85 "&AT_providernpi_HIT." ;
  put  @ 5  "03. Attending Provider Non-Matching Records.................................:" @ 85 "&AT_providernpi_NOHIT." ;
  put  @ 5  "04. Attending of Referring Provider Matched Records........................:" @ 85 "&AT_providernpi_rate." ;
  put ;
 
  put ;
  put  @ 5  "Member [Claims.Membno = Member.Membno]" ;
  put  @ 5  "--------------------------------------" ;
  put  @ 5  "01. Total Records...........................................................:" @ 85 "&MEM_TOTAL." ;
  put  @ 5  "02. Member Matching Records.................................................:" @ 85 "&MEM_HIT." ;
  put  @ 5  "03. Member Non-Matching Records.............................................:" @ 85 "&MEM_NOHIT." ;
  put  @ 5  "04. Percentage of Member Matched Records....................................:" @ 85 "&MEM_RATE."  ;
  put ;

  put  @ 5  "Process Start Time..........................................................:" @ 85  "&st_time."  ;
  put  @ 5  "Process End Time............................................................:" @ 85    ed_time ;
  put ;
  put ;
  put ;
  run;
%end;

%if &QIfiflag=N %then %do;

data _null_;
  length end_dttime $ 30. ;
  end_date = put(today(),date7.);
  /* SAS2AWS2: ReplacedFunctionCompress */
  end_time = kcompress(put(time(),time10.),':');
  /* SAS2AWS2: ReplacedFunctionCatx */
  end_dttime = catx('_' ,end_date,end_time);
  /* SAS2AWS2: ReplacedFunctionCompress */
  call symput('end_dttime',kcompress(end_dttime));
  ed_time = put(datetime(),DATETIME20.);
  /* SAS2AWS2: ReplacedFunctionCompress */
  call symput('ed_time',kcompress(ed_time));


  file print;

  title ;
  put ;
  put @ 20              "APD 837 Encounter Extract Control Report";
  put @ 20              "***********************************************************";
  put ;
  
  put @ 20              "Service Line level internal Reject and output count Details";
  put @ 20              "***********************************************************";
  put ;
  
  put ;
  put  @ 5  "01. Input Service Line Records..............................................:" @ 85 "&svc_count." ;   
  put ;
  put  @ 5  "02A. BILCOD reject[Rejected]................................................:" @ 85 "&Bil_cod_Rej_count." ;
  put  @ 5  "03. Service lines Records with NO  Claims Records [Rejected]................:" @ 85 "&ClmN_SvcY_Count." ;
  put  @ 5  "04. Claims with Service lines Records after merge...........................:" @ 85 "&ClmY_SvcY_Count." ;
  put ;       
  put;
  put  @ 5  "05. Institutional Inpatient Service Line Records(I) ........................:" @ 85 "&II_Count2." ;
  put  @ 5  "06. Institutional Outpatient Service Line Records(O) .......................:" @ 85 "&IO_Count2." ;
  put  @ 5  "07. Professional Service Line Records(P) ...................................:" @ 85 "&P_Count2." ;
  put ;       
  put  @ 5  "08. Claims with Service lines Records (IO & P with HoldFlag = N)[Rejected]..:" @ 85 "&Rej_Enc_N_IO_P." ;
  put  @ 5  "09. Claims Records Passed after Hold Code Process ..........................:" @ 85 "&Hold_Code_Pass." ;
  
  put ;
  put  @ 5  "10. Claims with Svc Records passed Hold Code after provider/member merge....:" @ 85 "&rec_encY2." ;

  put ;
  put  @ 5  "10A. Reject_DG_I service line level count(Rejected]..........................:" @ 85 "&Reject_DG_I." ;

  put ;
  put  @ 5  "11. Claims with Svc Records Passed Edit Matrix..............................:" @ 85 "&edit_pass." ;
  put  @ 5  "12. Claims with Svc Records Failed Edit Matrix..............................:" @ 85 "&edit_fail." ;
  


  put ;
  put  @ 5  "13. Total Svc Records Failed [02. + 03 + 02A. + 08. + 10A. + 12]............:" @ 85 "&ClmTot_Fail." ;
  put ;
  
  
  put  @ 5  "14. Professional Service Line Records (2400 + ).............................:" @ 85 "&prof_svc_count_p." ;
  put  @ 5  "14A. Dme Service Line Records (2400 + ).....................................:" @ 85 "&Dme_svc_count_p." ;
  put ;
  put  @ 5  "15. Institutional Service Line Records (2400 + )............................:" @ 85 "&inst_svc_count_p." ;
  put ;
  put  @ 5  "16. Institutional Service Line Records Inpatient(2400 + )...................:" @ 85 "&inst_I_SVC_count_P." ;
  put  @ 5  "17. Institutional Service Line Records Outpatient(2400 + )..................:" @ 85 "&inst_O_SVC_count_P." ;
  put ;


  put @ 20          " Claims level internal Reject and output count Deatils";
  put @ 20              "********************************************************";
  put ; 

  put ;
  put  @ 5  "01. Service line  table Claims level Records.................................:" @ 85 "&QI_and_clm_count."  ;
  put ;  
  put  @ 5  "02A. Bilcode [Rejected].....................................................:" @ 85 "&Bil_cod_Rej_count1." ;
  put  @ 5  "03. Service lines Records with NO  Claims Records [Rejected]................:" @ 85 "&ClmN_SvcY_Count1." ;
  put  @ 5  "04. Claims with Service lines Records after merge...........................:" @ 85 "&ClmY_SvcY_Count1." ;
  put ;       
  put;
  put  @ 5  "05. Institutional Inpatient Service Line Records(I) ........................:" @ 85 "&II_Count2_1." ;
  put  @ 5  "06. Institutional Outpatient Service Line Records(O) .......................:" @ 85 "&IO_Count2_1." ;
  put  @ 5  "07. Professional Service Line Records(P) ...................................:" @ 85 "&P_Count2_1." ;
  put ;       
  put  @ 5  "08. Claims with Service lines Records (IO & P with HoldFlag = N)[Rejected]..:" @ 85 "&Rej_Enc_N_IO_P_1." ;
  put  @ 5  "09. Claims Records Passed after Hold Code Process ..........................:" @ 85 "&ttt." ;

  
  put ;
  put  @ 5  "10. Claims with Svc Records passed Hold Code after provider/member merge....:" @ 85 "&rec_encY2_1." ;

  put ;
  put  @ 5  "10A. Reject_DG_I Claim level count(Rejected]................................:" @ 85 "&Reject_DG_I_2." ;

  put ;
  put  @ 5  "11. Claims Failed Edit Matrix...............................................:" @ 85 "&edit_fail_1." ;
  put  @ 5  "12. Claims Passed Edit Matrix...............................................:" @ 85 "&edit_pass_1." ;
  
  
  put ;
  put  @ 5  "13. Total Claims Records Failed [02. + 02A. + 03 + 08. + 10A. + 11.].......:" @ 85 "&ClmTot_Fail_1." ;
  put ;
  
  put  @ 5  "14. Professional Claims Records.............................................:" @ 85 "&prof_clm_count_p." ;
  put  @ 5  "14A. Dme Claims Records......................................................:" @ 85 "&Dme_clm_count_p." ;
  put ;
  put  @ 5  "15. Institutional Claims Records............................................:" @ 85 "&inst_clm_count_p." ;
  put ;
  put  @ 5  "16. Institutional Claims Records Inpatient(2400 + )..........................:" @ 85 "&inst_I_clm_count_P." ;
  put  @ 5  "17. Institutional Claims Records Outpatient(2400 + ).........................:" @ 85 "&inst_O_clm_count_P." ;
  put ;

  put  @ 5  "*****************************************************************************";
  put  @ 5  "** NOTE: Provider / Member table merge is done after Hold Code Pass and    **";
  put  @ 5  "**      before Edit Matrix Process                                         **";
  put  @ 5  "*****************************************************************************";

  put ;
	put  @ 5  "Billing Provider  [Claims.blpnpi = Providernpi.NPIID]" ; ;
	put  @ 5  "-----------------------------------------------------------------" ;
	put  @ 5  "01. Total Records...........................................................:" @ 85 "&BI_providernpi_Total." ;
	put  @ 5  "02. Billing Provider Matching Records.......................................:" @ 85 "&BI_providernpi_HIT." ;
	put  @ 5  "03. Billing Provider Non-Matching Records...................................:" @ 85 "&BI_providernpi_NOHIT." ;
	put  @ 5  "04. Percentage of Billing Provider Matched Records..........................:" @ 85 "&BI_providernpi_rate." ;
	put ;


  put ;
  put  @ 5  "Operating Physician [Claims.oprnpi = Providernpi.NPIID]" ;
  put  @ 5  "------------------------------------------------------" ;
  put  @ 5  "01. Total Records...........................................................:" @ 85 "&OP_TOTAL." ;
  put  @ 5  "02. Operating Physician Matching Records....................................:" @ 85 "&OP_HIT." ;
  put  @ 5  "03. Operating Physician Non-Matching Records................................:" @ 85 "&OP_NOHIT." ;
  put  @ 5  "04. Percentage of Operating Physician Matched Records.......................:" @ 85 "&OP_RATE." ;
  put ;

  put ;
  put  @ 5  "Attending Provider [Claims.atpnpi = Providernpi.NPIID]"  ;
  put  @ 5  "-----------------------------------------------------------------" ;
  put  @ 5  "01. Total Records...........................................................:" @ 85 "&AT_providernpi_Total." ;
  put  @ 5  "02. Attending Provider Matching Records.....................................:" @ 85 "&AT_providernpi_HIT." ;
  put  @ 5  "03. Attending Provider Non-Matching Records.................................:" @ 85 "&AT_providernpi_NOHIT." ;
  put  @ 5  "04. Attending of Referring Provider Matched Records........................:" @ 85 "&AT_providernpi_rate." ;
  put ;

  put ;
  put  @ 5  "Member [Claims.Membno = Member.Membno]" ;
  put  @ 5  "--------------------------------------" ;
  put  @ 5  "01. Total Records...........................................................:" @ 85 "&MEM_TOTAL." ;
  put  @ 5  "02. Member Matching Records.................................................:" @ 85 "&MEM_HIT." ;
  put  @ 5  "03. Member Non-Matching Records.............................................:" @ 85 "&MEM_NOHIT." ;
  put  @ 5  "04. Percentage of Member Matched Records....................................:" @ 85 "&MEM_RATE."  ;
  put ;


  put  @ 5  "Process Start Time..........................................................:" @ 85 "&st_time."  ;
  put  @ 5  "Process End Time............................................................:" @ 85    ed_time ;
  put ;
  put ;
  put ;
  run;

%end;
proc sql;
update &R42173..medical_job_mstr
set 
	end_time	 =   "&ed_time." ,
	claim_count  =   "&ClmY_SvcY_Count1." ,
	%if &QIfiflag=Y %then %do;
	   svc_count =   "&QI_and_SV_count." ,                                
	%end;
	%else %do;
	   svc_count ="&svc_count." ,
	%end;

	ClmY_SvcY_Count     = "&ClmY_SvcY_Count."   ,               
	Inst_Inp_Count      ="&II_Count2." ,
	Inst_Out_Count      ="&IO_Count2.",
	Prof_Count          ="&P_Count2.",
	rec_encY2           ="&rec_encY2.",
	edit_pass           ="&edit_pass.",               
	prof_clm_count      ="&prof_clm_count_p.",                               
	prof_svc_count      ="&prof_svc_count_p.",               
	inst_clm_count      ="&inst_clm_count_p.",               
	inst_svc_count      ="&inst_svc_count_p.",
	compno              ="&Compno."      
where run_id = "&run_id."
;
quit;

%Mend;
%Control_Report();

Data Report(keep=run_date run_id sequence_number  Compno output_file_name output_dataset_name Load_flag typecode split_status LOBCOD Translated_file_name treo_flag claim_cnt line_cnt total_cnt);
		retain run_date run_id sequence_number  Compno LOBCOD typecode output_file_name output_dataset_name  Load_flag  split_status treo_flag   Translated_file_name claim_cnt line_cnt total_cnt; /*272515 added cnt variable*/
	Set &R42173..medical_job_mstr;	 
	where missing(sequence_number)=0;
Run;
Proc sort data=Report;by descending run_id ;
Run;
/* VVKR Code Start -- B-75041 */
data Report(drop=run_id_julian two_old two_old1_n run_id_n two_old1); /*B-120951*/
set Report;
attrib run_id_julian format=date9. two_old format=date9.;
/* SAS2AWS2: ReplacedFunctionStrip-ReplacedFunctionSubstr */
run_id_julian=input(kstrip(ksubstr(run_id,1,5)),julian6.);
two_old=intnx('year',today(),-2,'same');

/*B-120951 Start Here*/
two_old1=put(two_old,julian6.);   
/* SAS2AWS2: ReplacedFunctionSubstr */
run_id_n = input(ksubstr(run_id,1,5), 8.);
two_old1_n = input(two_old1, 8.);
if run_id_n gt two_old1_n then output;
/*if run_id_julian gt two_old then output;*/ /*B-120951 End Here*/
run;
/* VVKR Code End -- B-75041 */
/* Proc download data=Report; */
/* run; */
/* Endrsubmit; */

%if &extn_type.=xls %then %do; /*B-244088 extension change*/
	PROC EXPORT DATA= Report	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type."
		           DBMS= &extn_type. REPLACE;sheet='REPORT';
RUN;
%end;

PROC EXPORT DATA= Report	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report"
		           DBMS=xlsx REPLACE;sheet='REPORT';
RUN;
/********************************************************************************************/
/****     Archiving the Current run data - Start                                         ****/ 
/********************************************************************************************/

%macro handle_error;
/* data logfile; */
/*   length Message $ 250; */
/*   infile "&log_file." truncover; */
/*   input Message $ 1-250; */

data logfile;
  length Message $ 250;
  infile "&drv2./Encounters_Reporting/APD/Current/&lib._837_Log_File_&Compno._&fi_ds_dt._&run_id..log" truncover;
  input Message $ 1-250;
LineNum = _n_;
  if ((Message =: "WARNING")								/* Checks for warnings               */
      /* SAS2AWS2: ReplacedFunctionIndex */
      and (kindex(Message,"Your system is scheduled to expire") = 0)
      /* SAS2AWS2: ReplacedFunctionIndex */
      and (kindex(Message,"The Base Product product with which") = 0)
      /* SAS2AWS2: ReplacedFunctionIndex */
      and (kindex(Message,"expire within ") = 0)
      /* SAS2AWS2: ReplacedFunctionIndex */
      and (kindex(Message,"will be expiring soon") = 0)
	  /* SAS2AWS2: ReplacedFunctionIndex */
	  and (kindex(Message,"The SAS/CONNECT product with which")=0))

    /* SAS2AWS2: ReplacedFunctionIndex */
    or ((Message =: "ERROR") and (kindex(message,"The setting of the SYSPRINT ")=0))									/* Checks for errors                 */
    /* SAS2AWS2: ReplacedFunctionIndex */
    or (kindex(Message,"unin") ~= 0) ;
	/* Checks for unintialized variables */

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

run;

data logfile;
set logfile;
/* SAS2AWS2: ReplacedFunctionIndex */
if (kindex(message,"Variable mcor2")~=0) or (kindex(message,"Variable col2")~=0) then delete;
  
run;
%mend handle_error;


%macro data_archive;

/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Edit_Matrix_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Control_Table_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/Prof_layout_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/INST_layout_script_&compno..sas";
/* SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
x "rm -f &drv2./Encounters_Reporting/APD/Temp/varlist_script_&compno..sas";


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
%let path1 = &drv72./Encounters_Reporting/APD/Archive ;
%let dirid2=%sysfunc(dopen(&path1));


                data _null_;
                length ddir $10.;
                    /* SAS2AWS2: ReplacedFunctionTranslate */
                    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
                               call symput('ddir',ddir);
                run;

                /* SAS2AWS2: ReplacedSlash */
                %let destpath = &path1./&ddir;


/*                 %let dirid2=%sysfunc(dopen(&destpath)); */
/*                 data _null_ ; */
/*                                SAS2AWS2: ReplacedSlash-UpdatedOSCommands */
/*                                commd="mkdir &path1./&ddir"; */
/*                                call system(commd); */
/*                 run; */
/*                 %let rc=%sysfunc(dclose(&dirid2)); */


/*                 data _null_ ; */
/*                                SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
/*                                archi1 ="cp &drv2./Encounters_Reporting/APD/Current/*.* &destpath./"; */
/*                                call system(archi1); */
/*                 run; */

   				
data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &destpath. --recursive --exclude '*' --include '*.*'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

/* 				  data _null_ ; */
/*                                SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
/*                                archi1 ="cp &drv2./Encounters_Reporting/APD/Current/Prof_837*.*txt &drv2./Encounters_Reporting/APD/EDI/837p/In/*.*"; /* 477378 */
/*                                call system(archi1);  */
/*    				run; */

/*Please check*/
data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837p/In/To_Onprem --recursive --exclude '*' --include 'Prof_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837p/In/&ddir. --recursive --exclude '*' --include 'Prof_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

/* 			    data _null_ ; */
/*                                SAS2AWS2: ReplacedSlash-ChangedFolderName-UpdatedOSCommands */
/*                                archi1 ="cp &drv2./Encounters_Reporting/APD/Current/Inst_837*.*txt &drv2./Encounters_Reporting/APD/EDI/837i/In/*.*"; /* 477378 */
/*                                call system(archi1); */
/*    				run; */

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837i/In/To_Onprem --recursive --exclude '*' --include 'Inst_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;   		

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/  &drv72./Encounters_Reporting/APD/EDI/837i/In/&ddir. --recursive --exclude '*' --include 'Inst_837*.*txt'  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;		
			/*	PROC EXPORT DATA= Report	 	
				 OUTFILE= "&destpath./Medical_Job_Mstr_Report.xls"
				 DBMS=EXCEL REPLACE;
				RUN;*/

				%if &extn_type.=xls %then %do;/*B-244088 extension change*/
				/* B-92443 added run_id */
				PROC EXPORT DATA= Report	 
		           /* SAS2AWS2: ReplacedSlash */
		           OUTFILE= "&drv2./Encounters_Reporting/APD/Current/Medical_Job_Mstr_Report_&run_id..&extn_type."
		           DBMS=&extn_type. REPLACE;sheet='REPORT';
				RUN;
			%end;
				
				PROC EXPORT DATA= Report	 
		           /* SAS2AWS2: ReplacedSlash */
		           OUTFILE= "&drv2./Encounters_Reporting/APD/Current/Medical_Job_Mstr_Report_&run_id."
		           DBMS=xlsx REPLACE;sheet='REPORT';
				RUN;
				
				/*Please Check */			
data _null_;/*B-244088 extension change*/
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/Medical_Job_Mstr_Report_&run_id..&extn_type. &drv72./Encounters_Reporting/APD/Archive/&ddir./ --sse --acl bucket-owner-full-control";
call system(cmd);
run;
   
/*	%if &QIfiflag=Y %then %do;*/
		Data temp;		
			length File_name1 $270.;
			/* SAS2AWS2: ReplacedFunctionScan */
			file_name=kscan("&fname.",1,'.');
			/* SAS2AWS2: ReplacedFunctionTrim */
			file_name1=ktrim(file_name)||kcompress("_")||kcompress("&run_id.");
			call symput("fname",File_name);	
			call symput("fname1",File_name1);
		Run;	
		%LET fname=&fname.;
		%LET fname1=&fname1.;
/* proc printto ; */
/* run; */
%handle_error;	
%nobs(LOGFILE); 
%let nobs1 =&nobs.;
%if &nobs1. = 0 %then %do;
	data mail_file;			
			status = 'PROCESSED';		
		RUN;

				/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
				%let path1 = &drv72./Encounters_Reporting/APD/Import/Archive;
				%let dirid2=%sysfunc(dopen(&path1));


                data _null_;
                length ddir $10.;
                    /* SAS2AWS2: ReplacedFunctionTranslate */
                    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
                               call symput('ddir',ddir);
                run;

                /* SAS2AWS2: ReplacedSlash */
                %let destpath = &path1./&ddir;


/*                 %let dirid2=%sysfunc(dopen(&destpath)); */
/*                 data _null_ ; */
/*                                SAS2AWS2: ReplacedSlash-UpdatedOSCommands */
/*                                commd="mkdir &path1./&ddir"; */
/*                                call system(commd); */
/*                 run; */
/*                 %let rc=%sysfunc(dclose(&dirid2)); */

		/* CHG0038264 - create summary file with process time ----> start */
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	%if %sysfunc(fileexist("&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv")) =0 %then %do;
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
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
			/* SAS2AWS2: ReplacedFunctionCompress */
			filesize=kcompress(round((finfo(fid,'File Size (bytes)'))/1024)||' KB');
			Recordcount="&recordcount.";                                                                                                    
			drop fid;
		run;
		PROC EXPORT DATA= filename_count	 
				            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
				            OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv"
				           dbms=csv replace;
		RUN;
/* 		PROC EXPORT DATA= filename_count	  */
/* 				            SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 				            OUTFILE= "&drv2./Encounters_Reporting/APD/Archive/&ddir./QIfile_process_Report.csv" */
/* 				           replace; */
/* 		RUN; */

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

		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv"
		out=filename_count dbms=csv replace;
		run;
		data filename_count;
				retain filename start_time end_time diff_minutes filesize Recordcount;
				format start_time end_time DATETIME20. filesize $20.;
				length filename $100. filesize $20. Recordcount 8. ;
				set filename_count;
		run;
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
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
			/* SAS2AWS2: ReplacedFunctionCompress */
			filesize=kcompress(round((finfo(fid,'File Size (bytes)'))/1024)||' KB');
			Recordcount="&recordcount.";                                                                                                    
			drop fid;
		run;
		proc append base=filename_count data=filename_count1 force;
		run;

		PROC EXPORT DATA= filename_count	 
				            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
				            OUTFILE= "&drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv"
				           dbms = csv replace;
		RUN;
/* 		PROC EXPORT DATA= filename_count	  */
/* 					SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 					OUTFILE= "&drv2./Encounters_Reporting/APD/Archive/&ddir./QIfile_process_Report.csv" */
/* 					replace; */
/* 		RUN;	 */
		
data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv72./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Control_Table/QIfile_process_Report.csv &drv72./Encounters_Reporting/APD/Archive/&ddir./  --sse --acl bucket-owner-full-control";
call system(cmd);
run;
		
	%end;
	/* CHG0038264 - create summary file with process time ----> end */
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 		%if (%sysfunc(system(move "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/&fname..TXT"  "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/archive/&ddir/&fname1..TXT")) gt 0 ) */
/* 				%then %do; */
/* 				%put &fname..TXT was not moved to Archive folder ; */
/* 			%end;  */
/* 				%else %do; */
/* 				%put &fname..TXT was  moved to Archive folder ; */
/* 		%end; */

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

		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		%let path1 = &drv72./Encounters_Reporting/APD/Import/Error ;
				%let dirid2=%sysfunc(dopen(&path1));


                data _null_;
                length ddir $10.;
                    /* SAS2AWS2: ReplacedFunctionTranslate */
                    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
                               call symput('ddir',ddir);
                run;

                /* SAS2AWS2: ReplacedSlash */
                %let destpath = &path1./&ddir;


/*                 %let dirid2=%sysfunc(dopen(&destpath)); */
/*                 data _null_ ; */
/*                                SAS2AWS2: ReplacedSlash-UpdatedOSCommands */
/*                                commd="mkdir &path1./&ddir"; */
/*                                call system(commd); */
/*                 run; */
/*                 %let rc=%sysfunc(dclose(&dirid2)); */

		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 		%if (%sysfunc(system(move "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/&fname..TXT"  "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/error/&ddir/&fname1..TXT")) gt 0 ) */
/* 				%then %do; */
/* 				%put &fname..TXT was not moved to error folder ; */
/* 			%end;  */
/* 				%else %do; */
/* 				%put &fname..TXT was  moved to error folder ; */
/* 				%put "Proces was aborted due to errors for &fname..TXT"; */
/* 				%let nobs1 = 0; */
/* 				SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 				%sysExec move "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/*.TXT"  "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/error/&ddir/";/* HS - 569650 */
/*  */
/* 		%end; */


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

/*B-244088 extension change*/
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

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19101 Program End execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

/********************************************************************************************/
/****Archiving the Current run data - End                                                      ****/ 
/********************************************************************************************/

/********************************************************************************************/
/****                                                                                        ****/ 
/****  End of Program                                                                  ****/ 
/****                                                                                 ****/ 
/********************************************************************************************/ 


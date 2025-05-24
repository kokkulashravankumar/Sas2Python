
/***       
$Header: $ 
***/ 

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19102 Program Start execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */
%global Typecode_Inst;
*Rsubmit;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/record_count.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/ICD10_Beg_Date.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Inovalon_delete_Control_File.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/APD_Reject.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 


data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/APD_Reject_Claim.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/CAS_Segment_Negative_Chk.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Inpatient_Proc_Code.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/ndcsvc_analysis.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;


data _null_;	/*[B-315447] - DS - Updates*/ 
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Missing_pidate.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Claims_Bilcod.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/modifier_blankout.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NDCSVC_Blankout.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Modifier_control.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Inovalon_delete_Control_File.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Member_Xwalk.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/ADMTDIAG_Blankout.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/NAMI_Control_Table.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Holdcode_Rejectlogic_Bypass.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/SSN_Control_Table.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/SHP_Diag_Rejected.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

/*B-244088 commented*/
/*data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/COS_PRVSPEC.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; */


/*B-244088 Added*/
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/COS_Control_Table.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
cmd1=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/PRVSPCD_Control_Table.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
call system(cmd1);
run; 



/*data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Exchgmembno_Flag.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;*//* B-274606 */

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Typecode.xls &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;


data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Provspec_Xwalk.xlsx &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

data _null_; /*B-278974 Added*/
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/amount_xwalk.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

/* B-278970 - DS - Added */

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Internal_Reject_Ctrl.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_; /*B-289556 Added*/
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Provspec_Logic.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_; /*B-289556 Added*/ 
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Taxonomy_Logic.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_; /*B-287495 Added*/ 
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Cos_Logic.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;


Data _null_;
	 a=sleep(60,1);
run;

/*B-278974 Select Amount_Xwalk sheet from the control file - Start's Here*/

Data  Amount_Xwalk;
    infile "&drv2./Encounters_Reporting/APD/Control_Table/amount_xwalk.csv" truncover delimiter=',' DSD lrecl=32767 firstobs = 2 ;  
    format clamno 	$20.  lineno 	$05.  claamt netalwamt mcrnetalwamt mcdnetalwamt topamt mcrtopamt mcdtopamt cstshrnetalcoiamt 
	cstshrnetalcopamt cstshrnetaldedamt  cstshrtopaycoiamt cstshrtopaycopamt cstshrtopaydedamt 20.2
    ;
    input    clamno 	$  lineno $  claamt $ netalwamt $ mcrnetalwamt $ mcdnetalwamt $  topamt $ mcrtopamt $ mcdtopamt $ cstshrnetalcoiamt $ 
	cstshrnetalcopamt $   cstshrnetaldedamt $ cstshrtopaycoiamt $ cstshrtopaycopamt $ cstshrtopaydedamt $
	;
run;

proc sort data=Amount_Xwalk;by clamno lineno;run;
/*B-278974 End's Here*/

/* [B-222732] - [SK] - Typecode.xls added here */

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Typecode.xls"
		out=Typecode_I dbms=xls replace;
run;
		
data Typecode_2;
	set Typecode_I;
	Bilcod1=put(Bilcod, best.);
	run;
			
proc sql ;
select "'"||kcompress(Bilcod1)||"'" into :Typecode_Inst separated by ','  from Typecode_2;
quit;

%put &=Typecode_Inst.;
/* [B-222732] - [SK] - Typecode.xls updates ends here */

%let REJECT_DG_I_1=0;
%include InpPrg(RGA19501.sas) ;

data _null_;
                length ddir $10.;
                    ddir=ktranslate(put(&today_dt.,mmddyy10.),'-','/');
                    call symput('ddir',ddir);					
                run;
/**********************************************************************************************/
/*                                                                                            */
/*   Parameters                                                                               */
/*                                                                                            */
/**********************************************************************************************/
Data _null_ ; 
     put "QIfiflag =  &QIfiflag" ;   
     put "run_id   =  &run_id     " ;
	 put "svcdat   =  &svcdat     " ; 
	 put "Compno   =  &Compno     " ;
 Run ;    
 %macro NotQIfile;
%if &QIfiflag = N %then %do;

/**********************************************************************************************/
/*                                                                                            */
/*     Selection of claims to process                                                         */
/*                                                                                            */
/*     1. No manual input file : Select the records based on dates                            */
/*                                                                                            */
/**********************************************************************************************/
/* [B-222732] -[SD]- if block added to read history tables (svclines_hist, cliams_hist, svcholds_hist) */
%Macro dataset;
	Data temp(keep=clamno);
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;      /*[B-222732]*/
		set &libn..svclines_hist(keep=clamno membno aptrndat); /*[B-222732]*/
	%end;
	%else %do;
	    set &libn..svclines(keep=clamno membno aptrndat);
	%end;
    	where (&begfirstd <= aptrndat <= &endfirstd);  
	Run;     
	Proc sort data=temp  out=tt1 nodupkey;by clamno;run;
	%nobs(tt1);
	Data test;
		a = ceil(&nobs/2000);
		do i = 1 to a;
			stvl = i*2000-1999;
			endvl = i*2000;
			output;
		end;
	Run;
	data test2;
		set test;
		length b $100;
		if _n_ = 1 then do;
			/* SAS2AWS2: ReplacedFunctionStrip */
			b = "Where clamno in (%str(&)clamno_"||kstrip(put(i,8.))||".)";
		end;
		else do;
			/* SAS2AWS2: ReplacedFunctionStrip */
			b = " or clamno in (%str(&)clamno_"||kstrip(put(i,8.))||".)";
		end;
		temp=1;
	Run;
	Proc sort data=test2;by temp;run;
	Data test3;
		set test2;
		by temp;
		Length value $10000.;
		retain value;
		if first.temp then value=Trim(b);
		/* SAS2AWS2: ReplacedFunctionTrim */
		else value=ktrim(value)||Trim(b);
		call symput ('cond',value);
	Run;
%nobs(test3);
%if &nobs = 0 %then %do;
	Data svclines &RGA191..cas_svclines(keep=clamno lineno apsvcstat revind); /* VVKR Created cas_svclines -- B-75041 */
	 %if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	  set &libn..svclines_hist(Keep=&Keepv_svc. obs=0) ;  /*[B-222732]*/	
     %end;
	%else %do; 
      set &libn..svclines(Keep=&Keepv_svc. obs=0) ;
    %end;	  
	run;
	Data svcholds;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do; /*[B-222732]*/
	 set &libn..svcholds_hist(keep=clamno lineno holdcd typcod obs=0);	
	%end;
	%else %do;
     set &libn..svcholds(keep=clamno lineno holdcd typcod obs=0);	/* VVKR added typcod -- B-75041 */
    %end;	 
	run;
	Data claims;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..claims_hist(Keep=&Keepv_Claim. obs=0); /*[B-222732]*/
    %end;
	%else %do;
     set &libn..claims(Keep=&Keepv_Claim. obs=0);
    %end;	 
	run;
%end;
%else %do;
	Proc sql ;
			  /* SAS2AWS2: ReplacedFunctionStrip */
 			 select  kstrip(put(count(stvl),8.)) into :cnt from test;
		       /* SAS2AWS2: ReplacedFunctionStrip */
  		     select kstrip(put(stvl,8.)) into :stvl_1 -:stvl_&cnt from test;
			   /* SAS2AWS2: ReplacedFunctionStrip */
  			 select kstrip(put(endvl,8.)) into :endvl_1 -:endvl_&cnt from test;
		quit;
%do i = 1 %to &cnt;
Proc sql noprint;
 select  quote(clamno) into :clamno_&i SEPARATED  by "," from tt1 where monotonic() between &&stvl_&i. and &&endvl_&i.;
quit;
%end;
	Data svclines &RGA191..cas_svclines(keep=clamno lineno apsvcstat revind); /* VVKR Created cas_svclines -- B-75041 */
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..svclines_hist(Keep=&Keepv_svc.) ; /*[B-222732]*/
    %end;
	%else %do;
     set &libn..svclines(Keep=&Keepv_svc.) ; 
    %end;	 
	&cond.;
	run;
	Data svcholds;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do; /*[B-222732]*/
	 set &libn..svcholds_hist(keep=clamno lineno holdcd typcod);	
	%end;
	%else %do; 
	  set &libn..svcholds(keep=clamno lineno holdcd typcod);	/* VVKR added typcod -- B-75041 */
	%end;
	&cond.;
	run;
	Data claims;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..claims_hist(Keep=&Keepv_Claim.); /*[B-222732]*/
	%end;
	%else %do;
	 set &libn..claims(Keep=&Keepv_Claim.);
	%end;
	&cond.;
	run;
%end;

%mend;
%dataset; 
 data &RGA191..svclines_&compno.     ;
	set svclines(Keep=&Keepv_svc. where = 
	    ( (
			(&begfirstd <= aptrndat <= &endfirstd ) and (svcstat = 'PA' or svcstat = 'PD')and revind ^= 'Y' )
		  )	
			%if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do;/*ALM#7174 - added lob, B-96285 added QHP */ 
				and lobcod in (&lobcode.)
			%end; 
        )
		end = eof; 
		retain 	svc_count 	0		;
		svc_count + 1 ;	
		claimfrq 	= '1' ;            
		icn		= '                '; 			
	     /* SAS2AWS2: ReplacedFunctionCompress */
 	    call symput('svc_count',kcompress(svc_count));
		CLAAMT=round(CLAAMT,.01);
        NETALWAMT = round(NETALWAMT,.01);
		dedamt = round(dedamt,.01);
		coiamt = round(coiamt,.01);
		COPAMT = round(COPAMT,.01);
		ALWAMT = round(ALWAMT,.01);
		nonamt = round(nonamt,.01);
		rcvamt = round(rcvamt,.01);
		dscamt = round(dscamt,.01);						
		topamt = round(topamt,.01);
		mcrnetalwamt=round(mcrnetalwamt,.01);
		mcdnetalwamt=round(mcdnetalwamt,.01);
		mcdtopamt=Round(mcdtopamt,.01);
		mcrtopamt=Round(mcrtopamt,.01);
		covunt=Round(covunt,.01);	/* HS 569650 - kv added */
	
		if (svccod >= "00100" and svccod <= "01999") or  (svccod >= "99100" and svccod <= "99140") then do;
	
				if modcod in ('AA','AD','QK','QS','QX','QY','QZ') then 
					 do;	
					 	modcod=modcod;
					 end;
				else if modcod not in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;
					if modcd2  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					  do;
							temp=modcod;
							modcod=modcd2;
							modcd2=temp;
					  end;
					else if modcd3  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
						do;	
							temp=modcod;
							modcod=modcd3;
							modcd3=temp;
						end;
					else if modcd4  in  ('AA','AD','QK','QS','QX','QY','QZ') then
						do;
							temp=modcod;
							modcod=modcd4;
							modcd4=temp;
						end;
					end;
			if modcd2 in ('AA','AD','QK','QS','QX','QY','QZ') then 
				   do;	
				   		modcd2=modcd2;
				   end;
			else if modcd2 not in  ('AA','AD','QK','QS','QX','QY','QZ') then
				do;
				if	modcd3  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;
						temp=modcd2;
						modcd2=modcd3;
						modcd3=temp;
					end;
				else if modcd4  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;	
					    temp=modcd2;
						modcd2=modcd4;
						modcd4=temp;
					end;
				end;
			if modcd3 in ('AA','AD','QK','QS','QX','QY','QZ') then 
				   do;	
				   		modcd3=modcd3;
				   end;
			else if modcd3 not in  ('AA','AD','QK','QS','QX','QY','QZ') then 
				do;
				if modcd4  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;
						temp=modcd3;
						modcd3=modcd4;
						modcd4=temp;
					end;
				end;

		if modcod not in ('AA','AD','QK','QS','QX','QY','QZ') then modcod='AA';
	 	end;
    run;
	Proc sort data=&RGA191..svclines_&compno. out=&RGA191..source;by clamno lineno;run;

	
	%if  &compno.=34   %then %do;
		Data &RGA191..svclines_&compno.;
		set &RGA191..svclines_&compno.;
        	where LOBCOD in ("&LOB.");  
		Run; 
    %end;
	%else %if  &compno.=60 %then %do;
		Data &RGA191..svclines_&compno.;
		set &RGA191..svclines_&compno.;
        	where LOBCOD in ("&LOB.");  
		Run;
	%end;
	%else %if  &compno.=45 and "&LOB." eq "MPPO" %then %do;/*B-248997*/
		Data &RGA191..svclines_&compno.;
		set &RGA191..svclines_&compno.;
        	where LOBCOD in ("&LOB.");  
		Run;
	%end;
	%else %if &compno.=30 %then %do;
		Data &RGA191..svclines_&compno.;
			set &RGA191..svclines_&compno.;
        	where grpnum='MAXMUM' ;
		Run;			
	%end ;	
	proc sql;
		select  count(*) into :total_obs  from &RGA191..svclines_&compno.; 
    Quit;

%end; 
%mend;
%NotQIfile; 
/**********************************************************************************************/
/*                                                                                            */
/*     Selection of claims to process                                                         */
/*                                                                                            */
/*     2. Manual input file ( Either New submission file or Adjustment file)                  */
/*                                                                                            */
/*        a) With Claim frequency 1(new submissions) : Select all the claim numbers           */
/*            with matching claim numbers and assosiated lines. Check if inputs               */
/*             are with claim frequency 1                                                     */
/*                                                                                            */
/*        b) With Claim frequency 7(Adjustments    ) : Select all the claim numbers           */
/*            with matching claim numbers and assosiated lines. Check if the claim frequency  */
/*            for inputs is 7. Get the claim lines and see if the claim frequency is 1. If yes*/
/*            then process else drop records.                                                 */
/*                                                                                            */
/**********************************************************************************************/
%macro QIfile;
%if &QIfiflag=Y %then %do;
  %if &compno.=01 and &LOB. = DSNP %then %do;
	%qidsnp;
 %end;
 %else %do;
  Data &RGA191..finclaims_&compno.  ;
    /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
    infile "&drv2./Encounters_Reporting/APD/Import/&fname." truncover delimiter='09'x DSD lrecl=32767
                                                 firstobs = 2 ;  
    informat clamno 	$20.  /* Updated length 13 to 20 */
			 lineno 	$05.  /* Updated length 03 to 05 */
			 clamno_mcd 	$20. 
			 lineno_mcd 	$05.
			 clamno_mcr 	$20. 
			 lineno_mcr 	$05.			 
             claimfrq 	$01.
			 icn 		$22. /* 541126 - ICN length increased from 15 to 20, B-136184 from 20 to 22 */
		;
    input    clamno 	$ 
			 lineno		$
			 claimfrq 	$ 
			 icn		$
		;

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;
		
			clamno_mcd=''; clamno_mcr=''; lineno_mcd=''; lineno_mcr='';
			mcd_lin_missfl=0; mcr_lin_missfl=0;
	run;	
 %end;
data _null_;
set &RGA191..finclaims_&compno.  ; 
 if find(kupcase("&fname."),"N4TON5_ADD") gt 0  then call symputx('N4TON5_icn_add','Y','G');  /*B-249002 Added */
 else call symputx('N4TON5_icn_add','N','G'); 
run;

	/* B-166920 */
proc sql noprint;
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_Inovalon_delete_rej, :enctyp_code_Inovalon_delete_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='Inovalon_delete_reject' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_eligibility, :enctyp_code_eligibility  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='eligibility' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_DG_SVCCOD, :enctyp_code_DG_SVCCOD  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='DG_SVCCOD' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_Rollup_Rej_svctyp_HG, :enctyp_code_Rollup_Rej_svctyp_HG  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='Rollup_Reject for service type HG claims' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_SVCCOD_Reject_HG, :enctyp_code_SVCCOD_Reject_HG  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='SVCCOD_Reject_for_HG' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_Inpatient_Paid_Rej, :enctyp_code_Inpatient_Paid_Rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='Inpatient_Paid_Rejection' and edittyp='H';

	/* B-168796 */
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_bilcod_rej, :enctyp_code_bilcod_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='BILCOD' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_holdcode_rej, :enctyp_code_holdcode_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='Holdcode_Reject' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_proc_code_date_rej, :enctyp_code_proc_code_date_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='procedure_code_date_reject' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_EIPDG_rej, :enctyp_code_EIPDG_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='Inpatient_DG_Line_Reject' and edittyp='H';
	/* SAS2AWS2: ReplacedFunctionCompress */
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_Rollup_Reject, :enctyp_code_Rollup_Reject  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='Rollup_Reject' and edittyp='H';
	
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_MEM_socsec_rej, :enctyp_code_MEM_socsec_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='MEM_socsec' and edittyp='H';	
	
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_NDCSVC_Procbill_rej, :enctyp_code_NDCSVC_Procbill_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='IN_2400_SV202_2_SL_HCPCS' and edittyp='H';	 /* B-261072 */
	
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_NDCSVC_Proc_rej, :enctyp_code_NDCSVC_Proc_rej  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='SVCCOD' and edittyp='H';	 /* B-261072 */
	
	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_hiosid_reject, :enctyp_code_hiosid_reject  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='Hiosid_Reject' and edittyp='H';	 /* B-274606 */	
	
	/* [B-335998] - DS added */	

	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_RFPRVNPI, :enctyp_code_RFPRVNPI  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='RF_PROVNUMDT' and edittyp='H';

	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_RNPRVNPI, :enctyp_code_RNPRVNPI  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='RN_PROVNUMDT' and edittyp='H';

	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_ATPRVNPI, :enctyp_code_ATPRVNPI  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='AT_PROVNUMDT' and edittyp='H';

	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_OPPRVNPI, :enctyp_code_OPPRVNPI  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='OP_PROVNUMDT' and edittyp='H';

	Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") into :edit_type_BIPRVNPI, :enctyp_code_BIPRVNPI  SEPARATED by ','
	from APD_enceditmatrix where encapdfld ='BI_PROVNUMDT' and edittyp='H';	

	
quit;



	/* CR# CHG0040549 - commented /* CR# CHG0040374 - added Inovalon delete reject logic     ----> start  	
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Inovalon_delete_Control_File.xlsx"
			out=Inovalon_delete_Control_File dbms=xlsx replace;
	Run;
	
	data Inovalon_delete_Control_File;
	length clamno $20.;
	set Inovalon_delete_Control_File;
	run;
	
	Proc sort data=	Inovalon_delete_Control_File nodupkey;by clamno ;run;
	Proc sort data=	&RGA191..finclaims_&compno. ;by clamno ;run;
	Data &RGA191..finclaims_&compno. (drop=edit_code encapdfld edit_chk_type Recsubind typecode)  
		 &RGA191..Inovalon_delete_reject(drop= claimfrq icn);
		length edit_chk_type $100. edit_code $5. encapdfld $50.;
		Merge &RGA191..finclaims_&compno.(in=_a) Inovalon_delete_Control_File(in=_b);
		by clamno ;
		if _a and _b  and missing(icn) eq 1 then do; 
			edit_code = '99948' ;
			encapdfld =  'Inovalon_delete_reject' ;
			edit_chk_type = 'Inovalon_delete_reject';
			Recsubind = 'N' ;
			typecode = ' '; 
			output &RGA191..Inovalon_delete_reject;
		end;
		else if _a and _b  and missing(icn) ne 1 and claimfrq eq '7' then do; 
			edit_code = '99948' ;
			encapdfld =  'Inovalon_delete_reject' ;
			edit_chk_type = 'Inovalon_delete_reject';
			Recsubind = 'N' ; 
			typecode = ' '; 
			output &RGA191..Inovalon_delete_reject;
		end;
		else if _a then output &RGA191..finclaims_&compno.;
	run;	

%macro vars(dsn,chr,out);                                                                                                               
	%let dsid=%sysfunc(open(&dsn));                                                                                                        
	%let n=%sysfunc(attrn(&dsid,nvars));                                                                                                 
	data &out;                                                                                                                            
		set &dsn(rename=(                                                                                                                    
		%do i = 1 %to &n;                                                                                                                 
			%let var=%sysfunc(varname(&dsid,&i));                                                                                            
			&var=&var&chr                                                                                                              
		%end;));                                                                                                                            
		%let rc=%sysfunc(close(&dsid));                                                                                                        
run;    
%mend vars; 

		/* CR# CHG0040374 - added Inovalon delete reject logic     ----> end  	*/
%Macro dataset;
	Proc sort data=&RGA191..finclaims_&compno.   out=tt1 nodupkey;by clamno;run;
	%nobs(tt1);
	Data test;
		a = ceil(&nobs/2000);
		do i = 1 to a;
			stvl = i*2000-1999;
			endvl = i*2000;
			output;
		end;
	Run;
	data test2;
		set test;
		length b $100;
		if _n_ = 1 then do;
			/* SAS2AWS2: ReplacedFunctionStrip */
			b = "Where clamno in (%str(&)clamno_"||kstrip(put(i,8.))||".)";
		end;
		else do;
			/* SAS2AWS2: ReplacedFunctionStrip */
			b = " or clamno in (%str(&)clamno_"||kstrip(put(i,8.))||".)";
		end;
		temp=1;
	Run;
	Proc sort data=test2;by temp;run;
	Data test3;
		set test2;
		by temp;
		Length value $10000.;
		retain value;
		if first.temp then value=Trim(b);
		/* SAS2AWS2: ReplacedFunctionTrim */
		else value=ktrim(value)||Trim(b);
		call symput ('cond',value);
	Run;     
	%nobs(test3);
%if &nobs = 0 %then %do;

	Data svclines9 &RGA191..cas_svclines9(keep=clamno lineno apsvcstat revind); /* VVKR Created cas_svclines -- B-75041 */
    %if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..svclines_hist(Keep=&Keepv_svc. obs=0) ; /*[B-222732]*/
    %end;
	%else %do;
     set &libn..svclines(Keep=&Keepv_svc. obs=0) ;
    %end;	 
	run;
	
	Data svcholds9;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
 	 set &libn..svcholds_hist(keep=clamno lineno holdcd typcod obs=0);	/*[B-222732]*/
	%end;
	%else %do;
	 set &libn..svcholds(keep=clamno lineno holdcd typcod obs=0);	/* VVKR added typcod -- B-75041 */
	%end;
	run;
	Data claims9;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..claims_hist(Keep=&Keepv_Claim. obs=0); /*[B-222732]*/
    %end;
	%else %do;
     set &libn..claims(Keep=&Keepv_Claim. obs=0);
    %end;	 
	run;
		%vars(svclines9,_mcr,svclines_mcr)   ; 
		%vars(svcholds9,_mcr,svcholds_mcr)   ; 
		%vars(claims9,_mcr,claims_mcr)   ; 
	
		proc sql;
			create table svclines as select a.* , b.* from svclines9 a left join svclines_mcr b on a.clamno=b.clamno_mcr order by clamno, lineno;
			create table svcholds as select a.* , b.* from svcholds9 a left join svcholds_mcr b on a.clamno=b.clamno_mcr order by clamno, lineno;
			create table claims as select a.* , b.* from claims9 a left join claims_mcr b on a.clamno=b.clamno_mcr order by clamno;
			create table &RGA191..cas_svclines as select a.* , '' as apsvcstat_mcr, '' as revind_mcr from &RGA191..cas_svclines9 a order by clamno, lineno;
		quit;
%end;
%else %do;
	Proc sql ;
			  /* SAS2AWS2: ReplacedFunctionStrip */
 			 select  kstrip(put(count(stvl),8.)) into :cnt from test;
		       /* SAS2AWS2: ReplacedFunctionStrip */
  		     select kstrip(put(stvl,8.)) into :stvl_1 -:stvl_&cnt from test;
			   /* SAS2AWS2: ReplacedFunctionStrip */
  			 select kstrip(put(endvl,8.)) into :endvl_1 -:endvl_&cnt from test;
		quit;

%do i = 1 %to &cnt;

  Proc sql noprint;
 select  quote(clamno) into :clamno_&i SEPARATED  by "," from tt1 where monotonic() between &&stvl_&i. and &&endvl_&i.;
  quit;

%end;
	Data svclines9 &RGA191..cas_svclines9(keep=clamno lineno apsvcstat revind); /* VVKR Created cas_svclines -- B-75041 */
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..svclines_hist(Keep=&Keepv_svc.) ; /*[B-222732]*/
	%end;
	%else %do;	
	 set &libn..svclines(Keep=&Keepv_svc.) ;	 
	%end;
     &cond.;	
	run;
	

	Data svcholds9;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..svcholds_hist(keep=clamno lineno holdcd typcod); /*[B-222732]*/
	%end;
	%else %do;
	 set &libn..svcholds(keep=clamno lineno holdcd typcod);/* VVKR added typcod -- B-75041 */
	%end;
	&cond.;
	run;
	Data claims9;
	%if &compno. =01 and &hist_ctrl_flag. =Y %then %do;
	 set &libn..claims_hist(Keep=&Keepv_Claim.);  /*[B-222732]*/
	%end;
	%else %do;
	 set &libn..claims(Keep=&Keepv_Claim.);
	%end;
	&cond.;
		/* CR# CHG0033169 - added logic to map rate code from control file */ 
	 %let i = 1 ;
	 %do %while(&i. <= &Rate_code_total.); 
         if valA01=&&Rate_Code&i. then valA01=&&New_Rate_Code&i.;
		 if valA02=&&Rate_Code&i.  then valA02=&&New_Rate_Code&i.;
		 if valA03=&&Rate_Code&i.  then valA03=&&New_Rate_Code&i.;
		 if valA04=&&Rate_Code&i.  then valA04=&&New_Rate_Code&i.;
		 if valA05=&&Rate_Code&i.  then valA05=&&New_Rate_Code&i.;
		 if valA06=&&Rate_Code&i.  then valA06=&&New_Rate_Code&i.;
		 if valA07=&&Rate_Code&i.  then valA07=&&New_Rate_Code&i.;
		 if valA08=&&Rate_Code&i.  then valA08=&&New_Rate_Code&i.;
		 if valA09=&&Rate_Code&i.  then valA09=&&New_Rate_Code&i.;
		 if valA10=&&Rate_Code&i.  then valA10=&&New_Rate_Code&i.;
		 if valA11=&&Rate_Code&i.  then valA11=&&New_Rate_Code&i.;
		 if valA12=&&Rate_Code&i.  then valA12=&&New_Rate_Code&i.;
		%let i = %eval(&i. + 1);
	%end;
    %let i = 1 ;
	run;
		Data svclines1; set svclines9(obs=0); Run;
		Data svcholds1; set svcholds9(obs=0); Run;
		Data claims1; set claims9(obs=0); Run;
		
		%vars(svclines1,_mcr,svclines_mcr)   ; 
		%vars(svcholds1,_mcr,svcholds_mcr)   ; 
		%vars(claims1,_mcr,claims_mcr)   ; 
	
		proc sql;
			create table svclines as select a.* , b.* from svclines9 a left join svclines_mcr b on a.clamno=b.clamno_mcr order by clamno, lineno;
			create table svcholds as select a.* , b.* from svcholds9 a left join svcholds_mcr b on a.clamno=b.clamno_mcr order by clamno, lineno;
			create table claims as select a.* , b.* from claims9 a left join claims_mcr b on a.clamno=b.clamno_mcr order by clamno;
			create table &RGA191..cas_svclines as select a.* , '' as apsvcstat_mcr, '' as revind_mcr from &RGA191..cas_svclines9 a order by clamno, lineno;
		quit;
%end;

%mend;
%if &compno.=01 and &LOB. = DSNP %then %do;
 %datdsnp;
%end;
%else %do;
 %dataset; 
%end; 

	Proc sort data=&RGA191..finclaims_&compno. out=&RGA191..source;by clamno lineno;run;
 	proc sort data = &RGA191..finclaims_&compno. out=&RGA191..finclaims_sort_&compno. dupout=&RGA191..Reject_Dup_claim_line_&compno. nodupkey ; 
	 by clamno lineno;
 	Run;


	/* CR# CHG0040549 - kv added logic to read apd_reject control file ----> start */
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/APD_Reject.xlsx"
		out=APD_Reject dbms=xlsx
		replace;
	Run;
	
	data APD_Reject;
	length clamno $20. lineno $5.;
	set APD_Reject;
	run;

	%nobs(APD_Reject);
	%if &nobs. = 0 %then %do;
		data APD_Reject_cntrl_file;
		length clamno $20. lineno $5. edit_code $5. encapdfld $50. edit_chk_type $100. recsubind $1. typecode $1.;	
		format clamno $20. lineno $5. edit_code $5. encapdfld $50. edit_chk_type $100. recsubind $1. typecode $1.;
		set APD_Reject;	
		run;
	%end;
	%else %do;
	proc sort data =APD_Reject nodupkey;
	by clamno lineno;
	run;

	data APD_Reject_cntrl_file(drop = claimfrq icn)  
		 &RGA191..finclaims_sort_&compno.(drop = edit_code 	encapdfld  edit_chk_type Recsubind typecode);
	length edit_code $5. encapdfld $50. edit_chk_type $100.;	
	format 	edit_code $5. encapdfld $50. edit_chk_type $100.;	
	merge APD_Reject(in=a)   &RGA191..finclaims_sort_&compno.(in=b);
	by clamno lineno;
	if a and b then output APD_Reject_cntrl_file;
	else if b then output &RGA191..finclaims_sort_&compno.;
	run;
	%end;
	/* CR# CHG0040549 - kv added logic to read NDC_SVCCODE_reject control file ----> end */


	/* B-166920 added logic to read APD_Reject_Claim control file ----> start */
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/APD_Reject_Claim.xlsx"
		out=APD_Reject_Claim dbms=xlsx
		replace;
	Run;
	
	data APD_Reject_Claim;
	length clamno $20. ;
	set APD_Reject_Claim;
	run;

	%nobs(APD_Reject_Claim);
	%if &nobs. = 0 %then %do;
		data APD_Reject_Claim_cntrl_file;
		length clamno $20. edit_code $5. encapdfld $50. edit_chk_type $100. recsubind $1. typecode $1.;	
		format clamno $20. edit_code $5. encapdfld $50. edit_chk_type $100. recsubind $1. typecode $1.;
		set APD_Reject_Claim;	
		run;
	%end;
	%else %do;
	proc sort data =APD_Reject_Claim nodupkey;
	by clamno;
	run;

	data APD_Reject_Claim_cntrl_file(drop = claimfrq icn)  
		 &RGA191..finclaims_sort_&compno.(drop = edit_code 	encapdfld  edit_chk_type Recsubind typecode);
	length edit_code $5. encapdfld $50. edit_chk_type $100.;	
	format 	edit_code $5. encapdfld $50. edit_chk_type $100.;	
	merge APD_Reject_Claim(in=a)   &RGA191..finclaims_sort_&compno.(in=b);
	by clamno;
	if a and b then output APD_Reject_Claim_cntrl_file;
	else if b then output &RGA191..finclaims_sort_&compno.;
	run;

	proc sort data =&RGA191..finclaims_sort_&compno.;
	by clamno lineno;
	run;
	%end;
	
	proc sort data =svclines;
	by clamno lineno;
	run;
	
	
	/* B-166920  ----> end */
	
	
	/*B-278974 Amount Swap from Control File - Start's Here*/
	data svclines ;
	merge svclines (in=a) Amount_Xwalk(in=b);
	by clamno lineno;
	if a;
	run;
	
	proc sort data =svclines;
	by clamno lineno;
	run;
	
	/*B-278974 Amount Swap from Control File - End's Here*/

	proc sql;
		select  count(*) into :total_obs  from &RGA191..finclaims_&compno. ;
    Quit;
	Data &RGA191..svclines_1_&compno.
	  &RGA191..Reject_Svclines_&compno. 	;
     		 merge &RGA191..finclaims_sort_&compno.(in=A) 
			svclines(in=B Keep=&Keepv_svc. &Keepv_svc_mcr.);	
	  by clamno lineno;
	  retain 	QI_and_SV_count 		0 	;   
      if A=1 and B=1  then 
		do; 
			QI_and_SV_count + 1 ;		
			/* SAS2AWS2: ReplacedFunctionCompress */
			call symput('QI_and_SV_count',kcompress(QI_and_SV_count ));
			output &RGA191..svclines_1_&compno. ;				
		end;
	  if A=1 and B=0 then
	  	do;
		   output &RGA191..Reject_Svclines_&compno. ;
		end ;        
 	run;

 	Data &RGA191..svclines_&compno.;
 		set &RGA191..svclines_1_&compno.;

 		where ( (claimfrq = '1' and icn eq '' and revind = 'N') or (claimfrq in (/*'7',*/'8') and icn ne ''  and revind ne 'N')  or
		(claimfrq = '1' and icn ne '' ) or (claimfrq in ('7') and icn ne ''  and revind = 'N')  
			%if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do; /*ALM#7174 - added lob, B-96285 added QHP */ 
				and lobcod in (&lobcode.)
			%end; 
			); 
		/*HS 	567501 kv - modified revind condition *//* CR# CHG0034118 - Claim freq 7 condition added */	
		if (svccod >= "00100" and svccod <= "01999") or  (svccod >= "99100" and svccod <= "99140") then do;
	
				if modcod in ('AA','AD','QK','QS','QX','QY','QZ') then 
					 do;	
					 	modcod=modcod;
					 end;
				else if modcod not in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;
					if modcd2  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					  do;
							temp=modcod;
							modcod=modcd2;
							modcd2=temp;
					  end;
					else if modcd3  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
						do;	
							temp=modcod;
							modcod=modcd3;
							modcd3=temp;
						end;
					else if modcd4  in  ('AA','AD','QK','QS','QX','QY','QZ') then
						do;
							temp=modcod;
							modcod=modcd4;
							modcd4=temp;
						end;
					end;

			if modcd2 in ('AA','AD','QK','QS','QX','QY','QZ') then 
				   do;	
				   		modcd2=modcd2;
				   end;
			else if modcd2 not in  ('AA','AD','QK','QS','QX','QY','QZ') then
				do;
				if	modcd3  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;
						temp=modcd2;
						modcd2=modcd3;
						modcd3=temp;
					end;
				else if modcd4  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;	
					    temp=modcd2;
						modcd2=modcd4;
						modcd4=temp;
					end;
				end;		
			if modcd3 in ('AA','AD','QK','QS','QX','QY','QZ') then 
				   do;	
				   		modcd3=modcd3;
				   end;
			else if modcd3 not in  ('AA','AD','QK','QS','QX','QY','QZ') then 
				do;
				if modcd4  in  ('AA','AD','QK','QS','QX','QY','QZ') then 
					do;
						temp=modcd3;
						modcd3=modcd4;
						modcd4=temp;
					end;
				end;

		if modcod not in ('AA','AD','QK','QS','QX','QY','QZ') then modcod='AA';
	 	end;
		
        CLAAMT=round(CLAAMT,.01);
        NETALWAMT = round(NETALWAMT,.01);
		dedamt = round(dedamt,.01);
		coiamt = round(coiamt,.01);
		COPAMT = round(COPAMT,.01);
		ALWAMT = round(ALWAMT,.01);
		nonamt = round(nonamt,.01);
		rcvamt = round(rcvamt,.01);
		dscamt = round(dscamt,.01);						
		topamt = round(topamt,.01);	
		mcrnetalwamt=round(mcrnetalwamt,.01);
		mcdnetalwamt=round(mcdnetalwamt,.01);
		mcdtopamt=Round(mcdtopamt,.01);
		mcrtopamt=Round(mcrtopamt,.01);
		covunt=Round(covunt,.01);	/* HS 569650 - kv added */
		
		CLAAMT_MCR=round(CLAAMT_MCR,.01);
        NETALWAMT_MCR = round(NETALWAMT_MCR,.01);
		dedamt_MCR = round(dedamt_MCR,.01);
		coiamt_MCR = round(coiamt_MCR,.01);
		COPAMT_MCR = round(COPAMT_MCR,.01);
		ALWAMT_MCR = round(ALWAMT_MCR,.01);
		nonamt_MCR = round(nonamt_MCR,.01);
		rcvamt_MCR = round(rcvamt_MCR,.01);
		dscamt_MCR = round(dscamt_MCR,.01);						
		topamt_MCR = round(topamt_MCR,.01);
		mcrnetalwamt_MCR=round(mcrnetalwamt_MCR,.01);
		mcdnetalwamt_MCR=round(mcdnetalwamt_MCR,.01);
		mcdtopamt_MCR=Round(mcdtopamt_MCR,.01);
		mcrtopamt_MCR=Round(mcrtopamt_MCR,.01);
		covunt_MCR=Round(covunt_MCR,.01);
		
	Run;
	%if  &compno.=34  or &compno.=60 %then %do;
		Data &RGA191..svclines_&compno.;
		set &RGA191..svclines_&compno.;
        	where LOBCOD in ("&LOB.");  
		Run; 
    %end;	
    %else %if &compno.=45 and "&LOB." eq "MPPO" %then %do; /*B-248997*/
		Data &RGA191..svclines_&compno.;
			set &RGA191..svclines_&compno.;
        	where LOBCOD in ("&LOB."); 
		Run;			
	%end ;
	/*%else %if &compno.=30 %then %do;
		Data &RGA191..svclines_&compno.;
			set &RGA191..svclines_&compno.;
        	where grpnum='MAXMUM' ;
		Run;			
	%end ;		B-145499 commented */	

	proc sql;
		select  count(*) into :total_obs_post_revind  from &RGA191..svclines_&compno. ;
    Quit;  
	Proc sql noprint ;
		Select count(clamno) into :QI_count from &RGA191..finclaims_&compno.;	
		Select count(clamno) into :QI_count_Unique from &RGA191..finclaims_sort_&compno.;	
		Select count(clamno) into :QI_and_SV_count from &RGA191..svclines_&compno.;
		Select count(distinct(clamno)) into :QI_count_clm from &RGA191..finclaims_&compno.;		
	quit;
	data _Null_ ;
		/* SAS2AWS2: ReplacedFunctionCompress */
		call symput('QI_not_SV_count',kcompress(%eval(&QI_count_Unique. - &QI_and_SV_count. )));
	run;
	data _Null_ ;
		/* SAS2AWS2: ReplacedFunctionCompress */
		call symput('QI_duplicate',kcompress(%eval(&QI_count. - &QI_count_Unique. )));
	run;
%end;
%mend;
%QIfile;

/* HS 542635 - Added Proc delete statement */
/*Proc delete data=finclaims_&compno.;run;*/
Proc delete data=finclaims_sort_&compno.;run;

Proc sql noprint ;
	Select count(distinct(clamno)) into :QI_and_clm_count from &RGA191..svclines_&compno.;
quit;

/**********************************************************************************************/
/*                                                                                            */ 
/*                   Hold code logic                                                          */ 
/* Assumption : All the entries in svchold table will have matching entry in holdcode table   */
/*                                                                                            */ 
/**********************************************************************************************/

/* B-108297 Hold code reject and service line fill start */

Proc sort data=svcholds; by clamno lineno ;run;
Proc sort data=&RGA191..svclines_&compno.;by clamno lineno ;run;

Data &RGA191..svcholds_fill(keep= clamno count_holdcd count_miss_V count_miss_XR lineno holdcd_lineno_rej temp_holdcd temp_revcod temp_ndcsvc temp_ndcuom temp_cptmd1-temp_cptmd4 temp_ndcqty);
	Merge &RGA191..svclines_&compno.(in=_a) 
		  svcholds(in=_b where=(holdcd in('$V','XR')) );
	by clamno lineno;
	
	if first.clamno then do;  /* reset counters to 0*/
  	  count_holdcd=0;
	  count_miss_V=0;
  	  count_miss_XR=0;
	end;
	
	if _a and _b and formcd = 'U' and ndcsvc ne ' ' and revcod ne ' ' and claamt = 0 then do;  /* identify Inst reject line and assign temp values */
	  count_holdcd+1;
 	  holdcd_lineno_rej=lineno;
	  temp_holdcd=holdcd;
	  temp_revcod=revcod;
	  temp_ndcsvc=ndcsvc;
	  temp_ndcuom=ndcuom;
	  temp_cptmd1=cptmd1;
	  temp_cptmd2=cptmd2;
	  temp_cptmd3=cptmd3;
	  temp_cptmd4=cptmd4;
	  temp_ndcqty=ndcqty;
	end;

	if revcod='' and ndcsvc='' and ndcuom='' and cptmd1='' and cptmd2='' and cptmd3='' and cptmd4='' then do; /* identify and count fill lines */
	  if temp_holdcd = '$V' then count_miss_V+1;
	  if temp_holdcd = 'XR' and ndcqty in(.,0) then count_miss_XR+1;
	end;
 
	if last.clamno and count_holdcd=1 and (count_miss_V=1 or count_miss_XR=1) then output;  /* output single hold code, single fill line */
	retain temp_revcod temp_ndcsvc temp_ndcuom temp_cptmd1-temp_cptmd4 temp_ndcqty holdcd_lineno_rej temp_holdcd;
run;

Data &RGA191..svclines_&compno.;
Merge &RGA191..svclines_&compno.(in=_a) 
	  &RGA191..svcholds_fill(in=_b keep= clamno count_holdcd count_miss_V count_miss_XR temp_revcod temp_ndcsvc temp_ndcuom temp_cptmd1-temp_cptmd4 temp_ndcqty temp_holdcd);
by clamno;
if _a;
if _a and _b then do;  /* replace blank fill values with hold code reject temp values */

  if count_holdcd=1 and count_miss_V=1 and revcod='' and cptmd1='' and cptmd2='' and cptmd3='' and cptmd4='' and ndcsvc='' and ndcuom='' then do;
   	revcod=temp_revcod;
	ndcsvc=temp_ndcsvc;
	ndcuom=temp_ndcuom;
	cptmd1=temp_cptmd1;
	cptmd2=temp_cptmd2;
	cptmd3=temp_cptmd3;
	cptmd4=temp_cptmd4; 
    if temp_holdcd='$V' then ndcqty=covunt;	
  end;
  else	
  if count_holdcd=1 and count_miss_XR=1 and revcod='' and cptmd1='' and cptmd2='' and cptmd3='' and cptmd4='' and ndcsvc='' and ndcuom='' and ndcqty in(.,0) then do;
	revcod=temp_revcod;
	ndcsvc=temp_ndcsvc;
	ndcuom=temp_ndcuom;
	cptmd1=temp_cptmd1;
	cptmd2=temp_cptmd2;
	cptmd3=temp_cptmd3;
	cptmd4=temp_cptmd4; 
    if temp_holdcd='XR' then ndcqty=temp_ndcqty;
  end;
end;
drop temp_revcod temp_ndcsvc temp_ndcuom temp_cptmd1-temp_cptmd4 temp_ndcqty formcd;
run;

/* B-108297 Hold code reject and service line fill end   */


/*B-120951 Included the control file CAS_Segment_Negative_Chk - Starts Here*/
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/CAS_Segment_Negative_Chk.xlsx"
		out=CAS_Segment_Negative_Chk dbms=xlsx replace;
Run;
proc sql noprint;
		/* SAS2AWS2: ReplacedFunctionCompress */
		select  kcompress(Negative_AMT_Flag)  into :CAS_Negative_chk  from CAS_Segment_Negative_Chk;
Quit;
/*B-120951 Included the control file CAS_Segment_Negative_Chk - Ends Here*/


/*B-110462 - Sart here*/
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Inpatient_Proc_Code.xlsx"
		out=Inpatient_Proc_Code dbms=xlsx replace;
Run;

data Inpatient_Proc_Code1(keep=clamno  Proc_cod1-Proc_cod25 proc_dt1-proc_dt25);
length  Proc_cod1-Proc_cod25 $8.;
set Inpatient_Proc_Code;
format proc_dt_1-proc_dt_25 $10.;
format proc_dt1-proc_dt25 $8.;
ARRAY ctrl_proc_dt (25) proc_dt_1-proc_dt_25 ;
ARRAY new_proc_dt (25) proc_dt1-proc_dt25;
DO i = 1 TO 25;
if missing(ctrl_proc_dt{i})=0 then do; 
/* SAS2AWS2: ReplacedFunctionCats-ReplacedFunctionSubstr */
new_proc_dt{i}=cats(ksubstr(ctrl_proc_dt{i},7,4),ksubstr(ctrl_proc_dt{i},1,2),ksubstr(ctrl_proc_dt{i},4,2));end;
end;
drop i proc_dt_1-proc_dt_25;
run;

/*B-110462 - End ere*/


/*CR# CHG0040374 - Added logic to map NDCSVC, NDCUOM & NDCQTY for service lines present in the control file */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/ndcsvc_analysis.xlsx"
		out=ndcsvc_analysis dbms=xlsx replace;
Run;
Proc sort data=	ndcsvc_analysis(rename = (ndcsvc=temp_ndcsvc	ndcuom=temp_ndcuom	ndcqty=temp_ndcqty)) nodupkey;
by clamno lineno;
run;
Proc sort data=&RGA191..svclines_&compno.;by clamno lineno ;run;
Data &RGA191..svclines_&compno.(drop=temp_ndcsvc  temp_ndcuom  temp_ndcqty);
Merge &RGA191..svclines_&compno.(in=_a) ndcsvc_analysis(in=_b) &RGA191..svcholds_fill(in=_c keep=clamno lineno);  /* B-108297 */
by clamno lineno;
if _a;
if _a and _b and not _c then do;  /* B-108297 exclude svcholds_fill claims previously mapped */
	ndcsvc=temp_ndcsvc;	
	ndcuom=temp_ndcuom;	
	ndcqty=temp_ndcqty;
end;
run;


/*CR# CHG0038619 - Added logic to map temporary AP transaction date for claims present in the control file */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Missing_pidate.xlsx"  /*[B-315447] - DS - Updates*/ 
		out=Missing_pidate dbms=xlsx replace;
Run;
Proc sort data=	Missing_pidate(rename = (pidate =Temp_pidate)) nodupkey;by clamno ;run;
Proc sort data=&RGA191..svclines_&compno.;by clamno  ;run;
Data &RGA191..svclines_&compno.(drop=Temp_pidate);
Merge &RGA191..svclines_&compno.(in=_a) Missing_pidate(in=_b);
by clamno ;
if _a;
if _a and _b  and missing(pidate) eq 1 then do; 
	pidate=Temp_pidate;
end;
run;

/* HS 569650 - added logic to read bilcode control file  */

Proc sort data=&RGA191..svclines_&compno.;by clamno ;run;


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Claims_Bilcod.xlsx"
		out=Claims_Bilcod dbms=xlsx replace;
Run;
Proc sort data=	Claims_Bilcod nodupkey;by clamno ; /*where Compno = "&compno.";*/run;/* CR# CHG0035563 - commented company condition */
Data 	&RGA191..svclines_&compno(rename=(Bilcod=current_bilcod));
	merge &RGA191..svclines_&compno.(in=_a) Claims_Bilcod(in=_b);
	if _a;
	by clamno ;
Run;
/* CR# CHG0035563 - Added logic to assign blank Modifier code values for claims present in the control file */


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/modifier_blankout.xlsx"
		out=modifier_blankout dbms=xlsx replace;
Run;
Proc sort data=	modifier_blankout nodupkey;by clamno lineno; run;
Proc sort data=&RGA191..svclines_&compno.;by clamno lineno ;run;
Data &RGA191..svclines_&compno./*(drop=new_modcod)*/;
*length modcod modcd2 modcd3 modcd4 $2.;
Merge &RGA191..svclines_&compno.(in=_a) modifier_blankout(in=_b);
by clamno lineno;
if _a;
/*CHG0039138 - Added logic to assign blank values for all modifier code values*/
if _a and _b then do;
modcod='';
modcd2='';
modcd3='';
modcd4='';
end;
run;	


/*CR# CHG0035563 - Added logic to assign blank NDCSVC values for claims present in the control file */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NDCSVC_Blankout.xlsx"
		out=NDCSVC_Blankout dbms=xlsx replace;
Run;
Proc sort data=	NDCSVC_Blankout nodupkey;by clamno lineno;run;
Proc sort data=&RGA191..svclines_&compno.;by clamno lineno ;run;
Data &RGA191..svclines_&compno.(drop=NDCSVC_upd);
Merge &RGA191..svclines_&compno.(in=_a) NDCSVC_Blankout(in=_b);
by clamno lineno;
if _a;
if _a and _b then do; 
	NDCSVC=NDCSVC_upd;
	NDCQTY=.;
	NDCUOM="";
end;

/* CR# CHG0037844 - Added logic to consider HC service type claims as 'DG' by KV*/
if svctyp ='HC' and svccod ='D7911' and svcstat='PA' then svctyp='DG';
run;

/* B-92443 New Bilcod logic */
data Bilcods_wth_out_x Bilcods_wth_x;
set cntrl_Bilcod;
where not missing(Bilcod);
length b2 $3.;
/* SAS2AWS2: ReplacedFunctionIndex-ReplacedFunctionUpcase */
if kindex(kupcase(Bilcod),'X')>0 then do;
     /* SAS2AWS2: ReplacedFunctionSubstr */
     b1=ksubstr(Bilcod,1,2);
     do i=0 to 9;
           /* SAS2AWS2: ReplacedFunctionCats */
           b2=cats(b1,i);
           output Bilcods_wth_x;
     end;
end;
else output Bilcods_wth_out_x;
run;

Proc sql noprint;	
/* SAS2AWS2: ReplacedFunctionCompress */
Select kcompress("'"||BILCOD||"'") into :rej_Bilcod SEPARATED by ',' from Bilcods_wth_out_x;	/* B-96285 INST or PROF */
quit;

Proc sql noprint;
/* SAS2AWS2: ReplacedFunctionCompress */
Select kcompress("'"||b2||"'") into :rej_Bilcodx SEPARATED by ',' from Bilcods_wth_x;
quit; 
/* B-92443 end */

/* B-117000 Replace Diag codes and POAs with diag_xwalk control file 
proc sort data=diag_xwalk; by clamno; run;
data claims;
merge claims(in=_a) diag_xwalk(in=_b);
by clamno;
if _a;
run;											B-160659 commented */

/* B-160659 diag_xwalk.xlsx control file swap start */
data calims_diagcd_org (keep= clamno &Keepv_ClmDiag.);
set claims;
run; 

data diag_xwalk2(keep=clamno PWK_Segment) diag_xwalk1;
length diag_concat $200.;
set &RGA191..diag_xwalk;
/* SAS2AWS2: ReplacedFunctionCatx */
diag_concat=catx('',&Keepv_ClmDiag_Comma.);
if missing(diag_concat)	=1 then output diag_xwalk2;
if missing(diag_concat)	=0 then output diag_xwalk1;
drop diag_concat logic;
run;

proc sort data=claims; by clamno; run;
proc sort data=diag_xwalk1; by clamno; run;
proc sort data=diag_xwalk2; by clamno; run;

data claims;
merge claims(in=_a) diag_xwalk1(in=_b) diag_xwalk2(in=_c);
by clamno;
if _a;
run;

proc sql;
create table calims_Diagcd as select a.* from calims_diagcd_org a inner join diag_xwalk1 b on a.clamno=b.clamno order by a.clamno;
quit;
/* B-160659 diag_xwalk.xlsx control file swap end */

/* B-92443 start suppress duplicate Dx code within each obs */

data claim_nodiag(drop=&Keepv_ClmDiag.)
	 claim_diag  (keep=clamno &Keepv_ClmDiag.);
 set claims;
run;

%dup_Dx_chk;

proc sql;
  create table claims(drop=clamnox) as
    select a.*, b.* 
    from claim_nodiag a left join claim_diag (rename=(clamno=clamnox)) b
    on (a.clamno = b.clamnox)
	order by clamno;
quit;
/* B-92443 end */ 
Data &RGA191..svclines_&compno.(Drop = edit_chk_type edit_code encapdfld  Recsubind holdcd_lineno_rej) 	
		 &RGA191..ClmN_SvcY_&compno._&run_id. (keep = clamno lineno edit_chk_type edit_code encapdfld  Recsubind)	
		 &RGA191..Reject_Bilcod_&compno._&run_id. (keep = clamno lineno edit_chk_type edit_code encapdfld  Recsubind bilcod claimfrq icn)
		 &RGA191..Reject_svcholds (keep = clamno lineno edit_chk_type edit_code encapdfld  Recsubind typecode);			  
		 
	    merge	claims (IN=A Keep=&Keepv_Claim. PWK_Segment)	/* B-160659 added PWK */
				&RGA191..svclines_&compno.(IN=B)
				&RGA191..svcholds_fill(IN=C keep=clamno holdcd_lineno_rej);                
				by clamno ;
  		length 	
  				CMSpatstat 	$2. 
				clm_distat  $2.
				revenue_cd	$4.
				revenue_cd_mcr $4.
				edit_code 	$5.  /* HS 567501 */
				encapdfld   $50.
				edit_chk_type $100.
				hiosid /*$16.*/$24.		/* B-146417 */
  				;
		retain 	ClmN_SvcY_Count	0
				ClmY_SvcY_Count	0
				II_Count2       0
				IO_Count2       0
                P_Count2        0  				

				ClmY_SvcY_Count1 0
				II_Count2_1 0
				IO_Count2_1 0
				P_Count2_1 0				
				;
			clm2_provno = provno ;
 		hiosid = '';		/* B-146417 initialize */

		if missing(revcod) = 0 then  
		do;
				revenue_cd = revcod ;
		end;
		else if svctyp = "RV" then
		do;
				revenue_cd = svccod ;
		end;		
		if missing(revcod_mcr) = 0 then  
		do;
				revenue_cd_mcr = revcod_mcr ;
		end;
		else if svctyp_mcr = "RV" then
		do;
				revenue_cd_mcr = svccod_mcr ;
		end;
/* ALM # 5160 - added MCDNETALWAMT, MCRNETALWAMT, MCDTOPAMT, MCRTOPAMT derivation logic ---> start */
/* B-267578 - DS - Commenting exsiting mcdnetalwamt,mcrnetalwamt,mcdtopamt,mcrtopamt logic and updating new logic */
		
								 /*if mcdnetalwamt gt 0 and mcrnetalwamt gt 0 then 
								 do;
									
								 end;
								 else if  mcdnetalwamt gt 0 then do;
										mcrnetalwamt=round(sum(cstshrnetalcoiamt,cstshrnetalcopamt,cstshrnetaldedamt),.01);
								 end;
								 else if  mcrnetalwamt gt 0 then do;
								 	     mcdnetalwamt=round(sum(cstshrnetalcoiamt,cstshrnetalcopamt,cstshrnetaldedamt),.01);
								 end;

								  if mcdtopamt gt 0 and mcrtopamt gt 0 then 
								 do;
									
								 end;
								 else if  mcdtopamt gt 0 then do;
										mcrtopamt=round(sum(cstshrtopaycopamt,cstshrtopaycoiamt,cstshrtopaydedamt),.01);
								 end;                          
								 else if  mcrtopamt gt 0 then do;
								 	     mcdtopamt=round(sum(cstshrtopaycopamt,cstshrtopaycoiamt,cstshrtopaydedamt),.01);
								 end;
								 */
								 /* B-267578 - New Logic for mcdnetalwamt & mcdtopamt */
								 
								 mcdnetalwamt=round(sum(cstshrnetalcoiamt,cstshrnetalcopamt,cstshrnetaldedamt,mcdnetalwamt),.01);								
								 mcdtopamt=round(sum(cstshrtopaycopamt,cstshrtopaycoiamt,cstshrtopaydedamt,mcdtopamt),.01);
								 
								 /*  B-267578 - update ends here */
								 
								 mcdnetalwamt_old=mcdnetalwamt;
								 mcrnetalwamt_old=mcrnetalwamt;
								 /*B-329832 - DS - only for cap c and topamt > 0 assiging preamt instead of topamt*/
								 if capind ='C' and topamt gt  0 then do; 
											NETALWAMT_old=preamt;
											NETALWAMT_old_mcr=preamt_mcr;
								 end;
							     else do;
								            NETALWAMT_old=NETALWAMT;
											NETALWAMT_old_mcr=NETALWAMT_mcr;								 
								 end;
								 
/* ALM # 5160 - added MCDNETALWAMT, MCRNETALWAMT, MCDTOPAMT, MCRTOPAMT derivation logic ---> end */

 	  if A=0 and B=1 then
		do;
			ClmN_SvcY_Count + 1 ;
			
			     edit_chk_type = 'Record not avaliable claim table';
				 edit_code = '      ';
				 encapdfld = '                               ';
			     Recsubind = 'N';   		

			output &RGA191..ClmN_SvcY_&compno._&run_id. ;
		end;
		/* SAS2AWS2: ReplacedFunctionCompress */
		call symput('ClmN_SvcY_Count',kcompress(ClmN_SvcY_Count));
 
		if A=1 and B = 1 then 
		do;
				      if bilcod=current_bilcod then bilcod=Encounter_bilcod; /* HS 569650 - added to map encounter bill code */

					/* SAS2AWS2: ReplacedFunctionUpcase */
					if kupcase(bilcod) in (&rej_Bilcod.) and "&edit_type_bilcod_rej." eq "H" and typecode in (&enctyp_code_bilcod_rej.) then 	/* B-96285 added rejection, B-168796 */
					    Do;
					  		 	 edit_chk_type = 'Bilcod Reject';
								 edit_code ="99995" ;
								 encapdfld = 'Bilcod_Reject';
							     Recsubind = 'N';
								%let rej_Bilcod=&rej_Bilcod.;	
								%put &=rej_Bilcod.;				
								output &RGA191..Reject_Bilcod_&compno._&run_id. ;
							 delete;			
					    End;
				    Else If "&edit_type_bilcod_rej." eq "H" and typecode in (&enctyp_code_bilcod_rej.) and /* B-168796 */
							/* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionLength-ReplacedFunctionTrim */
							formcd = 'U' and ( missing(ktrim(kleft(Compress(bilcod))))=1 or klength(ktrim(kleft(Compress(bilcod))))< 3 or
										 bilcod in (&rej_Bilcodx.) ) Then			/* B-92443 added bilcod control file reject */
						Do;
								 edit_chk_type = 'Bilcod Reject';
								 edit_code = "99995"; /* HS 567501 */
								 encapdfld = 'Bilcod_Reject';/* HS 567501 */
							     Recsubind = 'N';
								%let rej_Bilcodx=&rej_Bilcodx.;	/* B-92443 */
								%put &=rej_Bilcodx.; 			/* B-92443 */ 
								output &RGA191..Reject_Bilcod_&compno._&run_id. ;
							 delete;
						End;
					/* SAS2AWS2: ReplacedFunctionSubstr */
					/*(Else If formcd = 'U' and ksubstr(bilcod,1,2) in ('11','12','28','41','65','66','86' -- VVKR Commented - B-75041 ) Then*/ /*CHG0037492 - added new bill code*/
					Else If formcd = 'U' and ksubstr(bilcod,1,2) in ( &Typecode_Inst. ) Then /*B-222732 Replace Hardcode value with control file macro*/
					    Do;							
		        				 typecode = 'I';
								 II_Count2  + 1 ;  
/* ALM # 5160 - commented */
/*								mcdnetalwamt_old=mcdnetalwamt;*/
/*								 mcrnetalwamt_old=mcrnetalwamt;*/
/**/
/*								 if mcdnetalwamt gt 0 and mcrnetalwamt gt 0 then*/
/*								 do;*/
/*									*/
/*								 end;*/
/*								 else if  mcdnetalwamt gt 0 then do;*/
/*										mcrnetalwamt=round(sum(cstshrnetalcoiamt,cstshrnetalcopamt,cstshrnetaldedamt),.01);*/
/*								 end;*/
/*								 else if  mcrnetalwamt gt 0 then do;*/
/*								 	     mcdnetalwamt=round(sum(cstshrnetalcoiamt,cstshrnetalcopamt,cstshrnetaldedamt),.01);*/
/*								 end;*/
		   		   		End;
				    Else  If formcd = 'U' Then
						Do;
		             			 typecode = 'O';  								 
								 Netalwamt=topamt; 
								 Netalwamt_mcr=topamt_mcr; 
								 mcdnetalwamt=round(mcdtopamt,.01);
								 mcrnetalwamt=round(mcrtopamt,.01);
/* ALM # 5160 - commented */	
/*								 mcdnetalwamt_old=mcdnetalwamt;*/
/*								 mcrnetalwamt_old=mcrnetalwamt;							 */

/*								 if mcdnetalwamt gt 0 and mcrnetalwamt gt 0 then */
/*								 do;*/
/*									*/
/*								 end;*/
/*								 else if  mcdnetalwamt gt 0 then do;*/
/*										mcrnetalwamt=round(sum(cstshrtopaycopamt,cstshrtopaycoiamt,cstshrtopaydedamt),.01);										*/
/*								 end;                          */
/*								 else if  mcrnetalwamt gt 0 then do;*/
/*								 	     mcdnetalwamt=round(sum(cstshrtopaycopamt,cstshrtopaycoiamt,cstshrtopaydedamt),.01);*/
/*								 end;*/


								 IO_Count2  + 1 ; 							 
								 
		                End;
		         	Else If formcd = 'H' Then 
						Do;
		             			 typecode = 'P';								 
								 Netalwamt=topamt;
								 Netalwamt_mcr=topamt_mcr;
								 mcdnetalwamt=round(mcdtopamt,.01);
								 mcrnetalwamt=round(mcrtopamt,.01);	
/* ALM # 5160 - commented */
/*								 mcdnetalwamt_old=mcdnetalwamt;*/
/*								 mcrnetalwamt_old=mcrnetalwamt;	*/

/*								  if mcdnetalwamt gt 0 and mcrnetalwamt gt 0 then */
/*								 do;*/
/*									*/
/*								 end;*/
/*								 else if  mcdnetalwamt gt 0 then do;*/
/*										mcrnetalwamt=round(sum(cstshrtopaycopamt,cstshrtopaycoiamt,cstshrtopaydedamt),.01);*/
/*								 end;                          */
/*								 else if  mcrnetalwamt gt 0 then do;*/
/*								 	     mcdnetalwamt=round(sum(cstshrtopaycopamt,cstshrtopaycoiamt,cstshrtopaydedamt),.01);*/
/*								 end;*/

								 P_Count2  + 1 ;						 	
								 
		        		End;
					 ELSE if "&edit_type_bilcod_rej." eq "H" and typecode in (&enctyp_code_bilcod_rej.)	then /* B-168796 */
				   		Do;
								 edit_chk_type = 'Bilcod Reject';
								 edit_code = "99995"; /* HS 567501 */
								 encapdfld = 'Bilcod_Reject';/* HS 567501 */
							     Recsubind = 'N';  
				   				output &RGA191..Reject_Bilcod_&compno._&run_id. ;
								Delete;
						End;

					if "&edit_type_holdcode_rej." eq "H" and typecode in (&enctyp_code_holdcode_rej.) and /* B-168796 */
						lineno=holdcd_lineno_rej and typecode in ('I','O') then do;			/* B-108297 reject */
						edit_chk_type='Hold Code Reject $V XR';
						encapdfld='Holdcode_Reject';
						Recsubind='N';
						edit_code = '99971';
					output &RGA191..Reject_svcholds;
					delete;
					end;
 
           		if first.clamno then 
		 			do;
						ClmY_SvcY_Count1+1;
						if typecode in ('I') then II_Count2_1 + 1;
						if typecode in ('O') then IO_Count2_1 + 1;
						if typecode in ('P') then P_Count2_1 + 1;
					end;
				/* SAS2AWS2: ReplacedFunctionCompress */
				call symput('II_Count2',kcompress(II_Count2));
			    /* SAS2AWS2: ReplacedFunctionCompress */
		    	call symput('IO_Count2',kcompress(IO_Count2));
				/* SAS2AWS2: ReplacedFunctionCompress */
				call symput('P_Count2',kcompress(P_Count2));

				/* SAS2AWS2: ReplacedFunctionCompress */
				call symput('ClmY_SvcY_Count1',kcompress(ClmY_SvcY_Count1));
			    /* SAS2AWS2: ReplacedFunctionCompress */
	    		call symput('II_Count2_1',kcompress(II_Count2_1));
				/* SAS2AWS2: ReplacedFunctionCompress */
				call symput('IO_Count2_1',kcompress(IO_Count2_1));
				   /* SAS2AWS2: ReplacedFunctionCompress */
				call symput('P_Count2_1',kcompress(P_Count2_1));   

				if distat in(&distat_reject.) and typecode in ('I','O') then distat= '  ';	/* B-108297 patient status code Inst reject */			
			    clm_distat = DISTAT ;
				CMSpatstat = put(DISTAT,$disstat.);
				/* DISTAT	  = CMSpatstat ;   */
				if missing(CMSpatstat)=0 then DISTAT = CMSpatstat; /*CR# CHG0035563*/

				if missing(admtyp) = 0 then 
					admtype = put(admtyp,$Medicareadmtype.);
				else 
					admtype = admtyp ;
				if missing(admtype) = 1 then admtype = '9' ;  

				if missing(ADMSRC) = 1 then
				do;
					if admtyp = 'NB' then ADMSRC = '1';
					else ADMSRC = '9';
				end;		
				if missing(atpnpi) = 1 then atpnpi = attphy; /* CR# CHG0036691 */
/* CR# CHG0031183 KV - if length of modifier code not equal to 2 then assigned it as null */
				/* SAS2AWS2: ReplacedFunctionLength */
				if klength(modcod) ne 2 then modcod='';
				/* SAS2AWS2: ReplacedFunctionLength */
				if klength(modcd2) ne 2 then modcd2='';
				/* SAS2AWS2: ReplacedFunctionLength */
				if klength(modcd3) ne 2 then modcd3='';
				/* SAS2AWS2: ReplacedFunctionLength */
				if klength(modcd4) ne 2 then modcd4='';
				if admtdiag in (&icd10cd1.) then admtdiag = "   " ; /*CR# CHG0034712 - Added logic to null dummy admitting diagnosis code*/ 				
				ClmY_SvcY_Count + 1 ;
				/* SAS2AWS2: ReplacedFunctionCompress */
				call symput('ClmY_SvcY_Count',kcompress(ClmY_SvcY_Count));
				output &RGA191..svclines_&compno.;
		end;			
	run;	

/*B-289556 Start's Here*/
data Provspec_Logic;	 
	infile "&drv2./Encounters_Reporting/APD/Control_Table/Provspec_Logic.csv" truncover
         	delimiter=',' DSD lrecl=32767 firstobs=2;
	length	compno $2. lobcod $4. hierarchy 3. mmcorcat $100. formcd $3. provspec $10. Description $400.;
	input  
	      compno $
		  lobcod $
		  hierarchy 
		  mmcorcat $
		  formcd $
		  provspec $
		  Description $;		  
	array charvars(*) _character_;
	do i=1 to dim(charvars);
	 charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
	end;
	drop i;
run;

proc sql;
select count(*) into: prv_cnt from Provspec_Logic where kcompress(compno) = "&compno." and kcompress(lobcod) = "&LOB_.";
quit;

/* Provspec Logic Starts here */

%macro prvspcd_cos_mapping();
	
	data &RGA191..svclines_&compno.;
		length prvspcd $3. cos $2.;
    format prvspcd $3. cos $2.;
   set &RGA191..svclines_&compno.;
   cos = '';
   prvspcd = '';
  run;
  

%if &prv_cnt eq 0 %then %goto exit1; /* If the contril file logic does not apply for the comp or LOB */
	
		data Provspec_ctrl_taxonomy(keep= c_hierarchy c_mmcorcat c_compno c_lobcod c_formcd c_provspec c_Specialty_Description )
		     Provspec_ctrl_nontaxonomy(keep= c_hierarchy c_mmcorcat c_compno c_lobcod c_formcd c_provspec c_Specialty_Description );
			length c_compno $4. c_lobcod $8. c_hierarchy 3. c_Specialty_Description $400. c_mmcorcat $200. 
		       c_formcd $7. c_provspec  $10.;       
			set    Provspec_Logic;
			where kcompress(compno) = "&compno." and kcompress(lobcod) = "&LOB_.";

			c_compno		 		 = compno;
			c_lobcod		 		 = lobcod;
			c_hierarchy      = hierarchy;
			c_mmcorcat       = mmcorcat;
			c_formcd         = formcd;
			c_provspec       = provspec;
			c_Specialty_Description = Description;

			if c_compno ne 'NA' then c_compno = "'"||(compress(c_compno))||"'";
			if c_lobcod ne 'NA' then c_lobcod = "'"||tranwrd(trim(compress(c_lobcod)), ",", "','")||"'";
			if c_formcd ne 'NA' then c_formcd = "'"||tranwrd(trim(compress(c_formcd)), ",", "','")||"'";
			if find(c_mmcorcat,'CONTAINS','i')>0 then 
			c_mmcorcat=compress(tranwrd("'/"||tranwrd(trim(compress(c_mmcorcat)), ",", "|")||"/'","Contains",""));
			else
			c_mmcorcat="'"||tranwrd(trim(compress(c_mmcorcat)), ",", "','")||"'";
			if upcase(c_provspec) = 'TAXONOMY' then output Provspec_ctrl_taxonomy ;
			else output Provspec_ctrl_nontaxonomy ;
		run;


		proc sort data = Provspec_ctrl_nontaxonomy; by descending c_hierarchy; run;
		proc sql noprint;
		select count(*)  into :NObs from Provspec_ctrl_nontaxonomy;
		select c_mmcorcat  into :tmp_mcor1-:tmp_mcor%left(&NObs) from Provspec_ctrl_nontaxonomy;
		select c_compno    into :tmp_compno1-:tmp_compno%left(&NObs) from Provspec_ctrl_nontaxonomy;
		select c_lobcod    into :tmp_lobcod1 -:tmp_lobcod%left(&NObs) from Provspec_ctrl_nontaxonomy;
		select c_formcd    into :tmp_formcd1-:tmp_formcd%left(&NObs) from Provspec_ctrl_nontaxonomy;
		select c_provspec  into :tmp_provspec1-:tmp_provspec%left(&NObs) from Provspec_ctrl_nontaxonomy;
		select c_hierarchy into :tmp_hierarchy1-:tmp_hierarchy%left(&NObs) from Provspec_ctrl_nontaxonomy;
		quit;

/*  PRVSPCD POPULATED FOR NONTAX MCORCAT FROM CONTROL FILE VS SOURCE MCORCAT MATCH. */		

		data Provspec_ctrl_nontaxonomy_1a;
		length prvspcd_xwalk $3.;
		format prvspcd_xwalk $3.;
		set &RGA191..svclines_&compno.;
		%prvspcd_loop;
		run;
		
		data Provspec_ctrl_nontaxonomy_1;		
		set Provspec_ctrl_nontaxonomy_1a;
		if hierarchy eq '' and prvspcd_xwalk eq '' then delete;
		run;		

		proc sort data = Provspec_ctrl_nontaxonomy_1 (keep=clamno lineno hierarchy prvspcd_xwalk) ; 
		by clamno hierarchy; run;

		proc sort data = Provspec_ctrl_nontaxonomy_1 nodupkey out = Provspec_ctrl_nontaxonomy_final(keep=clamno prvspcd_xwalk) ; 
		by clamno; run;

		proc sort data = Provspec_ctrl_taxonomy; by descending c_hierarchy; run;

		proc sql noprint;
		select count(*)  into :taxcnt from Provspec_ctrl_taxonomy;
		quit;
	  
	%if &taxcnt. > 0 %then %do; /* Process only if Taxonomy records exsits */
			proc sql noprint;
			select count(*)  into :NObs from Provspec_ctrl_taxonomy;
			select c_mmcorcat  into :tmp_mcor1-:tmp_mcor%left(&NObs) from Provspec_ctrl_taxonomy;
			select c_compno    into :tmp_compno1-:tmp_compno%left(&NObs) from Provspec_ctrl_taxonomy;
			select c_lobcod    into :tmp_lobcod1 -:tmp_lobcod%left(&NObs) from Provspec_ctrl_taxonomy;
			select c_formcd    into :tmp_formcd1-:tmp_formcd%left(&NObs) from Provspec_ctrl_taxonomy;
			select c_provspec  into :tmp_provspec1-:tmp_provspec%left(&NObs) from Provspec_ctrl_taxonomy;
			quit;
			
			/*  PRVSPCD POPULATED AS TAXONOMY FOR TAXONOMY RECORDS FROM CONTROL FILE VS SOURCE MCORCAT MATCH. */			
			data Provspec_ctrl_taxonmy_1;
			length prvspcd_xwalk $3.;
			format prvspcd_xwalk $3.;
			set &RGA191..svclines_&compno.;
			%prvspcd_loop;
			run;				

			/* THE PROVSPCD FOR TAXONOMY RECORDS*/
			data Provspec_ctrl_taxonmy_2(keep=clamno origclamno npiid_act formcd prvspcd_xwalk prvtxy pcpcod provno)  ; /*SD - B-308002 */
			set Provspec_ctrl_taxonmy_1;
			where kupcase(prvspcd_xwalk) = 'TAX';
			if formcd = 'U' then npiid_act = atpnpi;
			if formcd = 'H' then npiid_act = rndnpi;
			run;
			proc sort data = Provspec_ctrl_taxonmy_2 nodupkey; by clamno; run;
			
			/* Original claim loop logic starts */
			data taxonomy_npiid_miss(rename = (npiid_act=npiid_org));
			set Provspec_ctrl_taxonmy_2;
			where npiid_act eq '' and origclamno ne ''  ;	
			actual_clamno = clamno;
			tmp_clamno = origclamno;
			run;
			
			data taxonomy_npiid_good(keep = actual_clamno npiid_org);
			set taxonomy_npiid_miss(obs=0);
			run;
					%macro test();
						%nobs(taxonomy_npiid_miss);
						%let bdcnt = &nobs.;
						%put &bdcnt.;
						%let i = 1;  
							/* Loop start */
						%do %while(&bdcnt. >0 and &i. <11);
							proc sql;
							create table it&i. as
							select a.actual_clamno as actual_clamno,b.origclamno as origclamno, 
							case 
								when b.formcd = 'U' then b.atpnpi 
								when b.formcd = 'H' then b.rndnpi 
							end as npiid_org		
							from taxonomy_npiid_miss  a
							left join &libn..claims b
							on a.tmp_clamno=b.clamno;
							quit;		
						
						data taxonomy_npiid_miss taxonomy_npiid_good&i.(keep=actual_clamno npiid_org) ;
						 set it&i.;
						  if npiid_org ne '' or origclamno eq '' then output taxonomy_npiid_good&i.;
						  if npiid_org eq '' and origclamno ne '' then output taxonomy_npiid_miss;
						run;
						
						proc append base = taxonomy_npiid_good data = taxonomy_npiid_good&i.; run;		
						
						data taxonomy_npiid_miss(drop=origclamno);
						 set taxonomy_npiid_miss;
						 tmp_clamno = origclamno;
						run;
						
						Proc delete data=taxonomy_npiid_good&i.;run;
						Proc delete data=it&i.;run;
						
						%nobs(taxonomy_npiid_miss);
						    %let bdcnt = &nobs.;
						    %if &bdcnt. > 0 %then %do;
						      %let i = %eval(&i. + 1);
						    %end;
						%put &i.;
				%end; /* while loop -  end */
				 
				%mend;
				%test;

				proc sort data = taxonomy_npiid_good(rename=(actual_clamno=clamno))nodupkey;by clamno; run;
				proc sort data = Provspec_ctrl_taxonmy_2; by clamno; run;	
					
					/* Merging nonblank NPI records and original claim NPI together */
					data Provspec_taxonomy_npi(keep=clamno npiid_act prvspcd_xwalk prvtxy pcpcod provno); /*SD - B-308002*/
					merge Provspec_ctrl_taxonmy_2(in =a) taxonomy_npiid_good (in =b);
					by clamno;
					if a;
					if a and b then do;		
							if missing(npiid_act) =1 then npiid_act=npiid_org;			
					end;
					run;
					
		/* Original claim loop logic ends */
		/*  SD - B-308002 - commenting the old taxonomy Logic */  
		/*
		
					data Taxonomy_Xwalk(keep = Specialty_Code taxonomy) ;	 
								infile "&drv2./Encounters_Reporting/APD/Control_Table/Taxonomy_Logic.csv" truncover
										delimiter=',' DSD lrecl=32767 firstobs=2;
								length	Specialty_Code $3. Desc $200. Doh $100. taxonomy $10.;
								input  
									Specialty_Code $
									Desc $
									Doh $
									taxonomy $;
									
								array charvars(*) _character_;
								do i=1 to dim(charvars);
								charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
								end;
								drop i;
					run;
							
					proc sort data = Taxonomy_Xwalk nodupkey; by Taxonomy; run;	
	 */
					proc sql; /*SD - B-308002  - pulled spccod and pulled all 5 taxcodes to match controlfile*/
					create table prvspc_taxonomy_hlthprov as
					select a.clamno,a.prvtxy,a.pcpcod,a.provno,c.spccod  
									,b.hlthprovtaxocode1 
									,b.hlthprovtaxocode2 
									,b.hlthprovtaxocode3 
									,b.hlthprovtaxocode4 
									,b.hlthprovtaxocode5 
								
					from Provspec_taxonomy_npi a
					left join &RGA191..providernpi_&compno. b
					on a.npiid_act=b.npiid
					left join &libn2..provspec c
					on a.provno=c.provno;
					quit;
					
					/* SD - B-308002 - commented below steps */

				/*Proc sql;
				Create table spc1 as
				select b.Specialty_code as Specialty_code1,a.*
				from prvspc_taxonomy_hlthprov a 
				left join Taxonomy_Xwalk b
				on a.hlth_cd1=b.Taxonomy;
				quit;

				Proc sql;
				Create table spc2 as
				select b.Specialty_code as Specialty_code2,a.*
				from spc1 a 
				left join Taxonomy_Xwalk b
				on a.hlth_cd2=b.Taxonomy;
				quit;

				Proc sql;
				Create table spc3 as
				select b.Specialty_code as Specialty_code3,a.*
				from spc2 a 
				left join Taxonomy_Xwalk b
				on a.hlth_cd3=b.Taxonomy;
				quit;

				Proc sql;
				Create table spc4 as
				select b.Specialty_code as Specialty_code4,a.*
				from spc3 a 
				left join Taxonomy_Xwalk b
				on a.hlth_cd4=b.Taxonomy;
				quit;

				Proc sql;
				Create table spc5 as
				select b.Specialty_code as Specialty_code5,a.*
				from spc4 a 
				left join Taxonomy_Xwalk b
				on a.hlth_cd5=b.Taxonomy;
				quit;
				

				data Provspec_taxnxwlk_result(keep=clamno prvspcd_txnxwlk);
				 set spc5;
                   if Specialty_code1 ne '' then prvspcd_txnxwlk=Specialty_code1;
				    else if Specialty_code2 ne '' then prvspcd_txnxwlk=Specialty_code2;
				    else if Specialty_code3 ne '' then prvspcd_txnxwlk=Specialty_code3;
				    else if Specialty_code4 ne '' then prvspcd_txnxwlk=Specialty_code4;
				    else if Specialty_code5 ne '' then prvspcd_txnxwlk=Specialty_code5;
				Run;
				*/ 
				
					options lrecl=32767; /*SD - B-308002 - taxonomy control file map*/
					
					filename txn "&drv2./Encounters_Reporting/APD/Control_Table/temp_enc_all.txt";							
					
					data Provspec_taxnxwlk_result_t;
					set prvspc_taxonomy_hlthprov;
					%inc txn;					
					end;
					run;
					
					proc sql;
					create table Provspec_taxnxwlk_result_t1 as
					select a.*,min(hierarchy) as min_hierarchy
					from Provspec_taxnxwlk_result_t a
					group by clamno
					having hierarchy = min(hierarchy);
					quit;
					
					data Provspec_taxnxwlk_result(keep=clamno prvspcd_txnxwlk);
					set Provspec_taxnxwlk_result_t1;
					if klength(strip(prvspcd_txnxwlk)) = 2 then prvspcd_txnxwlk = '0'||strip(prvspcd_txnxwlk);
					if prvspcd_txnxwlk = '999' then prvspcd_txnxwlk = '';
					run; /*SD - B-308002 - updates ends here*/
								
				proc sort data = Provspec_ctrl_nontaxonomy_final; by clamno; run;
				proc sort data = Provspec_taxnxwlk_result nodupkey; by clamno; run;
								
				data prvspcd_final(keep = clamno prvspcd_xwalk );
					merge Provspec_ctrl_nontaxonomy_final(in =a) Provspec_taxnxwlk_result (in =b);
					by clamno;
					if a and b then do;
					  if missing(prvspcd_txnxwlk) = 0 then do;
					     prvspcd_xwalk = prvspcd_txnxwlk; 
					  end;
					end;
					if b and not a then do;
					prvspcd_xwalk = prvspcd_txnxwlk;
					end;
					if a and not b then do;
					prvspcd_xwalk = prvspcd_xwalk;
					end;
				run;
				
				proc sort data = prvspcd_final 
				out = prvspcd_final(rename =(prvspcd_xwalk = prvspcd1));
				by clamno;
				run;
				
%end; /* Process only Taxonomy Records exsists */
%else %do; /* If no input from control table for 'tax' */
				data prvspcd_final(rename=(prvspcd_xwalk = prvspcd1));
				 set Provspec_ctrl_nontaxonomy_final;
				run;
%end;

proc sort data = prvspcd_final; by clamno; run;
proc sort data = &RGA191..svclines_&compno.; by clamno; run;


data &RGA191..svclines_&compno.;  /* merging prvspcd to svclines source dataset */
 merge &RGA191..svclines_&compno.(in =a) prvspcd_final (in =b);
 by clamno; 
    if a; 
    if a and b then do;
    prvspcd = prvspcd1;
    end;
run;

/*PRVSPCD Logic End's Here*/

/*COS Logic starts Here*/
data Cos_Logic;	 
	infile "&drv2./Encounters_Reporting/APD/Control_Table/Cos_Logic.csv" truncover
         	delimiter=',' DSD lrecl=32767 firstobs=2;
	length	compno $2. lobcod $4. hierarchy 3. formcd $3. mmcorcat $100. bilcod $1000. pos $5.
	provspec $3. cos $2.; /* CR# CHG0031183 kv - added descrp */
	input  
	      compno $
		  lobcod $
		  hierarchy 
		  formcd $
		  mmcorcat $
		  bilcod $
		  pos $
		  provspec $
		  cos $;		  
	array charvars(*) _character_;
	do i=1 to dim(charvars);
	 charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
	end;
	drop i;
run;

data Cos_Logic_1(keep= cos_compno cos_lobcod  cos_hierarchy cos_formcd 
                      cos_mmcorcat cos_bilcod cos_poscod cos_prvspcd cos_cos );
     
length cos_compno $4. cos_lobcod $8. cos_hierarchy 3. cos_formcd $7. cos_mmcorcat $200. cos_bilcod $5000. 
		cos_prvspcd $5. cos_cos $2.;     
set    Cos_Logic;
where kcompress(compno) = "&compno." and kcompress(lobcod) = "&LOB_.";

cos_hierarchy    = hierarchy;
cos_compno		 = compno;
cos_lobcod		 = lobcod;
cos_cos          = cos;
cos_formcd       = formcd;
cos_mmcorcat     = mmcorcat;
cos_bilcod		 = bilcod;
cos_poscod		 = pos;
cos_prvspcd	     = provspec;

if cos_compno ne 'NA' then cos_compno = "'"||(compress(cos_compno))||"'";
if cos_formcd ne 'NA' then cos_formcd = "'"||tranwrd(trim(compress(cos_formcd)), ",", "','")||"'";
if cos_mmcorcat ne 'NA' and find(cos_mmcorcat,'CONTAINS','i')>0 then 
cos_mmcorcat=compress(tranwrd("'/"||tranwrd(trim(compress(cos_mmcorcat)), ",", "|")||"/'","Contains",""));
else if
cos_mmcorcat ne 'NA' then cos_mmcorcat="'"||tranwrd(trim(compress(cos_mmcorcat)), ",", "','")||"'";
if cos_bilcod ne 'NA' then cos_bilcod = "'"||tranwrd(trim(compress(cos_bilcod)), ",", "','")||"'";
if cos_poscod ne 'NA' then cos_poscod = "'"||(compress(cos_poscod))||"'";
if cos_prvspcd ne 'NA' then cos_prvspcd = "'"||(compress(cos_prvspcd))||"'";
run;

proc sort data = Cos_Logic_1; by descending cos_hierarchy; run;
proc sql noprint;
select count(*)  into :NObs from Cos_Logic_1;
select cos_compno    		into :cos_compno1-:cos_compno%left(&NObs) 		from Cos_Logic_1;
select cos_formcd    		into :cos_formcd1-:cos_formcd%left(&NObs) 		from Cos_Logic_1;
select cos_mmcorcat  		into :cos_mmcorcat1-:cos_mmcorcat%left(&NObs) 	from Cos_Logic_1;
select cos_bilcod  	 		into :cos_bilcod1-:cos_bilcod%left(&NObs) 		from Cos_Logic_1;
select cos_poscod    		into :cos_poscod1-:cos_poscod%left(&NObs) 		from Cos_Logic_1;
select cos_prvspcd   		into :cos_prvspcd1-:cos_prvspcd%left(&NObs) 	from Cos_Logic_1;
select cos_cos       		into :cos_cos1-:cos_cos%left(&NObs) 			from Cos_Logic_1;
select cos_hierarchy     	into :cos_hierarchy1-:cos_hierarchy%left(&NObs) from Cos_Logic_1;
quit;

data svclines_cos(keep = clamno hierarchy cos cos1);
length cos cos1  $2.;
format cos cos1  $2.;
set &RGA191..svclines_&compno.;
%cos_loop;
run;

proc sort data = svclines_cos; by clamno hierarchy; run;
proc sort data = &RGA191..svclines_&compno.; by clamno; run;
proc sort data = svclines_cos nodupkey out = svclines_cos_final(keep=clamno cos cos1);by clamno; run;

data &RGA191..svclines_&compno.; /* merging cos to svclines source dataset */
 merge &RGA191..svclines_&compno.(in =a) svclines_cos_final (in =b);
 by clamno;
    if a;
    if a and b then do;
    cos = cos1;
    end; 
run;

%exit1:
%mend;
%prvspcd_cos_mapping;

/*B-287495 End's Here*/

/* B-274606 */
data _null_;
	set HIOSID(WHERE = (compno="&compno."  and  lobcod="&lob."));
		call symput('hiosflg',kcompress(hiosflg));
run;

proc sql ;
 %encounter_odsvw;
 create table odsdat1 as
 select * from connection to ods
 (
  select distinct
         a.hf_member_num_cd
       , substr(a.hix_policy_num_cd,1,14) as hix_policy_num_cd
       , a.hios_plan_id_cd   
	   , case
	     when a.company_cd = '34' and a.lob like 'CC%' then 'CCC'
		 when a.company_cd = '34' and a.lob in ('FDA') then a.lob
		 when a.company_cd = '42' and a.lob in ('EP','QHP') then a.lob
		 when a.company_cd = '45' and a.lob in ('HFIC') then ''
		 when a.company_cd = '30' and a.benefit_package_cd in ('DMCR')  then 'DSNP'
		 when a.company_cd = '01' and a.benefit_package_cd like 'DM%' then 'DSNP'
		 when a.company_cd = '01' and a.benefit_package_cd like 'DH%' then 'DSNP'
	     else ''                      end as lob_ods
	   , a.hix_person_id_cd
	   , a.hix_member_id_Cd
	   , a.exchange_subscriber_id_cd
	   , a.cin_cd
	   , case
	   	 when a.company_cd = '30' and a.benefit_package_cd in ('DMCR') then '01'
		 when a.company_cd = '01' and a.benefit_package_cd like 'DM%' then '01'
		 when a.company_cd = '01' and a.benefit_package_cd like 'DH%' then '01'
	     else a.company_cd            end as company_cd
	   , a.coverage_effective_dt
	   , a.coverage_expiration_dt
    from ods_vw_service.mv_consume_customer_health_coverage as a 
   where a.di_audit_source_system_delete_flg = FALSE	
  );
  disconnect from ods;
quit;

Data odsdata;
 Length membno $23.;
 Set odsdat1; 
 where company_cd="&compno." and lob_ods="&lob.";
  membno=kcompress(hf_member_num_cd,'');
Run;

proc sort data=odsdata; by membno; run;
proc sort data=&RGA191..svclines_&compno.; by membno; run;

proc sql;
create table odsclm1 as
select a.membno, a.clamno, b.lob_ods ,b.hios_plan_id_cd ,b.hix_policy_num_cd ,b.company_cd ,b.hix_person_id_cd ,b.hix_member_id_Cd, b.exchange_subscriber_id_cd ,b.cin_cd ,b.coverage_effective_dt ,b.coverage_expiration_dt
from &RGA191..svclines_&compno. a ,odsdata b
where a.membno=b.membno
and b.coverage_effective_dt <= a.firstdos
and b.coverage_expiration_dt >= a.firstdos
;
quit;

proc sort data=odsclm1 nodups; by clamno descending coverage_expiration_dt descending coverage_effective_dt; run;

data odsclm odselsp(keep=membno clamno comment1);
 set odsclm1; by clamno;
 if (&compno.=01 or &compno.=20 or &compno.=34 or &compno.=42 or (&compno.=45 and "&LOB." ne "MPPO")) then do;
	if first.clamno then output odsclm;
	else do;
		length comment1 $120;
		comment1='member having overlapping eligiblity span in ODS view ods_vw_service.mv_consume_customer_health_coverage';
		output odselsp;
	end;
 end;
run;

proc sort data=odsclm nodups; by membno clamno; run;
proc sort data=odselsp nodups; by membno clamno; run;
proc sort data=&RGA191..svclines_&compno.; by membno clamno; run;

Data &RGA191..svclines_&compno.;
   merge &RGA191..svclines_&compno. (in=a)  odsclm (in=b);
      by membno clamno;
	  if a;
Run;

Data &RGA191..svclines_&compno. &RGA191..hiosid_reject;
length edit_chk_type $100. edit_code $5. encapdfld $50.;
set &RGA191..svclines_&compno.;
 if ("&hiosflg." = "Y") then do;
	if ((&compno.=01 or &compno.=20 or &compno.=42) and hix_policy_num_cd='') or 
	   ((&compno.=45 and "&LOB." ne "MPPO")and hios_plan_id_cd='')	and "&edit_type_hiosid_reject." eq "H" and typecode in (&enctyp_code_hiosid_reject.)	
		then do;
			edit_chk_type='Hiosid_Reject';
			encapdfld='Hiosid_Reject';
			Recsubind='N';
			edit_code = '99937'   ;  		
			output &RGA191..hiosid_reject;
		end;
	else output &RGA191..svclines_&compno.;
 end;
 else output &RGA191..svclines_&compno.;
Run;

%nobs(odselsp);
%let odsobs=&nobs.;

%if &odsobs. > 0 %then %do; 
	PROC EXPORT DATA= odselsp	 
				OUTFILE= "&drv2./Encounters_Reporting/APD/Current/RGA19201_&run_id._&compno._ODS_ELIGSPAN.csv"
				DBMS=csv REPLACE;
	RUN;
%end;

/* B-274606 */

/* eligibility macro B-136184, B-146417 added hiosid */
%macro eligibility;
%if "&elig_chk." eq "Y" and (&compno=30 or &compno=45) %then %do; /* B-145499 added MCR (&compno. eq 34 and "&LOB." eq "CCC") */
	proc sql; 
  		create table Encstelig as 
		select distinct a.membno, a.beneffdt2, a.benexpdt, a.mbrsuplid, a.runid, a.isano, a.hiosid, b.supide
  		from &libn..Encsteligoub a, &libn..Encsteliginb b
  		where a.membno = b.membno and a.mbrsuplid = b.supide and
			  a.isano = b.isano and b.recsts='ACCEPT' 						/* B-145499 added isano */
  		order by a.mbrsuplid, a.runid, a.isano, a.benexpdt desc;
	quit;	

	data Encsteliglast;
		set Encstelig;
		by mbrsuplid runid;
		if last.mbrsuplid and last.runid then output Encsteliglast;	
	run;

	proc sql;
  		create table ClaimsAccptd as 
		select distinct a.clamno, a.lineno, b.hiosid
  		from &RGA191..svclines_&compno. a, Encsteliglast b
  		where a.membno = b.membno and ((beneffdt2 <= svcdat and benexpdt = .) or (beneffdt2 <= svcdat <= benexpdt))		
  		order by a.clamno, a.lineno;
	quit;

	proc sort data=	&RGA191..svclines_&compno.; by clamno lineno ; run;

	Data &RGA191..svclines_&compno. (Drop = edit_chk_type edit_code encapdfld  Recsubind)  
		 &RGA191..eligibility_reject(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode);
		length edit_chk_type $100. edit_code $5. encapdfld $50.;
		Merge &RGA191..svclines_&compno.(in=_a) ClaimsAccptd(in=_b);
		by clamno lineno ;
		if _a;
		if _a and not _b and "&edit_type_eligibility." eq "H" and typecode in (&enctyp_code_eligibility.) then do;	/* if not accepted */ /* B-166920 */	  
			edit_code = '99942' ;
			encapdfld =  'Member Not Eligibile Effective Span' ;
			edit_chk_type = 'Member Not Eligibile Effective Span';
			Recsubind = 'N' ;
			output &RGA191..eligibility_reject;
		 end;
		else output &RGA191..svclines_&compno.;	
	run;
%end;
%mend;
%eligibility;


/* B-105917 start Modifier control */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Modifier_control.xlsx"
		out=Modifier_control dbms=xlsx replace;
Run;

Proc sort data=Modifier_control nodupkey; by clamno lineno; run;
Proc sort data=&RGA191..svclines_&compno.;by clamno lineno ;run;

Data &RGA191..svclines_&compno.(drop=modifier1-modifier4);
merge &RGA191..svclines_&compno.(in=_a) Modifier_control(in=_b);
by clamno lineno;
if _a;
if _a and _b   then do;
  if typecode='P' then do;
	modcod=modifier1;	
	modcd2=modifier2;	
	modcd3=modifier3;
	modcd4=modifier4;
  end;
  else do; 
	cptmd1=modifier1;
	cptmd2=modifier2;
	cptmd3=modifier3;
	cptmd4=modifier4;
  end;
end;
run;  

/* B-105917 end */


	/* CR# CHG0040549 - added Inovalon delete reject logic     ----> start  	*/
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Inovalon_delete_Control_File.xlsx"
			out=Inovalon_delete_Control_File dbms=xlsx replace;
	Run;
	Proc sort data=	Inovalon_delete_Control_File nodupkey;by clamno ;run;
	Proc sort data=	&RGA191..svclines_&compno. ;by clamno ;run;
	Data &RGA191..svclines_&compno. (Drop = edit_chk_type edit_code encapdfld  Recsubind)  
		 &RGA191..Inovalon_delete_reject(drop= claimfrq icn);
		 format lineno $05. claimfrq $01. icn $22.; /*CR# CHG0040549 - added, B-136184 icn from 20 to 22 */
		length edit_chk_type $100. edit_code $5. encapdfld $50.;
		Merge &RGA191..svclines_&compno.(in=_a) Inovalon_delete_Control_File(in=_b);
		by clamno ;
		if _a and _b  and missing(icn) eq 1 and "&edit_type_Inovalon_delete_rej." eq "H" and typecode in (&enctyp_code_Inovalon_delete_rej.) then do; /* B-166920 */
			edit_code = '99948' ;
			encapdfld =  'Inovalon_delete_reject' ;
			edit_chk_type = 'Inovalon_delete_reject';
			Recsubind = 'N' ;
			output &RGA191..Inovalon_delete_reject;
		end;
		else if _a and _b  and missing(icn) ne 1 and claimfrq eq '7' and "&edit_type_Inovalon_delete_rej." eq "H" and typecode in (&enctyp_code_Inovalon_delete_rej.) then do; /* B-166920 */
			edit_code = '99948' ;
			encapdfld =  'Inovalon_delete_reject' ;
			edit_chk_type = 'Inovalon_delete_reject';
			Recsubind = 'N' ; 
			output &RGA191..Inovalon_delete_reject;
		end;
		else if _a then output &RGA191..svclines_&compno.;
	run;	
		/* CR# CHG0040549 - added Inovalon delete reject logic     ----> end  	*/
		/*CR# CHG0033881 - added logic to map member number from control file */
Proc sort data=&RGA191..svclines_&compno.;by /*membno*/ clamno;run;/*CR# CHG0038705 */

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Member_Xwalk.xlsx"
		out=Member_details dbms=xlsx replace;
		run;
			
/*CR# CHG0035401 -added proc sort statement */
proc sort data  = Member_details(rename =(membno=membno_upd)) nodupkey;
by /*membno*/ clamno;/*CR# CHG0038705 */
run;
Data &RGA191..svclines_&compno.(drop=membno_upd );
Merge &RGA191..svclines_&compno.(in=_a) Member_details(in=_b keep = clamno membno_upd);
by /*membno*/ clamno;/*CR# CHG0038705 */
if _a;
if _a and _b then membno=membno_upd;


if valcd01='24' and valA01=0 then valcd01='';  /*  CHG0038083   */
if valcd02='24' and valA02=0 then valcd02='';  /*  CHG0038083   */
if valcd03='24' and valA03=0 then valcd03='';  /*  CHG0038083   */
if valcd04='24' and valA04=0 then valcd04='';  /*  CHG0038083   */
if valcd05='24' and valA05=0 then valcd05='';  /*  CHG0038083   */
if valcd06='24' and valA06=0 then valcd06='';  /*  CHG0038083   */
if valcd07='24' and valA07=0 then valcd07='';  /*  CHG0038083   */
if valcd08='24' and valA08=0 then valcd08='';  /*  CHG0038083   */
if valcd09='24' and valA09=0 then valcd09='';  /*  CHG0038083   */
if valcd10='24' and valA10=0 then valcd10='';  /*  CHG0038083   */
if valcd11='24' and valA11=0 then valcd11='';  /*  CHG0038083   */
if valcd12='24' and valA12=0 then valcd12='';  /*  CHG0038083   */
run;

/* B-97137 outpatient_rate_cd macro*/
%macro outpatient_rate_cd;	
	Data &RGA191..svclines_&compno.;
	 set &RGA191..svclines_&compno.;
	 if typecode = "I" then do;	

	 %let i = 1 ;
	 %do %while(&i. <= &OP_Rate_code_total.); 
         if valcd01 in(&&valcd01OP&i.) and valA01 in(&&vala01OP&i.) then do; valcd01=''; valA01=.; end;
		 if valcd02 in(&&valcd02OP&i.) and valA02 in(&&vala02OP&i.) then do; valcd02=''; valA02=.; end;
		 if valcd03 in(&&valcd03OP&i.) and valA03 in(&&vala03OP&i.) then do; valcd03=''; valA03=.; end;
		 if valcd04 in(&&valcd04OP&i.) and valA04 in(&&vala04OP&i.) then do; valcd04=''; valA04=.; end;
		 if valcd05 in(&&valcd05OP&i.) and valA05 in(&&vala05OP&i.) then do; valcd05=''; valA05=.; end;
		 if valcd06 in(&&valcd06OP&i.) and valA06 in(&&vala06OP&i.) then do; valcd06=''; valA06=.; end;
		 if valcd07 in(&&valcd07OP&i.) and valA07 in(&&vala07OP&i.) then do; valcd07=''; valA07=.; end;
		 if valcd08 in(&&valcd08OP&i.) and valA08 in(&&vala08OP&i.) then do; valcd08=''; valA08=.; end;
		 if valcd09 in(&&valcd09OP&i.) and valA09 in(&&vala09OP&i.) then do; valcd09=''; valA09=.; end;
		 if valcd10 in(&&valcd10OP&i.) and valA10 in(&&vala10OP&i.) then do; valcd10=''; valA10=.; end;
		 if valcd11 in(&&valcd11OP&i.) and valA11 in(&&vala11OP&i.) then do; valcd11=''; valA11=.; end;
		 if valcd12 in(&&valcd12OP&i.) and valA12 in(&&vala12OP&i.) then do; valcd12=''; valA12=.; end;

		%let i = %eval(&i. + 1);
	%end;
    %let i = 1 ;

	end;
	run;

	%mend;
%outpatient_rate_cd;

		/*CR# CHG0035563 - added logic to assign blank admitting diagnosis code for claims present in the control file */
Proc sort data=&RGA191..svclines_&compno.;by clamno;run;


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/ADMTDIAG_Blankout.xlsx"
		out=ADMTDIAG_Blankout dbms=xlsx replace;
		run;
proc sort data  = ADMTDIAG_Blankout nodupkey;
by clamno;
run;
Data &RGA191..svclines_&compno.(drop=ADMTDIAG_upd);
Merge &RGA191..svclines_&compno.(in=_a) ADMTDIAG_Blankout(in=_b);
by clamno;
if _a;
if _a and _b then ADMTDIAG=ADMTDIAG_upd;
run;
/* HS# 565133 added revenue code, encounter source code mapping logic---> start */
proc sql;
    create table &RGA191..svclines_T_&compno. as
    select s.*, x.encounter_svccod,y.rev_cod
    from &RGA191..svclines_&compno. s left join SVCTYP_HG_Transportation_XWALK x
    on (s.clamno = x.clamno) and (s.lineno=x.lineno) left join revcod_analysis y
    on (s.clamno = y.clamno) and (s.lineno=y.lineno) ;
quit;
data &RGA191..svclines_T1_&compno.(drop=rev_cod  encounter_svccod) ; 
	set &RGA191..svclines_T_&compno. ;
     if revenue_cd eq ''  then do ;
        revenue_cd=rev_cod ;
     end ;
	 if encounter_svccod ne ''  then do ;
	 	Encounter_svccod_f=1;
        svccod=encounter_svccod ;
     end ;
	 /* CR# CHG0035401 - Hardcode service code as A0100 for HG Trans Claims */
	 /* SAS2AWS2: ReplacedFunctionSubstr */
	 if ksubstr(svccod,1,2) eq 'NY' then do;
          svccod = 'A0100'; 
		  Encounter_svccod_f=1;
     end;
	  
run ;

/* HS# 565133 added revenue code, encounter source code mapping logic---> end */

/* HS# 565133 Assigned null values for value code ---> start */

proc sort data = NUBC_VALUE_REJECT nodupkey;
by clamno;
run;
proc sort data = &RGA191..svclines_T1_&compno.;
by clamno;
run;

data &RGA191..svclines_val_&compno.;
merge &RGA191..svclines_T1_&compno.(in=a) NUBC_VALUE_REJECT(in=b);
by clamno;
if a;
if a and b then do;
valcd01 = '';
valcd02 = '';
valcd03 = '';
valcd04 = '';
valcd05 = '';
valcd06 = '';
valcd07 = '';
valcd08 = '';
valcd09 = '';
valcd10 = '';
valcd11 = '';
valcd12= '';
valA01 = .; 
valA02 = .; 
valA03 = .; 
valA04 = .; 
valA05 = .; 
valA06 = .; 
valA07 = .; 
valA08 = .; 
valA09 = .; 
valA10 = .; 
valA11 = .;  
valA12 = .; 
end;
run;
 
/* CR# CHG0034810 - added logic to identify the rate code having less han 4 characters when value code = 24 ---> start */
data &RGA191..svclines_val_&compno.;
set &RGA191..svclines_val_&compno.;
array value_amt{12}    $      valA01 valA02 valA03 valA04 valA05  valA06 valA07 
                                            valA08 valA09 valA10 valA11  valA12; 
array value_amt_temp{12}    $      valA01_01 valA02_01 valA03_01 valA04_01 valA05_01  valA06_01 valA07_01 
                                            valA08_01 valA09_01 valA10_01 valA11_01  valA12_01; 
do i = 1 to 12;
	if value_amt{i} gt 0 then do; /*[B-315447] - DS - Updates*/ 
	      /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionSubstr */
	      if ksubstr(kscan(kcompress(put(value_amt{i},13.2)),2,'.'),1,1)='0' then 
	              /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan */
	              value_amt_temp{i}=kscan(kcompress(put(value_amt{i},13.2)),1,'.');
	      /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionSubstr */
	      else if ksubstr(kscan(kcompress(put(value_amt{i},13.2)),2,'.'),2,1)='0' then
	             /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionSubstr */
	             value_amt_temp{i}=kcompress(kscan(kcompress(put(value_amt{i},13.2)),1,'.')||(ksubstr(kscan(kcompress(put(value_amt{i},13.2)),2,'.'),1,1)));
	      /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan */
	      else  value_amt_temp{i}=kcompress(kscan(kcompress(put(value_amt{i},13.2)),1,'.')||kcompress(kscan(kcompress(put(value_amt{i},13.2)),2,'.')));
	end;
	else do;
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan */
		value_amt_temp{i}= kscan(kcompress(put(value_amt{i},13.2)),1,'.');
	end;
end;
/* SAS2AWS2: ReplacedFunctionLength */
if valcd01='24' and klength(valA01_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd02='24' and klength(valA02_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd03='24' and klength(valA03_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd04='24' and klength(valA04_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd05='24' and klength(valA05_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd06='24' and klength(valA06_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd07='24' and klength(valA07_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd08='24' and klength(valA08_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd09='24' and klength(valA09_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd10='24' and klength(valA10_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd11='24' and klength(valA11_01) lt 4 then val_rej=1;
      /* SAS2AWS2: ReplacedFunctionLength */
      else if valcd12='24' and klength(valA12_01) lt 4 then val_rej=1;
drop i valA01_01 valA02_01 valA03_01 valA04_01 valA05_01  valA06_01 valA07_01 
       valA08_01 valA09_01 valA10_01 valA11_01  valA12_01 ;
run; 
/* CR# CHG0034810 - added logic to identify the rate code having less han 4 characters when value code = 24 ---> end */
/* HS# 565133 Assigned null values for value code ---> end */

/* CR CHG0036424 - update value code and value amount from NAMI control table for matching claims ---> start */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NAMI_Control_Table.xlsx"
	out=NAMI_Control_Table dbms=xlsx
	replace;
Run;
proc sort data = &RGA191..svclines_val_&compno. out = &RGA191..svclines_val_nami_&compno. ;
by clamno;
run;

proc sort data = NAMI_Control_Table(keep=clamno Value_Code Value_Amount)  nodupkey;
by clamno;
run;

data &RGA191..svclines_val_&compno.;
merge &RGA191..svclines_val_nami_&compno.(in = a)   NAMI_Control_Table(in = b keep=clamno Value_Code Value_Amount);
by clamno;
if a;
if a and b then do;
	if missing(valcd12) =1   then do; 
		valcd12 = Value_Code;
		valA12 = Value_Amount;
	end;
end;
drop Value_Code Value_Amount;
run;

/* CR CHG0036424 - update value code and value amunt from NAMI control table for matching claims ---> end */
/*CR# CHG0039334 - added logic to reject entire claim if 'DG' service line service code length is less than 5 --> start */
%macro Reject_DGSVCCOD;
data _null_;
set APD_enceditmatrix(where = (editcode='99950'));
call symput('Othcheckflag',Othcheckflag);
run;
%PUT &Othcheckflag.;

%if &Othcheckflag. =Y %then %do;
	data   &RGA191..temp_Reject_DGSVCCOD;
		set &RGA191..svclines_val_&compno.;
/*		%If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;*/ /* CR# CHG0040XXX - added company 34 condition */
			/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
			if svctyp = 'DG' and klength(kcompress(svccod)) < 4
			 and "&edit_type_DG_SVCCOD." eq "H" and typecode in (&enctyp_code_DG_SVCCOD.); /* B-166920 */
/*		%end;*/
/*		%else %do;*/
/*			if svctyp = 'DG' and length(compress(svccod)) < 5 ;*/
/*		%end;*/ /* B-110462 commented */
		
	run;
	proc sort data = &RGA191..svclines_val_&compno.;
		by clamno;
	run;
	proc sort data = &RGA191..temp_Reject_DGSVCCOD nodupkey;
		by clamno;
	run;
	Data Reject_DGSVCCOD;
		merge  &RGA191..svclines_val_&compno.(in=_a keep=clamno lineno typecode)  &RGA191..temp_Reject_DGSVCCOD(in=_b keep=clamno);
		if _a and  _b;
		by clamno;
		if _a and _b then do; 
			edit_code = '99950' ;
			encapdfld =  'DG_SVCCOD' ;
			edit_chk_type = 'DG service type service code length < 5';
			Recsubind = 'N' ; 
		end;
	Run;
	Data   &RGA191..svclines_val_&compno.;
		merge  &RGA191..svclines_val_&compno.(in=_a)  &RGA191..temp_Reject_DGSVCCOD(in=_b keep=clamno);
		if _a and not _b;
		by clamno;
	Run;
%end;
%else %do;

%end;
%mend Reject_DGSVCCOD;
%Reject_DGSVCCOD;
/*CR# CHG0039334 - added logic to reject entire claim if 'DG' service line service code length is less than 5  --> end */
/* CR# CHG0036203 - added inpatient DG line reject logic before all roll up logic ---> start */
%macro dgline;
data &RGA191..svclines_val_&compno.;
set &RGA191..svclines_val_&compno.;
  %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
	CAS15=0;
/*	if claamt  <  mcdnetalwamt then Claamt=mcdnetalwamt;*/
/*	else if claamt < mcrnetalwamt then claamt=mcrnetalwamt;*/ /*B-199802*/

	if (claamt  > mcdnetalwamt)  then do;
	   CAS12=sum(claamt,-sum(mcdnetalwamt,mcrnetalwamt)); 
	   CAS12=round(CAS12,.01);
	end;
	else do;
	   CAS12=0;
	end;
  %end;
  %else %do;
	CAS15=sum(nonamt,rcvamt);
	CAS15=round(CAS15,.01);
/*    if claamt  <=  netalwamt then Claamt=NETALWAMT;*/ /*B-199802*/
	if (claamt  > NETALWAMT)  then do;
	   CAS12=sum(claamt,-sum(NETALWAMT,dedamt,coiamt,COPAMT,CAS15,dscamt)); 
	   CAS12=round(CAS12,.01);
	end;
	else do;
	   CAS12=0;
	end;
  %end;
run;
%mend dgline;
%dgline;
/*CR# CHG0038705 - Added logic to skip hold code reject logic if any claim having  'DG' service type  ---> start */
proc sort data =&RGA191..svclines_val_&compno.(where = (svctyp = 'DG')) out= &RGA191..CLAIMS_dg_&compno. nodupkey;
by clamno;
run;
proc sort data =&RGA191..svclines_val_&compno. ;
by clamno;
run;
Data &RGA191..svclines_val_&compno.;
merge &RGA191..svclines_val_&compno.(in =a) &RGA191..CLAIMS_dg_&compno.(in=b keep =clamno);
by clamno;
if a;
if a and b then do;
hold_reject_flag1 = 'Y' ;
end;
run;
/*CR# CHG0038705 - Added logic to skip hold code reject logic if any claim having  'DG' service type  ---> end */
proc sort data = &RGA191..svclines_val_&compno.;
by clamno descending lineno;
run;
Data &RGA191..svclines_val_&compno.(Drop = edit_chk_type edit_code encapdfld  Recsubind)											
	&RGA191..Reject_DG_I_&Compno._&run_id.(keep = clamno lineno  edit_chk_type encapdfld  Recsubind typecode icn claimfrq  edit_code);	
	length edit_chk_type $100. edit_code $5. encapdfld $50.;
	set &RGA191..svclines_val_&compno. 
	;
	by clamno descending lineno;
	retain 	
	 	CLAAMT_dg_line 0
		NETALWAMT_dg_line 0
		ALWAMT_dg_line 0
		dedamt_dg_line 0
		coiamt_dg_line 0
		COPAMT_dg_line 0
		CAS12_dg_line 0
		CAS15_dg_line 0
		dscamt_dg_line 0
		mcdnetalwamt_dg_line 0
		mcrnetalwamt_dg_line 0
		mcdnetalwamt_old_dg_line 0 /* ALM# 5160 - added */
		mcrnetalwamt_old_dg_line 0 /* ALM# 5160 - added */
		NETALWAMT_old_dg_line 0 /* ALM# 5160 - added */
		unitct_dg_line 0 /* ALM# 5160 - added */
		covunt_dg_line 0 /* ALM# 5160 - added */
		alwunt_dg_line 0 /* ALM# 5160 - added */
		mcrcovunt_dg_line 0 /* ALM# 5160 - added */
		svccod_dg_line  '      '
		Inp_dg_line_flag 'N'
		mcdcovunt_dg_line 0 /*B-222679 added*/
		;	
			
	if first.clamno then 
	do;
		CLAAMT_dg_line=0;
		NETALWAMT_dg_line=0;
		ALWAMT_dg_line=0;
		dedamt_dg_line=0;
		coiamt_dg_line=0;
		COPAMT_dg_line=0;
		CAS12_dg_line=0;
		CAS15_dg_line=0;
		dscamt_dg_line=0;		
		mcdnetalwamt_dg_line=0;
		mcrnetalwamt_dg_line=0;	
		mcdnetalwamt_old_dg_line=0; /* ALM# 5160 - added */
		mcrnetalwamt_old_dg_line=0; /* ALM# 5160 - added */
		NETALWAMT_old_dg_line=0; /* ALM# 5160 - added */
		unitct_dg_line=0; /* ALM# 5160 - added */
		covunt_dg_line=0; /* ALM# 5160 - added */
		alwunt_dg_line=0; /* ALM# 5160 - added */
		mcrcovunt_dg_line=0; /* ALM# 5160 - added */
		svccod_dg_line = '      ';
    	Inp_dg_line_flag = "N" ;
    	mcdcovunt_dg_line =0;  /*B-222679 added*/
   end;
		
		if  typecode = "I" and svcstat = "PA" and svctyp = 'DG' then /* HS 569650 kv - added svctyp */
		do;
	
		CLAAMT_dg_line+CLAAMT;
		NETALWAMT_dg_line+NETALWAMT;
		ALWAMT_dg_line+ALWAMT;
		dedamt_dg_line+dedamt;
		coiamt_dg_line+coiamt;
		COPAMT_dg_line+COPAMT;
		CAS12_dg_line+CAS12;
		CAS15_dg_line+CAS15;
		dscamt_dg_line+dscamt;	
		mcdnetalwamt_dg_line+mcdnetalwamt;
	    mcrnetalwamt_dg_line+mcrnetalwamt;	
		mcdnetalwamt_old_dg_line+mcdnetalwamt_old;/* ALM# 5160 - added */
	    mcrnetalwamt_old_dg_line+mcrnetalwamt_old;/* ALM# 5160 - added */
		NETALWAMT_old_dg_line+NETALWAMT_old; /* ALM# 5160 - added */
		unitct_dg_line+unitct; /* ALM# 5160 - added */
		covunt_dg_line+covunt; /* ALM# 5160 - added */
		alwunt_dg_line+alwunt; /* ALM# 5160 - added */
		mcrcovunt_dg_line+mcrcovunt; /* ALM# 5160 - added */	
		mcdcovunt_dg_line+mcdcovunt; /*B-222679 added*/
		end;
	
		if svctyp = 'DG' and typecode = "I" /*and svcstat = "PA"*/ /* HS 569650 kv - added svcstat, HS 565133 kv - commented svcstat */
			and "&edit_type_EIPDG_rej." eq "H" and typecode in (&enctyp_code_EIPDG_rej.) then 	/* B-168796 */
		do;
			svccod_dg_line = svccod ;
			Inp_dg_line_flag = "Y";   
			Inp_dg_line_cnt + 1 ;			
			edit_chk_type = 'Inpatient DG Line Reject';
			edit_code = 'EIPDG';
			encapdfld = 'Inpatient_DG_Line_Reject';
			Recsubind = 'N';
			output &RGA191..Reject_DG_I_&Compno._&run_id. ;			
		delete;
		end;
		
		/* SAS2AWS2: ReplacedFunctionCompress */
		call symput('Inp_dg_line_cnt',kcompress(Inp_dg_line_cnt));
		output &RGA191..svclines_val_&compno. ;	
	run;
/* CR# CHG0036203 - added inpatient DG line reject logic before all roll up logic ---> end */
/* HS 565133 KV - Roll up logic for HG claims --->  start */

/* B-168796 start */
proc sql;
create table hg_svccod as
select distinct a.clamno, a.lineno 
from &RGA191..svclines_val_&compno. a, Pas_code b
where a.svctyp='HG' and a.svccod=b.mhs_service_code;

create table cpt_claamt as
select a.clamno, a.lineno as rollup_lineno, a.claamt
from &RGA191..svclines_val_&compno. a, Pas_code b 
where a.cptcod=b.CPT_Code;

create table cpt_claamt_max as
select distinct clamno, rollup_lineno label='rollup_lineno'
from cpt_claamt 
group by clamno
having claamt=max(claamt);
quit;

/*B-248992 HG Claims remove duplicate Rollup line */
proc sort data=cpt_claamt_max; by clamno rollup_lineno;run;
proc sort data=cpt_claamt_max nodupkey; by clamno ;run;

proc sql;
create table hg_pas as
select distinct a.clamno, a.lineno, b.rollup_lineno
from hg_svccod a left join cpt_claamt_max b
on a.clamno=b.clamno;
quit;
/*B-168796 end */

Data SVCTYP_HG_XWALK;
	Set SVCTYP_HG_XWALK hg_pas;	/* B-168796 added hg_pas */
	Where Rollup_lineno ne "";
run;

Proc sort data=SVCTYP_HG_XWALK nodupkey;by clamno lineno rollup_lineno;run; 	/* B-168796 added */
*Rsubmit;
proc sql;
create table &RGA191..svclines_&compno. as select a.*,b.Rollup_lineno
from &RGA191..svclines_val_&compno. a left join SVCTYP_HG_XWALK b
on a.clamno =b.clamno and a.lineno=b.lineno;
quit;


%macro hg_rollup;	 /*B-274371 Added rollup logic */

data HG_ROLLUP_CLAIMS(keep = clamno lineno Rollup_lineno);
set &RGA191..svclines_&compno.;
where Rollup_lineno ne '';
run;

proc sort data=HG_ROLLUP_CLAIMS;by clamno rollup_lineno ;run;

Proc transpose data=HG_ROLLUP_CLAIMS out=HG_ROLLUP_CLAIMS_1( rename = (rollup_lineno =lineno) drop= _name_ _label_)  prefix=rollup;
by clamno rollup_lineno;
var lineno  ;
run;

data HG_ROLLUP_CLAIMS_2;
set HG_ROLLUP_CLAIMS_1;
where clamno ne'';
run;

%nobs(HG_ROLLUP_CLAIMS_2);
 %if &nobs > 0 %then %do;

%let fid = %sysfunc(open(HG_ROLLUP_CLAIMS_2,i));
%let cnt = %eval(%sysfunc(attrn(&fid.,nvar))-2);
%put &cnt;
%LET rc = %SYSFUNC(CLOSE(&fid.));	

data HG_ROLLUP_CLAIMS_3(keep=clamno lineno rollup_line1);
	      set HG_ROLLUP_CLAIMS_2;
	      length rollup_line1 $1000.;
	      rollup_line1  = catx(',',of rollup1-rollup&cnt.);
	run;
%end;
 %else %do;
  data HG_ROLLUP_CLAIMS_3; 
  	set HG_ROLLUP_CLAIMS_2;
  	length rollup_line1 $1000.;
  	rollup_line1='     ';
  Run; 
 %end;
%mend;
%hg_rollup;

/* VVKR Code Start -- B-75041 */

proc sql noprint;
	create table &RGA191..cas_arp_1_svclines as 
		select a.*,b.apsvcstat,b.revind from (select clamno,lineno,rollup_lineno,typecode,capind from &RGA191..svclines_&compno. where rollup_lineno ne '') as a
		left join &RGA191..cas_svclines b on a.clamno eq b.clamno and a.lineno eq b.lineno; 
	create table &RGA191..cas_arp_2_svcholds as
		select a.*,b.holdcd,b.typcod from &RGA191..cas_arp_1_svclines a left join (select clamno,lineno,holdcd,typcod from svcholds where typcod eq 'E') as b on a.clamno eq b.clamno and a.lineno eq b.lineno order by clamno,Rollup_lineno ; 
	create table &RGA191..cas_arp_3_unique as select distinct clamno,Rollup_lineno,apsvcstat,revind,typcod from &RGA191..cas_arp_2_svcholds 
	where (apsvcstat ne 'PD') or (revind ne 'N') or (typcod ne 'E') order by clamno,Rollup_lineno;
quit;

data &RGA191..cas_arp_4_rp_cas_claims &RGA191..cas_arp_5_rp_no_cas_claims ; /* Select observations from cas_arp_02 dataset which are not in cas_arp_03 dataset goes to cas_arp_04 elase cas_arp_05 */
	merge &RGA191..cas_arp_2_svcholds(in=a) &RGA191..cas_arp_3_unique(in=b keep=clamno Rollup_lineno);
	by clamno Rollup_lineno;
	if a eq 1 and b eq 0 then output &RGA191..cas_arp_4_rp_cas_claims;
	else output &RGA191..cas_arp_5_rp_no_cas_claims;
run;

data &RGA191..cas_arp_6_rp_fst_claim; /* Select first clamno and lineno for rollup claims to derive CAS Segment */
	set &RGA191..cas_arp_4_rp_cas_claims;
	by clamno Rollup_lineno;
	if first.clamno and first.Rollup_lineno;
run;

data &RGA191..cas_arp_7_rp_no_fst_claim; /* Select first clamno and lineno for rollup claims to not derive CAS Segment */
	set &RGA191..cas_arp_5_rp_no_cas_claims;
	by clamno Rollup_lineno;
	if first.clamno and first.Rollup_lineno;
run;

proc sort data=&RGA191..cas_arp_6_rp_fst_claim(rename=(lineno=lineno1 Rollup_lineno=Rollup_lineno1)) 
	out=&RGA191..cas_arp_8_rp_fst_claim_srt(rename=(Rollup_lineno1=lineno lineno1=Rollup_lineno));
	by clamno Rollup_lineno1;
run;

proc sort data=&RGA191..cas_arp_7_rp_no_fst_claim(rename=(lineno=lineno1 Rollup_lineno=Rollup_lineno1)) 
	out=&RGA191..cas_arp_9_rp_no_fst_claim_srt(rename=(Rollup_lineno1=lineno lineno1=Rollup_lineno));
	by clamno Rollup_lineno1;
run;

/* VVKR Code End -- B-75041 */

data &RGA191..svclines_&compno.;
set &RGA191..svclines_&compno.;
if lineno ne rollup_lineno and rollup_lineno ne '' and 
"&edit_type_Rollup_Rej_svctyp_HG." eq "H" and typecode in (&enctyp_code_Rollup_Rej_svctyp_HG.) /* B-166920 */
then  do;
lineno = rollup_lineno;
end;
run;

Proc summary data=&RGA191..svclines_&compno. nway missing;
class clamno lineno;
    var CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt  mcdnetalwamt mcrnetalwamt NETALWAMT_old unitct covunt alwunt mcrcovunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr; /* ALM# added  NETALWAMT_old to mcrnetalwamt_old*/
output out=&RGA191..svclines_HG_&compno. sum()= ; 
run;

proc sort data = &RGA191..svclines_val_&compno.;
by clamno lineno;
run;
proc sort data = &RGA191..svclines_HG_&compno.;
by clamno lineno;
run;
Data Rollup_claims_HG;
	merge &RGA191..svclines_val_&compno.(in=_a drop=CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt  mcdnetalwamt mcrnetalwamt NETALWAMT_old
	unitct covunt alwunt mcrcovunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr) /* ALM# 5160 - added  NETALWAMT_old unitct covunt alwunt mcrcovunt mcdnetalwamt_old mcrnetalwamt_old*/
	&RGA191..svclines_HG_&compno.(in=_b);
	if _a and not _b;
	by clamno lineno;
run;
Data &RGA191..svclines_&compno.;
 	Merge &RGA191..svclines_val_&compno.(in=_a drop=CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt  mcdnetalwamt mcrnetalwamt NETALWAMT_old
	unitct covunt alwunt mcrcovunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr)  /* ALM# 5160 - added  NETALWAMT_old unitct covunt alwunt mcrcovunt mcdnetalwamt_old mcrnetalwamt_old */
	      &&RGA191..svclines_HG_&compno.(in=_b );
    if _a and _b;
	by clamno lineno;
Run;

Data Rollup_claims_HG;
length edit_chk_type $100. edit_code $5. encapdfld $50.;
set Rollup_claims_HG;
		edit_chk_type='Rollup_Reject for service type HG claims';
		encapdfld='Rollup_Reject for service type HG claims';
		Recsubind='N';
		edit_code = '99964'   ;                    
run;
Proc sort data=Rollup_claims_HG nodupkey;by clamno lineno;run;
/* HS 565133 KV - Roll up logic for HG claims  --->   end */


/* CR# CHG0036579 - added logic to skip hold code rejection for claims available in HOLDCODE_REJECT control file ---> start */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Holdcode_Rejectlogic_Bypass.xlsx"
      out=Holdcode_reject dbms=xlsx
      replace;
Run;
proc sort data = Holdcode_reject nodupkey;
by clamno lineno;
run;
proc sort data = &RGA191..svclines_&compno. ;
by clamno lineno;
run;

data &RGA191..svclines_&compno. ;
merge &RGA191..svclines_&compno.(in = a)  Holdcode_reject(in =b);
by clamno lineno;
if a ;
if a and b then do;
hold_reject_flag = 'Y' ;
end;
run;
/* CR# CHG0036579 - added logic to skip hold code rejection for claims available in HOLDCODE_REJECT control file ---> end */

proc sql noprint;
create table &RGA191..svcholds1_&compno._&run_id. as 
	select svc1.clamno,						  
		   svc1.lineno,
		   svc1.svcstat, svc1.apsvcstat,hold_reject_flag1,	 /* CR# CHG0038705  - added apsvcstat hold_reject_flag1*/
		   svchld1.holdcd, hold_reject_flag	 /* CR# CHG0036579  - added hold_reject_flag */
 	from   &RGA191..svclines_&compno. svc1	left join  svcholds  svchld1 
	on 	   svc1.clamno = svchld1.clamno and svc1.lineno = svchld1.lineno
	;
Quit;

Data Holdcd_N5_main(keep=clamno N5_flag);
set &RGA191..svcholds1_&compno._&run_id.;
where holdcd='N5';
N5_flag='Y';
run;	
Data Holdcd_N4_main(keep=clamno N4_flag);
set &RGA191..svcholds1_&compno._&run_id.;
where holdcd='N4';
N4_flag='Y';
run;

Proc sort data=Holdcd_N5_main nodupkey;by clamno;run;
Proc sort data=Holdcd_N4_main nodupkey;by clamno;run;

%macro ccc_O9;						/* B-97137 added O9 macro */
Data Holdcd_O9_main(keep=clamno O9_flag);
set &RGA191..svcholds1_&compno._&run_id.(where=(holdcd='O9'));
O9_flag=' ';
%if &compno.=34 %then %do;
O9_flag='Y';
%end;
run;
Proc sort data=Holdcd_O9_main nodupkey;by clamno;run;
%mend ccc_O9;
%ccc_O9;

Data &RGA191..svcholds1_&compno._&run_id.;
merge &RGA191..svcholds1_&compno._&run_id.(in=_a) Holdcd_N4_main(in=_b) Holdcd_N5_main(in=_c) Holdcd_O9_main(in=_d);
if _a;
by clamno;
run;	

proc sort data=&libn2..Holdcode out=Holdcd_Denial1(keep=holdcd medsflag) nodupkey ;
     by holdcd medsflag ;
run ;

data Holdcd_blank;
holdcd='  ';
medsflag='Y';
run;

data Holdcd_Denial1;
set Holdcd_Denial1 Holdcd_blank;
run;
Proc sort data=&RGA191..svcholds1_&compno._&run_id.;by holdcd;run;
proc sort data=Holdcd_Denial1;by holdcd;run;

/* [B-136184]_[OSDS Stories for August B-133711 B-131960 B-133710 B-139368] start APD holdcode logic */
data &RGA191..svcholds1_&compno._&run_id.;
     merge &RGA191..svcholds1_&compno._&run_id.(in=a) Holdcd_Denial1(in=b);
     by holdcd;
run;

data Admin_Denial_1 exclude_holdcd_1;
set &RGA191..svcholds1_&compno._&run_id.;
/*	 merge &RGA191..svcholds1_&compno._&run_id.(in=a) Holdcd_Denial1(in=b);
     by holdcd;
     if a and b;
    [B-136184]_[OSDS Stories for August B-133711 B-131960 B-133710 B-139368] end APD holdcode logic */
/*   if (holdcd in ('D1','03','32') and N5_flag='Y') then medsflag='Y';	   B-97137 commented */
	 if O9_flag='Y' or N5_flag='Y' then medsflag='Y';					/* B-97137 include all N5 hold codes or CCC O9 hold codes for submission */
	 else if (holdcd not in ('D1') and svcstat = 'PA') then medsflag='Y';/* CR# CHG0035401 - added */
	 else if ( apsvcstat = 'PA') then medsflag='Y';/* CR# CHG0038705 - skip hold code reject logic if apsvcstat='PA'*/
     if medsflag='Y' or hold_reject_flag = 'Y' or hold_reject_flag1='Y' then output Admin_Denial_1;/* CR# CHG0036579  - added hold_reject_flag */
     else output exclude_holdcd_1;
Run;

proc sort data=exclude_holdcd_1 out=exclude_holdcd nodupkey;
    by clamno lineno;
run;
proc sort data=Admin_Denial_1;
    by clamno lineno holdcd;
run;

data Admin_Denial;
    merge Admin_Denial_1(in=in1) exclude_holdcd(in=in2 keep=clamno lineno);
    by clamno lineno ;
    if in1 and not in2;
run;
proc sort data=Admin_Denial(keep=clamno holdcd lineno)
 out=Admin_Denial_A(keep=clamno  lineno) nodupkey;
     by clamno lineno ;
run;

Data &RGA191..svclines2_&compno. &RGA191..encounters_N_IO_P_&compno._&run_id. ;
merge &RGA191..svclines_&compno.(in=_a) Admin_Denial_A(in=_b);
if _a and _b then output &RGA191..svclines2_&compno.;
if _a and not _b then do;
	if "&edit_type_holdcode_rej." eq "H" and typecode in (&enctyp_code_holdcode_rej.) 	/* B-168796 holdcode_rej */
	then  output &RGA191..encounters_N_IO_P_&compno._&run_id.;
	else  output &RGA191..svclines2_&compno.; 
end;
by clamno lineno;
run;
/* HS 542635 - Added Proc delete statement */
Proc delete data=&RGA191..svclines_&compno.;run;

Data &RGA191..encounters_N_IO_P_&compno._&run_id.(keep=Recsubind clamno edit_chk_type  encapdfld lineno typecode svcstat edit_code netalwamt);  /* HS 567501 - added */
length edit_chk_type $100. edit_code $5. encapdfld $50.;
	set &RGA191..encounters_N_IO_P_&compno._&run_id.;
	edit_chk_type='Hold Code Reject';
	encapdfld='Holdcode_Reject';
	Recsubind='N';
	edit_code = '99971'; /* HS 567501 - added */
run;
Proc sort data=&RGA191..encounters_N_IO_P_&compno._&run_id. nodupkey;
	by clamno lineno;
Run;
Proc sort data=exclude_holdcd_1 out=report_D1 nodupkey;
	by clamno lineno;where holdcd in ('D1','03','32');
Run;
proc sql;
Create table hold_rej as
 select holdcd, count(*) as count from report_D1 group by 1;
 quit;
	proc sql;
		select  count(*) into :total_obs_hold  from report_D1 ;
    Quit;
PROC EXPORT DATA= hold_rej	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_B.xls"
		           DBMS=xls REPLACE;sheet='HOLD_REJ';
RUN;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_B.xls &drv72./Encounters_Reporting/APD/Archive/&ddir.  --sse --acl bucket-owner-full-control";
call system(cmd);
run;
 /**********************************************************************************************/
/*                                                                                            */ 
/*                   Merging claims and service lines                                         */ 
/*                                                                                            */ 
/**********************************************************************************************/
Proc Summary data=&RGA191..svclines2_&compno. nway missing; 
		Class  clamno;																				
		Var SVCDAT ENDDAT pidate  ; /*[B-315447] - DS - Updates*/ 
		Output out=&RGA191..svclines_Date_&compno.(Drop=_Freq_ _type_) 
		min(SVCDAT_MCR)=SVCDAT_MCR1 max(ENDDAT_MCR)=ENDDAT_MCR1 max(pidate_MCR)=max_pidate_MCR 		/*B-315473*/
		min(SVCDAT)=SVCDAT1 max(ENDDAT)=ENDDAT1 max(pidate)=max_pidate ;	/*[B-315447] - DS - Updates*/  
Run;
%Macro temp;	
  proc sql;	
	create table &RGA191..svclines2_1_&compno. as 	
		select a.*,b.svccod as Tmp_svccod
		from &RGA191..svclines2_&compno. a left join &libn2..Medssvccdxwalk b 
		on a.svccod=b.mhssvccod  and a.bencod=b.bencod and b.Compno = "&compno."  and a.lobcod=b.lobcod;		
	quit;

	data &RGA191..svclines2_2_&compno.(drop = tmp_svccod) &RGA191..Reject_svccod;
		set &RGA191..svclines2_1_&compno.;

		if  (SVCTYP='HG' and  missing(tmp_svccod)=0) then do; /* HS 565133 - modified HG svccod rejection condition */
			Svccod=tmp_svccod;
			output &RGA191..svclines2_2_&compno.;
		end;
		else if  Encounter_svccod_f = 1 then do;
			output &RGA191..svclines2_2_&compno.;
		end;
		else if SVCTYP='HG' and  missing(tmp_svccod)=1 and "&edit_type_SVCCOD_Reject_HG." eq "H" and typecode in (&enctyp_code_SVCCOD_Reject_HG.) then do; /* B-166920 */
		   output &RGA191..Reject_svccod;
		end;
		else do;
			output &RGA191..svclines2_2_&compno.;
		end;

	
	run;
	/* HS 542635 - Added Proc deletestatement */

	Proc delete data=&RGA191..svclines2_1_&compno.;run;

%mend;
	%temp;
	Data &RGA191..Reject_svccod1(keep=Recsubind clamno edit_chk_type  encapdfld lineno typecode claimfrq icn svcstat edit_code netalwamt); /* HS 567501 - added svcstat edit_code */
	length edit_chk_type $100. edit_code $5. encapdfld $50.;
		set &RGA191..Reject_svccod;
		edit_chk_type='SVCCOD Reject for HG';
		encapdfld='SVCCOD_Reject_for_HG';
		Recsubind='N';
		edit_code = '99969'; /* HS 567501 - added */                       
	run;

	proc sort data = &RGA191..svclines2_2_&compno. out = &RGA191..svclines3_&compno.;
		by clamno   ;
	Run;    
	/* HS 542635 - Added Proc deletestatement */
	Proc delete data=&RGA191..svclines2_2_&compno.;run;

	data &RGA191..svclines3_&compno.;
	merge &RGA191..svclines3_&compno.(in=_a) &RGA191..svclines_Date_&compno.(in=_b);
	if _a;
	by clamno;
	run;

	/* HS 542635 - Added Proc deletestatement */
	Proc delete data=&RGA191..svclines_Date_&compno.;run;


/**********************************************************************************************/
/*                                                                                            */ 
/*                  Flag encouters with Hold code indicator                                     */ 
/*                                                                                            */ 
/**********************************************************************************************/
/************************************************************************************************/
/*		Sorting Encounter Dataset 															 	*/
/*			1. encounters_N_II - Entire encounter where  EncFlag is 'N' and Typecode is II		*/
/*			2. encounters_N_IO_P - Svclines where EncFlag is 'N' and Typecode is IO,P			*/
/*			3. encountersY - Encounters passed Hold Code logic									*/
/*			2. Expected values for EncFlag is 'N' and 'Y' only.									*/
/*			3. Expected values for typecode is 'II' , 'IO' and  'P' only 						*/
 /***********************************************************************************************/
 /***********************************************************************************************/  
 /*                                                                                             */
 /*   Include provider demographics                                                             */
 /*                                                                                             */
 /***********************************************************************************************/  

Data &RGA191..providernpi_&compno.;
	set providernpi_&compno.;
run;
proc sort data=&RGA191..providernpi_&compno. 
		  out=&RGA191..providernpi_&compno. nodupkey;
          by npiid ;
run;
proc datasets lib=&RGA191. nolist;
/* SAS2AWS2: CommentedModify */
	     modify providernpi_&compno. ;
         index create npiid /unique ;
Run; 
Data 	&RGA191..provider_&compno.
	/ view = &RGA191..provider_&compno. ;											  
         Set &libn..provider(keep = provno  npiid fednum socsec  effdat	Plstnam  pfstnam padrln1 padrln2 pcitycd pstacod pzipcod  mdcaid );                     
 		  if missing(provno) = 1 then delete;  
		/* CR# CHG0038449 - Added compress function for zip code to keep only digits */
			/* SAS2AWS2: ReplacedFunctionCompress */
			pzipcod=kcompress(pzipcod,'','kd');
			/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranslate */
			pcitycd=compbl(kcompress(ktranslate(pcitycd,' ','-'),"","ka"));		/* B-108297 city alpha */
		  output &RGA191..provider_&compno. ;
     run;
     proc sort data=&RGA191..provider_&compno. out=&RGA191..provider2_&compno. nodupkey;
          by provno effdat ;
     run;
     data &RGA191..provider3_&compno.(rename = (pzipcod1 =pzipcod));
         set &RGA191..provider2_&compno.;
             by provno  ;
			 length mrg_provno $15 ;
			 mrg_provno = provno ;
			 /* SAS2AWS2: ReplacedFunctionCompress */
			 pzipcod1 = kcompress(pzipcod,"@:*`~#-= ");/*CR# CHG0034118 - added equal sign in compress function*/
			 drop pzipcod;
         if last.provno then output &RGA191..provider3_&compno.;
     run;/*
      proc datasets lib=&RGA191. nolist;
	     modify provider3_&compno. ;
         index create mrg_provno /unique ;
     run;
 */
/* HS 542635 - Added Proc delete statement */
	 proc datasets lib= work nolist  ;
	 delete svclines_Date_&compno. provider2_&compno. provider_&compno. ;
	 run;
	/* HS 542635 - Added Proc delete statement */

%macro Member_pull;
	/* B-96285 moved to 501 program
	   %let keepv_members=     membno MLSTNAM MFSTNAM MMIDFULL	BTHDAT SEXCOD 
				        MADRLN1 MADRLN2 MCITYCD MSTACOD MZIPCOD relcod 
						subsno socsec; */	
	      Data &RGA191..members_&compno.;
		  set &libn..members(keep=&keepv_members. mdcasno)   &libn..NULLMEMBER(keep=&keepv_members.) ; 		   
		  /* CR# CHG0038449 - Added compress function for zip code to keep only digits */
			/* SAS2AWS2: ReplacedFunctionCompress */
			MZIPCOD=kcompress(MZIPCOD,'','kd');
			/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranslate */
			MCITYCD=compbl(kcompress(ktranslate(MCITYCD,' ','-'),"","ka"));		/* B-108297 city alpha */
			/* [B-222732] - [SD] - Additional SSN Blank out conditions added */ 
			socsec     =      kcompress(socsec,"@:*`~#-= ") 	;
			if "&edit_type_MEM_socsec_rej." ne "H" then do;
							if ksubstr((ktrim(socsec)),1,1) = '9' then socsec = '                  ';			
              if klength((ktrim(socsec))) < 9     then  socsec = '                  ';
              if socsec in (&socsec_blnk_out.)  then  socsec = '                  ';              
              if ksubstr(socsec,1,3) eq '000' or ksubstr(socsec,6,4) eq '0000' or ksubstr(socsec,4,2) eq '00' or ksubstr(socsec,1,3) eq '666' then socsec = '                  ';
           end;
			/* [B-222732] - [SD] - Additional SSN Blank out conditions ends here */
			
			  rename socsec=M_socsec;
		 Run; 	 		    
		
		 
%mend;

%Member_pull;

/* CR# CHG0038947 - logic to Map the Member demographic information from medicare when it is missing in Compare -- start */
%macro compcare_member;
%if &compno.=34  %then %do;
proc sql;

create table comp_blnk_memb as select * from &RGA191..svclines3_&compno. 
where membno not in (select distinct membno from &RGA191..members_&compno.);

quit;
%nobs(comp_blnk_memb);
	%if &nobs gt 0 %then %do;
	
		proc sql;

		create table medicare_memb as select * from medicare.members 
		where membno  in (select distinct membno from comp_blnk_memb);
		create table medicare_nullmemb as select * from medicare.NULLMEMBER 
		where membno  in (select distinct membno from comp_blnk_memb);

		quit;
			      Data &RGA191..members_medicare;
				  set medicare_memb(keep=&keepv_members. mdcasno)   medicare_nullmemb(keep=&keepv_members.) ; 
				   
				  /* CR# CHG0038449 - Added compress function for zip code to keep only digits */
					/* SAS2AWS2: ReplacedFunctionCompress */
					MZIPCOD=kcompress(MZIPCOD,'','kd');
					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranslate */
					MCITYCD=compbl(kcompress(ktranslate(MCITYCD,' ','-'),"","ka"));		/* B-108297 city alpha */
					/* [B-222732] - [SD] - Additional SSN Blank out conditions added */
						socsec     =      kcompress(socsec,"@:*`~#-= ") 	;				  
					  if "&edit_type_MEM_socsec_rej." ne "H" then do;
										if ksubstr((ktrim(socsec)),1,1) = '9' then socsec = '                  ';			
              			if klength((ktrim(socsec))) < 9     then  socsec = '                  ';
              			if socsec in (&socsec_blnk_out.)  then  socsec = '                  ';              
              			if ksubstr(socsec,1,3) eq '000' or ksubstr(socsec,6,4) eq '0000' or ksubstr(socsec,4,2) eq '00' or ksubstr(socsec,1,3) eq '666' then socsec = '                  ';
           			end;
  
					  rename socsec=M_socsec;
				 Run; 
				 proc append base = &RGA191..members_&compno. data =&RGA191..members_medicare;
				 run;
 	%end;
	%else %do;
	%end;
 %end;
 %mend compcare_member;
 %compcare_member;
 /* CR# CHG0038947 - logic to Map the Member demographic information from COMPCAID when it is missing in Compare -- end */
 
%macro dsnp_member;
 %if &compno.=01 and &LOB. = DSNP %then %do;
 
		Data &RGA191..members_30;
		  set &libdsnp..members(keep=&keepv_members. mdcasno)   &libn..NULLMEMBER(keep=&keepv_members.) ; 		   
			MZIPCOD=kcompress(MZIPCOD,'','kd');
			MCITYCD=compbl(kcompress(ktranslate(MCITYCD,' ','-'),"","ka"));	
			socsec     =      kcompress(socsec,"@:*`~#-= ") 	;
			if "&edit_type_MEM_socsec_rej." ne "H" then do;
							if ksubstr((ktrim(socsec)),1,1) = '9' then socsec = '                  ';			
              if klength((ktrim(socsec))) < 9     then  socsec = '                  ';
              if socsec in (&socsec_blnk_out.)  then  socsec = '                  ';              
              if ksubstr(socsec,1,3) eq '000' or ksubstr(socsec,6,4) eq '0000' or ksubstr(socsec,4,2) eq '00' or ksubstr(socsec,1,3) eq '666' then socsec = '                  ';
            end;
			rename socsec=M_socsec;
		Run; 
		
		proc sort data=&RGA191..members_&compno. nodups; by membno; run;
		proc sort data=&RGA191..members_30 nodups; by membno; run;
		
		Data &RGA191..members_medicare1 &RGA191..members_&compno.;
			set &RGA191..members_&compno.
				&RGA191..members_30
				; by membno;
		Run;
		
		Proc sort data=&RGA191..members_&compno. nodups; by membno; run;
		Proc sort data=&RGA191..members_medicare1 nodups; by membno; run;
		
		%vars(&RGA191..members_medicare1,_mcr,&RGA191..members_medicare2)   ; 
		
	Data 	&RGA191..provider_30 ;											  
         Set &libdsnp..provider(keep = provno  npiid fednum socsec  effdat	Plstnam  pfstnam padrln1 padrln2 pcitycd pstacod pzipcod  mdcaid );                     
 		  if missing(provno) = 1 then delete;  
			pzipcod=kcompress(pzipcod,'','kd');
			pcitycd=compbl(kcompress(ktranslate(pcitycd,' ','-'),"","ka"));	
		  output &RGA191..provider_30 ;
     run;
     proc sort data=&RGA191..provider_30 out=&RGA191..provider2_30 nodupkey;
          by provno effdat ;
     run;
     data &RGA191..provider3_30(rename = (pzipcod1 =pzipcod));
         set &RGA191..provider2_30;
             by provno  ;
			 length mrg_provno $15 ;
			 mrg_provno = provno ;
			 pzipcod1 = kcompress(pzipcod,"@:*`~#-= ");
			 drop pzipcod;
         if last.provno then output &RGA191..provider3_30;
     run;
	 
	 	proc sort data=&RGA191..provider3_&compno. nodups; by provno; run;
		proc sort data=&RGA191..provider3_30 nodups; by provno; run;
		
		Data &RGA191..provider3_&compno.;
			set &RGA191..provider3_&compno.
				&RGA191..provider3_30
				; by provno;
		Run;
	
	 proc sort data=&RGA191..provider3_&compno. nodupkey; by provno; run;

      proc datasets lib=&RGA191. nolist;
	     modify provider3_&compno. ;
         index create mrg_provno /unique ;
     run;
 %end;
 %else %do;
		Data &RGA191..members_medicare1;
		  set &libdsnp..members(keep=&keepv_members. mdcasno obs=0)   &libn..NULLMEMBER(keep=&keepv_members. obs=0) ; 		   
			MZIPCOD='';
			MCITYCD='';	
			M_socsec='';			
		Run; 
		
		%vars(&RGA191..members_medicare1,_mcr,&RGA191..members_medicare2)   ; 
		
	 proc datasets lib=&RGA191. nolist;
	     modify provider3_&compno. ;
         index create mrg_provno /unique ;
     run;
 %end;
%mend dsnp_member;
%dsnp_member;
/* B-96285 added QHP subscriber */
%macro qhp_hfic_subscriber;
%if (&compno.=45 and "&LOB." ne "MPPO") or (&compno. eq 42 and "&LOB." eq "QHP") %then %do;	/* B-121242  */  /*B-248997*/
	 Data sub;
	 set &RGA191..members_&compno.;
	   /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
	   SUB_MLSTNAM  =    	tranwrd(kcompress(MLSTNAM,"@:*`~#"),'Ñ','N') ;
	   /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
	   SUB_MFSTNAM  =     	tranwrd(kcompress(MFSTNAM,"@:*`~#"),'Ñ','N') ;
	   /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
	   SUB_MMIDFULL =     	tranwrd(kcompress(MMIDFULL,"@:*`~#"),'Ñ','N') ;
	   SUB_MEMBNO   =     	MEMBNO      ;
	   SUB_BTHDAT   =     	BTHDAT      ;  
	   SUB_SEXCOD   =     	SEXCOD      ;  
	   SUB_MDCARID 	= 	  	MEMBNO;
	   SUB_MEDCTY 	= 	  	'' ; 
	   /* SAS2AWS2: ReplacedFunctionCompress */
	   SUB_MADRLN1	=		kcompress(MADRLN1,"@:*`~#") 	;
	   /* SAS2AWS2: ReplacedFunctionCompress */
	   SUB_MADRLN2 	=   	kcompress(MADRLN2,"@:*`~#") 	;
	   /* SAS2AWS2: ReplacedFunctionCompress */
	   SUB_MCITYCD 	=   	kcompress(MCITYCD,"@:*`~#") 	;
	   /* SAS2AWS2: ReplacedFunctionCompress */
	   SUB_MSTACOD 	=   	kcompress(MSTACOD,"@:*`~#") 	;
	   /* SAS2AWS2: ReplacedFunctionCompress */
	   SUB_MZIPCOD 	=   	kcompress(MZIPCOD,"@:*`~#-= ") 	;/*CR# CHG0034118 - added equal sign in compress*/
	   SUB_socsec   =		M_socsec;
	   keep Membno SUB_MLSTNAM SUB_MFSTNAM SUB_MMIDFULL SUB_MEMBNO SUB_BTHDAT
	   		SUB_SEXCOD  SUB_MDCARID SUB_MEDCTY SUB_MADRLN1 SUB_MADRLN2 SUB_MCITYCD
			SUB_MSTACOD SUB_MZIPCOD SUB_socsec;
     run;
 %end;
 %mend qhp_hfic_subscriber;
 %qhp_hfic_subscriber;

      proc sort data=&RGA191..members_&compno.  nodupkey;
          by membno ;
      run;
       proc datasets lib=&RGA191. nolist;
/* SAS2AWS2: CommentedModify */
	     modify members_&compno. ;
         index create membno /unique ;
       run;  
	   
      proc sort data=&RGA191..members_medicare2  nodupkey;
          by membno_mcr ;
      run;
	  
	    proc datasets lib=&RGA191. nolist;
	     modify members_medicare2;
         index create membno_mcr /unique ;
		run;
	 
	 
	Data PCPCOD;
	set &libn..Provider(keep=provno npiid);
	rename provno=pcpcod
		   npiid=pcpnpiid;
	Run;
	Proc sort data=PCPCOD nodupkey;by pcpcod;run;
	Proc sort data=&RGA191..svclines3_&compno. out=&RGA191..encounters_T_&compno._&run_id.;by pcpcod;run;	
/* HS 542635 - Added Proc delete statement */
	Proc delete data=&RGA191..svclines3_&compno.;run;
	Data &RGA191..encounters_T_&compno._&run_id.;
		Merge &RGA191..encounters_T_&compno._&run_id.(in=_a) PCPCOD(in=_b);
		if _a;
		by pcpcod;
	run;

%macro log;
data &RGA191..encountersY2_&compno._&run_id.(keep=&keepv_imp. PWK_Segment CLAAMT_dg_line ALWAMT_dg_line NETALWAMT_dg_line dedamt_dg_line  coiamt_dg_line hiosid /* B-146417 add hiosid, B-160659 add PWK */
hix_policy_num_cd hios_plan_id_cd lob_ods hix_person_id_cd hix_member_id_Cd exchange_subscriber_id_cd cin_cd /* B-274606 */
COPAMT_dg_line CAS12_dg_line CAS15_dg_line dscamt_dg_line mcdnetalwamt_dg_line NETALWAMT_old_dg_line unitct_dg_line mcdnetalwamt_old_dg_line mcrnetalwamt_old_dg_line /* ALM# 5160 - Added keep variables */
covunt_dg_line alwunt_dg_line mcrcovunt_dg_line mcrnetalwamt_dg_line  svccod_dg_line  Inp_dg_line_flag Inp_dg_line_cnt subsno relcod mcdcovunt_dg_line
cos prvspcd) /* CR# CHG0036203 - added keep vars,  B-96285 QHP subsno relcod*/ /* B-289556 Added - cos prvspcd*/
		 stat(keep =&keepv_stat.) ;  
				retain 	rec_encY2	0	;
			set	&RGA191..encounters_T_&compno._&run_id. end = eof;

		clm_provno = provno  ;				
		set &RGA191..members_&compno. key = membno /unique ; 
      if _iorc_ =  0 then
		     do;  
			  MEM_HIT + 1;   			 
	          /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
	          MEM_MLSTNAM  	=    	tranwrd(kcompress(MLSTNAM,"@:*`~#"),'Ñ','N') ;
	          /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
	          MEM_MFSTNAM   =     	tranwrd(kcompress(MFSTNAM,"@:*`~#"),'Ñ','N') ;
	          /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
	          MEM_MMIDFULL  =     	tranwrd(kcompress(MMIDFULL,"@:*`~#|"),'Ñ','N') ; /*HS 541126 kv - Added pipe symbol */
			  MEM_MEMBNO   	=     	MEMBNO;
			  MEM_MDCARID 	= 	  	MEMBNO;
			  MEM_subsno 	= 	  	subsno; 				   			
	          MEM_BTHDAT   	=     	BTHDAT      ;  
	          MEM_SEXCOD   	=     	SEXCOD      ;  			 
			  MEM_MEDCTY 	= 	  	'' ; 
			  /* SAS2AWS2: ReplacedFunctionCompress */
			  MEM_MADRLN1	=		kcompress(MADRLN1,"@:*`~#") 	;
			  /* SAS2AWS2: ReplacedFunctionCompress */
			  MEM_MADRLN2 	=   	kcompress(MADRLN2,"@:*`~#") 	;
			  /* SAS2AWS2: ReplacedFunctionCompress */
			  MEM_MCITYCD 	=   	kcompress(MCITYCD,"@:*`~#") 	;
			  /* SAS2AWS2: ReplacedFunctionCompress */
			  MEM_MSTACOD 	=   	kcompress(MSTACOD,"@:*`~#") 	;
			  /* SAS2AWS2: ReplacedFunctionCompress */
			  MEM_MZIPCOD 	=   	kcompress(MZIPCOD,"@:*`~#-= ") 	;	/*CR# CHG0034118 - added equal sign in compress function*/
			  /* SAS2AWS2: ReplacedFunctionCompress */
			  MEM_socsec     =      kcompress(M_socsec,"@:*`~#-= ") 	;  /*CR# CHG0034712 - added compress function*/
			  MEM_Temp      =       mdcasno;
			  dsnp_membno   = membno;
	  end ;	  
      else 
      do;
   			 _error_ 	=  0 ;
		      MEM_NOHIT + 1;
	          MEM_MLSTNAM  	=   ' ' ;	
	          MEM_MFSTNAM   =   ' ' ;	
	          MEM_MMIDFULL  =   ' ' ;	
	          MEM_MEMBNO   	=   ' ' ;	
	          MEM_BTHDAT   	=   .	;	
	          MEM_SEXCOD   	=   ' ' ;	
			  MEM_mcaidno 	= 	' ' ;	
			  MEM_MEDCTY 	= 	' ' ;	
			  MEM_MADRLN1	=	' ' ;	
			  MEM_MADRLN2 	=   ' ' ;	
			  MEM_MCITYCD 	=   ' ' ;	
			  MEM_MSTACOD 	=   ' ' ;	
			  MEM_MZIPCOD 	=   ' ' ;
			  MEM_socsec      = ' ';
			  MEM_Temp ='';
      end;
		set &RGA191..members_medicare2 key = membno_mcr /unique ; 
      if _iorc_ =  0 then
		     do;
	          MEM_MLSTNAM_MCR  	=    	tranwrd(kcompress(MLSTNAM_mcr,"@:*`~#"),'Ñ','N') ;
	          MEM_MFSTNAM_MCR   =     	tranwrd(kcompress(MFSTNAM_mcr,"@:*`~#"),'Ñ','N') ;
	          MEM_MMIDFULL_MCR  =     	tranwrd(kcompress(MMIDFULL_mcr,"@:*`~#|"),'Ñ','N'); 
			  MEM_MEMBNO_MCR   	=     	MEMBNO_mcr;
			  MEM_MDCARID_MCR 	= 	  	MEMBNO_mcr;
			  MEM_subsno_MCR 	= 	  	subsno_mcr; 				   			
	          MEM_BTHDAT_MCR   	=     	BTHDAT_mcr;  
	          MEM_SEXCOD_MCR   	=     	SEXCOD_mcr;  			 
			  MEM_MEDCTY_MCR 	= 	  	'' ; 
			  MEM_MADRLN1_MCR	=		kcompress(MADRLN1_mcr,"@:*`~#") 	;
			  MEM_MADRLN2_MCR 	=   	kcompress(MADRLN2_mcr,"@:*`~#") 	;
			  MEM_MCITYCD_MCR 	=   	kcompress(MCITYCD_mcr,"@:*`~#") 	;
			  MEM_MSTACOD_MCR	=   	kcompress(MSTACOD_mcr,"@:*`~#") 	;
			  MEM_MZIPCOD_MCR 	=   	kcompress(MZIPCOD_mcr,"@:*`~#-= ") 	;
			  MEM_socsec_MCR    =       kcompress(M_socsec_mcr,"@:*`~#-= ") ; 
			  MEM_Temp_MCR      =       mdcasno_mcr;
			  dsnp_membno_mcr   = 		membno_mcr;
	  end ;	  
      else 
      do;
   			 _error_ 	=  0 ;
	          MEM_MLSTNAM_MCR  	=    	'';
	          MEM_MFSTNAM_MCR   =     	'';
	          MEM_MMIDFULL_MCR  =     	''; 
			  MEM_MEMBNO_MCR   	=     	'';
			  MEM_MDCARID_MCR 	= 	  	'';
			  MEM_subsno_MCR 	= 	  	''; 				   			
	          MEM_BTHDAT_MCR   	=     	'';  
	          MEM_SEXCOD_MCR   	=     	'';  			 
			  MEM_MEDCTY_MCR 	= 	  	''; 
			  MEM_MADRLN1_MCR	=		'';
			  MEM_MADRLN2_MCR 	=   	'';
			  MEM_MCITYCD_MCR 	=   	'';
			  MEM_MSTACOD_MCR	=   	'';
			  MEM_MZIPCOD_MCR 	=   	'';
			  MEM_socsec_MCR    =       ''; 
			  MEM_Temp_MCR      =       '';
      end;	  
  		mrg_provno = provno; 
		set &RGA191..provider3_&compno. key = mrg_provno / unique ;                                                                               
			if _iorc_ = 0 then                                                                       
			do;		                                                                                   
			         BI_HIT + 1 ;     
				  	 BI_PROVNO=PROVNO; 
					 BIPRVNPI_Prov 		= 	NPIID     	;
					 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 Plstnam_1 =tranwrd(kcompress(Plstnam,"@:*`~#"),'Ñ','N') ;
					 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 pfstnam_1  =tranwrd(kcompress(pfstnam,"@:*`~#"),'Ñ','N') ;
					 NPIID_1=NPIID;
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 padrln1_1 = kcompress(padrln1,"@:*`~#");
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 padrln2_1 = kcompress(padrln2,"@:*`~#");
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 pcitycd_1=kcompress(pcitycd,"@:*`~#");
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 pstacod_1=kcompress(pstacod,"@:*`~#");
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 pzipcod_1=kcompress(pzipcod,"@:*`~#- "); /* 477378 */
					 FEDNUM_1=FEDNUM;
					 mdcaid_1=mdcaid;					                                                    			                                                                                         
			end;                                                                                     
			else 
			do;	
					 _error_ 	=  0 ;
					 BI_NOHIT + 1;																						                                          
					 BIPRVNPI_Prov 		= 	'' ;     	
					 Plstnam_1 ='' ;
					 pfstnam_1  ='' ;
					 NPIID_1='';
					 padrln1_1 = '';
					 padrln2_1 = '';
					 pcitycd_1='';
					 pstacod_1='';
					 pzipcod_1='';
					 FEDNUM_1 =''; /* B-301333 DS */

					 		            																						                                           
								 
			end; 

		npiid=blpnpi;    
		Set &RGA191..providernpi_&compno. key=npiid /unique;
		if _iorc_ = 0 and  ((missing(Last_Name)=0 or  missing(First_Name)=0) or ( (missing(Last_Name)=1 and  missing(First_Name)=1) and 
											((firstdos gt NPIDEACTDATE and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=1) 
				or (missing(PROVENUMDATE)=1 and missing(NPIDEACTDATE)=1) 
				or (firstdos lt PROVENUMDATE and missing(PROVENUMDATE)=0 and missing(NPIDEACTDATE)=1) 
						or ((firstdos lt PROVENUMDATE or firstdos gt NPIDEACTDATE) and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=0)))) then                                                                       
			do;		    
					
			         BI_providernpi_HIT + 1 ;		
					 BIPRVNPI=npiid;	
					 BI_PRV_ENTC 		= 	ENTITYTYP 		 	;			           																						                                               
					    /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 BI_PRV_LST  		=  	tranwrd(kcompress(Last_Name,"@:*`~#"),'Ñ','N') ;   
					 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 BI_PRV_FST  		=  	tranwrd(kcompress(First_Name,"@:*`~#"),'Ñ','N') ;
						     /* SAS2AWS2: ReplacedFunctionCompress */
					 BI_PRV_ADD1 		= 	kcompress(PROVFIRPRACADDR ,"@:*`~#")     ;  	  
						     /* SAS2AWS2: ReplacedFunctionCompress */
					 BI_PRV_ADD2   		=	kcompress(PROVSECPRACADDR,"@:*`~#")     ;  	  
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 BI_PRV_CITY   		=	kcompress(PROVPRACCITY ,"@:*`~#") ;
					   /* SAS2AWS2: ReplacedFunctionCompress */
					 BI_PRV_STATE 		=   kcompress(PROVPRACSTATE,"@:*`~#")   	;  
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 BI_PRV_ZIP    		= 	kcompress(PROVPRACPOSTAL ,"@:*`~#- ")   	;
					 BIPRVTXY           =   HLTHPROVTAXOCODE1; 
					 BI_PRV_EIN    		= 	FEDNUM_1    	;    /* B-301333 DS */
					 
					 /*B-335998 - DS - Updates starts here  */
					 BI_PROVNUMDT     = PROVENUMDATE ;	 
					 BI_NPIDEACTDT		=	NPIDEACTDATE;
					 BI_NPIREACTDT		=	NPIREACTDATE;
					 /*B-335998 - DS - Updates ends here  */
					

            end;                                                                                     
			else 
			do;		 _error_ 	=  0 ;
					 BI_providernpi_HIT + 1 ;	  
					 npiid=BIPRVNPI_Prov;					 	                
					 Set &RGA191..providernpi_&compno. key=npiid /unique;
					 if _iorc_ = 0 and  ((missing(Last_Name)=0 or  missing(First_Name)=0) or ( (missing(Last_Name)=1 and  missing(First_Name)=1) and 
											((firstdos gt NPIDEACTDATE and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=1) 
				or (missing(PROVENUMDATE)=1 and missing(NPIDEACTDATE)=1) 
				or (firstdos lt PROVENUMDATE and missing(PROVENUMDATE)=0 and missing(NPIDEACTDATE)=1) 
						or ((firstdos lt PROVENUMDATE or firstdos gt NPIDEACTDATE) and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=0)))) then                                                                       
					 do;		    
						 
				     BI_providernpi_HIT + 1 ;		
						 BIPRVNPI=npiid;	
						 BI_PRV_ENTC 		= 	ENTITYTYP 		 	;			           																						                                               
						    /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
						 BI_PRV_LST  		=  	tranwrd(kcompress(Last_Name,"@:*`~#"),'Ñ','N') ;   
						   /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
						 BI_PRV_FST  		=  	tranwrd(kcompress(First_Name,"@:*`~#"),'Ñ','N');  
							     /* SAS2AWS2: ReplacedFunctionCompress */
						 BI_PRV_ADD1 		= 	kcompress(PROVFIRPRACADDR ,"@:*`~#")     ;  	  
							     /* SAS2AWS2: ReplacedFunctionCompress */
						 BI_PRV_ADD2   		=	kcompress(PROVSECPRACADDR,"@:*`~#")     ;  	  
						 /* SAS2AWS2: ReplacedFunctionCompress */
						 BI_PRV_CITY   		=	kcompress(PROVPRACCITY ,"@:*`~#")  ;
						   /* SAS2AWS2: ReplacedFunctionCompress */
						 BI_PRV_STATE 		=   kcompress(PROVPRACSTATE,"@:*`~#")   	;  
						 /* SAS2AWS2: ReplacedFunctionCompress */
						 BI_PRV_ZIP    		= 	kcompress(PROVPRACPOSTAL ,"@:*`~#- ")   	;
						 BIPRVTXY           =   HLTHPROVTAXOCODE1; 	
						 BI_PRV_EIN    		= 	FEDNUM_1    	;  /* B-301333 DS */
						 
						BI_PROVNUMDT    =   PROVENUMDATE ;	 
						BI_NPIDEACTDT		=	NPIDEACTDATE;
						BI_NPIREACTDT		=	NPIREACTDATE;
						
		            end; 
                    else
					do;	
						 
						 BI_providernpi_NOHIT + 1 ;	
						 BI_PRV_ENTC 		= 	'' 		 	;			           																						                                               
						 BI_PRV_LST  		=   '' 		 	;																										                                           
						 BI_PRV_FST  		=  	'' 		 	;
						 BI_PRV_ADD1 		= 	'' 		 	;		  																				                                           
						 BI_PRV_ADD2   		=	'' 		 	;	 	  																					                                           
						 BI_PRV_CITY   		=	'' 		 	;	  	  																					                                           
						 BI_PRV_STATE 		=   '' 		 	;																								                                           
						 BI_PRV_ZIP    		= 	'' 		 	;	 
						 BIPRVTXY           =   '' 		 	;
						 BI_PRV_EIN         =   ''          ;	
						 
						 BI_PROVNUMDT        =   '';	 
						 BI_NPIDEACTDT		=	'';
						 BI_NPIREACTDT		=	'';
						
					end;
            end;
        npiid = rpanpi ;	
		set &RGA191..providernpi_&compno.  key = npiid / unique ;                                                                                        
			if _iorc_ = 0 and  ((missing(Last_Name)=0 or  missing(First_Name)=0) or ( (missing(Last_Name)=1 and  missing(First_Name)=1) and 
											((firstdos gt NPIDEACTDATE and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=1) 
				or (missing(PROVENUMDATE)=1 and missing(NPIDEACTDATE)=1) 
				or (firstdos lt PROVENUMDATE and missing(PROVENUMDATE)=0 and missing(NPIDEACTDATE)=1) 
						or ((firstdos lt PROVENUMDATE or firstdos gt NPIDEACTDATE) and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=0)))) then                                                                       
			do;		                                                                                   
			         RF_HIT + 1;					 
					    /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 RF_PHY_LST  		=  	tranwrd(kcompress(Last_Name,"@:*`~#"),'Ñ','N') ;   
					 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 RF_PHY_FST  		=  	tranwrd(kcompress(First_Name,"@:*`~#"),'Ñ','N') ;
					 RF_PHY_npiid		=   npiid;
					 RF_PHY_EIN     	= 	EIN    	; 
 					 RF_PHY_PRVTXY      =   HLTHPROVTAXOCODE1;
 					 
 					 RF_PROVNUMDT     =  PROVENUMDATE;  
					 RF_NPIDEACTDT		=	NPIDEACTDATE;
					 RF_NPIREACTDT		=	NPIREACTDATE;
					 
					   	           																						                                           
            end;                                                                                     
			else 
			do;
					  _error_ 	=  0 ;
			         RF_NOHIT + 1;																															                                               
					 RF_PHY_LST  		=  	' ' ;														                                           
					 RF_PHY_FST  		=  	' ' ;
					 RF_PHY_npiid		=   '';	
					 RF_PHY_EIN         =   ' ' ;  
					 RF_PHY_PRVTXY      = ' ';
					 
					 RF_PROVNUMDT     = '';  
					 RF_NPIDEACTDT		=	'';
					 RF_NPIREACTDT		=	'';
					 
					             																						                                                                                                                          			
			end; 
			 npiid = pcpnpiid ;	
		set &RGA191..providernpi_&compno.  key = npiid / unique ;                                                                                        
			if _iorc_ = 0 and  ((missing(Last_Name)=0 or  missing(First_Name)=0) or ( (missing(Last_Name)=1 and  missing(First_Name)=1) and 
											((firstdos gt NPIDEACTDATE and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=1) 
				or (missing(PROVENUMDATE)=1 and missing(NPIDEACTDATE)=1) 
				or (firstdos lt PROVENUMDATE and missing(PROVENUMDATE)=0 and missing(NPIDEACTDATE)=1) 
						or ((firstdos lt PROVENUMDATE or firstdos gt NPIDEACTDATE) and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=0)))) then                                                                       
			do;		                                                                                   
			         RF_HIT_1 + 1;					 
					    /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 RF_PHY_LST_1  		=  	tranwrd(kcompress(Last_Name,"@:*`~#"),'Ñ','N') ;   
					 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 RF_PHY_FST_1  		=  	tranwrd(kcompress(First_Name,"@:*`~#"),'Ñ','N') ;
					 RF_PHY_npiid_1		=   npiid;
					 RF_PHY_EIN_1     	= 	EIN    	; 
 					 RF_PHY_PRVTXY_1      =   HLTHPROVTAXOCODE1; 
					   	           																						                                           
            end;                                                                                     
			else 
			do;
					  _error_ 	=  0 ;
			         RF_NOHIT_1 + 1;																															                                               
					 RF_PHY_LST_1  		=  	' ' ;														                                           
					 RF_PHY_FST_1  		=  	' ' ;	
					 RF_PHY_npiid_1		=   '';	
					 RF_PHY_EIN_1         =   ' ' ;  
					 RF_PHY_PRVTXY_1      = ' ';			 
					             																						                                                                                                                          			
			end; 
			npiid = rndnpi ;	
		set &RGA191..providernpi_&compno.  key = npiid / unique ;                                                                                        
			if _iorc_ = 0 and  ((missing(Last_Name)=0 or  missing(First_Name)=0) or ( (missing(Last_Name)=1 and  missing(First_Name)=1) and 
											((firstdos gt NPIDEACTDATE and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=1) 
				or (missing(PROVENUMDATE)=1 and missing(NPIDEACTDATE)=1) 
				or (firstdos lt PROVENUMDATE and missing(PROVENUMDATE)=0 and missing(NPIDEACTDATE)=1) 
						or ((firstdos lt PROVENUMDATE or firstdos gt NPIDEACTDATE) and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=0)))) then                                                                       
			do;		                                                                                   
			         RN_HIT + 1;					 
					    /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 RN_PHY_LST  		=  	tranwrd(kcompress(Last_Name,"@:*`~#"),'Ñ','N') ;   
					   /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 RN_PHY_FST  		=  	tranwrd(kcompress(First_Name,"@:*`~#"),'Ñ','N') ;  
					 RN_PHY_NPiid		=   npiid;
					 RN_PHY_EIN     	= 	EIN    	; 
 					 RN_PHY_PRVTXY      =   HLTHPROVTAXOCODE1;
					   /* SAS2AWS2: ReplacedFunctionCompress */
					 RN_PHY_ADD1 		= 	kcompress(PROVFIRPRACADDR ,"@:*`~#")     ;  	/* HS 569650 - added */  
						   /* SAS2AWS2: ReplacedFunctionCompress */
					 RN_PHY_ADD2   		=	kcompress(PROVSECPRACADDR,"@:*`~#")     ;  /* HS 569650 - added */	  
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 RN_PHY_CITY   		=	kcompress(PROVPRACCITY ,"@:*`~#")     ;  /* HS 569650 - added */
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 RN_PHY_STATE 		=   kcompress(PROVPRACSTATE,"@:*`~#")   	;  	/* HS 569650 - added */
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 RN_PHY_ZIP    		= 	kcompress(PROVPRACPOSTAL ,"@:*`~#- ")   	; /* HS 569650 - added */
					 
					 RN_PROVNUMDT     = PROVENUMDATE ;	 
					 RN_NPIDEACTDT		=	NPIDEACTDATE;
					 RN_NPIREACTDT		=	NPIREACTDATE;
					   	           																						                                           
            end;                                                                                     
			else 
			do;
					  _error_ 	=  0 ;
			         RN_NOHIT + 1;																															                                               
					 RN_PHY_LST  		=  	' ' ;														                                           
					 RN_PHY_FST  		=  	' ' ;
					 RN_PHY_npiid		=   '';	
					 RN_PHY_EIN         =   ' ' ;  
					 RN_PHY_PRVTXY      = ' ';
					 RN_PHY_ADD1 		= 	' ';  	/* HS 569650 - added */  																				                                           
					 RN_PHY_ADD2   		=	' ';  /* HS 569650 - added */	  																					                                           
					 RN_PHY_CITY   		=	' ';  /* HS 569650 - added */	  																					                                           
					 RN_PHY_STATE 		=   ' ';  	/* HS 569650 - added */																							                                           
					 RN_PHY_ZIP    		= 	' '; /* HS 569650 - added */
					 
					 RN_PROVNUMDT     = '' ;	 
					 RN_NPIDEACTDT		=	'';
					 RN_NPIREACTDT		=	'';
					 
					             																						                                                                                                                          			
			end; 
		npiid = oprnpi ;	
		set &RGA191..providernpi_&compno.  key = npiid / unique ;                                                                                        
			if _iorc_ = 0 and  ((missing(Last_Name)=0 or  missing(First_Name)=0) or ( (missing(Last_Name)=1 and  missing(First_Name)=1) and 
											((firstdos gt NPIDEACTDATE and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=1) 
				or (missing(PROVENUMDATE)=1 and missing(NPIDEACTDATE)=1) 
				or (firstdos lt PROVENUMDATE and missing(PROVENUMDATE)=0 and missing(NPIDEACTDATE)=1) 
						or ((firstdos lt PROVENUMDATE or firstdos gt NPIDEACTDATE) and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=0)))) then                                                                       
			do;		                                                                                   
			         OP_HIT + 1;					 
					    /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 OP_PHY_LST  		=  	tranwrd(kcompress(Last_Name,"@:*`~#"),'Ñ','N') ;   
					 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranwrd */
					 OP_PHY_FST  		=  	tranwrd(kcompress(First_Name,"@:*`~#"),'Ñ','N') ;
					 OPPRVNPI 		    =   npiid;
					 OP_PHY_EIN     	= 	EIN    	; 
 					 OP_PHY_PRVTXY      =   HLTHPROVTAXOCODE1;
 					 
 					 OP_PROVNUMDT     = PROVENUMDATE ;	  
					 OP_NPIDEACTDT		=	NPIDEACTDATE;
					 OP_NPIREACTDT		=	NPIREACTDATE;
					 
					   	           																						                                           
            end;                                                                                     
			else 
			do;
					  _error_ 	=  0 ;
			         OP_NOHIT + 1;																															                                               
					 OP_PHY_LST  		=  	' ' ;														                                           
					 OP_PHY_FST  		=  	' ' ;
					 OPPRVNPI 		    =   '';	
					 OP_PHY_EIN         =   ' ' ;  
					 OP_PHY_PRVTXY      = ' ';
					 
					 
					 OP_PROVNUMDT     = '' ;	  
					 OP_NPIDEACTDT		=	'';
					 OP_NPIREACTDT		=	'';
					 
					             																						                                                                                                                          			
			end; 	
		NPIID=atpnpi;		
		set &RGA191..providernpi_&compno.  key = npiid / unique ;                                                                                       
			if _iorc_ = 0 and  ((missing(Last_Name)=0 or  missing(First_Name)=0) or ( (missing(Last_Name)=1 and  missing(First_Name)=1) and 
											((firstdos gt NPIDEACTDATE and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=1) 
				or (missing(PROVENUMDATE)=1 and missing(NPIDEACTDATE)=1) 
				or (firstdos lt PROVENUMDATE and missing(PROVENUMDATE)=0 and missing(NPIDEACTDATE)=1) 
						or ((firstdos lt PROVENUMDATE or firstdos gt NPIDEACTDATE) and missing(NPIDEACTDATE)=0 and missing(PROVENUMDATE)=0)))) then                                                                       
			do;		                                                                                   
			         AT_providernpi_HIT + 1 ;				 
					
					    /* SAS2AWS2: ReplacedFunctionCompress */
					 AT_PHY_LST  	=  	kcompress(Last_Name,"@:*`~#");   
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 AT_PHY_FST  	=  	kcompress(First_Name,"@:*`~#")     ;
					 AT_PHY_PRVTXY  =   HLTHPROVTAXOCODE1; 
					 ATPRVNPI 		=   NPIID;
					 AT_PHY_EIN     = 	EIN    	;  
					   /* SAS2AWS2: ReplacedFunctionCompress */
					 AT_PHY_ADD1 	= 	kcompress(PROVFIRPRACADDR ,"@:*`~#")     ;  	/* HS 569650 - added */  
						   /* SAS2AWS2: ReplacedFunctionCompress */
					 AT_PHY_ADD2   	=	kcompress(PROVSECPRACADDR,"@:*`~#")     ;  /* HS 569650 - added */	  
						   /* SAS2AWS2: ReplacedFunctionCompress */
					 AT_PHY_CITY   	=	kcompress(PROVPRACCITY ,"@:*`~#")     ;  /* HS 569650 - added */	  
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 AT_PHY_STATE 	=   kcompress(PROVPRACSTATE,"@:*`~#")   	;  	/* HS 569650 - added */
					 /* SAS2AWS2: ReplacedFunctionCompress */
					 AT_PHY_ZIP    	= 	kcompress(PROVPRACPOSTAL ,"@:*`~#- ")   	; /* HS 569650 - added */
					 
					 AT_PROVNUMDT   = PROVENUMDATE ;  
					 AT_NPIDEACTDT	=	NPIDEACTDATE;
					 AT_NPIREACTDT	=	NPIREACTDATE;

					 
			end; 
			else 
			do;
					_error_ 	=  0 ;
					AT_providernpi_NoHIT + 1 ;
					
					 AT_PHY_LST  	=  	'';   																					                                           
					 AT_PHY_FST  	=  ''     ;   
					 AT_PHY_PRVTXY  =   ''; 
					 ATPRVNPI 		=   '';
					 AT_PHY_EIN     = 	''   	;
					 AT_PHY_ADD1 	= 	'';  	/* HS 569650 - added */  																				                                           
					 AT_PHY_ADD2   	=	'';  /* HS 569650 - added */	  																					                                           
					 AT_PHY_CITY   	=	'';  /* HS 569650 - added */	  																					                                           
					 AT_PHY_STATE 	=   '';  	/* HS 569650 - added */																							                                           
					 AT_PHY_ZIP    	= 	''; /* HS 569650 - added */
					 
					 AT_PROVNUMDT   = '' ;  
					 AT_NPIDEACTDT	=	'';
					 AT_NPIREACTDT	=	'';

			end; 
 if eof then output stat;

 rec_encY2 + 1;
 /* SAS2AWS2: ReplacedFunctionCompress */
 call symput('rec_encY2',kcompress(rec_encY2)) ;
 output &RGA191..encountersY2_&compno._&run_id. ;
run;   
%mend;
%log;


/* CR# CHG0031701 added logic to map SSN from control file for matching claims ---> start */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/SSN_Control_Table.xlsx"
		out=SSN_Control_Table dbms=xlsx replace;
Run;
proc sort data = &RGA191..encountersY2_&compno._&run_id. ; by clamno ; run;
Proc sort data=	SSN_Control_Table nodupkey;by clamno ; run;
Data 	&RGA191..encountersY2_&compno._&run_id.(drop = socsec_upd);
format MEM_socsec $18.;
	merge &RGA191..encountersY2_&compno._&run_id.(in=_a) SSN_Control_Table(in=_b);
	by clamno ;
	if _a and _b then do;
	MEM_socsec = socsec_upd;
	end;
	if _a ;

Run;
/* CR# CHG0031701 added logic to map SSN from control file for matching claims ---> end */

/* HS 569650 - added criteria 4 macro to handle multiple TCN changes */
%macro criteria4;
%if "&flag." = "Criteria 4" %then %do;

proc sort data = &RGA191..encountersY2_&Compno._&run_id. out = test_sample1(keep = clamno icn)  nodupkey;
by clamno icn;
run;
proc sort data = &RGA191..encountersY2_&Compno._&run_id.;
by clamno icn;
run;
data test_sample11;
set test_sample1;
by clamno icn;
if first.clamno then sum = 0;
sum+1;
run;


data &RGA191..encountersY2_&Compno._&run_id. (drop = clamno rename = ( clamno_new = clamno temp = clamno_old));
length clamno_new  $20. temp $20.;
merge &RGA191..encountersY2_&Compno._&run_id.(in=a) test_sample11 (in=b);
by clamno icn;
if a and b then do;
/* SAS2AWS2: ReplacedFunctionCompress */
clamno_new = kcompress(clamno)||put(sum,z2.);
temp = clamno;
end;
if a;
run;
%end;
%else %do;
%Put "Validation Success";
data &RGA191..encountersY2_&Compno._&run_id. ;
set &RGA191..encountersY2_&Compno._&run_id.;
clamno_old = clamno;
run;
%end;		
%mend criteria4;
%criteria4;
/* HS 542635 - Added Proc delete statement */
Proc delete data=&RGA191..members_&compno.;run;
Proc delete data=&RGA191..provider3_&compno.;run;
Proc delete data=&RGA191..encounters_T_&compno._&run_id. ;run;

Data &RGA191..encountersY2B_&Compno._&run_id.;
set &RGA191..encountersY2_&Compno._&run_id.;
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if klength(kcompress(BI_PRV_EIN))=9 then BI_PRV_EIN =kcompress(BI_PRV_EIN);
	else BI_PRV_EIN ='';

    /* SAS2AWS2: ReplacedFunctionLength */
    if klength(kcompress(BI_PRV_ZIP)) = 9 then
	do;
	    BI_PRV_ZIP=BI_PRV_ZIP;
	end;
	/* SAS2AWS2: ReplacedFunctionLength */
	else if klength(kcompress(BI_PRV_ZIP)) = 5 then
	do;
	    /* SAS2AWS2: ReplacedFunctionCompress */
	    BI_PRV_ZIP=kcompress(BI_PRV_ZIP||'9998');
	end; 
	else 
	do;
		BI_PRV_ZIP='';
	end;	


    /* SAS2AWS2: ReplacedFunctionLength */
    if klength(kcompress(pzipcod_1)) = 9 then
	do;
	    pzipcod_1=pzipcod_1;
	end;
	/* SAS2AWS2: ReplacedFunctionLength */
	else if klength(kcompress(pzipcod_1)) = 5 then
	do;
	    /* SAS2AWS2: ReplacedFunctionCompress */
	    pzipcod_1=kcompress(pzipcod_1||'9998');
	end; 
	else 
	do;
		pzipcod_1='';
	end;	

	/* SAS2AWS2: ReplacedFunctionLength */
	if klength(kcompress(MEM_MZIPCOD)) = 9 then
    do;
       MEM_MZIPCOD =MEM_MZIPCOD;
    end;
    /* SAS2AWS2: ReplacedFunctionLength */
    else if klength(kcompress(MEM_MZIPCOD)) = 5 then
    do;
       /* SAS2AWS2: ReplacedFunctionCompress */
       MEM_MZIPCOD  =kcompress(MEM_MZIPCOD||'9998');
    end; 
	else 
	do;
	   MEM_MZIPCOD  ='';
	end;	
	
	if klength(kcompress(MEM_MZIPCOD_MCR)) = 9 then
    do;
       MEM_MZIPCOD_MCR =MEM_MZIPCOD_MCR;
    end;
    else if klength(kcompress(MEM_MZIPCOD_MCR)) = 5 then
    do;
       MEM_MZIPCOD  =kcompress(MEM_MZIPCOD_MCR||'9998');
    end; 
	else 
	do;
	   MEM_MZIPCOD_MCR  ='';
	end;		
Compno=&compno.;
run;
Proc sql noprint ;
	Select count(distinct(clamno)) into :rec_encY2_1 from &RGA191..encountersY2_&Compno._&run_id. ;		
quit; 
/*HS 542635 - Added Proc delete statement */
Proc delete data=&RGA191..encountersY2_&Compno._&run_id.;run;
Proc sort data=&RGA191..encountersY2B_&Compno._&run_id. out=&RGA191..encountersY_&compno._&run_id.;by clamno;run;
/* HS 542635 - Added Proc delete statement */
Proc delete data=&RGA191..encountersY2B_&Compno._&run_id.;run;

/* B-105917 start ******************************************************************************************/
/*** Drop the Proc code and Proc date from main dataset and create separate dataset**/
data &RGA191..encountersY_&compno._&run_id. (drop=PRPROCDR  PROCDR2-PROCDR6 PRPROCDT PROCDT2-PROCDT6)
	&RGA191..claims_proc (keep=clamno origclamno typecode PRPROCDR  PROCDR2-PROCDR6  PRPROCDT PROCDT2-PROCDT6 
	rename =(PRPROCDR=Proc_Cod1  PROCDR2-PROCDR6=Proc_Cod2-Proc_Cod6 PRPROCDT=Proc_dt1 PROCDT2-PROCDT6=Proc_dt2-Proc_dt6));
	length PRPROCDR  PROCDR2-PROCDR6 $20.;
set &RGA191..encountersY_&compno._&run_id.;
run;

proc sort data=&RGA191..claims_proc nodupkey;by clamno;run;

/*** Split the Inst claims based on typecode ***/
data &RGA191..claims_inst;
set &RGA191..claims_proc;
if typecode ne 'P' then output &RGA191..claims_inst;
run;

/*** Join the Inst claims with claimsext to pull the proc_code1 to proc_code25 and proc_date1 to proc_date25  ***/
proc sql;
create table &RGA191..claimsext_proc_clamno as 
select a.origclamno, b.*
from &RGA191..claims_inst a
inner join &libn2..claimsext(keep=clamno procedurecode1-procedurecode25 proceduredate1-proceduredate25) b
on a.clamno=b.clamno;
quit;

/* B-166917 added joins by origclamno for proc codes */
proc sql;
create table &RGA191..claimsext_proc_origclamno_1 as 
select b.*
from &RGA191..claims_inst a
inner join &libn2..claimsext(keep=clamno procedurecode1-procedurecode25 proceduredate1-proceduredate25) b
on a.origclamno=b.clamno;
quit;

proc sql;
create table &RGA191..claimsext_proc_origclamno_2 as 
select a.clamno, b.*
from &RGA191..claims_inst a
inner join &RGA191..claimsext_proc_origclamno_1(keep=clamno procedurecode1-procedurecode25 proceduredate1-proceduredate25 rename=(clamno=origclamno)) b
on a.origclamno=b.origclamno;
quit;

data &RGA191..claimsext_proc;
set &RGA191..claimsext_proc_clamno &RGA191..claimsext_proc_origclamno_2;
run;

proc sort data=&RGA191..claimsext_proc nodupkey;by clamno;run;
proc sort data=&RGA191..claims_proc(drop=origclamno);by clamno;run;

/*** Macro to transpose the claims and claimsext proc code and proc date from column to rows ***/
%macro trans_clm(dsn= ,VAR_PROC_COD= ,VAR_PROC_DT= ,n=);
data &RGA191..&dsn._cod_dt(keep=clamno proc_cod proc_dt i rename =(i=count1));
set &RGA191..&dsn.;
by clamno;
length proc_cod $20. ;
array proc_code_array{&n.} $ &VAR_PROC_COD.;
array proc_date_array{&n.} $ &VAR_PROC_DT. ;
do i=1 to &n.;
proc_cod=proc_code_array{i};
proc_dt=put(proc_date_array{i},yymmddn8.);
output;
end;
run;
%mend;
%trans_clm(dsn=claims_proc, VAR_PROC_COD= Proc_Cod1-Proc_Cod6, VAR_PROC_DT= Proc_dt1-Proc_dt6 , n=6); 
%trans_clm(dsn=claimsext_proc, VAR_PROC_COD= procedurecode1-procedurecode25,VAR_PROC_DT=proceduredate1-proceduredate25, n=25); 

proc sort data=&RGA191..claims_proc_cod_dt;by clamno count1;run;
proc sort data=&RGA191..claimsext_proc_cod_dt;by clamno count1;run;

/*** Generate sequence number for the Claims and Claimsext dataset ***/
%macro order(dsn= ,cnt= );
data &RGA191..&dsn.;
  set &RGA191..&dsn. (where=(proc_cod not in(&icd10cd2.)));  /* proc code control file exclusion */
  by clamno;  
  if first.clamno then count = &cnt.;
  else count + 1;
  drop count1;
run;
%mend;
%order(dsn=claims_proc_cod_dt,cnt=1);
%order(dsn=claimsext_proc_cod_dt,cnt=7);

/** Merge the claims and Claimsext dataset **/
data &RGA191..claims_proc_cod_dt_01;
set &RGA191..claims_proc_cod_dt  &RGA191..claimsext_proc_cod_dt;
run;

/** Sort and Remove the Duplicate Proc Code and Proc Date **/
proc sort data=&RGA191..claims_proc_cod_dt_01;by clamno count;run;
proc sort data=&RGA191..claims_proc_cod_dt_01 out=&RGA191..claims_proc_cod_dt_02 nodupkey;
by clamno proc_cod proc_dt;
run;
proc sort data=&RGA191..claims_proc_cod_dt_02;by clamno count;run;

/*** Macro to transpose the claims and claimsext proc code and proc date from rows to columns ***/
DATA &RGA191..claims_proc_cod_dt_03 (Keep=clamno Proc_cod1-Proc_cod25 Proc_dt1-Proc_dt25);
  set &RGA191..claims_proc_cod_dt_02; 
  by clamno;
  array proc_code_array {25} $  Proc_cod1-Proc_cod25;
  array proc_date_array {25} $  Proc_dt1-Proc_dt25;
  
  retain i Proc_cod1-Proc_cod25 Proc_dt1-Proc_dt25;
  if FIRST.clamno then do;
    i = 1;
	do j=1 to 25;
		proc_code_array{j}=" ";
		proc_date_array{j}=" ";
	end;
  end;
  proc_code_array(i)=Proc_Cod;
  proc_date_array(i)=Proc_dt;
  i = i + 1;
   if LAST.clamno;
RUN;

proc sort data=&RGA191..claims_proc_cod_dt_03;by clamno;run;
proc sort data=Inpatient_Proc_Code1;by clamno;run;

data &RGA191..claims_proc_cod_dt_03;
merge &RGA191..claims_proc_cod_dt_03 (in=a) Inpatient_Proc_Code1 (in=b); /*B-110462 Added Alias Name*/
if a;
by clamno;
run;

/* B-261073 - Added new logic to populate the procvind 0 if matches with control file */

proc sort data=Inpatient_Proc_Code;by clamno;
run;

proc sort data =&RGA191..encountersY_&compno._&run_id.;by clamno;
run;


data &RGA191..encountersY_&compno._&run_id.;
merge &RGA191..encountersY_&compno._&run_id. (in=a) Inpatient_Proc_Code (in=b keep =clamno);
if a;
if a and b then procvind = '0';
by clamno;
run;

/* B-261073 - ends */

/** Merge the claims proc code and proc date with main dataset  **/		/* B-114226 added reject */
data &RGA191..encountersY_&compno._&run_id. (drop = edit_chk_type edit_code encapdfld  Recsubind )
 	 &RGA191..procedure_code_date_reject (keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode);
	 length edit_chk_type $100. edit_code $5. encapdfld $50.;
merge &RGA191..encountersY_&compno._&run_id.(in=ina) &RGA191..claims_proc_cod_dt_03(in=inb) ;
by clamno;
if ina;
if  typecode = "I" and "&edit_type_proc_code_date_rej." eq "H" and typecode in (&enctyp_code_proc_code_date_rej.) then do;	/* B-168796 */
		if  (missing(Proc_cod1) and not missing(Proc_dt1)) or (not missing(Proc_cod1) and missing(Proc_dt1)) or
			(missing(Proc_cod2) and not missing(Proc_dt2)) or (not missing(Proc_cod2) and missing(Proc_dt2)) or
			(missing(Proc_cod3) and not missing(Proc_dt3)) or (not missing(Proc_cod3) and missing(Proc_dt3)) or
			(missing(Proc_cod4) and not missing(Proc_dt4)) or (not missing(Proc_cod4) and missing(Proc_dt4)) or
			(missing(Proc_cod5) and not missing(Proc_dt5)) or (not missing(Proc_cod5) and missing(Proc_dt5)) or
			(missing(Proc_cod6) and not missing(Proc_dt6)) or (not missing(Proc_cod6) and missing(Proc_dt6)) or
			(missing(Proc_cod7) and not missing(Proc_dt7)) or (not missing(Proc_cod7) and missing(Proc_dt7)) or
			(missing(Proc_cod8) and not missing(Proc_dt8)) or (not missing(Proc_cod8) and missing(Proc_dt8)) or
			(missing(Proc_cod9) and not missing(Proc_dt9)) or (not missing(Proc_cod9) and missing(Proc_dt9)) or
			(missing(Proc_cod10) and not missing(Proc_dt10)) or (not missing(Proc_cod10) and missing(Proc_dt10)) or
			(missing(Proc_cod11) and not missing(Proc_dt11)) or (not missing(Proc_cod11) and missing(Proc_dt11)) or
			(missing(Proc_cod12) and not missing(Proc_dt12)) or (not missing(Proc_cod12) and missing(Proc_dt12)) or
			(missing(Proc_cod13) and not missing(Proc_dt13)) or (not missing(Proc_cod13) and missing(Proc_dt13)) or
			(missing(Proc_cod14) and not missing(Proc_dt14)) or (not missing(Proc_cod14) and missing(Proc_dt14)) or
			(missing(Proc_cod15) and not missing(Proc_dt15)) or (not missing(Proc_cod15) and missing(Proc_dt15)) or
			(missing(Proc_cod16) and not missing(Proc_dt16)) or (not missing(Proc_cod16) and missing(Proc_dt16)) or
			(missing(Proc_cod17) and not missing(Proc_dt17)) or (not missing(Proc_cod17) and missing(Proc_dt17)) or
			(missing(Proc_cod18) and not missing(Proc_dt18)) or (not missing(Proc_cod18) and missing(Proc_dt18)) or
			(missing(Proc_cod19) and not missing(Proc_dt19)) or (not missing(Proc_cod19) and missing(Proc_dt19)) or
			(missing(Proc_cod20) and not missing(Proc_dt20)) or (not missing(Proc_cod20) and missing(Proc_dt20)) or
			(missing(Proc_cod21) and not missing(Proc_dt21)) or (not missing(Proc_cod21) and missing(Proc_dt21)) or
			(missing(Proc_cod22) and not missing(Proc_dt22)) or (not missing(Proc_cod22) and missing(Proc_dt22)) or
			(missing(Proc_cod23) and not missing(Proc_dt23)) or (not missing(Proc_cod23) and missing(Proc_dt23)) or
			(missing(Proc_cod24) and not missing(Proc_dt24)) or (not missing(Proc_cod24) and missing(Proc_dt24)) or
			(missing(Proc_cod25) and not missing(Proc_dt25)) or (not missing(Proc_cod25) and missing(Proc_dt25)) then 
			do;
		       edit_chk_type = 'Procedure code or date missing';
			   encapdfld='procedure_code_date_reject';
			   Recsubind = 'N' ;
			   edit_code = '99944'; 
			   output &RGA191..procedure_code_date_reject;
			end;
		else output  &RGA191..encountersY_&compno._&run_id.;	
end;	
else output  &RGA191..encountersY_&compno._&run_id.;
run; 
/* B-105917 end ********************************************************************************************/

/* B-103975 moved prior to rollup start */
Data Npi_340b;
set RGA193.Npi_340b;
/* SAS2AWS2: ReplacedFunctionCompress */
npiid1=kcompress("'"||npiid||"'");
run;

Proc sql noprint;
Select npiid1 into :binpiid SEPARATED
 by ',' from Npi_340b;
quit;
/* B-103975 moved prior to rollup end */
data inst inst_OP Prof;									/* B-97137 added inst_OP for new outpatient rollup */
	set &RGA191..encountersY_&compno._&run_id.;
	/* HS 565133 KV - added mapping logic for IN_2400_SV202_2_SL_HCPCS */
	if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1) then do;
				end;
				else if typecode="O" then do;
							if missing(cptcod)=0 then IN_2400_SV202_2_SL_HCPCS  = cptcod ;
							else IN_2400_SV202_2_SL_HCPCS  = svccod;
				End;
	/* HS 565133 KV - added mapping logic for IN_2400_SV202_2_SL_HCPCS */

	if typecode in ("O") and BIPRVNPI in(&binpiid.) and IN_2400_SV202_2_SL_HCPCS in(&NDC_Svccod.) then do;
		cptmd1	=	"UD";				 	/* B-117000 allows claim roll up of UD lineno */
		IN_2400_SV202_3_SL_MOD1 = cptmd1;  	/* B-103975 moved from RGA19401		B-117000 replaced hardcoded "UD" with cpt modifier */	
		end;

  /*if typecode in ("I","O") Then output inst; */
	if typecode in ("I") then output inst;

	else if typecode in ("O") then do;
	/* B-103975 create common CPTMD roll variable  start */
		array cptvar(*) cptmd1-cptmd4;
		array c_code(*) $ c_code1-c_code4;
		do i=1 to 4;
  			c_code(i)= cptvar(i);
		end;
 		do until (sorted);
    		sorted=1;
    		do i = 1 to dim(c_code)-1;
      			if c_code(i) > c_code(i+1) then do;
        			temp=c_code(i+1);
        			c_code(i+1)=c_code(i);
        			c_code(i)=temp;
        			sorted=0;
      			end;
    		end;
  		end;
		length cptmd_roll $ 12;
		/* SAS2AWS2: ReplacedFunctionCatx-ReplacedFunctionLeft */
		cptmd_roll=kleft(catx(' ',of c_code1-c_code4));
		drop i sorted temp c_code1-c_code4;
	/* B-103975 create common CPTMD roll variable  end */
		output inst_OP;											 
	end;

	else if typecode in ("P") then do;
	/* B-103975 create common MODCD roll variable  start */
		modcd1=modcod;
		array modvar(*) modcd1-modcd4;
		array m_code(*) $ m_code1-m_code4;
		do i=1 to 4;
  			m_code(i)= modvar(i);
		end;
		do until (sorted);
    		sorted=1;
    		do i = 1 to dim(m_code)-1;
      			if m_code(i) > m_code(i+1) then do;
        			temp=m_code(i+1);
        			m_code(i+1)=m_code(i);
        			m_code(i)=temp;
        			sorted=0;
      			end;
    		end;
  		end;
		length modcd_roll $ 12;
		/* SAS2AWS2: ReplacedFunctionCatx-ReplacedFunctionLeft */
		modcd_roll=kleft(catx(' ',of m_code1-m_code4));
		drop i sorted temp modcd1 m_code1-m_code4;
	/* B-103975 create common MODCD roll variable  end */
		output prof;
	end;
Run;

Proc sort data=Prof; by clamno lineno; run;  /* B-97137 added for lineno consistency prior to rollup */
Proc sort data=inst; by clamno lineno; run;  /* B-97137 added for lineno consistency prior to rollup */
Proc sort data=inst_OP; by clamno lineno; run;  /* B-97137 added for lineno consistency prior to rollup */

%macro prof;
%nobs(prof);
 %if &nobs = 0 %then %do;
   data Prof_final2 prof_roll_skip; /* B-168796 skip */
  		set Prof;
		length rollup_line $1000.;
	    rollup_line  = '   ';     
   Run;
 %end;
 %else %do;

data prof(drop=rollup_line) prof_roll_skip; /* B-168796 skip */
set prof;
if "&edit_type_Rollup_Reject." eq "H" and typecode in (&enctyp_code_Rollup_Reject.) then output prof;
else do;
	length rollup_line $1000.;
	rollup_line  = '';
	output prof_roll_skip;
end;
run;

/*Proc sort data=Prof;by clamno   membno provno svcdat enddat  svccod modcod modcd2 modcd3 modcd4;*/ /*HS 565133 - commented existing rollup logic*/
Proc sort data=Prof; /*by clamno   membno provno svcdat   svccod formcd;  HS 565133 - added new rollup logic*/
					   by clamno membno provno svcdat svccod formcd modcd_roll NDCSVC;	/* B-103975 new rollup */
run;

Proc transpose data=Prof out=Prof_1(drop=_NAME_ _LABEL_) prefix=rollup;
/*by clamno   membno provno svcdat enddat  svccod modcod modcd2 modcd3 modcd4;*//*HS 565133 - commented existing rollup logic*/
/*by clamno   membno provno svcdat   svccod formcd; HS 565133 - added new rollup logic*/
by clamno membno provno svcdat svccod formcd modcd_roll NDCSVC;	/* B-103975 new rollup */
var lineno;
run;

%macro chkroll; *B-168796 added macro;
%let dsid = %sysfunc(open(Prof_1,i));
%let vcnt = %eval(%sysfunc(attrn(&dsid.,nvar)));	
%LET rc = %SYSFUNC(CLOSE(&dsid.));
%put &vcnt; 
%if &vcnt. gt 8 %then %do;
data Prof_1;
set Prof_1;
rename rollup1=lineno;
run;
%end;
%else %do;
data Prof_1;
set Prof_1(keep=clamno membno provno svcdat svccod formcd modcd_roll NDCSVC);
lineno= '   ';
run;
%end;
%mend chkroll;
%chkroll;

%let fid = %sysfunc(open(Prof_1,i));
%let cnt = %eval(%sysfunc(attrn(&fid.,nvar))-8);	/* B-103975 new rollup changed -6 to -11 to -8 */
%put &cnt;
%LET rc = %SYSFUNC(CLOSE(&fid.));

%if &cnt.=1 %then %do;
	data Prof_final2;
	      set Prof;
	      length rollup_line $1000.;
	      rollup_line  = '   ';      
	run;
	
%end;
%else %do;

data Prof_1_1;
	      set Prof_1;
	      length rollup_line $1000.;
	      /* SAS2AWS2: ReplacedFunctionCatx */
	      rollup_line  = catx(',',of rollup2-rollup&cnt.);
	run;


Proc summary data=Prof nway missing;
    var CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt mcdnetalwamt mcrnetalwamt NETALWAMT_old mcrcovunt covunt unitct alwunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr;/*ALM#5160 - added */
/*    class clamno   membno provno svcdat enddat  svccod modcod modcd2 modcd3 modcd4;*//*HS 565133 - commented existing rollup logic*/
/*	 class clamno   membno provno svcdat   svccod formcd;  HS 565133 - added new rollup logic*/
     class clamno membno provno svcdat svccod formcd modcd_roll NDCSVC;	/* B-103975 new rollup */	 
output out=Prof_SUM(drop=_TYPE_ _FREQ_) sum=;
run;

data Prof_final;
merge Prof_1_1(in=_a) Prof_SUM(in=_b);
if _a;
/*by clamno  membno provno svcdat enddat  svccod modcod modcd2 modcd3 modcd4;*//*HS 565133 - commented old rollup logic*/
/*by clamno   membno provno svcdat   svccod formcd;  HS 565133 - added new rollup logic*/
by clamno membno provno svcdat svccod formcd modcd_roll NDCSVC;	/* B-103975 new rollup */
run;

/* HS 542635 - Added Proc delete statement */
Proc delete data=Prof_1_1;run;
Proc delete data=Prof_SUM;run;

Data prof_final1;
set Prof_final;
run;
/* HS 542635 - Added Proc delete statement */
Proc delete data=Prof_final;run;

Proc sort data=prof_final1;by clamno lineno;run;
proc sort data=prof;by clamno lineno;run;

data prof_final2(drop=modcd_roll);
merge  prof_final1(in=_a) 
Prof(in=_b drop=  membno provno svcdat /*enddat*/  svccod modcd_roll NDCSVC /* B-103975 new rollup */ formcd /*HS 565133 - modified drop variables */
CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt mcdnetalwamt mcrnetalwamt NETALWAMT_old mcrcovunt covunt unitct alwunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr);/* ALM#5160 - added */
if _a;
by clamno lineno;
run;
/* HS 542635 - Added Proc delete statement */

Proc delete data=prof_final1;run;
Proc delete data=Prof;run;
%end;
%end;
%mend;
%prof;

	
%macro temp;
%nobs(inst);
 %if &nobs = 0 %then %do;
  data inst_final2 inst_roll_skip; /* B-168796 skip */
  	set inst;
	length rollup_line $1000.;
	rollup_line  = '   ';     
  Run; 
 %end;
 %else %do;

data inst(drop=rollup_line) inst_roll_skip; /* B-168796 skip */
set inst;
if "&edit_type_Rollup_Reject." eq "H" and typecode in (&enctyp_code_Rollup_Reject.) then output inst;
else do;
	length rollup_line $1000.;
	rollup_line  = '';
	output inst_roll_skip;
end;
run;

/*Proc sort data=inst;by clamno   membno svcdat enddat provno svccod cptcod revcod CPTMD1 cptmd1 cptmd2 cptmd3 cptmd4;*//*HS 565133 - commented existing rollup logic */
Proc sort data=inst;by clamno   membno svcdat  provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd;/*HS 565133 - added new rollup logic*/
Run;

Proc transpose data=inst out=inst_1(drop=_name_ _LABEL_) prefix=rollup;
/*	by clamno   membno svcdat enddat provno svccod cptcod revcod CPTMD1 cptmd1 cptmd2 cptmd3 cptmd4;*//*HS 565133 - commented existing rollup logic */
	by clamno   membno svcdat  provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd;/*HS 565133 - added new rollup logic*/
	var lineno;
run;

%macro chkroll2; *B-168796 added macro;
%let dsid = %sysfunc(open(inst_1,i));
%let vcnt = %eval(%sysfunc(attrn(&dsid.,nvar)));	
%LET rc = %SYSFUNC(CLOSE(&dsid.));
%put &vcnt; 
%if &vcnt. gt 7 %then %do;
data inst_1;
set inst_1;
rename rollup1=lineno;
run;
%end;
%else %do;
data inst_1;
set inst_1(keep=clamno membno svcdat provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd);
lineno= '   ';
run;
%end;
%mend chkroll2;
%chkroll2;

%let fid = %sysfunc(open(inst_1,i));
%let cnt = %eval(%sysfunc(attrn(&fid.,nvar))-7);
%put &cnt;
%LET rc = %SYSFUNC(CLOSE(&fid.));
%if &cnt.=1 %then %do;
	data inst_final2;
	      set inst;
	      length rollup_line $1000.;
	      rollup_line  = '   ';      
	run;	

%end;
%else %do;

	data inst_1_1;
	      set inst_1;
	      length rollup_line $1000.;
	      /* SAS2AWS2: ReplacedFunctionCatx */
	      rollup_line  = catx(',',of rollup2-rollup&cnt.);
	run;
	Proc summary data=inst nway missing;
    var CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt  mcdnetalwamt mcrnetalwamt NETALWAMT_old mcrcovunt covunt unitct alwunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr;/*ALM#5160 - added*/
/*    class clamno   membno svcdat enddat provno svccod cptcod revcod CPTMD1  cptmd2 cptmd3 cptmd4;*//*HS 565133 - commented existing rollup logic */
	class clamno   membno svcdat  provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd;/*HS 565133 - added new rollup logic*/
output out=inst_SUM(drop=_TYPE_ _FREQ_) sum=;
run;
data inst_final;
merge inst_1_1(in=_a) inst_SUM(in=_b);
if _a;
/*by clamno   membno svcdat enddat provno svccod cptcod revcod CPTMD1  cptmd2 cptmd3 cptmd4;*//*HS 565133 - commented existing rollup logic */
by clamno   membno svcdat  provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd;/*HS 565133 - added new rollup logic*/
run;
/* HS 542635 - Added Proc delete statement */
Proc delete data=inst_1_1;run;
Proc delete data=inst_SUM;run;

Data inst_final1;
set inst_final;
run;
/* HS 542635 - Added Proc delete statement */

Proc delete data=inst_final;run;

Proc sort data=inst_final1;by clamno lineno;run;
proc sort data=inst;by clamno lineno;run;

data inst_final2;
merge  inst_final1(in=_a) 
inst(in=_b drop=  membno svcdat /*enddat*/ provno /*svccod cptcod revcod  cptmd1 cptmd2 cptmd3 cptmd4*/ revenue_cd IN_2400_SV202_2_SL_HCPCS formcd /*HS 565133 - modified drop variables */
CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt mcdnetalwamt mcrnetalwamt NETALWAMT_old mcrcovunt covunt unitct alwunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr);/*ALM#5160 - added*/
if _a;
by clamno lineno;
run;	
/* HS 542635 - Added Proc delete statement */

Proc delete data=inst_final1;run;
Proc delete data=inst;run;
%end;
%end;
%mend;
%temp;


%macro temp_OP;		/* B-97137 new outpatient rollup */
%nobs(inst_OP);
 %if &nobs = 0 %then %do;
  data inst_final2_OP inst_OP_roll_skip; /* B-168796 skip */
  	set inst_OP;
	length rollup_line $1000.;
	rollup_line  = '   ';     
  Run; 
 %end;
 %else %do;

data inst_OP(drop=rollup_line) inst_OP_roll_skip; /* B-168796 skip */
set inst_OP;
if "&edit_type_Rollup_Reject." eq "H" and typecode in (&enctyp_code_Rollup_Reject.) then output inst_OP;
else do;
	length rollup_line $1000.;
	rollup_line  = '';
	output inst_OP_roll_skip;
end;
run;

Proc sort data=inst_OP;by clamno membno svcdat provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd NDCSVC cptmd_roll; /* B-103975 new rollup */
Run;

Proc transpose data=inst_OP out=inst_1_OP(drop=_name_ _LABEL_) prefix=rollup;
	by clamno membno svcdat provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd NDCSVC cptmd_roll;	/* B-103975 new rollup */
	var lineno;
run;

%macro chkroll3; *B-168796 added macro;
%let dsid = %sysfunc(open(inst_1_OP,i));
%let vcnt = %eval(%sysfunc(attrn(&dsid.,nvar)));	
%LET rc = %SYSFUNC(CLOSE(&dsid.));
%put &vcnt; 
%if &vcnt. gt 9 %then %do;
data inst_1_OP;
set inst_1_OP;
rename rollup1=lineno;
run;
%end;
%else %do;
data inst_1_OP;
set inst_1_OP(keep=clamno membno svcdat provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd NDCSVC cptmd_roll);
lineno= '   ';
run;
%end;
%mend chkroll3;
%chkroll3;

%let fid = %sysfunc(open(inst_1_OP,i));
%let cnt = %eval(%sysfunc(attrn(&fid.,nvar))-9);	
%put &cnt;
%LET rc = %SYSFUNC(CLOSE(&fid.));
%if &cnt.=1 %then %do;
	data inst_final2_OP;
	      set inst_OP;
	      length rollup_line $1000.;
	      rollup_line  = '   ';      
	run;
%end;
%else %do;

	data inst_1_1;
	      set inst_1_OP;
	      length rollup_line $1000.;
	      /* SAS2AWS2: ReplacedFunctionCatx */
	      rollup_line  = catx(',',of rollup2-rollup&cnt.);
	run;
	Proc summary data=inst_OP nway missing;
    var CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt  mcdnetalwamt mcrnetalwamt NETALWAMT_old mcrcovunt covunt unitct alwunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr;/*ALM#5160 - added*/
	class clamno membno svcdat provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd NDCSVC cptmd_roll;	/* B-103975 new rollup */
output out=inst_SUM(drop=_TYPE_ _FREQ_) sum=;
run;
data inst_final_OP;
merge inst_1_1(in=_a) inst_SUM(in=_b);
if _a;
by clamno membno svcdat provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd NDCSVC cptmd_roll;	/* B-103975 new rollup */
run;

Proc delete data=inst_1_1;run;
Proc delete data=inst_SUM;run;

Data inst_final1_OP;
set inst_final_OP;
run;

Proc delete data=inst_final_OP;run;

Proc sort data=inst_final1_OP;by clamno lineno;run;
proc sort data=inst_OP;by clamno lineno;run;

data inst_final2_OP(drop=cptmd_roll); /* B-103975 new rollup */
merge  inst_final1_OP(in=_a) 
inst_OP(in=_b drop=  membno svcdat provno revenue_cd IN_2400_SV202_2_SL_HCPCS formcd NDCSVC cptmd_roll /* B-103975 new rollup */
CLAAMT NETALWAMT dedamt coiamt COPAMT ALWAMT nonamt rcvamt dscamt topamt mcdnetalwamt mcrnetalwamt NETALWAMT_old mcrcovunt covunt unitct alwunt mcdnetalwamt_old mcrnetalwamt_old netalwamt_mcr topamt_mcr);
if _a;
by clamno lineno;
run;	

Proc delete data=inst_final1_OP;run;
Proc delete data=inst_OP;run;
%end;
%end;
%mend;
%temp_OP;


data &RGA191..encountersY_T_&compno._&run_id.;
set prof_final2 inst_final2 inst_final2_OP
	prof_roll_skip inst_roll_skip inst_OP_roll_skip; /* B-168796 */
run;

/* HS 542635 - Added Proc delete statement */
Proc delete data=prof_final2;run;
Proc delete data=inst_final2;run;
Proc delete data=inst_final2_OP;run;

*Rsubmit;
proc sort data= &RGA191..encountersY_&compno._&run_id.(keep=clamno lineno claimfrq icn typecode)
	out=before_Rollup;
	by clamno lineno;
run;
Proc sql noprint;
Select count(distinct(clamno)) into :ttt from &RGA191..encountersY_&compno._&run_id. ;	
quit;
/* HS 542635 - Added Proc delete statement */
 /* Proc delete data=&RGA191..encountersY_&compno._&run_id.;run; VVKR Commented -- B-75041 */

proc sort data= &RGA191..encountersY_T_&compno._&run_id.(keep=clamno lineno)
	out=after_Rollup;
	by clamno lineno;
run;
Data Rollup_claims;
	merge before_Rollup(in=_a)  after_Rollup(in=_b);
	if "&edit_type_Rollup_Reject." eq "H" and typecode in (&enctyp_code_Rollup_Reject.); /* B-168796 */
	if _a and not _b; 
	by clamno lineno;
run; 
Data Rollup_claims;
length edit_chk_type $100. edit_code $5. encapdfld $50.;
set Rollup_claims;
		edit_chk_type='Rollup Reject';
		encapdfld='Rollup_Reject';
		Recsubind='N';
		edit_code = '99970'; /* HS 567501 - added */                       
run;
Proc sort data=Rollup_claims nodupkey;by clamno lineno;run;
%macro temp;
Data &RGA191..encountersY1_&compno._&run_id.;
set &RGA191..encountersY_T_&compno._&run_id.;
  %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
	CAS15=0;
	/*if claamt  <  mcdnetalwamt then Claamt=mcdnetalwamt;
	else if claamt < mcrnetalwamt then claamt=mcrnetalwamt;*/

	if (claamt  > mcdnetalwamt)  then do;
	   CAS12=sum(claamt,-sum(mcdnetalwamt,mcrnetalwamt)); 
	   CAS12=round(CAS12,.01);
	end;
	else do;
	   CAS12=0;
	end;
  %end;
  %else %do;
	CAS15=sum(nonamt,rcvamt);
	CAS15=round(CAS15,.01);
    /*if claamt  <=  netalwamt then Claamt=NETALWAMT;*/
	if (claamt  > NETALWAMT)  then do;
	   CAS12=sum(claamt,-sum(NETALWAMT,dedamt,coiamt,COPAMT,CAS15,dscamt)); 
	   CAS12=round(CAS12,.01);
	end;
	else do;
	   CAS12=0;
	end;
  %end;
Run;
/* HS 542635 - Added Proc delete statement */
Proc delete data=&RGA191..encountersY_T_&compno._&run_id.;
run;

proc sort data = &RGA191..encountersY1_&compno._&run_id.
		  out = &RGA191..encountersY3_&Compno._&run_id.;
	      by clamno descending lineno ;
Run;
/* HS 542635 - Added Proc delete statement */

Proc delete data= &RGA191..encountersY1_&compno._&run_id.;
run;
/*CR#CHG0036203 - moved Inpatient DG line logic before roll up logic  */
/* 
Data &RGA191..encountersY4_&Compno._&run_id.(Drop = edit_chk_type edit_code encapdfld  Recsubind)											
	&RGA191..Reject_DG_I_&Compno._&run_id.(keep = clamno lineno  edit_chk_type encapdfld  Recsubind typecode icn claimfrq  edit_code);	
	length edit_chk_type $100. edit_code $5. encapdfld $50.;
	set &RGA191..encountersY3_&Compno._&run_id. 
	;
	by clamno descending lineno;
	retain 	
	 	CLAAMT_dg_line 0
		NETALWAMT_dg_line 0
		ALWAMT_dg_line 0
		dedamt_dg_line 0
		coiamt_dg_line 0
		COPAMT_dg_line 0
		CAS12_dg_line 0
		CAS15_dg_line 0
		dscamt_dg_line 0
		mcdnetalwamt_dg_line 0
		mcrnetalwamt_dg_line 0
		svccod_dg_line  '      '
		Inp_dg_line_flag 'N'
		;	
			
	if first.clamno then 
	do;
		CLAAMT_dg_line=0;
		NETALWAMT_dg_line=0;
		ALWAMT_dg_line=0;
		dedamt_dg_line=0;
		coiamt_dg_line=0;
		COPAMT_dg_line=0;
		CAS12_dg_line=0;
		CAS15_dg_line=0;
		dscamt_dg_line=0;		
		mcdnetalwamt_dg_line=0;
		mcrnetalwamt_dg_line=0;	
		svccod_dg_line = '      ';
    	Inp_dg_line_flag = "N" ;
   end;
		
		if  typecode = "I" and svcstat = "PA" and svctyp = 'DG' then /* HS 569650 kv - added svctyp 
		do;
	
		CLAAMT_dg_line+CLAAMT;
		NETALWAMT_dg_line+NETALWAMT;
		ALWAMT_dg_line+ALWAMT;
		dedamt_dg_line+dedamt;
		coiamt_dg_line+coiamt;
		COPAMT_dg_line+COPAMT;
		CAS12_dg_line+CAS12;
		CAS15_dg_line+CAS15;
		dscamt_dg_line+dscamt;	
		mcdnetalwamt_dg_line+mcdnetalwamt;
	    mcrnetalwamt_dg_line+mcrnetalwamt;	
		end;
	
		if svctyp = 'DG' and typecode = "I" /*and svcstat = "PA" then /* HS 569650 kv - added svcstat *//* HS 565133 kv - commented svcstat 
		do;
			svccod_dg_line = svccod ;
			Inp_dg_line_flag = "Y";   
			Inp_dg_line_cnt + 1 ;			
			edit_chk_type = 'Inpatient DG Line Reject';
			edit_code = 'EIPDG';
			encapdfld = 'Inpatient_DG_Line_Reject';
			Recsubind = 'N';
			output &RGA191..Reject_DG_I_&Compno._&run_id. ;			
		delete;
		end;
				call symput('Inp_dg_line_cnt',kcompress(Inp_dg_line_cnt));
		output &RGA191..encountersY4_&Compno._&run_id. ;	
	run;
*/ /*CR#CHG0036203 - moved Inpatient DG line logic before roll up logic  */

/*CR#CHG0036203 - Added  */
data &RGA191..encountersY4_&Compno._&run_id.;
set &RGA191..encountersY3_&Compno._&run_id.;
run;

/* HS 542635 - Added Proc delete statement */

Proc delete data=&RGA191..encountersY3_&Compno._&run_id. ;
run;

proc sort data = &RGA191..encountersY4_&Compno._&run_id. ;	
by clamno lineno ;
run;

Data &RGA191..encountersY4B0_&Compno._&run_id.;
Set  &RGA191..encountersY4_&Compno._&run_id.;
 by clamno lineno ;
/* ALM# 5160 - Commented old CAS mapping logic */
/*if typecode in ('P') then*/
/*	do;*/
/*           %If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;*/
/*		   	PR_2400_SV102_S_CHG_AMT=CLAAMT;*/
/*			PR_2430_SVD02_SL_PAID_AMT=MCDNETALWAMT;	*/
/*			PR_2430_CAS03=MCRNETALWAMT;*/
/*			PR_2430_CAS06=0;*/
/*			PR_2430_CAS09=0 ;  */
/*			PR_2430_CAS12=CAS12;		*/
/*			PR_2430_CAS15=0;*/
/*			PR_2430_CAS18=0 ;*/
/*			PR_2430_CAS21=0;	*/
/*		   %end;*/
/*		   %else %do;*/
/*			PR_2400_SV102_S_CHG_AMT=CLAAMT;*/
/*			PR_2430_SVD02_SL_PAID_AMT=NETALWAMT;	*/
/*			PR_2430_CAS03=dedamt;*/
/*			PR_2430_CAS06=coiamt;*/
/*			PR_2430_CAS09=COPAMT ;  */
/*			PR_2430_CAS12=CAS12;		*/
/*			PR_2430_CAS15=CAS15;*/
/*			PR_2430_CAS18=dscamt ;*/
/*			PR_2430_CAS21=0;	*/
/*		  %end;*/
/*end;*/
/*Else if typecode in('I','O') then*/
/*   do;*/
/*		 retain rev_flag 'Y'  ;  */
/*		 if first.clamno then rev_flag = "Y"  ;  */
/*		 if Inp_dg_line_flag = "Y" and svctyp = "RV" and rev_flag = "Y"  then */
/*		  	do;	     %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;*/
/*						IN_2400_SV203_SL_CHG_AMT =sum(CLAAMT,CLAAMT_dg_line) ;*/
/*						IN_2430_SVD02_SL_PAID_AM=sum(MCDNETALWAMT,MCDNETALWAMT_dg_line);	*/
/*						IN_2430_CAS03=sum(MCRNETALWAMT,MCRNETALWAMT_dg_line);*/
/*						IN_2430_CAS06=0;*/
/*				 		IN_2430_CAS09=0; */
/* 				 		IN_2430_CAS12=sum(CAS12,CAS12_dg_line);						*/
/*				 		IN_2430_CAS15=0;*/
/*				 		IN_2430_CAS18=0;*/
/*						IN_2430_CAS21=0 ;		*/
/* 						Temp_flag=1;		*/
/*					 %end;*/
/*					 %else %do;*/
/*  				    	IN_2400_SV203_SL_CHG_AMT =sum(CLAAMT,CLAAMT_dg_line) ;*/
/*						IN_2430_SVD02_SL_PAID_AM=sum(NETALWAMT,NETALWAMT_dg_line);	*/
/*						IN_2430_CAS03=sum(dedamt,dedamt_dg_line);*/
/*						IN_2430_CAS06=sum(coiamt,coiamt_dg_line);*/
/*				 		IN_2430_CAS09=sum(COPAMT,COPAMT_dg_line) ; */
/* 				 		IN_2430_CAS12=sum(CAS12,CAS12_dg_line);						*/
/*				 		IN_2430_CAS15=sum(CAS15,CAS15_dg_line);*/
/*				 		IN_2430_CAS18=sum(dscamt,dscamt_dg_line) ;*/
/*						IN_2430_CAS21=0 ;		*/
/* 						Temp_flag=1;		*/
/*					%end;*/
/*			 rev_flag = "N" ;			 */
/*			end;*/
/*			else if Inp_dg_line_flag = "Y" and svctyp = "RV"   then*/
/*			do;	 */
/*					%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;*/
/*						IN_2400_SV203_SL_CHG_AMT =CLAAMT ;*/
/*						IN_2430_SVD02_SL_PAID_AM=MCDNETALWAMT;	*/
/*						IN_2430_CAS03=MCRNETALWAMT;*/
/*						IN_2430_CAS06=0;*/
/*				 		IN_2430_CAS09=0 ; */
/* 				 		IN_2430_CAS12=CAS12;						*/
/*				 		IN_2430_CAS15=0;*/
/*				 		IN_2430_CAS18=0 ;*/
/*						IN_2430_CAS21=0 ;		*/
/*					%end;*/
/*					%else %do;	*/
/*						IN_2400_SV203_SL_CHG_AMT =CLAAMT ;*/
/*						IN_2430_SVD02_SL_PAID_AM=NETALWAMT;	*/
/*						IN_2430_CAS03=dedamt;*/
/*						IN_2430_CAS06=coiamt;*/
/*				 		IN_2430_CAS09=COPAMT ; */
/* 				 		IN_2430_CAS12=CAS12;						*/
/*				 		IN_2430_CAS15=CAS15;*/
/*				 		IN_2430_CAS18=dscamt ;*/
/*						IN_2430_CAS21=0 ;		*/
/*                   %end;	*/
/*			end;*/
/*		   	else*/
/*		    do;			*/
/*				  %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;*/
/*				  	IN_2400_SV203_SL_CHG_AMT =CLAAMT ;*/
/*					IN_2430_SVD02_SL_PAID_AM=MCDNETALWAMT;	*/
/*					IN_2430_CAS03=MCRNETALWAMT;*/
/*					IN_2430_CAS06=0;*/
/*				 	IN_2430_CAS09=0 ; */
/* 				 	IN_2430_CAS12=CAS12;*/
/*				 	IN_2430_CAS15=0;*/
/*				 	IN_2430_CAS18=0;*/
/*					IN_2430_CAS21=0 ;*/
/*				  %end;	*/
/*				  %else %do;*/
/*					IN_2400_SV203_SL_CHG_AMT =CLAAMT ;*/
/*					IN_2430_SVD02_SL_PAID_AM=NETALWAMT;	*/
/*					IN_2430_CAS03=dedamt;*/
/*					IN_2430_CAS06=coiamt;*/
/*				 	IN_2430_CAS09=COPAMT ; */
/* 				 	IN_2430_CAS12=CAS12;*/
/*				 	IN_2430_CAS15=CAS15;*/
/*				 	IN_2430_CAS18=dscamt;*/
/*					IN_2430_CAS21=0 ;*/
/*				%end;		*/
/*			 end;*/
/*	 end;*/
/* ALM# 5160 - added new CAS mapping logic */

/*B-120951 Included _Var to all CAS segment variable */ /*[B-272655] updated the CAPIND='C' CAS segment for company 34 */
%if  &compno.=01 and &LOB. =DSNP  %then %do;
%dsnpvar;
%end;
%else %do;
if typecode in ('P') and capind = 'F' then
	do;
        %If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
			PR_2400_SV104_S_LINE_CNT=UNITCT;
			PR_2430_SVD05_PD_UNITS=COVUNT;
		  PR_2400_SV102_S_CHG_AMT=CLAAMT;
			PR_2430_SVD02_SL_PAID_AMT=mcdnetalwamt ; 
			/*B-222679 Updated logic - Starts here*/
			/*PR_2430_CAS03_Var=mcrnetalwamt;
			PR_2430_CAS04_Var=MCRCOVUNT;
			PR_2430_CAS07_Var=SUM(claamt,-SUM(mcrnetalwamt, mcdnetalwamt));
			PR_2430_CAS08_Var=SUM(UNITCT,-COVUNT);*/
			PR_2430_CAS03_Var=SUM(claamt-mcdnetalwamt);
			PR_2430_CAS04_Var=MCDCOVUNT;
			PR_2430_CAS07_Var=0;
			PR_2430_CAS08_Var=0;			
			
			
			PR_2430_SVD05_PD_UNITS_MCR=COVUNT;
			PR_2430_SVD02_SL_PAID_AMT_MCR=mcrnetalwamt ;	
			PR_2430_CAS03_MCR_Var=SUM(claamt-mcrnetalwamt);
			PR_2430_CAS04_MCR_Var=MCRCOVUNT;
			PR_2430_CAS07_MCR_Var=0;
			PR_2430_CAS08_MCR_Var=0;
      /*B-222679 Updated logic - Ends here*/
		%end;
		%else %do;
			PR_2400_SV104_S_LINE_CNT=UNITCT;
			PR_2430_SVD05_PD_UNITS=COVUNT;
			PR_2400_SV102_S_CHG_AMT=CLAAMT;
			PR_2430_SVD02_SL_PAID_AMT=NETALWAMT;	
			PR_2430_CAS03_Var=sum(PR_2400_SV102_S_CHG_AMT,-PR_2430_SVD02_SL_PAID_AMT);
			PR_2430_CAS04_Var=SUM(PR_2400_SV104_S_LINE_CNT,-PR_2430_SVD05_PD_UNITS);
			PR_2430_CAS07_Var=0;
			PR_2430_CAS08_Var=0;
			
			/*B-222679 Updated logic - Starts here*/
			PR_2430_SVD05_PD_UNITS_MCR=0;
			PR_2430_SVD02_SL_PAID_AMT_MCR=0;
			PR_2430_CAS03_MCR_Var=0;
			PR_2430_CAS04_MCR_Var=0;
			PR_2430_CAS07_MCR_Var=0;
			PR_2430_CAS08_MCR_Var=0;
			/*B-222679 Updated logic - Ends here*/	
		 %end;
end;
else if typecode in ('P') and capind = 'C' then
	do;
        %If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
			PR_2400_SV104_S_LINE_CNT=UNITCT;
			PR_2430_SVD05_PD_UNITS=COVUNT;
		  PR_2400_SV102_S_CHG_AMT=CLAAMT;
			/*PR_2430_SVD02_SL_PAID_AMT=mcdnetalwamt;	*/
			PR_2430_SVD02_SL_PAID_AMT=0;	
			/*B-222679 Updated logic - Starts here*/
			/*PR_2430_CAS03_Var=mcrnetalwamt_old;
			PR_2430_CAS04_Var=MCRCOVUNT; 
			PR_2430_CAS07_Var=SUM(claamt,-SUM(mcrnetalwamt_old, mcdnetalwamt_old));
			PR_2430_CAS08_Var=SUM(UNITCT,-COVUNT);	*/		
			/*PR_2430_CAS03_Var=SUM(claamt-mcdnetalwamt);*/
			PR_2430_CAS03_Var=mcdnetalwamt;			
			PR_2430_CAS04_Var=MCDCOVUNT;
			PR_2430_CAS07_Var=SUM(claamt-mcdnetalwamt);;
			PR_2430_CAS08_Var=SUM(COVUNT-MCDCOVUNT);	
			
			
			PR_2430_SVD05_PD_UNITS_MCR=COVUNT;
			/*PR_2430_SVD02_SL_PAID_AMT_MCR=mcrnetalwamt_old;	
			PR_2430_CAS03_MCR_Var=SUM(claamt-mcrnetalwamt_old);*/
			PR_2430_SVD02_SL_PAID_AMT_MCR=0;
			PR_2430_CAS03_MCR_Var=mcrnetalwamt_old;
			PR_2430_CAS04_MCR_Var=MCRCOVUNT;
			PR_2430_CAS07_MCR_Var=SUM(claamt-mcrnetalwamt_old);
			PR_2430_CAS08_MCR_Var=SUM(COVUNT-MCRCOVUNT);
			/*B-222679 Updated logic - Ends here*/
			
		%end;
		%else %do;
			PR_2400_SV104_S_LINE_CNT=UNITCT;
			PR_2430_SVD05_PD_UNITS=COVUNT;
			PR_2400_SV102_S_CHG_AMT=CLAAMT;
			PR_2430_SVD02_SL_PAID_AMT=NETALWAMT;	
			PR_2430_CAS03_Var=NETALWAMT_old;
			PR_2430_CAS04_Var=ALWUNT;
			/*B-329832 - DS - only for cap c and topamt > 0 PR_2430_CAS07_Var updated with new logic*/
			if capind = 'C' and topamt > 0 then do; 
				PR_2430_CAS07_Var=sum(PR_2400_SV102_S_CHG_AMT,-(sum(PR_2430_CAS03_Var,PR_2430_SVD02_SL_PAID_AMT)));
			end;
			else do;
				PR_2430_CAS07_Var=sum(PR_2400_SV102_S_CHG_AMT,-PR_2430_CAS03_Var);
			end;
			PR_2430_CAS08_Var=SUM(PR_2400_SV104_S_LINE_CNT,-PR_2430_CAS04_Var);
			
			/*B-222679 Updated logic - Starts here*/
			PR_2430_SVD05_PD_UNITS_MCR=0;
			PR_2430_SVD02_SL_PAID_AMT_MCR=0;
			PR_2430_CAS03_MCR_Var=0;
			PR_2430_CAS04_MCR_Var=0;
			PR_2430_CAS07_MCR_Var=0;
			PR_2430_CAS08_MCR_Var=0;
			/*B-222679 Updated logic - Ends here*/
		%end;
end;
Else if typecode in('I') and capind = 'F' then
   do;
		 retain rev_flag 'Y'  ;  
		 if first.clamno then rev_flag = "Y"  ;  
		 if Inp_dg_line_flag = "Y" and svctyp = "RV" and rev_flag = "Y"  then 
		  	do;	   
			%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
						IN_2400_SV205_SL_COUNT=sum(UNITCT,UNITCT_dg_line);
			 			IN_2430_SVD05_PD_UNITS=sum(COVUNT,COVUNT_dg_line);
						IN_2400_SV203_SL_CHG_AMT =sum(CLAAMT,CLAAMT_dg_line) ;
						IN_2430_SVD02_SL_PAID_AM=sum(MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line);	
						/*B-222679 Updated logic - Starts here*/
						/*IN_2430_CAS03_Var=sum(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line);
						IN_2430_CAS04_Var=sum(MCRCOVUNT,MCRCOVUNT_dg_line);	
						IN_2430_CAS07_Var=SUM(SUM(CLAAMT,CLAAMT_dg_line),-SUM(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line,MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line));
						IN_2430_CAS08_Var=SUM(SUM(UNITCT,UNITCT_dg_line),-SUM(COVUNT,COVUNT_dg_line));	*/
						
						IN_2430_CAS03_Var=IN_2400_SV203_SL_CHG_AMT-sum(MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line);
						IN_2430_CAS04_Var=sum(MCDCOVUNT,MCDCOVUNT_dg_line);	
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;
						
						Temp_flag=1;	
					
						IN_2430_SVD05_PD_UNITS_MCR=sum(COVUNT,COVUNT_dg_line);
						IN_2430_SVD02_SL_PAID_AM_MCR=sum(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line);;	
						IN_2430_CAS03_MCR_Var=IN_2400_SV203_SL_CHG_AMT-sum(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line);
						IN_2430_CAS04_MCR_Var=sum(MCRCOVUNT,MCRCOVUNT_dg_line);
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;
						/*B-222679 Updated logic - Ends here*/

			%end;
			%else %do;
						IN_2400_SV205_SL_COUNT=sum(UNITCT,UNITCT_dg_line);
			 			IN_2430_SVD05_PD_UNITS=sum(COVUNT,COVUNT_dg_line);
						IN_2400_SV203_SL_CHG_AMT =sum(CLAAMT,CLAAMT_dg_line) ;
						IN_2430_SVD02_SL_PAID_AM=sum(NETALWAMT_old,NETALWAMT_old_dg_line);	
						IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
						IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;
 						Temp_flag=1;
 						
 						/*B-222679 Updated logic - Starts here*/
 						IN_2430_SVD05_PD_UNITS_MCR=0;
						IN_2430_SVD02_SL_PAID_AM_MCR=0;
						IN_2430_CAS03_MCR_Var=0;
						IN_2430_CAS04_MCR_Var=0;
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;	
						/*B-222679 Updated logic - Ends here*/
		
			%end;
			 rev_flag = "N" ;	

			end;
			else if Inp_dg_line_flag = "Y" and svctyp = "RV"   then
			do;	 
					%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
						IN_2400_SV205_SL_COUNT=UNITCT;
			 			IN_2430_SVD05_PD_UNITS=COVUNT;
						IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
						IN_2430_SVD02_SL_PAID_AM=MCDNETALWAMT_old;
						/*B-222679 Updated logic - Starts here*/	
						/*IN_2430_CAS03_Var=MCRNETALWAMT_old;
						IN_2430_CAS04_Var=MCRCOVUNT;
						IN_2430_CAS07_Var=SUM(CLAAMT,-SUM(mcrnetalwamt_old, mcdnetalwamt_old));
						IN_2430_CAS08_Var=SUM(UNITCT,-COVUNT);*/
						IN_2430_CAS03_Var=SUM(CLAAMT-MCDNETALWAMT_old);
						IN_2430_CAS04_Var=MCDCOVUNT;
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;

						
						IN_2430_SVD05_PD_UNITS_MCR=COVUNT;
						IN_2430_SVD02_SL_PAID_AM_MCR=MCRNETALWAMT_old;	
						IN_2430_CAS03_MCR_Var=SUM(CLAAMT-MCRNETALWAMT_old);
						IN_2430_CAS04_MCR_Var=MCRCOVUNT;
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;
						/*B-222679 Updated logic - Ends here*/

					%end;
					%else %do;	
						IN_2400_SV205_SL_COUNT=UNITCT;
			 			IN_2430_SVD05_PD_UNITS=COVUNT;
						IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
						IN_2430_SVD02_SL_PAID_AM=NETALWAMT_old;	
						IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
						IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;
						
						/*B-222679 Updated logic - Starts here*/
						IN_2430_SVD05_PD_UNITS_MCR=0;
						IN_2430_SVD02_SL_PAID_AM_MCR=0;
						IN_2430_CAS03_MCR_Var=0;
						IN_2430_CAS04_MCR_Var=0;
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;
						/*B-222679 Updated logic - Ends here*/
                   %end;	
			end;
		   	else
		    do;			
				  %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
					IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					IN_2430_SVD02_SL_PAID_AM=MCDNETALWAMT_old;
					/*B-222679 Updated logic - Starts here*/	
					/*IN_2430_CAS03_Var=MCRNETALWAMT_old;
					IN_2430_CAS04_Var=MCRCOVUNT;
					IN_2430_CAS07_Var=SUM(CLAAMT,-SUM(mcrnetalwamt_old, mcdnetalwamt_old));
					IN_2430_CAS08_Var=SUM(UNITCT,-COVUNT);*/					
					IN_2430_CAS03_Var=SUM(CLAAMT-MCDNETALWAMT_old);
					IN_2430_CAS04_Var=MCDCOVUNT;
					IN_2430_CAS07_Var=0;
					IN_2430_CAS08_Var=0;
					
					IN_2430_SVD05_PD_UNITS_MCR=COVUNT;
					IN_2430_SVD02_SL_PAID_AM_MCR=MCRNETALWAMT_old;	
					IN_2430_CAS03_MCR_Var=SUM(CLAAMT-MCRNETALWAMT_old);
					IN_2430_CAS04_MCR_Var=MCRCOVUNT;
					IN_2430_CAS07_MCR_Var=0;
					IN_2430_CAS08_MCR_Var=0;
					/*B-222679 Updated logic - Ends here*/
						
				  %end;	
				  %else %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
					IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					IN_2430_SVD02_SL_PAID_AM=NETALWAMT_old;	
					IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
					IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
					IN_2430_CAS07_Var=0;
					IN_2430_CAS08_Var=0;
					
					/*B-222679 Updated logic - Starts here*/
					IN_2430_SVD05_PD_UNITS_MCR=0;
					IN_2430_SVD02_SL_PAID_AM_MCR=0;
					IN_2430_CAS03_MCR_Var=0;
					IN_2430_CAS04_MCR_Var=0;
					IN_2430_CAS07_MCR_Var=0;
					IN_2430_CAS08_MCR_Var=0;
					/*B-222679 Updated logic - Ends here*/
				%end;		
			 end;
end;
Else if typecode in('I') and capind = 'C' then
   do;
		 retain rev_flag 'Y'  ;  
		 if first.clamno then rev_flag = "Y"  ;  
		 if Inp_dg_line_flag = "Y" and svctyp = "RV" and rev_flag = "Y"  then 
		  	do;	     
			%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
						IN_2400_SV205_SL_COUNT=sum(UNITCT,UNITCT_dg_line);
			 			IN_2430_SVD05_PD_UNITS=sum(COVUNT,COVUNT_dg_line);
						IN_2400_SV203_SL_CHG_AMT =sum(CLAAMT,CLAAMT_dg_line) ;
						/*IN_2430_SVD02_SL_PAID_AM=sum(MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line);*/
						IN_2430_SVD02_SL_PAID_AM=0;
						/*B-222679 Updated logic - Starts here*/
						/*IN_2430_CAS03_Var=sum(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line);
						IN_2430_CAS04_Var=sum(MCRCOVUNT,MCRCOVUNT_dg_line);
						IN_2430_CAS07_Var=SUM(SUM(CLAAMT,CLAAMT_dg_line),-SUM(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line,MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line));
						IN_2430_CAS08_Var=SUM(SUM(UNITCT,UNITCT_dg_line),-SUM(COVUNT,COVUNT_dg_line));*/						
						/*IN_2430_CAS03_Var=SUM(SUM(CLAAMT,CLAAMT_dg_line),-sum(MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line));*/
						IN_2430_CAS03_Var=sum(MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line);
						IN_2430_CAS04_Var=sum(MCDCOVUNT,MCDCOVUNT_dg_line);
						IN_2430_CAS07_Var=SUM(SUM(CLAAMT,CLAAMT_dg_line),-sum(MCDNETALWAMT_old,MCDNETALWAMT_old_dg_line));;
						IN_2430_CAS08_Var=SUM(sum(COVUNT,COVUNT_dg_line),-sum(MCDCOVUNT,MCDCOVUNT_dg_line));
						
						IN_2430_SVD05_PD_UNITS_MCR=sum(COVUNT,COVUNT_dg_line);
						/*IN_2430_SVD02_SL_PAID_AM_MCR=sum(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line);*/	
						IN_2430_SVD02_SL_PAID_AM_MCR=0;
						IN_2430_CAS03_MCR_Var=sum(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line);
						IN_2430_CAS04_MCR_Var=sum(MCRCOVUNT,MCRCOVUNT_dg_line);
						IN_2430_CAS07_MCR_Var=SUM(SUM(CLAAMT,CLAAMT_dg_line),-sum(MCRNETALWAMT_old,MCRNETALWAMT_old_dg_line));;
						IN_2430_CAS08_MCR_Var=SUM(sum(COVUNT,COVUNT_dg_line),-sum(MCRCOVUNT,MCRCOVUNT_dg_line));
						/*B-222679 Updated logic - Ends here*/
			%end;
			%else %do;
						IN_2400_SV205_SL_COUNT=sum(UNITCT,UNITCT_dg_line);
			 			IN_2430_SVD05_PD_UNITS=sum(COVUNT,COVUNT_dg_line);
						IN_2400_SV203_SL_CHG_AMT =sum(CLAAMT,CLAAMT_dg_line) ;
						IN_2430_SVD02_SL_PAID_AM=sum(NETALWAMT_old,NETALWAMT_old_dg_line);	
						IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
						IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;
						
						/*B-222679 Updated logic - Starts here*/
						IN_2430_SVD05_PD_UNITS_MCR=0;
						IN_2430_SVD02_SL_PAID_AM_MCR=0;
						IN_2430_CAS03_MCR_Var=0;
						IN_2430_CAS04_MCR_Var=0;
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;
						/*B-222679 Updated logic - Ends here*/	
 						Temp_flag=1;		
			%end;
			 rev_flag = "N" ;	
			end;
			else if Inp_dg_line_flag = "Y" and svctyp = "RV"   then
			do;	 
					%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
						IN_2400_SV205_SL_COUNT=UNITCT;
			 			IN_2430_SVD05_PD_UNITS=COVUNT;
						IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
						/*IN_2430_SVD02_SL_PAID_AM=mcdnetalwamt_old;*/
						IN_2430_SVD02_SL_PAID_AM=0;
						/*B-222679 Updated logic - Starts here*/	
						/*IN_2430_CAS03_Var=mcrnetalwamt_old;
						IN_2430_CAS04_Var=MCRCOVUNT;
						IN_2430_CAS07_Var=SUM(CLAAMT,-SUM(mcrnetalwamt_old, mcdnetalwamt_old));
						IN_2430_CAS08_Var=SUM(UNITCT,-COVUNT);	*/						

						IN_2430_CAS03_Var=mcdnetalwamt_old;
						IN_2430_CAS04_Var=MCDCOVUNT;
						IN_2430_CAS07_Var=CLAAMT-mcdnetalwamt_old;
						IN_2430_CAS08_Var=COVUNT-MCDCOVUNT;
						
						
						IN_2430_SVD05_PD_UNITS_MCR=COVUNT;
						IN_2430_SVD02_SL_PAID_AM_MCR=0;	
						IN_2430_CAS03_MCR_Var=mcrnetalwamt_old;
						IN_2430_CAS04_MCR_Var=MCRCOVUNT;
						IN_2430_CAS07_MCR_Var=CLAAMT-mcrnetalwamt_old;
						IN_2430_CAS08_MCR_Var=COVUNT-MCRCOVUNT;
						/*B-222679 Updated logic - Ends here*/
					%end;
					%else %do;	
						IN_2400_SV205_SL_COUNT=UNITCT;
			 			IN_2430_SVD05_PD_UNITS=COVUNT;
						IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
						IN_2430_SVD02_SL_PAID_AM=NETALWAMT_old;	
						IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
						IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;
						
						/*B-222679 Updated logic - Starts here*/
						IN_2430_SVD05_PD_UNITS_MCR=0;
						IN_2430_SVD02_SL_PAID_AM_MCR=0;
						IN_2430_CAS03_MCR_Var=0;
						IN_2430_CAS04_MCR_Var=0;
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;
						/*B-222679 Updated logic - Ends here*/
                   %end;	
			end;
		   	else
		    do;			
				  %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
				  	IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					/*IN_2430_SVD02_SL_PAID_AM=mcdnetalwamt_old;*/
					IN_2430_SVD02_SL_PAID_AM=0;
					/*B-222679 Updated logic - Starts here*/	
					/*IN_2430_CAS03_Var=mcrnetalwamt_old;
					IN_2430_CAS04_Var=MCRCOVUNT;
					IN_2430_CAS07_Var=SUM(CLAAMT,-SUM(mcrnetalwamt_old, mcdnetalwamt_old));
					IN_2430_CAS08_Var=SUM(UNITCT,-COVUNT);	*/
					
	
					IN_2430_CAS03_Var=mcdnetalwamt_old;
					IN_2430_CAS04_Var=MCDCOVUNT;
					IN_2430_CAS07_Var=CLAAMT-mcdnetalwamt_old;
					IN_2430_CAS08_Var=COVUNT-MCDCOVUNT;
					
					
					IN_2430_SVD05_PD_UNITS_MCR=COVUNT;
					IN_2430_SVD02_SL_PAID_AM_MCR=0;	
					IN_2430_CAS03_MCR_Var=mcrnetalwamt_old;
					IN_2430_CAS04_MCR_Var=MCRCOVUNT;
					IN_2430_CAS07_MCR_Var=CLAAMT-mcrnetalwamt_old;
					IN_2430_CAS08_MCR_Var=COVUNT-MCRCOVUNT;
					/*B-222679 Updated logic - Ends here*/
				  %end;	
				  %else %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
					IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					IN_2430_SVD02_SL_PAID_AM=NETALWAMT_old;	
					IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
					IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
					IN_2430_CAS07_Var=0;
					IN_2430_CAS08_Var=0;
					
					/*B-222679 Updated logic - Starts here*/
					IN_2430_SVD05_PD_UNITS_MCR=0;
					IN_2430_SVD02_SL_PAID_AM_MCR=0;
					IN_2430_CAS03_MCR_Var=0;
					IN_2430_CAS04_MCR_Var=0;
					IN_2430_CAS07_MCR_Var=0;
					IN_2430_CAS08_MCR_Var=0;
					/*B-222679 Updated logic - Ends here*/
				%end;		
			 end;
end;
Else if typecode in('O') and capind = 'F' then
   do;
	 %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
				  	IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					IN_2430_SVD02_SL_PAID_AM=MCDNETALWAMT;
					/*B-222679 Updated logic - Starts here*/	
					/*IN_2430_CAS03_Var=MCRNETALWAMT;
					IN_2430_CAS04_Var=MCRCOVUNT;
					IN_2430_CAS07_Var=SUM(CLAAMT,-SUM(mcrnetalwamt, mcdnetalwamt));
					IN_2430_CAS08_Var=SUM(UNITCT,-COVUNT);	*/
					IN_2430_CAS03_Var=CLAAMT-MCDNETALWAMT;
					IN_2430_CAS04_Var=MCDCOVUNT;
					IN_2430_CAS07_Var=0;
					IN_2430_CAS08_Var=0;
					
					
					IN_2430_SVD05_PD_UNITS_MCR=COVUNT;
					IN_2430_SVD02_SL_PAID_AM_MCR=MCRNETALWAMT;	
					IN_2430_CAS03_MCR_Var=CLAAMT- MCRNETALWAMT;
					IN_2430_CAS04_MCR_Var=MCRCOVUNT;
					IN_2430_CAS07_MCR_Var=0;
					IN_2430_CAS08_MCR_Var=0;
					/*B-222679 Updated logic - Ends here*/
				  %end;	
				  %else %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
					IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					IN_2430_SVD02_SL_PAID_AM=NETALWAMT;	
					IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
					IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
					IN_2430_CAS07_Var=0;
					IN_2430_CAS08_Var=0;
					
					/*B-222679 Updated logic - Starts here*/
					IN_2430_SVD05_PD_UNITS_MCR=0;
					IN_2430_SVD02_SL_PAID_AM_MCR=0;
					IN_2430_CAS03_MCR_Var=0;
					IN_2430_CAS04_MCR_Var=0;
					IN_2430_CAS07_MCR_Var=0;
					IN_2430_CAS08_MCR_Var=0;
					/*B-222679 Updated logic - Ends here*/
				%end;
 END;
Else if typecode in('O') and capind = 'C' then
   do;			
				  %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
				  	IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					/*IN_2430_SVD02_SL_PAID_AM=mcdnetalwamt;	*/
					IN_2430_SVD02_SL_PAID_AM=0;	
					/*B-222679 Updated logic - Starts here*/
					/*IN_2430_CAS03_Var=mcrnetalwamt_old;
					IN_2430_CAS04_Var=MCRCOVUNT;
					IN_2430_CAS07_Var=SUM(CLAAMT,-SUM(mcrnetalwamt_old, mcdnetalwamt_old));
					IN_2430_CAS08_Var=SUM(UNITCT,-COVUNT);*/					
					IN_2430_CAS03_Var=mcdnetalwamt;
					IN_2430_CAS04_Var=MCDCOVUNT;
					IN_2430_CAS07_Var=claamt-mcdnetalwamt;
					IN_2430_CAS08_Var=COVUNT-MCDCOVUNT;
					
					IN_2430_SVD05_PD_UNITS_MCR=COVUNT;
					IN_2430_SVD02_SL_PAID_AM_MCR=0;	
					IN_2430_CAS03_MCR_Var=mcrnetalwamt_old;
					IN_2430_CAS04_MCR_Var=MCRCOVUNT;
					IN_2430_CAS07_MCR_Var=claamt-mcrnetalwamt_old;
					IN_2430_CAS08_MCR_Var=COVUNT-MCRCOVUNT;
					/*B-222679 Updated logic - Ends here*/
				  %end;	
				  %else %do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
					IN_2400_SV203_SL_CHG_AMT =CLAAMT ;
					IN_2430_SVD02_SL_PAID_AM=NETALWAMT;	
					IN_2430_CAS03_Var=NETALWAMT_old;
					IN_2430_CAS04_Var=ALWUNT;
					/*B-329832 - DS - only for cap c and topamt > 0 IN_2430_CAS07_Var updated with new logic*/
					if capind = 'C' and topamt > 0 then do;
						IN_2430_CAS07_Var=sum(IN_2400_SV203_SL_CHG_AMT,-(sum(IN_2430_CAS03_Var,IN_2430_SVD02_SL_PAID_AM)));
					end;
					else do;
						IN_2430_CAS07_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_CAS03_Var);
					end;
					/*IN_2430_CAS07_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_CAS03_Var);*/
					IN_2430_CAS08_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_CAS04_Var);
					
					/*B-222679 Updated logic - Starts here*/
					IN_2430_SVD05_PD_UNITS_MCR=0;
					IN_2430_SVD02_SL_PAID_AM_MCR=0;
					IN_2430_CAS03_MCR_Var=0;
					IN_2430_CAS04_MCR_Var=0;
					IN_2430_CAS07_MCR_Var=0;
					IN_2430_CAS08_MCR_Var=0;
					/*B-222679 Updated logic - Ends here*/
				%end;		
end;
%end;
%Mod_Cod ;
%Mod_Cod_mcr ;
%CPT_cod;
%CPT_COD_mcr;
Run;

/* HS 542635 - Added Proc delete statement */
Proc sql noprint ;
	Select count(distinct(clamno)) into :Reject_DG_I_1 from &RGA191..encountersY4_&Compno._&run_id. ;		
quit; 

Proc delete data=&RGA191..encountersY4_&Compno._&run_id. ;run;
Data &RGA191..Temp_DG ;
set &RGA191..encountersY4B0_&Compno._&run_id.;
where temp_flag=1 ;
temp_flag1=1;
keep clamno temp_flag temp_flag1  ;
run;
/* ALM# 5160 - commented ---- */
/* 
Data &RGA191..Temp_DG1;
merge &RGA191..Temp_DG(in=_a) &RGA191..encountersY4B0_&Compno._&run_id.(in=_b
keep =clamno IN_2400_SV203_SL_CHG_AMT				
				  IN_2430_SVD02_SL_PAID_AM
				  IN_2430_CAS03
				  IN_2430_CAS06
				  IN_2430_CAS09
 				  IN_2430_CAS12
				  IN_2430_CAS15
				  IN_2430_CAS18 
				  IN_2430_CAS21				  
				  );
if _a;
by clamno;
run;
Proc summary data=&RGA191..Temp_DG1 nway missing;
class clamno  temp_flag1;
var IN_2400_SV203_SL_CHG_AMT
				
				  IN_2430_SVD02_SL_PAID_AM
				  IN_2430_CAS03
				  IN_2430_CAS06
				  IN_2430_CAS09
 				  IN_2430_CAS12
				  IN_2430_CAS15
				  IN_2430_CAS18 IN_2430_CAS21 				
				  
				 ;
output out=&RGA191..Temp_DG_final(Rename=(
				  IN_2400_SV203_SL_CHG_AMT=IN_2400_SV203_SL_CHG_AMT_DG				 
				  IN_2430_SVD02_SL_PAID_AM=IN_2430_SVD02_SL_PAID_AM_DG
				  IN_2430_CAS03=IN_2430_CAS03_DG
				  IN_2430_CAS06=IN_2430_CAS06_DG
				  IN_2430_CAS09=IN_2430_CAS09_DG
 				  IN_2430_CAS12=IN_2430_CAS12_DG
				  IN_2430_CAS15=IN_2430_CAS15_DG
				  IN_2430_CAS18=IN_2430_CAS18_DG
				  IN_2430_CAS21=IN_2430_CAS21_DG								 				  
				  ) drop=_type_ _freq_)
sum()= ; 
run;
Data &RGA191..encountersY4e1_&Compno._&run_id.;
Merge &RGA191..encountersY4B0_&Compno._&run_id.(in=_a) &RGA191..Temp_DG_final(in=_b);
if _a;
by clamno;
if temp_flag=1 and temp_flag1=1 then do;
				  IN_2400_SV203_SL_CHG_AMT=IN_2400_SV203_SL_CHG_AMT_DG;				 
				  IN_2430_SVD02_SL_PAID_AM=IN_2430_SVD02_SL_PAID_AM_DG;
				  IN_2430_CAS03=IN_2430_CAS03_DG;
				  IN_2430_CAS06=IN_2430_CAS06_DG;
				  IN_2430_CAS09=IN_2430_CAS09_DG;				  			
				  IN_2430_CAS15=IN_2430_CAS15_DG;
				  IN_2430_CAS18=IN_2430_CAS18_DG;
				  IN_2430_CAS21=IN_2430_CAS21_DG;				 
				  IN_2430_CAS12=Round(sum(IN_2400_SV203_SL_CHG_AMT,-sum(IN_2430_SVD02_SL_PAID_AM,IN_2430_CAS03,IN_2430_CAS06,IN_2430_CAS09,IN_2430_CAS15,IN_2430_CAS18)),.01);	                 
				  	;				 
end;
else if missing(temp_flag)=1 and temp_flag1=1 then do;
				 
				  IN_2400_SV203_SL_CHG_AMT=0;				
				  IN_2430_SVD02_SL_PAID_AM=0;
				  IN_2430_CAS03=0;
				  IN_2430_CAS06=0;
				  IN_2430_CAS09=0;
 				  IN_2430_CAS12=0;
				  IN_2430_CAS15=0;
				  IN_2430_CAS18=0;
				  IN_2430_CAS21=0;
				 
				;
end;
run;
*/
/* ALM# 5160 - added new logic */
Data &RGA191..Temp_DG1;
merge &RGA191..Temp_DG(in=_a) &RGA191..encountersY4B0_&Compno._&run_id.(in=_b
keep =clamno IN_2400_SV203_SL_CHG_AMT				
				  IN_2430_SVD02_SL_PAID_AM
				  IN_2430_CAS03_Var
				  IN_2430_CAS04_Var
				  IN_2430_CAS07_Var	
				  IN_2430_CAS08_Var  IN_2400_SV205_SL_COUNT 	IN_2430_SVD05_PD_UNITS
				  
				  /*B-222679 Added*/
				  IN_2430_SVD02_SL_PAID_AM_MCR
				  IN_2430_CAS03_MCR_Var
				  IN_2430_CAS04_MCR_Var
				  IN_2430_CAS07_MCR_Var	
				  IN_2430_CAS08_MCR_Var  IN_2430_SVD05_PD_UNITS_MCR
				  );
if _a;
by clamno;
run;
Proc summary data=&RGA191..Temp_DG1 nway missing;
class clamno  temp_flag1;
var IN_2400_SV203_SL_CHG_AMT
				
				  IN_2430_SVD02_SL_PAID_AM
				  IN_2430_CAS03_Var
				  IN_2430_CAS04_Var
				  IN_2430_CAS07_Var	
				  IN_2430_CAS08_Var  IN_2400_SV205_SL_COUNT 	IN_2430_SVD05_PD_UNITS
				  
				  /*B-222679 Added*/
				  IN_2430_SVD02_SL_PAID_AM_MCR
				  IN_2430_CAS03_MCR_Var
				  IN_2430_CAS04_MCR_Var
				  IN_2430_CAS07_MCR_Var	
				  IN_2430_CAS08_MCR_Var  IN_2430_SVD05_PD_UNITS_MCR
				 ;
output out=&RGA191..Temp_DG_final(Rename=(
				  IN_2400_SV203_SL_CHG_AMT=IN_2400_SV203_SL_CHG_AMT_DG				 
				  IN_2430_SVD02_SL_PAID_AM=IN_2430_SVD02_SL_PAID_AM_DG
				  IN_2430_CAS03_Var=IN_2430_CAS03_DG
				  IN_2430_CAS04_Var=IN_2430_CAS04_DG
				  IN_2430_CAS07_Var=IN_2430_CAS07_DG	
				  IN_2430_CAS08_Var=IN_2430_CAS08_DG 
				  IN_2400_SV205_SL_COUNT=IN_2400_SV205_SL_COUNT_DG 	
				  IN_2430_SVD05_PD_UNITS=IN_2430_SVD05_PD_UNITS_DG
				  
				  /*B-222679 Added*/
				  IN_2430_SVD02_SL_PAID_AM_MCR=IN_2430_SVD02_SL_PAID_AM_MCR_DG
				  IN_2430_CAS03_MCR_Var=IN_2430_CAS03_MCR_DG
				  IN_2430_CAS04_MCR_Var=IN_2430_CAS04_MCR_DG
				  IN_2430_SVD05_PD_UNITS_MCR=IN_2430_SVD05_PD_UNITS_MCR_DG
				  ) drop=_type_ _freq_)
sum()= ; 
run;
Data &RGA191..encountersY4e1_&Compno._&run_id.;
Merge &RGA191..encountersY4B0_&Compno._&run_id.(in=_a) &RGA191..Temp_DG_final(in=_b);
if _a;
by clamno;
if temp_flag=1 and temp_flag1=1 then do;
				  IN_2400_SV203_SL_CHG_AMT=IN_2400_SV203_SL_CHG_AMT_DG;				 
				  IN_2430_SVD02_SL_PAID_AM=IN_2430_SVD02_SL_PAID_AM_DG;
				  IN_2430_CAS03_Var=IN_2430_CAS03_DG;
				  IN_2430_CAS04_Var=IN_2430_CAS04_DG;
				  IN_2430_CAS07_Var=IN_2430_CAS07_DG;	
				  IN_2430_CAS08_Var=IN_2430_CAS08_DG;  
 				  IN_2400_SV205_SL_COUNT=IN_2400_SV205_SL_COUNT_DG; 	
				  IN_2430_SVD05_PD_UNITS=IN_2430_SVD05_PD_UNITS_DG; 
				  
				  /*B-222679 Added*/
				  IN_2430_SVD02_SL_PAID_AM_MCR=IN_2430_SVD02_SL_PAID_AM_MCR_DG;
				  IN_2430_CAS03_MCR_Var=IN_2430_CAS03_MCR_DG;
				  IN_2430_CAS04_MCR_Var=IN_2430_CAS04_MCR_DG;
				  IN_2430_SVD05_PD_UNITS_MCR=IN_2430_SVD05_PD_UNITS_MCR_DG;
end;
else if missing(temp_flag)=1 and temp_flag1=1 then do;
				 
				  IN_2400_SV203_SL_CHG_AMT=0;				
				  IN_2430_SVD02_SL_PAID_AM=0;
				  IN_2430_CAS03_Var=0;
				  IN_2430_CAS04_Var=0;
				  IN_2430_CAS07_Var=0;	
				  IN_2430_CAS08_Var=0;
 				  IN_2400_SV205_SL_COUNT=0; 	
				  IN_2430_SVD05_PD_UNITS=0;

          /*B-222679 Added*/
				  IN_2430_SVD02_SL_PAID_AM_MCR=0;
				  IN_2430_CAS03_MCR_Var=0;
				  IN_2430_CAS04_MCR_Var=0;
				  IN_2430_SVD05_PD_UNITS_MCR=0;
end;
run;



%macro dg_rollup; /* B-274371 DG Rollup Line Logic Starts Here*/

proc sort data=&RGA191..encountersY4B0_&Compno._&run_id.;by clamno lineno;run;
proc sort data=&RGA191..Temp_DG;by clamno;run;

Data &RGA191..Temp_DG_1(keep=clamno lineno);
Merge &RGA191..encountersY4B0_&Compno._&run_id.(in=_a) 
	  &RGA191..Temp_DG(in=_b keep= clamno);
by clamno;
if _a and _b;
run;

Data &RGA191..Temp_DG_Reject(keep = clamno lineno) ;
set &RGA191..Reject_DG_I_&Compno._&run_id. ;
run;

data Temp_DG_Rollup;
set Temp_DG_1 Temp_DG_Reject;
run;

proc sort data=Temp_DG_Rollup;by clamno lineno ;run;

Proc transpose data=Temp_DG_Rollup out=Temp_DG_Rollup_final(drop = _NAME_ _LABEL_ )  prefix=rollup;
by clamno ;
var lineno;
run;

data Temp_DG_Rollup_final;
set Temp_DG_Rollup_final;
where clamno ne '';
run;

%nobs(Temp_DG_Rollup_final);
 %if &nobs > 0 %then %do;

%let fid = %sysfunc(open(Temp_DG_Rollup_final,i));
%let cnt = %eval(%sysfunc(attrn(&fid.,nvar))-1);
%put &cnt;
%LET rc = %SYSFUNC(CLOSE(&fid.));	

data Temp_DG_Rollup_final_1(keep=clamno lineno rollup_line2);
	      set Temp_DG_Rollup_final;
	      length rollup_line2 $1000.;
		  lineno=rollup1;
	      rollup_line2  = catx(',',of rollup2-rollup&cnt.);
	run;
%end;
 %else %do;
  data Temp_DG_Rollup_final_1; 
  	set Temp_DG_Rollup_final;
  	length rollup_line2 $1000.;
  	rollup_line2='     ';
	lineno= '     ';
  Run; 
 %end;
%mend;
%dg_rollup;

/* B-274371 DG Rollup Line Logic Ends Here*/

/* B-274371 Merge DG & HG Rollup Line Logic Starts Here*/
proc sort data=HG_ROLLUP_CLAIMS_3;by clamno lineno;run;
proc sort data=Temp_DG_Rollup_final_1;by clamno lineno;run;

data HG_DG_ROLLUP(keep= clamno lineno rollup_line3);
merge HG_ROLLUP_CLAIMS_3(in=a) Temp_DG_Rollup_final_1(in=b);
by clamno lineno;
if a and b then do; rollup_line3=catx(',',rollup_line1,rollup_line2); end;
if a and not b then do; rollup_line3=rollup_line1; end;
if b and not a then do; rollup_line3=rollup_line2; end;
run;
/* B-274371 Merge DG & HG Rollup Line Logic Ends Here*/

/* B-96285 join QHP, HFIC subscribers */
%if (&compno.=45 and "&LOB." ne "MPPO") or (&compno. eq 42 and "&LOB." eq "QHP") %then %do;	/* B-121242 */  /*B-248997*/
Proc sql;
Create table encountersY4e1 as
	select a.*,b.SUB_MLSTNAM,b.SUB_MFSTNAM,b.SUB_MMIDFULL,b.SUB_MEMBNO,
			b.SUB_BTHDAT,b.SUB_SEXCOD,b.SUB_MDCARID,	
		    b.SUB_MEDCTY,b.SUB_MADRLN1,	b.SUB_MADRLN2,	
			b.SUB_MCITYCD,b.SUB_MSTACOD,b.SUB_MZIPCOD,SUB_socsec
	from &RGA191..encountersY4e1_&Compno._&run_id.  a left join sub b 
	on a.subsno=b.membno ;
Quit;
Data &RGA191..encountersY4e1_&Compno._&run_id.; 
Set encountersY4e1;
Run;
Proc delete data=encountersY4e1;
Run;
%end;

/* HS 542635 - Added Proc delete statement */
Proc delete data=&RGA191..encountersY4B0_&Compno._&run_id.;
Run;
Proc delete data=&RGA191..Temp_DG;
Run;
Proc delete data=&RGA191..Temp_DG1;
Run;
Proc delete data=&RGA191..Temp_DG_final;
Run;
%mend;
%temp;

Data Provider_temp(rename = (pzipcod1 =pzipcod));
	set &libn..provider(keep=provno npiid padrln1 padrln2 pcitycd pstacod pzipcod);
	*pzipcod1 = compress(pzipcod,"@:*`~#-= ");/*CR# CHG0034118 - added equal sign in compress function*/ 
	/* CR# CHG0038449 - Added compress function for zip code to keep only digits */
			/* SAS2AWS2: ReplacedFunctionCompress */
			pzipcod1=kcompress(pzipcod,'','kd');
			 drop pzipcod;
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionTranslate */
		pcitycd=compbl(kcompress(ktranslate(pcitycd,' ','-'),"","ka"));		/* B-108297 city alpha */
Run;
Proc sort data=provider_temp nodupkey; by provno;run;
Proc sort data=&RGA191..encountersY4e1_&Compno._&run_id.;by provno;run;
Data &RGA191..encountersY4e2_&Compno._&run_id.;
	Merge &RGA191..encountersY4e1_&Compno._&run_id.(in=_a ) provider_temp(in=_b);
	if _a;
	by provno;
Run;
/* HS 542635 - Added Proc delete statement */

Proc delete data=&RGA191..encountersY4e1_&Compno._&run_id.;run;
Proc sort data=&RGA191..encountersY4e2_&Compno._&run_id.;by npiid;run;
Data &RGA191..encountersY4e3_&Compno._&run_id.;
	Merge &RGA191..encountersY4e2_&Compno._&run_id.(in=_a) providernpi_&compno.(in=_b keep=npiid PROVFIRPRACADDR PROVSECPRACADDR PROVPRACCITY
PROVPRACSTATE PROVPRACPOSTAL);
	if _a;
	by npiid;
Run;
/* HS 542635 - Added Proc deletestatement */
Proc delete data=&RGA191..encountersY4e2_&Compno._&run_id.;run;
Proc delete data=provider_temp;run;
%macro temps;	/* B-96285 */
Data &RGA191..encountersY4e3_&Compno._&run_id.;
Set &RGA191..encountersY4e3_&Compno._&run_id.;
	if missing(IN_2400_SV203_SL_CHG_AMT)=1 then IN_2400_SV203_SL_CHG_AMT=0;
	if missing(IN_2430_SVD02_SL_PAID_AM)=1 then IN_2430_SVD02_SL_PAID_AM=0;
	if missing(IN_2430_CAS03_Var)=1 then IN_2430_CAS03_Var=0;
	if missing(IN_2430_CAS04_Var)=1 then IN_2430_CAS04_Var=0;/*ALM # 5160 - added*/
	if missing(IN_2430_CAS07_Var)=1 then IN_2430_CAS07_Var=0;/*ALM # 5160 - added*/
	if missing(IN_2430_CAS08_Var)=1 then IN_2430_CAS08_Var=0;/*ALM # 5160 - added*/
	if missing(IN_2400_SV205_SL_COUNT)=1 then IN_2400_SV205_SL_COUNT=0;/*ALM # 5160 - added*/
	if missing(IN_2430_SVD05_PD_UNITS)=1 then IN_2430_SVD05_PD_UNITS=0;/*ALM # 5160 - added*/
/*	if missing(IN_2430_CAS06)=1 then IN_2430_CAS06=0;*//*ALM # 5160 - commented*/
/*	if missing(IN_2430_CAS09)=1 then IN_2430_CAS09=0;*//*ALM # 5160 - commented*/
/*	if missing(IN_2430_CAS12)=1 then IN_2430_CAS12=0;*//*ALM # 5160 - commented*/
/*	if missing(IN_2430_CAS15)=1 then IN_2430_CAS15=0;*//*ALM # 5160 - commented*/
/*	if missing(IN_2430_CAS18)=1 then IN_2430_CAS18=0;*//*ALM # 5160 - commented*/
    if missing(PR_2400_SV102_S_CHG_AMT)=1 then PR_2400_SV102_S_CHG_AMT=0;
	if missing(PR_2430_SVD02_SL_PAID_AMT)=1 then PR_2430_SVD02_SL_PAID_AMT=0;
	if missing(PR_2430_CAS03_Var)=1 then PR_2430_CAS03_Var=0;
	if missing(PR_2430_CAS04_Var)=1 then PR_2430_CAS04_Var=0;/*ALM # 5160 - added*/
	if missing(PR_2430_CAS07_Var)=1 then PR_2430_CAS07_Var=0;/*ALM # 5160 - added*/
	if missing(PR_2430_CAS08_Var)=1 then PR_2430_CAS08_Var=0;/*ALM # 5160 - added*/
/*	if missing(PR_2430_CAS06)=1 then PR_2400_SV102_S_CHG_AMT=0;*//*ALM # 5160 - commented*/
/*	if missing(PR_2430_CAS09)=1 then PR_2430_CAS09=0;*//*ALM # 5160 - commented*/
/*	if missing(PR_2430_CAS12)=1 then PR_2430_CAS12=0;*//*ALM # 5160 - commented*/
/*	if missing(PR_2430_CAS15)=1 then PR_2430_CAS15=0;*//*ALM # 5160 - commented*/
/*	if missing(PR_2430_CAS18)=1 then PR_2430_CAS18=0;*//*ALM # 5160 - commented*/
/*	if missing(PR_2430_CAS21)=1 then PR_2430_CAS21=0;*//*ALM # 5160 - commented*/


%if &compno.=34  or (&compno.=01 and &LOB.=DSNP) %then %do;
		if missing(PR_2430_SVD02_SL_PAID_AMT_MCR)=1 then PR_2430_SVD02_SL_PAID_AMT_MCR=0;
		if missing(PR_2430_CAS03_MCR_Var)=1 then PR_2430_CAS03_MCR_Var=0;
		if missing(PR_2430_CAS04_MCR_Var)=1 then PR_2430_CAS04_MCR_Var=0;	
		if missing(IN_2430_SVD02_SL_PAID_AM_MCR)=1 then IN_2430_SVD02_SL_PAID_AM_MCR=0;
		if missing(IN_2430_SVD05_PD_UNITS_MCR)=1 then IN_2430_SVD05_PD_UNITS_MCR=0;
		if missing(IN_2430_CAS03_MCR_Var)=1 then IN_2430_CAS03_MCR_Var=0;
		if missing(IN_2430_CAS04_MCR_Var)=1 then IN_2430_CAS04_MCR_Var=0;
	%end;

	/* B-96285 QHP, HFIC subscribers */
     %if (&compno.=45 and "&LOB." ne "MPPO") or (&compno. eq 42 and "&LOB." eq "QHP") %then %do;	/* B-121242 */ /*B-248997*/
		/* SAS2AWS2: ReplacedFunctionLength */
		if klength(Compress(SUB_MZIPCOD)) = 9 then
    		do;
       		SUB_MZIPCOD =SUB_MZIPCOD;
    		end;
	    /* SAS2AWS2: ReplacedFunctionLength */
    	else if klength(Compress(SUB_MZIPCOD)) = 5 then
    		do;
		       /* SAS2AWS2: ReplacedFunctionCompress */
       		SUB_MZIPCOD  =kcompress(SUB_MZIPCOD||'9998');
    		end; 
		else 
			do;
	   		SUB_MZIPCOD  ='';
			end;
	%end; 
run;
%mend;
%temps;


data provider_NPIID ;
	set  &libn..provider(keep=npiid pfstnam Plstnam);
	where missing(npiid)=0 and (missing(Plstnam)=0 or missing(Plstnam)=0) ;
Run;
Proc sort data=provider_NPIID nodupkey;by npiid;run;

Proc sql;
Create table &RGA191..encountersY4e3_&Compno._&run_id._temp1 as
	select a.* ,/*b.Plstnam*/ compress(b.plstnam,"@:*`~#")  as RF_PHY_LST_T , /*b.pfstnam*/ compress(b.pfstnam,"@:*`~#") as RF_PHY_FST_T /* HS 569650 */
	from &RGA191..encountersY4e3_&Compno._&run_id. a left join provider_NPIID  b
on a.rpanpi=b.npiid ;
quit;

Proc sql;
Create table &RGA191..encountersY4e3_&Compno._&run_id._Temp2 as
	select a.*,/*b.Plstnam*/ compress(b.plstnam,"@:*`~#") as RN_PHY_LST_T , /*b.pfstnam*/ compress(b.pfstnam,"@:*`~#") as RN_PHY_FST_T /* HS 569650 */
	from &RGA191..encountersY4e3_&Compno._&run_id._Temp1 a left join provider_NPIID  b
on a.rndnpi=b.npiid ;
quit;

/* B-103975 moved prior to rollup   start */
Data Npi_340b;
set RGA193.Npi_340b;
/* SAS2AWS2: ReplacedFunctionCompress */
npiid1=kcompress("'"||npiid||"'");
run;

Proc sql noprint;
Select npiid1 into :binpiid SEPARATED
 by ',' from Npi_340b;
quit;
/*   B-103975 moved prior to rollup   end */

Data &RGA191..encountersY4e3_&Compno._&run_id.;
set &RGA191..encountersY4e3_&Compno._&run_id._Temp2;
if missing(RF_PHY_LST)=1 and missing(RF_PHY_FST)=1 then 
	do;
		RF_PHY_npiid=rpanpi;	
	    RF_PHY_LST=RF_PHY_LST_T;
		RF_PHY_FST=RF_PHY_FST_T;
	end;
if missing(RN_PHY_LST)=1 and missing(RN_PHY_FST)=1 then 
	do;
		RN_PHY_NPiid=rndnpi;	
	    RN_PHY_LST=RN_PHY_LST_T;
		RN_PHY_FST=RN_PHY_FST_T;

	end;
if   missing(Prdiagcd)=1 and missing(DIAGD2)=1 and missing(DIAGD3)=1 and missing(DIAGD4)=1 and missing(diagd5)=1 and
	 missing(diagd6)=1 and missing(diagd7)=1 and missing(diagd8)=1 and missing(diagd9)=1 and missing(diag10)=1 and
	 missing(diag11)=1 and missing(diag12)=1 and missing(diag13)=1 and missing(diag14)=1 and missing(diag15)=1 and
	 missing(diagn16)=1 and missing(diag17)=1 and missing(diag18)=1 and missing(diag19)=1 and missing(diag20)=1 and
	 missing(diag21)=1 and missing(diag22)=1 and missing(diag23)=1 and missing(diag24)=1 and missing(diag25)=1 then 
	 Prdiagcd=primdiag;

if missing(BIPRVNPI)=1 then do;
	BIPRVTXY=prvtxy;
	BI_PRV_ENTC=1;
	BI_PRV_LST=Plstnam_1 ; /* CR# CHG0032056 */
	BI_PRV_FST=pfstnam_1; /* CR# CHG0032056 */
	BIPRVNPI_new=mdcaid_1;
	BI_PRV_ADD1=padrln1_1;
	BI_PRV_ADD2=padrln2_1;
	BI_PRV_CITY=pcitycd_1;/* CR# CHG0032056 */
	BI_PRV_STATE=pstacod_1;
	BI_PRV_ZIP=pzipcod_1;
	BI_PRV_EIN=FEDNUM_1;
	
end;

Run;

/* HS 542635 - Added Proc delete statement */
Proc delete data = &RGA191..encountersY4e3_&Compno._&run_id._Temp1;
run;
Proc delete data =&RGA191..encountersY4e3_&Compno._&run_id._Temp2;
run;

data &RGA191..encountersY4e3_&Compno._&run_id.(drop = clamno rename = ( clamno_old = clamno temp = clamno_old)); /* HS 569650 - reassigned clamno for multiple TCN*/
length clamno_old $20. temp $20.;
set &RGA191..encountersY4e3_&Compno._&run_id.;
temp = clamno;
run;

%macro Mapping_COS;
Data tmprg101_lineno(drop = clmdate clmseqno clmdate1 clmdate2 clmdate3 mod div alphabet clmbat1  clmbat jclmno jclmno1 jclmno2);
set &RGA191..encountersY4e3_&Compno._&run_id.(keep=clamno lineno bilcod clatyp  clmstat clspclcd formcd provno) ;	   
      
		/* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionSubstr */
		clmdate = ksubstr(kleft(clamno),3,6);
        /* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionSubstr */
        clmseqno = ksubstr(kleft(clamno),9,5);
        clmdate = input(clmdate,mmddyy8.);     
		 /* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionSubstr */
		 clmbat1= ksubstr(kleft(clamno),1,2);
		 clmbat=input(clmbat1,2.);			
		 clmdate1 = juldate(clmdate);
		 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		 if klength(kcompress(clmdate1)) => 5 then do;
				 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
		 		clmdate2=clmbat||ksubstr(kcompress(clmdate1),2,4);
			end;
			else do;
				clmdate2=clmbat||clmdate1;
		 end;

		 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
 		if klength(kcompress(clmdate1)) => 5 then do;
				 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
		 		clmdate=ksubstr(kcompress(clmdate1),2,4);
			end;
			else do;
				clmdate=clmdate1;
		 	end;
			 jclmno1 = clmdate || clmseqno;


		 	format clmdate3 $32.;   alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  		if clmdate2 = 0 then clmdate3 = '0';
  		else
   			do while (clmdate2 ne 0);
   
		     /* SAS2AWS2: ReplacedFunctionLength */
		     mod = mod(clmdate2, klength(alphabet));
			
		    /* SAS2AWS2: ReplacedFunctionLength */
		    div = floor(clmdate2 / (klength(alphabet)));
			/* SAS2AWS2: ReplacedFunctionCats-ReplacedFunctionSubstr */
			clmdate3 = cats(ksubstr(alphabet,mod+1,1),clmdate3);
			clmdate2 = div;
   			end;

         jclmno2 =clmseqno||clmdate3;

		  if clmbat = '01' then do;
		 jclmno = jclmno1;
		 end;
		 else do;
		 jclmno = jclmno2;
		 end; 
		 /* SAS2AWS2: ReplacedFunctionCompress */
		 ecn = kcompress(jclmno);
run;
data svcxwalk(keep=svccod svctyp svccodto svctypto procind) ;
     set &libn2..svccodxwalk(where=(procind in ('B', 'M'))) ;
run ;
%macro shp_svccod;
	
%if &compno = 02 %then %do;

	proc sql _METHOD;
	create table shp_base as
	select s.*
	  from svclines as s
	     , tmprg101_lineno as a        /* tmprg101_lineno as a */
	  where s.clamno = a.clamno and s.lineno = a.lineno;

	quit;
	data Medssvccdxwalk;
		set &libn2..Medssvccdxwalk;
		where compno = "&compno.";
		run;

	proc sql;	
	create table shp_base1 as 	
		select a.*,b.svccod as Tmp_svccod
		from shp_base a left join Medssvccdxwalk b 
		on a.svccod=b.mhssvccod and a.benpkg =b.bencod  ;
	quit;
	

	data svc(drop = tmp_svccod  );
		set shp_base1;
		if tmp_svccod not in (" ")  then do;
		    svccod1 = svccod;
			Svccod=tmp_svccod;			
		end;			
	run;

%end;
%else %do;
	%if &compno = 34 %then %do;
	proc sql _METHOD;
	create table svc as
	select s.*
	  from svclines as s
	     , tmprg101_lineno as a        /* tmprg101_lineno as a */
	  where s.clamno = a.clamno and s.lineno = a.lineno ;

	quit;
	%end;
	%else %do;
	%if &compno = 60 %then %do;
		proc sql _METHOD;
	create table shp_base as
	select s.*
	  from svclines as s
	     , tmprg101_lineno as a        /* tmprg101_lineno as a */
	  where s.clamno = a.clamno and s.lineno = a.lineno;

	quit;
	data Medssvccdxwalk;
		set &libn2..Medssvccdxwalk;
		where compno = "&compno." and lobcod = "&lob.";
		run;

	proc sql;	
	create table shp_base1 as 	
		select a.*,b.svccod as Tmp_svccod
		from shp_base a left join Medssvccdxwalk b 
		on a.svccod=b.mhssvccod and a.benpkg=b.bencod;
	quit;
	data svc(drop = tmp_svccod );
	set shp_base1;
	if tmp_svccod not in (" ") then do;
		    svccod1 = svccod;
			Svccod=tmp_svccod;
		end;
		run;
	%end;
%else %do;
	proc sql _METHOD;
	create table svc as
	select s.*
	  from svclines as s
	     , tmprg101_lineno as a        /* tmprg101_lineno as a */
	  where s.clamno = a.clamno and s.lineno = a.lineno ;

	quit;
%end;%end;%end;

%mend;

%shp_svccod;
data slines0;
     set svc(keep=clamno lineno svctyp poscod  svccod svcstat patage svctyp revind);
/*	 where  revind='N' ; */
     if svccod = 'CNVPX' then svccod='99199' ;
  run;
 proc sql;
    create table slines1 as
    select s.*, x.svccodto, x.svctypto
    from slines0 s left join svcxwalk x
    on (s.svccod = x.svccod) and (s.svctyp=x.svctyp) ;
quit;
data svclines(drop=svccodto svctypto) ; set slines1 ;
     if svccodto ne '' and svctypto ne '' then do ;
        svccod=svccodto ;
        svctyp=svctypto ;
     end ;
run ;
proc sort data = svclines;
     by clamno lineno;
run;
proc sort data=svclines out=svclines1 (keep=clamno) nodupkey;
     by clamno;
run;
data svclines; set svclines; run;
proc sort data = tmprg101_lineno;
by clamno;
run;
data tmprg101f;
     merge tmprg101_lineno(in=a) svclines1(in=b);
     by clamno;
     if a and b;
run;
data prvdr_NoMiss_provno ;
	set &libn..provider ;
if missing(provno) = 0 ; 
run;
data en_prov;     
     set prvdr_NoMiss_provno
         (keep = provno prvtyp fednum socsec plstnam pfstnam parstat liceno mdcaid 
                 prvorg mdcare upincd effdat expdat npiid npiexpdat npireascd pzipcod);
run;
/* HS 542635 - Added Proc deletestatement */
Proc delete data = prvdr_NoMiss_provno;
run;
data tmprg101K (keep=provno nysprof nysmeds spccod);/* CR# CHG0036774 - added spccod */
     merge &libn2..provspec(in=pno) &libn2..spcxwalk(drop=srcfile dwloaddt);
     by spccod;
     if pno = 1;
run;
proc sort data = tmprg101K out=en_spec nodupkey;
     by provno;
run;
proc sql;
     create table en_prov2 as
     select a.*, s.nysmeds, s.spccod /* CR# CHG0036774 - added spccod */
     from en_prov a left join en_spec s
     on a.provno = s.provno;				 
run;
/* HS 542635 - Added Proc deletestatement */
Proc delete data = en_prov;
run;
Proc sort data=tmprg101f out=tmprg101_f;by provno;run;
data tmprg101g;
     merge tmprg101_f(in=p) en_prov2(keep=provno prvtyp mdcaid NYSMEDS spccod);/* CR# CHG0036774 - added spccod */
     by provno;
     if p = 1;
	 /* CR# CHG0036774 - SPCXWALK table update for professional */
	 if formcd = 'H' then do;
		if spccod = '0740' then NYSMEDS = '199';
		if spccod = 'FPEM' then NYSMEDS = '089';
		if spccod = 'LMFT' then NYSMEDS = '283';
		if spccod = 'PSSO' then NYSMEDS = '964';
		if spccod = 'REDI' then NYSMEDS = '909';
	 end;
  run;

proc sort data=tmprg101g;
     by clamno;
run;
data tmprg102(keep=clamno lineno formcd bilcod provno clspclcd clmstat ecn  );
     set tmprg101g;
run;
proc sort data=tmprg102;by clamno lineno;run;
data clinic;
     merge tmprg102(in=cno) svclines(keep=clamno lineno svctyp poscod lineno svccod svcstat patage );
     by clamno lineno ; 

     if cno = 1;
     if formcd = 'U' and (bilcod < "110" or bilcod > "129" or bilcod = "")
                     and clspclcd = "";
run;
data clxwlk;
     set clinic;

     if svctyp = 'CP' and (svccod ge '80049' and svccod le '80440') or
                          (svccod ge '81000' and svccod le '81099') or
                          (svccod ge '83890' and svccod le '84999') or
                          (svccod ge '86000' and svccod le '89399') then clspclcd = '599';

     if svctyp = 'CP' and (svccod ge '85002' and svccod le '85999') then clspclcd = '137';

     if svctyp = 'CP' and (svccod ge '80500' and svccod le '80502') then clspclcd = '135';

     if svctyp = 'CP' and (svccod ge '82000' and svccod le '83887') then clspclcd = '138';

     if svctyp = 'CP' and (svccod ge '74710' and svccod le '77799') or
                          (svccod ge '79000' and svccod le '79999') then clspclcd = '200';

     if svctyp = 'CP' and (svccod ge '70010' and svccod le '74485') then clspclcd = '201';

     if svctyp = 'CP' and (svccod ge '78000' and svccod le '78999') then clspclcd = '202';

     if svctyp = 'CP' and (svccod ge '92950' and svccod le '93350') or
                          (svccod ge '93501' and svccod le '93799') then clspclcd = '927';

     if svctyp = 'CP' and (svccod ge '99281' and svccod le '99288') then clspclcd = '901';
     if svctyp = 'CP' and (svccod ge '59000' and svccod le '59841') then clspclcd = '904';
     if svctyp = 'CP' and (svccod ge '56300' and svccod le '58999') then clspclcd = '905';
     if svctyp = 'CP' and (svccod ge '90935' and svccod le '90937') then clspclcd = '913';
     if svctyp = 'CP' and (svccod ge '95004' and svccod le '95120') then clspclcd = '915';
     if svctyp = 'CP' and (svccod ge '28285' and svccod le '28292') or
                          (svccod ge '11720' and svccod le '11750') then clspclcd = '918';
     if svctyp = 'CP' and (svccod ge '65222' and svccod le '67961') or
                          (svccod = '00140') then clspclcd = '919';
     if svctyp = 'CP' and (svccod ge '97110' and svccod le '97545') or
                          (svccod ge '97001' and svccod le '97002') then clspclcd = '920';
     if svctyp = 'CP' and (svccod ge '97003' and svccod le '97004') then clspclcd = '923';
     if svctyp = 'CP' and (svccod ge '97705' and svccod le '97799') or
                          (svccod ge '97010' and svccod le '97039') then clspclcd = '924';
     if svctyp = 'CP' and (svccod ge '93880' and svccod le '93990') or
                             (svccod = '35121') then clspclcd = '928';
     if svctyp = 'CP' and (svccod ge '94010' and svccod le '94799') then clspclcd = '929';
     if svctyp = 'CP' and (svccod ge '91010' and svccod le '91033') or
                          (svccod ge '47562' and svccod le '49320') or
                          (svccod ge '45330' and svccod le '45378') or
                          (svccod ge '43234' and svccod le '43268') or
                          (svccod ge '00800' and svccod le '00840') then clspclcd = '930';
     if svctyp = 'CP' and (svccod ge '95810' and svccod le '96117') then clspclcd = '931';
     if svctyp = 'CP' and (svccod ge '62000' and svccod le '64550') or
                          (svccod ge '00210' and svccod le '00220') then clspclcd = '932';
     if svctyp = 'CP' and (svccod ge '96400' and svccod le '96530') then clspclcd = '934';
     if svctyp = 'CP' and (svccod ge '92502' and svccod le '92599') or
                          (svccod ge '30117' and svccod le '31535') or
                          (svccod = '00120') then clspclcd = '935';
     if svctyp = 'CP' and (svccod ge '90875' and svccod le '90876') or
                          (svccod ge '90841' and svccod le '90844') or
                          (svccod ge '90804' and svccod le '90819') or
                          (svccod = '90855') then clspclcd = '945';
     if svctyp = 'CP' and (svccod in ('90857', '90853')) then clspclcd = '946';
     if svctyp = 'CP' and (svccod ge '29819' and svccod le '29882') or
                          (svccod ge '26565' and svccod le '27333') or
                          (svccod in ('23410', '01120', '27827')) then clspclcd = '950';
     if svctyp = 'CP' and (svccod ge '36406' and svccod le '36425') then clspclcd = '951';
     if svctyp = 'CP' and (svccod ge '69200' and svccod le '69631') or
                          (svccod ge '54152' and svccod le '54640') or
                          (svccod ge '49505' and svccod le '50394') or
                          (svccod ge '45505' and svccod le '47000') or
                          (svccod ge '36489' and svccod le '42830') or
                          (svccod ge '29125' and svccod le '29580') or
                          (svccod ge '27345' and svccod le '27630') or
                          (svccod ge '25111' and svccod le '26055') or
                          (svccod ge '12011' and svccod le '21320') or
                          (svccod in ('01260', '45100', '99024')) then clspclcd = '952';
     if svctyp = 'CP' and (svccod ge '50590' and svccod le '53675') then clspclcd = '953';
     if svctyp = 'CP' and (svccod ge '96900' and svccod le '96912') or
                          (svccod ge '10060' and svccod le '11440') then clspclcd = '956';
     if svctyp = 'CP' and (svccod ge '92002' and svccod le '92499') then clspclcd = '958';
     if svctyp = 'CP' and (svccod ge '90880' and svccod le '90899') or
                          (svccod ge '90845' and svccod le '90847') or
                          (svccod ge '90820' and svccod le '90825') or
                          (svccod in ('90801', '90862'))    then clspclcd = '964';
     if svctyp = 'HC' and (svccod ge 'G0108' and svccod le 'G0109') then clspclcd = '903';
     if svctyp = 'HC' and (svccod = 'J2790') then clspclcd = '904';
     if svctyp = 'HC' and (svccod ge 'P3000' and svccod le 'P3001') or
                          (svccod ge 'G0123' and svccod le 'G0124') then clspclcd = '905';
     if svctyp = 'HC' and (svccod ge 'Q0083' and svccod le 'Q0086') or
                          (svccod in ('A0020', 'Q0082', 'W7307')) then clspclcd = '920';
     if svctyp = 'HC' and (svccod = 'H5300') then clspclcd = '923';
     if svctyp = 'HC' and (svccod = 'E0943') then clspclcd = '924';
     if svctyp = 'HC' and (svccod = 'J0585') then clspclcd = '931';
     if svctyp = 'HC' and (svccod ge 'Q0136' and svccod le 'Q9934') or
                          (svccod ge 'Q0083' and svccod le 'Q0085') or
                          (svccod ge 'P9015' and svccod le 'P9019') or
                          (svccod ge 'J2930' and svccod le 'J9999') or
                          (svccod ge 'J0635' and svccod le 'J2405') or
                          (svccod = 'Q0081') then clspclcd = '934';
     if svctyp = 'HC' and (svccod ge 'L3914' and svccod le 'L8680') then clspclcd = '950';
     if svctyp = 'HC' and (svccod = '09110') then clspclcd = '951';
     if svctyp = 'HC' and (svccod = 'A4213') then clspclcd = '952';
     if svctyp = 'HC' and (svccod = 'V2632') then clspclcd = '958';

     if svctyp = 'RV' and svccod in ('450', '451') then clspclcd = '901';

     if poscod = '23' then clspclcd = '901';
run;


/*********************************************************
  Logic added 11/15/2001, by request of Jay Schechtman
**********************************************************/
data clxwlk;
     set clxwlk;
     if clspclcd = ' ' and patage < '18' then clspclcd = '936';
     if clspclcd = ' ' and patage >= '18' then clspclcd = '914';
run;
data clxwlk;
     set clxwlk;
     if clspclcd ne '';
run;

proc sort data=clxwlk nodupkey;
     by clamno clspclcd;
run;


proc transpose data=clxwlk out=clxwlk2(drop=_name_ _label_);
     by clamno;
     var clspclcd;
run;
data clxwlk3(keep=clamno clspclcd);
     set clxwlk2;

     if col1 ne ' ' then col1 = col1;
     if col2 ne ' ' then col2 = col2;
     if col3 ne ' ' then col3 = col3;
     if col4 ne ' ' then col4 = col4;
     if col5 ne ' ' then col5 = col5;
     if col6 ne ' ' then col6 = col6;
     if col7 ne ' ' then col7 = col7;
     if col8 ne ' ' then col8 = col8;

     if col1 = '901' then clspclcd = col1;
     if col2 = '901' then clspclcd = col2;
     if col3 = '901' then clspclcd = col3;
     if col4 = '901' then clspclcd = col4;
     if col5 = '901' then clspclcd = col5;
     if col6 = '901' then clspclcd = col6;
     if col7 = '901' then clspclcd = col7;
     if col8 = '901' then clspclcd = col8;

     if col1 ne '901' and col2 = ' ' then clspclcd = col1;
     if col1 ne '901' and col2 not in ('901','') and col3 = '' and
        col1 ne col2 then clspclcd = '999';
     if col1 ne '901' and col2 not in ('901','') and col3 not in ('901','')
        and col4 ne '901' and col5 ne '901' and col6 ne '901'
        and col6 ne '901' and col7 ne '901' and col8 ne '901'
        then clspclcd = '999';
run;
proc sql;
    create table tmprg101g_a as
    select a.*,b.clspclcd_new
    from tmprg101g a left join clxwlk3(rename=(clspclcd=clspclcd_new)) b
    on a.clamno=b.clamno;
quit;

data tmprg101g;
    set tmprg101g_a;
    if clspclcd_new ne '' then clspclcd=clspclcd_new;
    drop clspclcd_new;
run;
data spec(keep=provno nysprof nysmeds);
     merge &libn2..provspec(in=pno) &libn2..spcxwalk(drop=dwloaddt srcfile);
     by spccod;
     if pno = 1;
run;
/*************************************************
  Pick up one spec. code for one provider only
**************************************************/
proc sort data = spec out=en_spec nodupkey;
     by provno;
run;
data enc_i_i enc_o_o enc_p_p; 
     set tmprg101g;  	

     if formcd = 'U' and (bilcod >= "110" and bilcod <= "129") then do;
        EncId = 'I';		
				output enc_i_i;
     end;

     if formcd = 'U' and not (bilcod >= "110" and bilcod <= "129") then do;
        EncId = 'O';
        output Enc_O_o;
     end;
     if formcd = 'H' then do;
        EncId = 'P';
        output Enc_P_p;
     end;
    
run;
data enc_i2;
length prvspcd $5. cos $3.;  /*please check*/
	set enc_i_i;
	    cos ='11';
		prvspcd = '';
	 mmis = mdcaid;
	run;
proc sql;
     create table enc_i2a as
     select a.*, m.mdcaid_def, m.nysmeds_def length=3
     from enc_i2 a
     left join &libn2..medsoutnetwrkmmis(rename=(mdcaid=mdcaid_def nysmeds=nysmeds_def)) m
     on a.cos = m.cos ;      
quit;
data enc_i2b ;
    set enc_i2a ;
	  if mmis eq '' and  clatyp='ON' then do;
        mmis = mdcaid_def;
   
        if nysmeds='' then nysmeds=nysmeds_def;
    end;
   
run ;
	proc sql;
     create table cos_table_out as
     select a.*, c.medscosub92
     from en_spec a left join &libn2..medscosxwalk c
     on a.nysmeds = c.nysmeds;
quit;
proc sql;
     create table enc_o2 as
     select a.*, c.nysprof, c.medscosub92
     from enc_o_o a left join cos_table_out c
     on a.provno = c.provno;
quit;
proc sql;
   create table Enc_o3a as
   select a.*, b.poscod
   from Enc_o2  a,
        svclines b
   where a.clamno = b.clamno
   and a.lineno = b.lineno;       
quit ;
proc sql;
   create table Enc_o3b as
   select a.*, b.cos, b.cos as orig_cos
   from Enc_o3a  a,
  
    &libn2..Medsposcosxwalk b     
   where a.poscod = b.poscod;      
quit;

data Enc_o3b;
length cos $3.;  /*please check*/
set Enc_o3b;
run;

data Enc_o3c(DROP=MDCAID);
length prvspcd $5. cos $3.;  /*please check*/
set Enc_o3b;
       mmis = mdcaid;
     if cos in ('06','85','87') then PrvSpCd = clspclcd;   
     if cos='85' and prvspcd='901' then cos='87';  
     if cos ne '87' and poscod='23' then cos='87';   /* New for 11/02/07: Item 1.1.4.1 */     
     if cos in ('12','15','28','73') and nysmeds ne '' then prvspcd = nysmeds;   
run;
proc sql;
     create table enc_o4 as
     select a.*, m.mdcaid, m.nysmeds_def length=3
     from enc_o3c a left join &libn2..medsoutnetwrkmmis(rename=(nysmeds=nysmeds_def)) m
     on a.cos = m.cos ;      
quit;
data enc_o5 ;
length prvspcd $5. cos $3.;   /*please check*/
    set enc_o4 ;	
    if mmis eq '' and  clatyp='ON' then do;
        mmis = mdcaid;       
        if prvspcd='' then prvspcd=nysmeds_def;
    end;
    drop nysmeds_def ;
run ;
proc sql;
     create table cos_table_pro as
     select a.*, c.medscoshcfa 
     from en_spec a left join &libn2..medscosxwalk c
     on a.nysmeds = c.nysmeds;
quit;
proc sql;
     create table enc_p2 as
     select a.*, c.nysprof, c.medscoshcfa
     from enc_p_p a left join cos_table_pro c
     on a.provno = c.provno;
quit;
data enc_p3(DROP=mdcaid);  
    set enc_p2;
     cos = medscoshcfa ;
MMIS = mdcaid;     
if nysmeds ne '' then PrvSpCd = Nysmeds ;
run;

DATA enc_p3(drop =prvspcd cos);   /* please check */
  set enc_p3;
    length prvspcd1 $5.;
    length cos1 $3.;
    
    prvspcd1 = ksubstr(prvspcd,1,5);
    format prvspcd1 $5.;
    cos1 = ksubstr(cos,1,3);
    format cos1 $3.;
    run; 
    
    DATA enc_p3;
  set enc_p3(rename=(prvspcd1 = prvspcd cos1 = cos));
        run; 

proc sql;
     create table enc_p4 as
     select a.*, m.mdcaid, m.nysmeds_def length=5
     from enc_p3 a left join &libn2..medsoutnetwrkmmis(rename=(nysmeds=nysmeds_def)) m
     on a.cos = m.cos ;      
quit;
options mlogic ; 
Data _null_;
%let v1 = %nrstr(%OPSPEC%%);
%let v2 = %nrstr(%OPPCP%%);
%let v3 = %nrstr(%OPMAT%%);
%let v4 = %nrstr(%OPFP%%);
%let v5 = %nrstr(%OPOM%%);
%let v6 = %nrstr(%OPDENTAL%%);
%let v7 = %nrstr(%OPAS%%);
%let v8 = %nrstr(%OPRB%%);
%let v9 = %nrstr(%OPMH%%);
%let v10 = %nrstr(%OPSA%%);
%let v11 = %nrstr(%IPMS%%);
%let v12 = %nrstr(%IPSNF%%);
%put v1 = &v1;
%put v2 = &v2;
%put v3 = &v3;
%put v4 = &v4;
%put v5 = &v5;
%put v6 = &v6;
%put v7 = &v7;
%put v8 = &v8;
%put v9 = &v9;
%put v10 = &v10;
%put v11 = &v11;
%put v12 = &v12;
OPTIONS BUFNO=32 IBUFNO=32 UBUFNO=20 SORTSIZE=256M SUMSIZE=256M
        MSGLEVEL=I FULLSTIMER ;
%let file=enc_p4;    
%let ext=prof;
%put ext = &ext;
%put file = &file;
run;
proc sort data=&file out=meds_&file nodupkeys;
    by ecn clamno ;
run;
DATA meds_&file(drop =prvspcd);   /* please check */
  set meds_&file;
    length prvspcd1 $5.;
    
    prvspcd1 = ksubstr(prvspcd,1,5);
    format prvspcd1 $5.;
        run; 
    
    DATA meds_&file;
  set meds_&file(rename=(prvspcd1 = prvspcd));
        run; 

proc sql;
    create table meds_&file._&ext as
    select *
    from meds_&file
    where cos = " " ;     
quit;
%nobs(meds_&file._&ext) ;
%put &nobs ;
/* Bypass processing:Tony Sulit,Mike Rosas: Will not enter COS lkup if input File is empty */
%macro rep2 ;
    %if &nobs = 0 %then %do;
       %put "NO COS Processing Done FOR THIS COMPANY &COMPNO., COS PROCESS skipped.";
         %bypass;
    %end;
    %else %do;
         *%nobypass;/* CR# CHG0036774 - commented */
		%nobypass1;
    %end;
%mend rep2 ;

%macro nobypass;

proc sort data=meds_&file._&ext nodupkeys;
    by ecn;
run;
/* STEP 02 */
/* GET PLACE OF SERVICE FROM SVCLINES,clatyp from Claims for cos = " " */
proc sql;
    create table svc_&ext as
    select  m.*, s.mcorcat
    from  meds_&file._&ext m
    left join  svc s
    on m.clamno = s.clamno; quit;

proc sort data=svc_&ext  out=svc_&ext._2 nodupkeys;
    by  ecn clamno mcorcat clatyp lineno;
run;

/* STEP 03 */
/* COUNT MAX NUMBER OF MCORCAT COLUMN */
proc sql;
    create table cnt_&ext as
    select max(rec) as cnt
    from
    (select ecn,count(*) as rec
    from svc_&ext._2
    group by ecn); quit;

data _null_;
    set cnt_&ext;
    call symput('i',put(cnt,1.0));
run;
%put &i.;
proc transpose data=svc_&ext._2 out=svc_1_&ext(drop=_name_ _label_ ) prefix=mcor;
    by ecn clamno clatyp;
    var mcorcat;
run;


/* STEP 04 */
/* SEPARATE RECORDS WHICH HAVE SINGLE MCORCAT COLUMN */
/* NOTE: if only 1 column, log shows "NOTE: Variable mcor2 is un-initialized" which is not a concern */ 
data svc_mcor_1_&ext svc_mcor_2_&ext;
    set svc_1_&ext;
    if mcor2 = " " then output svc_mcor_1_&ext ;
    else output svc_mcor_2_&ext;
run;

data svc_mcor_1_&ext;
    length mcorcat $12.0;
    set svc_mcor_1_&ext;
    /* SAS2AWS2: ReplacedFunctionCatx */
    mcorcat = catx(" ",of mcor1-mcor&i);
run;

data svc_mcor_2_&ext;
    length mcorcat1 $50.;
    set svc_mcor_2_&ext;
    /* SAS2AWS2: ReplacedFunctionCatx */
    mcorcat1 = catx(" ",of mcor1-mcor&i);
run;

/*proc download data=svc_mcor_2_&ext; */
/*run;*/

proc sql;
    create table svc_mcor_2_1_&ext as
    select *,
            case when mcorcat1 like "&v1" then "OPSPEC"
                 when mcorcat1 like "&v2" then "OPSPEC"
                 when mcorcat1 like "&v3" then "OPSPEC"
                 when mcorcat1 like "&v4" then "OPSPEC"
                 when mcorcat1 like "&v5" then "OPSPEC"
                 when mcorcat1 like "&v6" then "OPSPEC"
                 when mcorcat1 like "&v7" then "OPSPEC"
                 when mcorcat1 like "&v8" then "OPSPEC"
                 when mcorcat1 like "&v9" then "OPSPEC"
                 when mcorcat1 like "&v10" then "OPSPEC"
                 when mcorcat1 like "&v11" then "OPSPEC"
                 when mcorcat1 like "&v12" then "OPSPEC"
                 when mcorcat1 = "OPTRER OPTRNE" then "OPTRER" /* 12/04/07:TS-New Logic installed */
				 when mcorcat1 = "OPLBXR OPPODI" then "OPPODI" /* Added for 208137 New logic */
				 when mcorcat1 = "OPLBXR OPVISION" then "OPLBXR" /* Added for 208137 New logic */
            end as mcorcat
    from svc_mcor_2_&ext(keep=ecn clamno mcor1--mcor&i mcorcat1 clatyp); quit;

data svc_mcor_&ext(keep = ecn clamno mcorcat clatyp);
    set svc_mcor_1_&ext
        svc_mcor_2_1_&ext;
run;

/* STEP 05 */
/* MAPPING MCORCAT VS CATEGORY OF SERVICE(COS) FOR PROFESSIONAL ENCOUNTERS */

proc sql;
    create table svc_mcor_1_2_&ext as
    select ecn,clamno,mcorcat,clatyp,
            case when mcorcat in ("OPSPEC","OPPCP","OPFP","OPMAT","OPRB","OPMH","OPAS","OPDENTAL",
                                "OPSA","OPPHARM",,"OPOP","OPOM","IPMS","IPSNF") then "01"
                when mcorcat = "OPDME" then "22"
                when mcorcat = "OPHOME" then "07"
                when mcorcat = "OPLBXR" then "16"
                when mcorcat in ("OPTRER","OPTRNE") then "19"
                when mcorcat = "OPVISION" then  "05"
                when mcorcat in ("OPOP","OPPODI") then "03"
            end as cos
    from svc_mcor_&ext; quit;
proc sql;
    create table svc_mcor_1_3_&ext as
    select s.* ,
        case when s.clatyp = "ON" and mmis = " " then m1.mdcaid
        else m.mmis end as mmis_n,
        case when s.clatyp = "ON" and mmis = " "  then m1.nysmeds
        else m.nysmeds end as nysmeds_n,
        case when s.clatyp = "ON" and m.nysmeds = " " then m1.nysmeds
        else m.nysmeds end as nysmeds_n1
    from svc_mcor_1_2_&ext  s
    left join meds_&file._&ext m
    on m.ecn = s.ecn
    join &libn2..medsoutnetwrkmmis m1
    on s.cos = m1.cos; quit;



proc sql;
    create table svc_mcor_1_4_&ext as
    select s.ecn, s.clamno, s.clatyp, s.cos, mmis_n as mmis, s1.mcorcat,
           coalesce(nysmeds_n,nysmeds_n1) as nysmeds_def
    from svc_mcor_1_2_&ext s
    left join svc_mcor_1_3_&ext s1
    on s.ecn = s1.ecn; quit;



proc sort data=svc_mcor_1_4_&ext ; by ecn clamno ; run;
proc sort data=&file ; by ecn clamno ; run;


proc sql;
      create table meds_&file._2 as
      select e.*,
      coalesce(s.cos,e.cos) as cos1,
      coalesce(s.mmis,e.mmis) as mmis1,
      coalesce(s.nysmeds_def,prvspcd) as prvspcd1
      from &file e
      left join svc_mcor_1_4_&ext s
      on e.ecn = s.ecn
      and e.clamno = s.clamno;
quit;

data meds_&file._2(drop=cos mmis prvspcd rename=(cos1=cos
                   mmis1=mmis prvspcd1=prvspcd));
  set meds_&file._2;
run;


*Rsubmit;
data enc_p5; 
LENGTH prvspcd $5. cos $3.; /*please check*/
set meds_&file._2;
    cnt=1;

  if mmis eq '' and  clatyp='ON' then do;
        mmis = mdcaid; 
        if prvspcd='' then prvspcd=nysmeds_def;
   
    end;
    drop nysmeds_def;
Run;

%mend nobypass;
/* CR# CHG0036774 - added new cos missing handle logic & prvspec logic by removing ECN for professional claims ---> start */
%macro nobypass1;

/* STEP 02 */
/* GET PLACE OF SERVICE FROM SVCLINES,clatyp from Claims for cos = " " */
proc sql;
    create table svc_&ext as
    select  m.*, s.mcorcat
    from  meds_&file._&ext m
    left join  svc s
    on m.clamno = s.clamno ; quit;

proc sort data=svc_&ext  out=svc_&ext._2 nodupkeys;
    by   clamno mcorcat clatyp lineno;
run;

/* STEP 03 */
/* COUNT MAX NUMBER OF MCORCAT COLUMN */
proc sql;
    create table cnt_&ext as
    select max(rec) as cnt
    from
    (select clamno,count(*) as rec
    from svc_&ext._2
    group by clamno); quit;

data _null_;
    set cnt_&ext;
    call symput('i',put(cnt,1.0));
run;
%put &i.;
proc transpose data=svc_&ext._2 out=svc_1_&ext(drop=_name_ _label_ ) prefix=mcor;
    by  clamno clatyp;
    var mcorcat;
run;


/* STEP 04 */
/* SEPARATE RECORDS WHICH HAVE SINGLE MCORCAT COLUMN */
/* NOTE: if only 1 column, log shows "NOTE: Variable mcor2 is un-initialized" which is not a concern */ 
data svc_mcor_1_&ext svc_mcor_2_&ext;
    set svc_1_&ext;
    if mcor2 = " " then output svc_mcor_1_&ext ;
    else output svc_mcor_2_&ext;
run;

data svc_mcor_1_&ext;
    length mcorcat $12.0;
    set svc_mcor_1_&ext;
    /* SAS2AWS2: ReplacedFunctionCatx */
    mcorcat = catx(" ",of mcor1-mcor&i);
run;

data svc_mcor_2_&ext;
    length mcorcat1 $50.;
    set svc_mcor_2_&ext;
    /* SAS2AWS2: ReplacedFunctionCatx */
    mcorcat1 = catx(" ",of mcor1-mcor&i);
run;

proc sql;
    create table svc_mcor_2_1_&ext as
    select *,
            case when mcorcat1 like "&v1" then "OPSPEC"
                 when mcorcat1 like "&v2" then "OPSPEC"
                 when mcorcat1 like "&v3" then "OPSPEC"
                 when mcorcat1 like "&v4" then "OPSPEC"
                 when mcorcat1 like "&v5" then "OPSPEC"
                 when mcorcat1 like "&v6" then "OPSPEC"
                 when mcorcat1 like "&v7" then "OPSPEC"
                 when mcorcat1 like "&v8" then "OPSPEC"
                 when mcorcat1 like "&v9" then "OPSPEC"
                 when mcorcat1 like "&v10" then "OPSPEC"
                 when mcorcat1 like "&v11" then "OPSPEC"
                 when mcorcat1 like "&v12" then "OPSPEC"
                 when mcorcat1 = "OPTRER OPTRNE" then "OPTRER" /* 12/04/07:TS-New Logic installed */
				 when mcorcat1 = "OPLBXR OPPODI" then "OPPODI" /* Added for 208137 New logic */
				 when mcorcat1 = "OPLBXR OPVISION" then "OPLBXR" /* Added for 208137 New logic */
            end as mcorcat
    from svc_mcor_2_&ext(keep= clamno mcor1--mcor&i mcorcat1 clatyp); quit;

data svc_mcor_&ext(keep =  clamno mcorcat clatyp);
    set svc_mcor_1_&ext
        svc_mcor_2_1_&ext;
run;

/* STEP 05 */
/* MAPPING MCORCAT VS CATEGORY OF SERVICE(COS) FOR PROFESSIONAL ENCOUNTERS */

proc sql;
    create table svc_mcor_1_2_&ext as
    select clamno,mcorcat,clatyp,
            case when mcorcat in ("OPSPEC","OPPCP","OPFP","OPMAT","OPRB","OPMH","OPAS","OPDENTAL",
                                "OPSA","OPPHARM",,"OPOP","OPOM","IPMS","IPSNF","PARALVII","OPMHSOB",
								"OPSUDOOS","PARALVLI","OPER","ADSC") then "01" /* CR# CHG0036774 - added PARALVII to ADSC */
                when mcorcat in ("OPDME","OPPERS") then "22" /* CR# CHG0036774 - added OPPERS */
                when mcorcat = "OPHOME" then "07"
                when mcorcat = "OPLBXR" then "16"
                when mcorcat in ("OPTRER","OPTRNE") then "19"
                when mcorcat = "OPVISION" then  "05"
                when mcorcat in ("OPOP","OPPODI") then "03"
            end as cos
    from svc_mcor_&ext; quit;
proc sql;
    create table svc_mcor_1_3_&ext as
    select s.* ,
        case when s.clatyp = "ON" and mmis = " " then m1.mdcaid
        else m.mmis end as mmis_n,
        case when s.clatyp = "ON" and mmis = " "  then m1.nysmeds
        else m.nysmeds end as nysmeds_n,
        case when s.clatyp = "ON" and m.nysmeds = " " then m1.nysmeds
        else m.nysmeds end as nysmeds_n1
    from svc_mcor_1_2_&ext  s
    left join meds_&file._&ext m
    on m.clamno = s.clamno
    join &libn2..medsoutnetwrkmmis m1
    on s.cos = m1.cos; quit;



proc sql;
    create table svc_mcor_1_4_&ext as
    select  s.clamno, s.clatyp, s.cos, mmis_n as mmis, s1.mcorcat,
           coalesce(nysmeds_n,nysmeds_n1) as nysmeds_def
    from svc_mcor_1_2_&ext s
    left join svc_mcor_1_3_&ext s1
    on s.clamno = s1.clamno; quit;



proc sort data=svc_mcor_1_4_&ext ; by  clamno ; run;
proc sort data=&file ; by  clamno ; run;


proc sql;
      create table meds_&file._2 as
      select e.*,
      coalesce(s.cos,e.cos) as cos1,
      coalesce(s.mmis,e.mmis) as mmis1,
      coalesce(s.nysmeds_def,prvspcd) as prvspcd1
      from &file e
      left join svc_mcor_1_4_&ext s
      on  e.clamno = s.clamno;
quit;

data meds_&file._2(drop=cos mmis prvspcd rename=(cos1=cos
                   mmis1=mmis prvspcd1=prvspcd));
  set meds_&file._2;
run;

DATA meds_&file._2(drop =prvspcd );   /* please check */
  set meds_&file._2;
    length prvspcd1 $5.;
       
    prvspcd1 = ksubstr(prvspcd,1,5);
    format prvspcd1 $5.;
  
    run; 
    
    DATA meds_&file._2;
  set meds_&file._2(rename=(prvspcd1 = prvspcd ));
        run; 

*Rsubmit;
data enc_p5; 
length prvspcd $5. cos $3.; /*please check*/
set meds_&file._2;
    cnt=1;

  if mmis eq '' and  clatyp='ON' then do;
        mmis = mdcaid; 
        if prvspcd='' then prvspcd=nysmeds_def;
   
    end;
    drop nysmeds_def;
Run;

%mend nobypass1;
/* CR# CHG0036774 - added new cos missing handle logic & prvspec logic by removing ECN for professional claims ---> end */
%macro bypass;
   data enc_p5; set enc_p4;    
   run;
%mend ;
%rep2;
data COS_poscod(Keep=clamno lineno prvspcd cos);
length prvspcd $5. cos $3.;
set enc_p5(drop=mmis mdcaid ) enc_i2(drop=mmis mdcaid ) Enc_o5(drop=mmis mdcaid ) ;
	  %if &compno. = 45 or &compno. = 30 or "&LOB." eq "QHP" %then %do; /* B-154145 blank QHP, HFIC, MCR */
	  cos='';
	  prvspcd='';
	  %end;
run;
%mend;
/*%Mapping_COS;*/ /*B-289556*/
/*B-289556 below lines commented*/

/*
Data inpatient_spcod1(keep=npiid inp_spccod prvtyp) inpatient_spcod2(keep=npiid inp_spccod prvtyp);
	set en_prov2(keep=npiid nysmeds prvtyp);
	if missing(npiid)=0 and missing(nysmeds)=0 and prvtyp="IP" then output inpatient_spcod1;
	else if missing(npiid)=0 and missing(nysmeds)=0 then output inpatient_spcod2;
	rename nysmeds=inp_spccod;
run;
Proc sort data=inpatient_spcod1 nodupkey;by npiid;run;
Proc sort data=inpatient_spcod2 nodupkey;by npiid;run;
data inpatient_spcod2_1 ;
merge inpatient_spcod1(in=_a keep=npiid) inpatient_spcod2(in=_b);
if _b and not _a;
by npiid;
run;
Data inpatient_spcod(drop=prvtyp);
set inpatient_spcod1 inpatient_spcod2_1;
run;
Proc sort data= inpatient_spcod nodupkey;by npiid;run;
proc sql;
     create table &RGA191..encountersY4e3A_&Compno._&run_id. as
     select a.*, s.inp_spccod
     from &RGA191..encountersY4e3_&Compno._&run_id. a left join inpatient_spcod s
     on a.ATPRVNPI = s.NPIID;				 
quit;
/* HS 542635 - Added Proc deletestatement

Proc delete data=&RGA191..encountersY4e3_&Compno._&run_id.;
run;
proc sort data = &RGA191..encountersY4e3A_&Compno._&run_id. out=&RGA191..encountersY4e3_&Compno._&run_id.;
	by clamno lineno ;
run;

/* HS 542635 - Added Proc deletestatement 
Proc delete data=&RGA191..encountersY4e3A_&Compno._&run_id.;
run;

Proc sort data=COS_poscod nodupkey;by clamno lineno;run;
Data &RGA191..encountersY4e3_&Compno._&run_id.;
merge &RGA191..encountersY4e3_&Compno._&run_id.(in=_a) COS_poscod(in=_b) ;
if _a;
by clamno lineno;
run;*/
Proc sort data=&RGA191..encountersY4e3_&Compno._&run_id.;by clamno;run;
data &RGA191..encountersY4e3_&Compno._&run_id.;
merge &RGA191..encountersY4e3_&Compno._&run_id.(in=_a) Holdcd_N5_main(in=_a) Holdcd_N4_main(in=_b);
if _a;
by clamno;
run;
data &RGA191..encountersY4e3_&Compno._&run_id.(drop = clamno rename = ( clamno_old = clamno temp = clamno_old)); /* HS 569650 - reassigned clamno for multiple TCN*/
length clamno_old $20. temp $20.;
set &RGA191..encountersY4e3_&Compno._&run_id.;
temp = clamno;

run;
*rsubmit;
%macro val;
Data &RGA191..encountersY4e3_&Compno._&run_id.  ;
set &RGA191..encountersY4e3_&Compno._&run_id.;
	PR_val_2430=Round(sum(PR_2430_SVD02_SL_PAID_AMT,PR_2430_CAS03_Var,PR_2430_CAS07_Var /*PR_2430_CAS06,PR_2430_CAS09 ,PR_2430_CAS12,PR_2430_CAS15,PR_2430_CAS18*/),.01);/*ALM # 5160 - COMMENTED*/
	PR_val_MCR_2430=Round(sum(PR_2430_SVD02_SL_PAID_AMT_MCR,PR_2430_CAS03_MCR_Var,PR_2430_CAS07_MCR_Var),.01);
	PR_val_2400=Round(PR_2400_SV102_S_CHG_AMT,.01);
	IN_val_2430=Round(sum(IN_2430_SVD02_SL_PAID_AM,IN_2430_CAS03_Var,IN_2430_CAS07_Var/*IN_2430_CAS06,IN_2430_CAS09 ,IN_2430_CAS12,IN_2430_CAS15,IN_2430_CAS18*/),.01);/*ALM # 5160 - COMMENTED*/
	IN_val_MCR_2430=Round(sum(IN_2430_SVD02_SL_PAID_AM_MCR,IN_2430_CAS03_MCR_Var,IN_2430_CAS07_MCR_Var),.01);	
	IN_val_2400=Round(IN_2400_SV203_SL_CHG_AMT,.01); /* 541126 Kv - added round function */
if clatyp='ON' and typecode in ('O','P') and missing(prvspcd)=1 then prvspcd='799';
%if &compno.=34 %then %do;
/* if missing(MEM_Temp)=0 then do;
 	 MEM_MEMBNO   	=     	MEM_Temp;
	 MEM_MDCARID   	= MEM_Temp;
 end;    */
  	 MEM_MEMBNO   	= cin_cd; /* B-274606 */
	 MEM_MDCARID   	= cin_cd; /* B-274606 */ 
%end;
%if &compno.=01 and &LOB. = DSNP %then %do;
	if mcd_lin_missfl=1 and cin_cd not in ('') then do;
		dsnp_membno=cin_cd; 
		MEM_MDCARID=cin_cd;
		MEM_MEMBNO= cin_cd;
	end;
	if mcr_lin_missfl=1 and cin_cd not in ('') then do;
		dsnp_membno_mcr=cin_cd; 
		MEM_MDCARID_MCR=cin_cd;
		MEM_MEMBNO_MCR= cin_cd;
	end;		
%end;
/* B-146417 comment
%else %if &compno.=30 %then %do;
	if missing(MEM_Temp)=0 then do;
	   MEM_MEMBNO   	=     	MEM_Temp;
	   MEM_MDCARID 	= 	  	MEM_Temp;
 	end;
%end; */

/* HS 569650 - added */
 if missing(NPIID_1)=1 and typecode ="P" and missing(RN_PHY_Npiid)=0 then do; /* CR# CHG0032056 kv - added RN_PHY_Npiid missing condition*/
 	Plstnam_1 =RN_PHY_LST ;/* CR# CHG0032056 kv */
	pfstnam_1 =RN_PHY_FST ; /* CR# CHG0032056 kv */
	NPIID_1=RN_PHY_Npiid;
	padrln1=RN_PHY_ADD1;  																					                                           
	padrln2=RN_PHY_ADD2;   			  																					                                           
	pcitycd=RN_PHY_CITY;   			  																					                                           
	pstacod=RN_PHY_STATE; 																								                                           
	pzipcod=RN_PHY_ZIP;    		
 end;
 

 if missing(NPIID_1)=1 and  typecode in ("I","O") and missing(ATPRVNPI)=0 then do; /* CR# CHG0032056 kv - added ATPRVNPI missing condition*/
    NPIID_1=ATPRVNPI;
	Plstnam_1 =AT_PHY_LST ;/* CR# CHG0032056 kv */
	pfstnam_1 =AT_PHY_FST ; /* CR# CHG0032056 kv */
	padrln1=AT_PHY_ADD1;  																					                                           
	padrln2=AT_PHY_ADD2;   			  																					                                           
	pcitycd=AT_PHY_CITY;   			  																					                                           
	pstacod=AT_PHY_STATE; 																								                                           
	pzipcod=AT_PHY_ZIP;    
  end;

dchrgdt=disdat;
admitdt=admdat;
Run;
%mend;
%val;

*rsubmit;
%macro kidsenrolid_mergeIns;
%if &masking.=N %then %do;

%if &compno=20 %then %do;
/*		data memdailyregister;
		set &libn..memdailyregister(keep=membno altnum  benpkg trndat);
		/* SAS2AWS2: ReplacedFunctionLength 
		if klength(altnum)=8;
		run;
		Data QH99 Final;
		set memdailyregister;
		if benpkg="CHX" then output QH99;
		else output Final;
		Run;
		Proc sort data=QH99 ;by membno descending trndat;run;
		Proc sort data=QH99 out=QH99_final nodupkey;by membno;run;
		Proc sort data=final;by membno;run;
		Data QH99_final1;
		merge QH99_final(in=_a) Final(in=_b keep=membno);
		if _a and not _b;
		by membno;
		run;
		Proc sort data=final  ;by membno descending trndat;run;
		proc sort data=final out=final1 nodupkey;by membno;run;
		Data memdailyregister_Final(keep=membno encenrolid);
		set final1 QH99_final1;
		rename altnum=encenrolid;
		run;

		proc sort data=chplus.kidsencenrolid out=kidsencenrolid_sort;
		by membno descending grpeffdt descending grpexpdt;
		run;
		proc sort data=kidsencenrolid_sort out=kidsencenrolid_nodup(keep=membno encenrolid) nodupkey;
		by membno ;
		run;
		proc sort data=memdailyregister_Final;by membno;run;
		data kidsencenrolid_sort_final;
		merge kidsencenrolid_nodup(in=_a) memdailyregister_Final(in=_b keep=membno);
		if _a and not _b;
		by membno;
		run;

		data entrollment_id(keep=membno encenrolid);
		set memdailyregister_Final kidsencenrolid_sort_final;
		/* SAS2AWS2: ReplacedFunctionLength 
		if klength(encenrolid)=8 and missing(membno)=0;
		run;
		proc sort data=entrollment_id out=kidsencenrolid_nodup;by membno;
		run;
		proc datasets library=work;
/* SAS2AWS2: CommentedModify 
				modify kidsencenrolid_nodup;
				index create membno;
		quit;

		proc sql;
		     create table Temp as
		     select a.*, s.encenrolid
		     from &RGA191..encountersY4e3_&Compno._&run_id. a left join kidsencenrolid_nodup s
		     on a.MEM_MEMBNO = s.membno
			 order by clamno; 		/* B-92443 added sort 
		run;

/*		Data &RGA191..encountersY4e3_&Compno._&run_id.;
		set Temp;
		MEM_MEMBNO=encenrolid;
		MEM_MDCARID=encenrolid;
		Run;  						  B-92443 commented */		

/* B-274606 */
data &RGA191..encountersY4e3_&Compno._&run_id.;
 set &RGA191..encountersY4e3_&Compno._&run_id.;
  encenrolid=hix_person_id_cd;
run;
/* B-274606 */

/* B-92443 start member xwalk encenrolid update */
	    proc sort data=Member_details(rename=(encenrolid=encenrolid_upd)) out=mem_details(keep=clamno encenrolid_upd) nodupkey;
		by clamno;
		run;

		Data &RGA191..encountersY4e3_&Compno._&run_id.;
		Merge &RGA191..encountersY4e3_&Compno._&run_id.(in=_a) mem_details(in=_b);
			by  clamno;
			if _a;
			if _a and _b then do;
				MEM_MEMBNO=encenrolid_upd;
				MEM_MDCARID=encenrolid_upd;
			end;
			else do;
				MEM_MEMBNO=encenrolid;
				MEM_MDCARID=encenrolid;
			end;
		Run;
/* B-92443 end */
%end;
%end;
%mend kidsenrolid_mergeIns;

%kidsenrolid_mergeIns;

%Macro Test();
/* CR# CHG0031183 kv - modified diagnosis code reamp logic -----> start */

/*%if &compno.=20 or &compno.=25 or &compno.=30  %then %do;*/
/*data Remap_diagcode;*/
/*	clamno='';*/
/*	run_id='';*/
/*	old_code='';*/
/*	new_code='';*/
/*run;*/
/*PROC EXPORT DATA= Remap_diagcode	 */
/*		            OUTFILE= "&drv2.\Encounters\NYSDOH\MEDS3toAPD\Extracts\Current\RGA19101_&run_id._&compno._00_D.csv" */
/*		           replace;*/
/*RUN;*/
/**/
/*%end;*/
/*%else %do;*/


	data Diag_control;
	 /* SAS2AWS2: ReplacedSlash-ChangedFolderName-ReplacedSpaceWithUnderscore */
	 infile "&drv2./Encounters_Reporting/APD/Control_Table/SHP_Diag_Rejected.csv" truncover
	        delimiter=',' DSD lrecl=32767 
	         firstobs=2;        /* Production - UNBLOCK */

	length   mhs_diag $12. meds_diag $12. descrp $50.; /* CR# CHG0031183 kv - added descrp */

	input /*clacnt*/ /* CR# CHG0031183 kv - commented */
	      mhs_diag $
		  meds_diag $
		 descrp $ /* CR# CHG0031183 kv - added descrp */;

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

	run;

/* CR# CHG0031183 kv - added logic to pull diagnosis code from hfall diagcode table */

data diagcode_Temp(keep=mhs_diag meds_diag);
set &libn2..diagcode;
	mhs_diag=diagcd;
	meds_diag=diagcd;
run;
Data diag_control;
	set Diag_control diagcode_Temp;
Run;
Proc sort data=diag_control nodupkey;
	by mhs_diag;
Run;

/* End */



	Data control;
	  set Diag_control ;
	  retain fmtname '$diagd';
	  rename mhs_diag = start meds_diag = label;
	Run;
	Proc sort data = control nodupkey;
	  by start;
	Run;
	Proc format cntlin = control;
	  run;
	  Proc sql noprint;
		/* SAS2AWS2: ReplacedFunctionCompress */
		select kcompress('"' || mhs_diag || '"')
		into :mhs_diag  separated by ','
		from Diag_control where descrp ne '';
	Quit;
Proc sort data=&RGA191..encountersY4e3_&Compno._&run_id. out=remap nodupkey;by clamno;run;

/*%if &compno.=01 %then %do;*/
/**/
/*    Data Mcorcat(keep=Clamno Mcorcat_flag) ;*/
/*		set &RGA191..encountersY4e3_&Compno._&run_id.; */
/*		where  Mcorcat in ('PARALVII','PARALVLI','OPHHCA','OPHHMS','OPHHNT','OPHHOT','OPHHPT','OPHHRN','OPHHRT','OPHHSS','OPHHST');*/
/*		 Mcorcat_flag=1;*/
/*	Run;*/
/*	Proc sort data=Mcorcat nodupkey;by Clamno;*/
/*	Run;*/
/*	Proc sort data=&RGA191..encountersY4e3_&Compno._&run_id.;by clamno;run;*/
/*	Proc sort data=Mcorcat;by clamno;run;*/
/*	Data &RGA191..encountersY4e3_&Compno._&run_id.;*/
/*		Merge &RGA191..encountersY4e3_&Compno._&run_id.(in=_a) Mcorcat(in=_b);*/
/*		if _a;*/
/*		by clamno;*/
/*	run;*/
/*	Data Remap_diagcode(keep=clamno run_id old_code new_code);*/
/*		Set &RGA191..encountersY4e3_&Compno._&run_id.(keep=clamno PRDIAGCD diagd2 diagd3 diagd4 diagd5 diagd6	*/
/*						    diagd7 diagd8 diagd9 diag10 diag11 diag12 diag13 diag14 diag15  diagn16 diag17 diag18  */
/*	                        diag19 diag20 diag21 diag22 diag23 diag24 diag25  Mcorcat_flag);*/
/*		array diag{25}    $     Prdiagcd DIAGD2  DIAGD3  DIAGD4 diagd5 diagd6 diagd7 diagd8 diagd9 diag10 diag11 diag12*/
/*	                            diag13 diag14 diag15 diagn16 diag17 diag18 */
/*							    diag19 diag20 diag21 diag22 diag23 diag24 diag25;*/
/*        if Mcorcat_flag=1 then do;*/
/*				do i=1 to 25;				     */
/*						if diag{i} in (&mhs_diag.) then do;*/
/*						    run_id="&run_id.";*/
/*					    	old_code=diag{i};*/
/*							new_code=put(diag{i},$diagd.) ;	*/
/*							output  ; 			    */
/*						end;*/
/*				  */
/*				end;*/
/*        end;*/
/*	Run;*/
/*	Data &RGA191..encountersY4e3_&Compno._&run_id.;*/
/*	set &RGA191..encountersY4e3_&Compno._&run_id.;*/
/**/
/*		array diag{25}    $     Prdiagcd DIAGD2  DIAGD3  DIAGD4 diagd5 diagd6 diagd7 diagd8 diagd9 diag10 diag11 diag12*/
/*		                        diag13      diag14 diag15 diagn16 diag17 diag18 */
/*								diag19 diag20 diag21 diag22 diag23 diag24 diag25;*/
/*      if Mcorcat_flag=1 then do; */
/*		do i=1 to 25;	*/
/*		 diag{i}  = put(diag{i},$diagd.) ;		*/
/*		end;*/
/*	  End;*/
/*	Run;*/
/*%end;*/
/*%else %if  &Compno.=02 or &compno.=34 or &compno.=60  %then %do;*/

/* CR# CHG0031183 kv - modified diagnosis code reamp logic -----> end */

	Data Remap_diagcode(keep=clamno run_id old_code new_code);
		Set &RGA191..encountersY4e3_&Compno._&run_id.(keep=clamno PRDIAGCD diagd2 diagd3 diagd4 diagd5 diagd6	
						    diagd7 diagd8 diagd9 diag10 diag11 diag12 diag13 diag14 diag15  diagn16 diag17 diag18  
	                        diag19 diag20 diag21 diag22 diag23 diag24 diag25);
		array diag{25}    $     Prdiagcd DIAGD2  DIAGD3  DIAGD4 diagd5 diagd6 diagd7 diagd8 diagd9 diag10 diag11 diag12
	                        diag13      diag14 diag15 diagn16 diag17 diag18 
							diag19 diag20 diag21 diag22 diag23 diag24 diag25;

		do i=1 to 25;				     
				if diag{i} in (&mhs_diag.) then do;
			    			run_id="&run_id.";
					    	old_code=diag{i};
							new_code=put(diag{i},$diagd.) ;	
					output  ; 			    
				end;
		  
		end;
	Run;
	Data &RGA191..encountersY4e3_&Compno._&run_id.;
	set &RGA191..encountersY4e3_&Compno._&run_id.;

		array diag{25}    $     Prdiagcd DIAGD2  DIAGD3  DIAGD4 diagd5 diagd6 diagd7 diagd8 diagd9 diag10 diag11 diag12
		                        diag13      diag14 diag15 diagn16 diag17 diag18 
								diag19 diag20 diag21 diag22 diag23 diag24 diag25;
		do i=1 to 25;	
		 diag{i}  = put(diag{i},$diagd.) ;		
		end;
	Run;
/*%end;*//* CR# CHG0031183 kv */
Proc sort data=Remap_diagcode nodupkey;
	by clamno run_id old_code new_code;
Run;

PROC EXPORT DATA= Remap_diagcode	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_D.csv"
		           DBMS=csv REPLACE;
RUN;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_D.csv &drv72./Encounters_Reporting/APD/Archive/&ddir.  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

/*%end;*//* CR# CHG0031183 kv */
%mend;
%test;

/* B-92443 start suppress duplicate Dx code within each obs */
data claim_nodiag(drop=&Keepv_ClmDiag.)
	 claim_diag  (keep=clamno &Keepv_ClmDiag.);
 set &RGA191..encountersY4e3_&Compno._&run_id.;
run;
 
%dup_Dx_chk;

proc sql;
  create table &RGA191..encountersY4e3_&Compno._&run_id.(drop=clamnox) as
    select a.*, b.* 
    from claim_nodiag a left join claim_diag (rename=(clamno=clamnox)) b
    on (a.clamno = b.clamnox)
	order by NPIID_1;
quit;
/* B-92443 end */

/*CHG0039138 - added logic to populate service facility location provider info from providernpi table if npiid is not missing*/
*proc sort data =&RGA191..encountersY4e3_&Compno._&run_id.;
*by NPIID_1;
*run;
data temp_providernpi;
set providernpi_&compno.(keep=npiid First_name Last_name PROVFIRPRACADDR PROVSECPRACADDR PROVPRACCITY PROVPRACSTATE 
PROVPRACPOSTAL);
rename 
npiid=NPIID_1
First_name =First_name1 
Last_name = Last_name1
PROVFIRPRACADDR = PROVFIRPRACADDR1
PROVSECPRACADDR =PROVSECPRACADDR1 
PROVPRACCITY = PROVPRACCITY1
PROVPRACSTATE =PROVPRACSTATE1 
PROVPRACPOSTAL= PROVPRACPOSTAL1
;
run;
proc sort data =temp_providernpi nodupkey;
by NPIID_1;
run;
data &RGA191..encountersY4e3_&Compno._&run_id.;
Merge &RGA191..encountersY4e3_&Compno._&run_id.(in=_a) temp_providernpi(in=b keep =NPIID_1 First_name1 Last_name1 
PROVFIRPRACADDR1 PROVSECPRACADDR1 PROVPRACCITY1 PROVPRACSTATE1 PROVPRACPOSTAL1 );
	by NPIID_1;
	if _a;
	/* SAS2AWS2: ReplacedFunctionCompress */
	if missing(NPIID_1)ne 1 and ((missing(kcompress(Plstnam_1)||kcompress(pfstnam_1)) eq 1) or
	/* SAS2AWS2: ReplacedFunctionCompress */
	(missing(kcompress(padrln1)||kcompress(padrln2)) eq 1) or  (missing(pcitycd) eq 1) or
	/* SAS2AWS2: ReplacedFunctionLength */
	((missing(pstacod) eq 1) or (klength(pstacod) lt 2)) or 	(missing(pzipcod) eq 1)) then do;
		Plstnam_1 =Last_name1 ;
		pfstnam_1 =First_name1 ; 
		padrln1=PROVFIRPRACADDR1;  																					                                           
		padrln2=PROVSECPRACADDR1;   			  																					                                           
		pcitycd=PROVPRACCITY1;   			  																					                                           
		pstacod=PROVPRACSTATE1; 																								                                           
		pzipcod=PROVPRACPOSTAL1;    
  end;
run;
proc sort data = &RGA191..encountersY4e3_&Compno._&run_id.
	out = &RGA191..encountersY5_&Compno._&run_id.;
	by clamno lineno ;
run;

/* Deva - supressing specail character */
data &RGA191..encountersY5_&Compno._&run_id.;
set &RGA191..encountersY5_&Compno._&run_id.;
array charvars(*) _character_;
do i=1 to dim(charvars);
charvars(i) = kcompress(charvars(i),"ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890`~!@#$%^&*()-_=+\|[]{};:',.<>?/","kis");
end;
drop i;
run;


/* CR# CHG0031183 kv - added Diagnosis code & procedure code  reassignment logic & Pointer Logic */

Proc sort data=APD_enceditmatrix(keep=COStype enctypcode) 
	out=cos_1 nodupkey; 
	by COStype enctypcode;where costype  ne "";
run;
Proc sql;
	/* SAS2AWS2: ReplacedFunctionCompress */
	select kcompress('"' || COStype || '"')
		into :COStype_P  separated by ','
		/* SAS2AWS2: ReplacedFunctionUpcase */
		from cos_1 where kupcase(enctypcode)='P';
Quit;
Proc sql;
	/* SAS2AWS2: ReplacedFunctionCompress */
	select kcompress('"' || COStype || '"')
		into :COStype_I  separated by ','
		/* SAS2AWS2: ReplacedFunctionUpcase */
		from cos_1 where kupcase(enctypcode)='I';
Quit;
Proc sql;
	/* SAS2AWS2: ReplacedFunctionCompress */
	select kcompress('"' || COStype || '"')
		into :COStype_O  separated by ','
		/* SAS2AWS2: ReplacedFunctionUpcase */
		from cos_1 where kupcase(enctypcode) ='O';
Quit;

data &RGA191..encountersY5_&Compno._&run_id.;
set &RGA191..encountersY5_&Compno._&run_id.;
/*********************************************************************************************/
/*                 Calling Macro for Diagnosis code reassignment & Pointer Logic    */
/*********************************************************************************************/

%pointer_diag; /*CR# CHG0031183 - added*/
/*********************************************************************************************/
/*               Calling Macro for procedure code reassignment & Pointer Logic                                       */
/*********************************************************************************************/

%proc_cod; /*CR# CHG0031183 - added*/
Prdiagcd = NewDiag1;
DIAGD2 = NewDiag2; 
DIAGD3 = NewDiag3; 
DIAGD4 = NewDiag4;
diagd5 = NewDiag5;
diagd6 = NewDiag6;
diagd7 = NewDiag7;
diagd8 = NewDiag8;
diagd9 = NewDiag9;
diag10 = NewDiag10;
diag11 = NewDiag11;
diag12 = NewDiag12;
diag13 = NewDiag13;     
diag14 = NewDiag14;
diag15 = NewDiag15;
diagn16 = NewDiag16; 
diag17 = NewDiag17;
diag18 = NewDiag18;
diag19 = NewDiag19;
diag20 = NewDiag20;
diag21 = NewDiag21;
diag22 = NewDiag22;
diag23 = NewDiag23;
diag24 = NewDiag24;
diag25 = NewDiag25;
/* CR# CHG0031183 kv - Modified diagnosis code indicator and procedure code indicator value as 'C' when it is 'U' */
if diagvind = 'U' then diagvind = 'C';
if procvind = 'U' then procvind = 'C';
if N5_flag  ='Y' and typecode in ("I","O") and missing(revenue_cd)=1  then revenue_cd = '0001'; /* CHG0031504 */
if N5_flag  ='Y' and typecode in ("I","O") and missing(revenue_cd_mcr)=1  then revenue_cd_mcr = '0001'; 

%macro cos_1;
%if &compno. ne 45 and &compno. ne 30 and "&LOB." ne "QHP" %then %do;	/* B-96285 exclude QHP, HFIC, MCR B-145499 */    
if typecode = "P" and COS not in (&COStype_P.) then COS=""; /* CHG0031624 */
if typecode = "I" and COS not in (&COStype_I.) then COS=""; /* CHG0031624 */
if typecode = "O" and COS not in (&COStype_O.) then COS=""; /* CHG0031624 */
%end;
%mend;
%cos_1;

 if missing(BI_PRV_ADD1)=0 then BI_PRV_ADD=BI_PRV_ADD1; /* CHG0031885 */
 else if missing(BI_PRV_ADD2)=0 then BI_PRV_ADD=BI_PRV_ADD2; /* CHG0031885 */

 if missing(Plstnam_1)=0 then prvnam=Plstnam_1; /* CHG0031885 */ 
 else if missing(pfstnam_1)=0 then prvnam=pfstnam_1; /* CHG0031885 */

 if missing(padrln1)=0 then padrln=padrln1; /* CHG0031885 */ 
 else if missing(padrln2)=0 then padrln=padrln2; /* CHG0031885 */
 
 if missing(MEM_MADRLN1)=0 then MEM_MADRLN=MEM_MADRLN1; /* CHG0033527 */ 
 else if missing(MEM_MADRLN2)=0 then MEM_MADRLN=MEM_MADRLN2; /* CHG0033527 */

   /*if (BIPRVNPI in (&binpiid.) and IN_2400_SV202_2_SL_HCPCS in (&NDC_Svccod.)) or CPT_Cod1 in ('UD') 
        or CPT_Cod2 in ('UD') or CPT_Cod3 in ('UD') or CPT_Cod4 in ('UD')*/ /* 22225 inculded by pradeep */
	/*then UD_flag=1;*/ /* CHG0032206 */ /*B-222732 Commented */

 BIPRVNPI_Updated=BIPRVNPI; /* CHG0032056 */

/* if missing(BIPRVNPI)=0 then BIPRVNPI=BIPRVNPI;*/
/* else if missing(BIPRVNPI_new)=0 then BIPRVNPI=BIPRVNPI_new;*/
/* CR# CHG0035401 - Added compress function for zip code */
	/* SAS2AWS2: ReplacedFunctionCompress */
	BI_PRV_ZIP = kcompress(BI_PRV_ZIP,"@:*`~=#");
	/* SAS2AWS2: ReplacedFunctionCompress */
	MEM_MZIPCOD = kcompress(MEM_MZIPCOD,"@:*`~=#");
	/* SAS2AWS2: ReplacedFunctionCompress */
	pzipcod = kcompress(pzipcod,"@:*`~=#");
run;

/* CR# CHG0038837 - added logic to create  exception report when category of service is missing --- start */
proc sort data= &RGA191..encountersY5_&Compno._&run_id.(where = (missing(cos) eq 1)) 
out = enc_cos_missing(Keep = clamno  typecode cos) nodupkey;
by clamno ;
run;

%macro E_file;
%if &compno. ne 45 and &compno. ne 30 and "&LOB." ne "QHP" %then %do;/* B-96285 exclude QHP, HFIC, MCR B-145499 */
PROC EXPORT DATA= enc_cos_missing	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_E.csv"
		           dbms = csv replace;
RUN;	

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_E.csv &drv72./Encounters_Reporting/APD/Archive/&ddir.  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 
%end;
%mend;
%E_file;
/* CR# CHG0038837 - added logic to create  exception report when category of service is missing --- end */


/* CR# CHG0036913 kv - added logic to map COS & PRVSPCD from user given control file   ---> start */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */

/*B-244088 commented*/
/*Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/COS_PRVSPEC.xlsx"
	out=COS_Control_Table dbms=xlsx
	replace;
	sheet = "COS";
Run;

data COS_Control_Table;
length clamno $20.;
set COS_Control_Table;
run;*/


/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
/*Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/COS_PRVSPEC.xlsx"
	out=PRVSPCD_Control_Table dbms=xlsx
	replace;
	sheet = "PRVSPEC";
Run;

data PRVSPCD_Control_Table;
length clamno $20. ;
set PRVSPCD_Control_Table;
run;*/

/*B-244088 Added*/
data COS_Control_Table;
	 
	 infile "&drv2./Encounters_Reporting/APD/Control_Table/COS_Control_Table.csv" truncover
	        delimiter=',' DSD lrecl=32767 
	         firstobs=2;        

	length   clamno $20. cos $2. ;

	input 
	      clamno $
           cos $;
		 
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

	run;

/*B-244088 Added*/
data PRVSPCD_Control_Table;
	 
	 infile "&drv2./Encounters_Reporting/APD/Control_Table/PRVSPCD_Control_Table.csv" truncover
	        delimiter=',' DSD lrecl=32767 
	         firstobs=2;        

	length   clamno $20. prvspec $3. ;

	input 
	      clamno $
           prvspec $;
		 
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

	run;

proc sort data= &RGA191..encountersY5_&Compno._&run_id.;
by clamno;
run;

proc sort data = COS_Control_Table(rename =(cos=cos_new ));
by clamno;
run;
proc sort data = PRVSPCD_Control_Table(rename =(PRVSPEC =prvspcd_new));
by clamno;
run;

data &RGA191..encountersY5_&Compno._&run_id.;
merge &RGA191..encountersY5_&Compno._&run_id.(in=a) COS_Control_Table(in=b) PRVSPCD_Control_Table(in=c);
by clamno;
if a;
if a and b then do;
	cos=cos_new;
end;
if a and c then do;
	prvspcd =prvspcd_new;
end;
drop cos_new prvspcd_new;
if prvspcd eq '' and typecode in ('I','O') then prvspcd ='799'; /* CR# CHG0037492 - added */
if prvspcd eq '' and typecode in ('P') then prvspcd ='999'; /* CR# CHG0037492 - added */

/* CR# CHG0038449 - added logic to reject zipcode if length is not equal to 9 */	
    /* SAS2AWS2: ReplacedFunctionLength */
    if klength(Compress(BI_PRV_ZIP)) = 9 then
	do;
	    BI_PRV_ZIP=BI_PRV_ZIP;
	end;
	/* SAS2AWS2: ReplacedFunctionLength */
	else if klength(Compress(BI_PRV_ZIP)) = 5 then
	do;
	    /* SAS2AWS2: ReplacedFunctionCompress */
	    BI_PRV_ZIP=kcompress(BI_PRV_ZIP||'9998');
	end; 
	else 
	do;
		BI_PRV_ZIP='';
	end;	


    /* SAS2AWS2: ReplacedFunctionLength */
    if klength(Compress(pzipcod)) = 9 then
	do;
	    pzipcod=pzipcod;
	end;
	/* SAS2AWS2: ReplacedFunctionLength */
	else if klength(Compress(pzipcod)) = 5 then
	do;
	    /* SAS2AWS2: ReplacedFunctionCompress */
	    pzipcod=kcompress(pzipcod||'9998');
	end; 
	else 
	do;
		pzipcod='';
	end;	

	/* SAS2AWS2: ReplacedFunctionLength */
	if klength(Compress(MEM_MZIPCOD)) = 9 then
    do;
       MEM_MZIPCOD =MEM_MZIPCOD;
    end;
    /* SAS2AWS2: ReplacedFunctionLength */
    else if klength(Compress(MEM_MZIPCOD)) = 5 then
    do;
       /* SAS2AWS2: ReplacedFunctionCompress */
       MEM_MZIPCOD  =kcompress(MEM_MZIPCOD||'9998');
    end; 
	else 
	do;
	   MEM_MZIPCOD  ='';
	end;
	
	if klength(Compress(MEM_MZIPCOD_MCR)) = 9 then
    do;
       MEM_MZIPCOD_MCR =MEM_MZIPCOD_MCR;
    end;
    else if klength(Compress(MEM_MZIPCOD_MCR)) = 5 then
    do;
       MEM_MZIPCOD_MCR  =kcompress(MEM_MZIPCOD_MCR||'9998');
    end; 
	else 
	do;
	   MEM_MZIPCOD_MCR  ='';
	end;	

if missing(cos) eq 1 and typecode in ('I','O') then cos ='87'; /* CR# CHG0038619 - added */
if missing(cos) eq 1 and typecode in ('P') then cos ='01'; /* CR# CHG0038619 - added */

		/*CR# CHG0040702 - Added round function for all amount variables to correct invalid decimal values */
	if typecode in ('I','O') then do;
		IN_2400_SV203_SL_CHG_AMT= round(IN_2400_SV203_SL_CHG_AMT,.01);
		IN_2430_SVD02_SL_PAID_AM= round(IN_2430_SVD02_SL_PAID_AM,.01);
		IN_2430_CAS03_Var= round(IN_2430_CAS03_Var,.01);
		IN_2430_CAS07_Var= round(IN_2430_CAS07_Var,.01);
		
		IN_2430_SVD02_SL_PAID_AM_MCR= round(IN_2430_SVD02_SL_PAID_AM_MCR,.01);
		IN_2430_CAS03_MCR_Var= round(IN_2430_CAS03_MCR_Var,.01);
	end;
	else if typecode in ('P','D') then do;
		PR_2400_SV102_S_CHG_AMT= round(PR_2400_SV102_S_CHG_AMT,.01);
		PR_2430_SVD02_SL_PAID_AMT= round(PR_2430_SVD02_SL_PAID_AMT,.01);
		PR_2430_CAS03_Var= round(PR_2430_CAS03_Var,.01);
		PR_2430_CAS07_Var= round(PR_2430_CAS07_Var,.01);

    PR_2430_SVD02_SL_PAID_AMT_MCR= round(PR_2430_SVD02_SL_PAID_AMT_MCR,.01);
		PR_2430_CAS03_MCR_Var= round(PR_2430_CAS03_MCR_Var,.01);
	end;

	if "&compno." = "45" or "&compno." = "30" or "&LOB." eq "QHP" then do; /* B-154145 blank QHP, HFIC, MCR */
		cos = '' ;
		prvspcd = '' ;
	end;
run; 
/* CR# CHG0036913 kv - added logic to map COS & PRVSPCD from user given control file   ---> end */

/* VVKR Code Start -- B-75041 */

proc sql noprint;
create table &RGA191..cas_dg01_jn_reject_svclines as
	select a.clamno,a.lineno,a.edit_code,a.edit_chk_type,a.typecode,b.capind,b.apsvcstat,b.revind,b.svctyp,b.alwamt,b.netalwamt,b.netalwamt_old
	from &RGA191..Reject_DG_I_&Compno._&run_id. as a left join &RGA191..svclines_val_nami_&compno. as b
	on a.clamno=b.clamno and a.lineno=b.lineno where a.edit_code = 'EIPDG';
quit;

proc sql noprint;
create table &RGA191..cas_dg02_TempFlag as
	select a.clamno,a.lineno,a.IN_2430_SVD02_SL_PAID_AM,a.Temp_Flag,b.lineno as lineno_reject,b.edit_code,b.edit_chk_type,b.typecode,b.capind,b.apsvcstat,b.revind,b.svctyp,b.alwamt,b.netalwamt,b.netalwamt_old
	from &RGA191..encountersY5_&Compno._&run_id. as a left join &RGA191..cas_dg01_jn_reject_svclines as b
	on a.clamno=b.clamno where a.Temp_flag=1 order by clamno,lineno;
quit;

proc sql noprint;
	create table &RGA191..cas_dg02_A_morthn1lineno as select * from &RGA191..cas_dg02_TempFlag where apsvcstat='PD' and revind='N' group by clamno,lineno having count(*)>1 order by clamno,lineno;
	create table &RGA191..cas_dg02_B_1lineno as select * from &RGA191..cas_dg02_TempFlag group by clamno,lineno having count(*)=1 order by clamno,lineno;
	create table &RGA191..cas_dg02_C_morthn1lineno as select a.*,b.apsvcstat as apsvcstat_lineno,b.revind as revind_lineno from &RGA191..cas_dg02_A_morthn1lineno a left join &RGA191..cas_svclines b 
		on a.clamno=b.clamno and a.lineno=b.lineno where a.apsvcstat='PD' and a.revind='N' order by clamno,lineno,lineno_reject;
	create table &RGA191..cas_dg02_D_1lineno as select a.*,b.apsvcstat as apsvcstat_lineno,b.revind as revind_lineno from &RGA191..cas_dg02_B_1lineno a left join &RGA191..cas_svclines b 
		on a.clamno=b.clamno and a.lineno=b.lineno where b.apsvcstat='PD' and b.revind='N' order by clamno,lineno,lineno_reject;
quit;

data &RGA191..cas_dg02_E_morthn1lineno;
	set &RGA191..cas_dg02_C_morthn1lineno;
	by clamno lineno lineno_reject;
	if first.clamno and first.lineno and first.lineno_reject;
run;

data &RGA191..cas_dg02_F_uniq_lineno;
	set &RGA191..cas_dg02_D_1lineno &RGA191..cas_dg02_E_morthn1lineno;
	by clamno lineno lineno_reject;
run;

data &RGA191..cas_dg02_G_PD_lineno;
	set &RGA191..cas_dg02_F_uniq_lineno;
	if (apsvcstat=apsvcstat_lineno and revind=revind_lineno) then do;
		if IN_2430_SVD02_SL_PAID_AM = 0 then output; /* Since paid claims not required to populate CAS segment (CAS09 to CAS12) not considering if IN_2430_SVD02_SL_PAID_AM  column have Greater than ZERO*/
	end;
run;

data &RGA191..cas_dg03_compare_Matched &RGA191..cas_dg03_compare_Not_Matched; /* This Step is testing purpose */
set &RGA191..cas_dg02_G_PD_lineno;
if IN_2430_SVD02_SL_PAID_AM ne netalwamt then do;
	Amt_Status='Not Matched';
	put clamno lineno IN_2430_SVD02_SL_PAID_AM netalwamt;
	output &RGA191..cas_dg03_compare_Not_Matched;
end;
else do;
	Amt_Status='Matched';
	output &RGA191..cas_dg03_compare_Matched;
end;
run;

proc sql noprint;
create table &RGA191..cas_dg04_svcholds as
	select a.*,b.holdcd,b.typcod from &RGA191..cas_dg02_G_PD_lineno as a left join 
	(select clamno,lineno,holdcd,typcod from svcholds where typcod eq 'E') as b on a.clamno eq b.clamno and a.lineno_reject eq b.lineno order by clamno,lineno ; 
quit;

data &RGA191..cas_dg05_dont_populate &RGA191..cas_dg05_populate;
set &RGA191..cas_dg04_svcholds;
by clamno lineno;
if (apsvcstat ne 'PD') or (revind ne 'N') or (typcod ne 'E') then do;
	cas_flag=.;
	output &RGA191..cas_dg05_dont_populate;
end;
else if (apsvcstat eq 'PD') or (revind eq 'N') or (typcod eq 'E') then output &RGA191..cas_dg05_populate;
run;

	%macro cas_dg_09_12(ipds,od,ln_var);
			proc sql noprint;
				create table &RGA191..&od._01_xwalk_typcod as /* Join XWALK dataset with holdcd column to get carc & nyscarc columns */
				select a.*,b.carc,b.nyscarc 
				from &RGA191..&ipds. as a left join (select * from cas_xwalk where typcod='E') as b on a.holdcd=b.holdcd
				order by clamno,&ln_var.,holdcd ;
			quit;

			data &RGA191..&od._02_y &RGA191..&od._02_n &RGA191..&od._02_null; /* Create tables based on nyscarc column value and cas_flag column based on typecode & capind columns*/
			set &RGA191..&od._01_xwalk_typcod;
				by clamno &ln_var.; 
				if typecode in ('P','D','I','O') and capind in ('F','C') then cas_flag=1;	
				if nyscarc='Y' then output  &RGA191..&od._02_y;
				else if nyscarc='N' then output  &RGA191..&od._02_n;
				else do;
					carc='272';
					output  &RGA191..&od._02_null;
				end;
			run;

			%macro get_first_clm(in_ds,ou_ds); /* extract records for each claim first lineno and holdcd */
				data &ou_ds.;
				set &in_ds.;
				by clamno &ln_var. holdcd; 
				if first.&ln_var. and first.holdcd then output;
				run;
			%mend get_first_clm;

			%get_first_clm(&RGA191..&od._02_y,&RGA191..&od._03_y);
			%get_first_clm(&RGA191..&od._02_n,&RGA191..&od._03_n);
			%get_first_clm(&RGA191..&od._02_null,&RGA191..&od._03_null);

			data &RGA191..&od._04_n; /* Excluding cas_4_n ('N') table claims if same claim exists in cas_4_y ('Y') table */
			merge &RGA191..&od._03_y(in=a keep=clamno &ln_var.) &RGA191..&od._03_n(in=b);
			by clamno &ln_var.; 
			if not a;
			run;

			data &RGA191..&od._05_y_n; /* Appending nyscarc = 'Y' & 'N' claims */
			set &RGA191..&od._03_y &RGA191..&od._04_n;
			by clamno &ln_var.; 
			run;

			data &RGA191..&od._06_null; /* Excluding cas_4_null ('NULL') table claims if same claim exists in cas_6_y_n ('Y' & 'N') table */
			merge &RGA191..&od._05_y_n(in=a keep=clamno &ln_var.) &&RGA191..&od._03_null(in=b);
			by clamno &ln_var.; 
			if not a;
			run;

			data &RGA191..&od._07_Final; /* Appending nyscarc = 'Y', 'N' & NULL claims */
			set &RGA191..&od._05_y_n &RGA191..&od._06_null;
			by clamno &ln_var.;
			run;
	%mend cas_dg_09_12;

	%cas_dg_09_12(ipds=cas_dg05_populate,od=cas_dg,ln_var=lineno_reject);	
	
	data &RGA191..cas_dg10_final;
		set &RGA191..cas_dg05_dont_populate &RGA191..cas_dg_07_Final;
	run;
	
	proc sort data=&RGA191..cas_dg10_final;
		by clamno lineno;
	run;
	
/* VVKR Code End -- B-75041 */

/* HS 542635 - Added Proc deletestatement */
Proc delete data=&RGA191..encountersY4e3_&Compno._&run_id.;run;

/* ALM# 7174 - Added logic to map Exchange member id instead of member id for QHP & EP line of business ---> START*/

/* SAS2AWS2: ReplacedSlash-ChangedFolderName *//*
proc import out=flag   datafile="&drv2./Encounters_Reporting/APD/Control_Table/Exchgmembno_Flag.csv"  dbms=csv  replace; run;
proc sql noprint; select flag   into :flag   from flag where lob eq "&lob."; quit;
%put &flag.;*//* B-274606 */

%macro member_mapping;
%if "&lob."  EQ "EP" /* and "&flag." = "Y" */ %then %do; /* B-274606 */
/*	Data claims_EP(keep = clamno membno firstdos provno benpkg lobcod);
		Set &RGA191..encountersY5_&Compno._&run_id. ;
    Run;
	proc sort data =claims_EP nodupkey;
	by clamno;
	run;

	/* step 1
	proc sql;
	create table claims_EP_Remit as select a.*, b.stmembno from
		claims_EP a left join &libn..Remit b on
		A.membno = B.membno and A.lobcod = B.lobcod and intnx('month',a.firstdos,0)= B.svcdat ;
	quit;
	proc sql;
	create table stmembno_count1 as select 
		distinct clamno, count(stmembno) as mem_count from claims_EP_Remit
		group by clamno;
	create table claims_EP_Remit1 as select a.*,b.mem_count from 
	claims_EP_Remit a left join stmembno_count1 b on a.clamno=b.clamno;
	quit;
	data Final1(rename = (stmembno = NYS_Member_id)) claims_EP_invalid;
		set claims_EP_Remit1;
	if mem_count eq 1 then output Final1;
	else output claims_EP_invalid;
	run;

	/* step 2
	proc sql;
	create table claims_EP_invalid1 as select a.*, b.perid,b.seqnum from
	claims_EP_invalid a left join &libn..Memdailyregister b on
	A.membno = B.membno and A.lobcod = B.lobcod and 
	A.firstdos GE B.memeff and  ((A.firstdos LE B.memexp) or (missing(b.memexp) eq 1)) ;
	quit;
	proc sort data = claims_EP_invalid1 out=claims_EP_invalid1(where =(perid ne ''));
	by clamno descending seqnum;
	run;
	data Final2(rename = (perid = NYS_Member_id));
	set claims_EP_invalid1;
	by clamno descending seqnum;
	if first.clamno and  first.seqnum then output;
	run;

	/* step 3
	proc sql;
	create table claims_EP_invalid1 as select * from claims_EP_invalid
			where clamno not in  (select distinct clamno from Final2);
		create table claims_EP_Remit as select a.clamno, a.membno, a.firstdos, a.provno, a.benpkg, a.lobcod, b.stmembno from
	claims_EP_invalid1 a left join &libn..Remit b on
	A.membno = B.membno and A.lobcod = B.lobcod  ;
	quit;
	proc sql;
	create table stmembno_count2 as select distinct clamno,
			count(stmembno) as  mem_count from claims_EP_Remit group by clamno;
	create table claims_EP_Remit1 as select a.*,b.mem_count from 
	claims_EP_Remit a left join stmembno_count2 b on a.clamno=b.clamno;
	quit;
	data Final3(rename = (stmembno = NYS_Member_id)) claims_EP_invalid;
	set claims_EP_Remit1;
	if mem_count eq 1 then output Final3;
	else output claims_EP_invalid;
	run;

	/* step 4
	proc sql;
	create table claims_EP_invalid1 as select a.*, b.perid,b.seqnum from
	claims_EP_invalid a left join &libn..Memdailyregister b on
	A.membno = B.membno and A.lobcod = B.lobcod 
	 ;
	quit;
	proc sort data = claims_EP_invalid1 out=claims_EP_invalid1(where =(perid ne ''));
	by clamno descending seqnum;
	run;
	data Final4(rename = (perid = NYS_Member_id));
	set claims_EP_invalid1;
	by clamno descending seqnum;
	if first.clamno and  first.seqnum then output;
	run;

	/* step 5
	proc sql;
	create table claims_EP_invalid1 as select * from claims_EP_invalid 
		where clamno not in  (select distinct clamno from Final4);
	create table claims_EP_Remit  as select a.clamno, a.membno, a.firstdos, a.provno, 
		a.benpkg, a.lobcod, b.stmembno from
		claims_EP_invalid1 a left join &libn..Remit b on
		A.membno = B.membno   ;
	quit;
	proc sql;
	create table stmembno_count3 as select distinct clamno, count(stmembno) as mem_count 
		from claims_EP_Remit  group by clamno;
	create table claims_EP_Remit1 as select a.*,b.mem_count from 
	claims_EP_Remit  a left join stmembno_count3 b on a.clamno=b.clamno;
	quit;
	data final5(rename = (stmembno = NYS_Member_id)) claims_EP_invalid;
	set claims_EP_Remit1;
	if mem_count eq 1 then output final5;
	else output claims_EP_invalid;
	run;

	/* step 6
	proc sql;
	create table claims_EP_invalid1 as select a.*, b.perid,b.seqnum from
	claims_EP_invalid a left join &libn..Memdailyregister b on
	A.membno = B.membno   ;
	quit;
	proc sort data = claims_EP_invalid1 out=claims_EP_invalid1(where =(perid ne ''));
	by clamno descending seqnum;
	run;
	data Final6(rename = (perid = NYS_Member_id));
	set claims_EP_invalid1;
	by clamno descending seqnum;
	if first.clamno and  first.seqnum then output;
	run;

DATA final1(drop =NYS_Member_id);   /* please check 
  set final1;
    length NYS_Member_id1 $20.;
        
    NYS_Member_id1 = ksubstr(NYS_Member_id,1,20);
    format NYS_Member_id1 $20.;
    run; 
    
    DATA final1;
  set final1(rename=(NYS_Member_id1 = NYS_Member_id));
        run; 

DATA final2(drop =NYS_Member_id);   /* please check 
  set final2;
    length NYS_Member_id1 $20.;
        
    NYS_Member_id1 = ksubstr(NYS_Member_id,1,20);
    format NYS_Member_id1 $20.;
    run; 
    
    DATA final2;
  set final2(rename=(NYS_Member_id1 = NYS_Member_id));
        run; 
        DATA final3(drop =NYS_Member_id);   /* please check 
  set final3;
    length NYS_Member_id1 $20.;
        
    NYS_Member_id1 = ksubstr(NYS_Member_id,1,20);
    format NYS_Member_id1 $20.;
    run; 
    
    DATA final3;
  set final3(rename=(NYS_Member_id1 = NYS_Member_id));
        run; 
        DATA final4(drop =NYS_Member_id);   /* please check 
  set final4;
    length NYS_Member_id1 $20.;
        
    NYS_Member_id1 = ksubstr(NYS_Member_id,1,20);
    format NYS_Member_id1 $20.;
    run; 
    
    DATA final4;
  set final4(rename=(NYS_Member_id1 = NYS_Member_id));
        run; 
        DATA final5(drop =NYS_Member_id);   /* please check 
  set final5;
    length NYS_Member_id1 $20.;
        
    NYS_Member_id1 = ksubstr(NYS_Member_id,1,20);
    format NYS_Member_id1 $20.;
    run; 
    
    DATA final5;
  set final5(rename=(NYS_Member_id1 = NYS_Member_id));
        run; 
        DATA final6(drop =NYS_Member_id);   /* please check 
  set final6;
    length NYS_Member_id1 $20.;
        
    NYS_Member_id1 = ksubstr(NYS_Member_id,1,20);
    format NYS_Member_id1 $20.;
    run; 
    
    DATA final6;
  set final6(rename=(NYS_Member_id1 = NYS_Member_id));
        run; 

	/* consolidated output 
	data final;
	length NYS_Member_id $20.;
	set final1 final2 final3 final4 final5 final6;
	run;
	proc sort data =final (keep= clamno membno firstdos provno benpkg lobcod NYS_Member_id) nodupkey;
	by clamno NYS_Member_id;
	run;
	proc sql;
		     create table Temp as
		     select a.*, s.NYS_Member_id
		     from &RGA191..encountersY5_&Compno._&run_id. a left join final s
		     on a.MEM_MEMBNO = s.membno and a.clamno = s.clamno;				 
		run;

		Data &RGA191..encountersY5_&Compno._&run_id.;
		set Temp;
		MEM_MEMBNO=NYS_Member_id;
		MEM_MDCARID=NYS_Member_id;
		MEM_subsno=NYS_Member_id;
		drop NYS_Member_id;
		Run; */

/* B-274606 */
DATA &RGA191..encountersY5_&Compno._&run_id.;
 set &RGA191..encountersY5_&Compno._&run_id.;
  MEM_MEMBNO=hix_person_id_cd;
  MEM_MDCARID=hix_person_id_cd;
  MEM_subsno=hix_person_id_cd;   
Run;
/* B-274606 */		

/*CR# CHG0038837 -added logic to map exchange member number and subscriber number from Member_Xwalk.xlsx control file -- start */
		proc sort data  = Member_details(keep = clamno exchgmembno exchgsubsno) nodupkey;
		by  clamno;
		run;

		proc sort data =&RGA191..encountersY5_&Compno._&run_id.;
		by clamno;
		run;

		Data &RGA191..encountersY5_&Compno._&run_id.(drop=exchgmembno exchgsubsno);
			Merge &RGA191..encountersY5_&Compno._&run_id.(in=_a) Member_details(in=_b);
			by  clamno;
			if _a;
			if _a and _b then do;
				MEM_MEMBNO=exchgmembno  ;
				MEM_MDCARID=exchgmembno;
				MEM_subsno=exchgsubsno;
			end;
		run;
/*CR# CHG0038837 -added logic to map exchange member number and subscriber number from Member_Xwalk.xlsx control file - end */
%end;

/* start member map QHP B-96285, B-133710 added relcod */
%else %if "&lob." eq "QHP" /* and "&flag." = "Y" */ %then %do; /* B-274606 */	
/*	data claims_QHP(keep = clamno membno relcod firstdos provno benpkg lobcod);
	set &RGA191..encountersY5_&Compno._&run_id. ;
	run;
	proc sort data =claims_QHP nodupkey;
	by clamno relcod;
	run;

	/* step 1   B-133710 added relcod 
	proc sql;
	create table claims_QHP_memdaily as select a.*, b.exchgmembno,b.seqnum,b.exchgsubsno from /*CR# CHG0038837 - added exchgsubsno
	claims_QHP a left join &libn..Memdailyregister b on
	A.membno = B.membno and A.relcod = B.relcod and A.Benpkg = B.Benpkg and A.firstdos GE B.memeff and ((A.firstdos LE B.memexp) 
	or (missing(b.memexp) eq 1)) ;
	quit;

	proc sort data = claims_QHP_memdaily out=claims_QHP_memdaily_valid(where =(exchgmembno ne ''));
	by clamno descending seqnum;
	run;

	data final1;
	set claims_QHP_memdaily_valid;
	by clamno descending seqnum;
	if first.clamno and  first.seqnum then output;
	run;

	/* step 2    B-133710 added relcod 
	proc sql;
	create table claims_QHP_memdaily_invalid as select a.clamno,a.membno,a.relcod,a.firstdos,a.provno,a.benpkg, b.exchgmembno,b.seqnum,b.exchgsubsno from /*CR# CHG0038837 - added exchgsubsno
	claims_QHP_memdaily(where =(exchgmembno eq '')) a left join &libn..Memdailyregister b on
	A.membno = B.membno and A.relcod = B.relcod and A.Benpkg = B.Benpkg  ;
	quit;

	proc sort data = claims_QHP_memdaily_invalid out=claims_QHP_memdaily_valid1(where =(exchgmembno ne ''));
	by clamno descending seqnum;
	run;

	data final2;
	set claims_QHP_memdaily_valid1;
	by clamno descending seqnum;
	if first.clamno and  first.seqnum then output;
	run;

	/* step 3    B-133710 added relcod 
	proc sql;
	create table claims_QHP_memdaily_invalid1 as select a.clamno,a.membno,a.relcod,a.firstdos,a.provno,a.benpkg, b.exchgmembno,b.seqnum,b.exchgsubsno from /*CR# CHG0038837 - added exchgsubsno
	claims_QHP_memdaily_invalid(where =(exchgmembno eq '')) a left join &libn..Memdailyregister b on
	A.membno = B.membno and A.relcod = B.relcod ;
	quit;

	proc sort data = claims_QHP_memdaily_invalid1 out=claims_QHP_memdaily_valid2(where =(exchgmembno ne ''));
	by clamno descending seqnum;
	run;

	data final3;
	set claims_QHP_memdaily_valid2;
	by clamno descending seqnum;
	if first.clamno and  first.seqnum then output;
	run;

	/* consolidated output 
	data final;
	set final1 final2 final3;
	run;

	proc sort data =final (keep= clamno membno relcod firstdos provno benpkg lobcod exchgmembno exchgsubsno) nodupkey; /*CR# CHG0038837 - added exchgsubsno
	by clamno /*exchgmembno membno relcod; /*CR# CHG0038837 - added membno, B-133710 added relcod 
	run;

		proc sql;
		     create table Temp as
		     select a.*, s.exchgmembno, s.exchgsubsno /*CR# CHG0038837 - added exchgsubsno
		     from &RGA191..encountersY5_&Compno._&run_id. a left join final s
		     on a.MEM_MEMBNO = s.membno and a.relcod = s.relcod and a.clamno = s.clamno;  /* B-133710 added relcod			 
		run;

		Data &RGA191..encountersY5_&Compno._&run_id.;
		set Temp;
		COS=''; /* B-96285 only for QHP 
		MEM_MEMBNO=exchgmembno;
		MEM_MDCARID=exchgmembno;
		Sub_MDCARID=/*exchgmembno exchgsubsno; /*CR# CHG0038837 
		MEM_subsno= /*exchgmembno exchgsubsno; /*CR# CHG0038837 
		SUB_MEMBNO= /*exchgmembno exchgsubsno; /*CR# CHG0038837 
		drop exchgmembno exchgsubsno;
		Run;
*/
/* B-274606 */
DATA &RGA191..encountersY5_&Compno._&run_id.;
 set &RGA191..encountersY5_&Compno._&run_id.;
	COS='';
  	MEM_MEMBNO=hix_member_id_cd;
	MEM_MDCARID=hix_member_id_cd;
	Sub_MDCARID=exchange_subscriber_id_cd;
	MEM_subsno=exchange_subscriber_id_cd;
	SUB_MEMBNO=exchange_subscriber_id_cd;
run;
/* B-274606 */
/*CR# CHG0038837 -added logic to map exchange member number and subscriber number from Member_Xwalk.xlsx control file -- start */
		proc sort data  = Member_details(keep = clamno exchgmembno exchgsubsno) nodupkey;
		by  clamno;
		run;

		proc sort data =&RGA191..encountersY5_&Compno._&run_id.;
		by clamno;
		run;

		Data &RGA191..encountersY5_&Compno._&run_id.(drop=exchgmembno exchgsubsno);
			Merge &RGA191..encountersY5_&Compno._&run_id.(in=_a) Member_details(in=_b);
			by  clamno;
			if _a;
			if _a and _b then do;
				MEM_MEMBNO=exchgmembno;
				MEM_MDCARID=exchgmembno;
				Sub_MDCARID=exchgsubsno;
				MEM_subsno=exchgsubsno;
				SUB_MEMBNO=exchgsubsno;
			end;
		run;
/*CR# CHG0038837 -added logic to map exchange member number and subscriber number from Member_Xwalk.xlsx control file - end */
%end; /* end member map QHP B-96285 */


/*B-247073 mdcasno swap starts here*/
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Member_Xwalk.xlsx"
		out=Member_details dbms=xlsx replace;
		run;

data Member_details;
rename mdcasno=mdcasno_upd;
set Member_details;
where missing(mdcasno)=0;
run;

proc sort data  = Member_details(keep = clamno mdcasno_upd) nodupkey;
		by  clamno;
		run;

		proc sort data =&RGA191..encountersY5_&Compno._&run_id.;
		by clamno;
		run;

Data &RGA191..encountersY5_&Compno._&run_id.(drop=mdcasno_upd );
			Merge &RGA191..encountersY5_&Compno._&run_id.(in=_a) Member_details(in=_b);
			by  clamno;
			if _a;
			if _a and _b then do;
				MEM_MEMBNO=mdcasno_upd;
				MEM_MDCARID=mdcasno_upd;
				Sub_MDCARID=mdcasno_upd;
				MEM_subsno=mdcasno_upd;
				SUB_MEMBNO=mdcasno_upd;
			end;
		run;
/*B-247073 mdcasno swap Ends here*/

%mend member_mapping;
%member_mapping;
/* ALM# 7174 - Added logic to map Exchange member id instead of member id for QHP & EP line of business ---> end*/

 
/* CR# S-22225 - added logic to read values from COSProvider_Specialty_codes_mapping control file 
  if valcode and valamount value matches then populate cos and prvspcd values from control file -- Starts here */
%macro cosprovider_mapping;
  %if &compno. ne 45 and &compno. ne 30 and "&LOB." ne "QHP" %then %do; /* B-154145 exclude QHP, HFIC, MCR */
    Data &RGA191..encountersY5_&Compno._&run_id.;
	set &RGA191..encountersY5_&Compno._&run_id.;
	
	 %let i = 1 ;
	 %do %while(&i. <= &COSProvider_code_total.); 

         /* SAS2AWS2: ReplacedFunctionStrip */
         if valcd01="&&Valcode&i."  and  valA01=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
         /* SAS2AWS2: ReplacedFunctionStrip */
         if valcd02="&&Valcode&i."  and  valA02=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd03="&&Valcode&i."  and  valA03=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd04="&&Valcode&i."  and  valA04=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd05="&&Valcode&i."  and  valA05=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd06="&&Valcode&i."  and  valA06=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd07="&&Valcode&i."  and  valA07=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd08="&&Valcode&i."  and  valA08=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
         /* SAS2AWS2: ReplacedFunctionStrip */
         if valcd09="&&Valcode&i."  and  valA09=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd10="&&Valcode&i."  and  valA10=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
		 /* SAS2AWS2: ReplacedFunctionStrip */
		 if valcd11="&&Valcode&i."  and  valA11=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
         /* SAS2AWS2: ReplacedFunctionStrip */
         if valcd12="&&Valcode&i."  and  valA12=&&Valamount&i. then do ;COS=kstrip(&&COS&i.) ;prvspcd=kstrip(&&prvspcd&i.) ;end;
         
		%let i = %eval(&i. + 1);
	%end;
    %let i = 1 ;
	run;
  %end;	
%mend;
%cosprovider_mapping;


/*B-247074  - Provider Speciality Crosswalk control fle logic - Start's Here*/

Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/Provspec_Xwalk.xlsx"
out=controlfile_import dbms=xlsx replace;
run;

%nobs(controlfile_import);
%let cnt1 = &nobs.;
%macro t1;
%if &cnt1. lt 1 %then %goto exit; /* Skip the control file logic if it's empty */

data controlfile_tmp(keep=x_Description x_bilcod x_poscod x_svccod x_modcod x_valcd x_vala
       x_cos x_mcorcat x_provspc x_new_provspc x_revcod);
length x_Description x_bilcod x_poscod x_svccod	x_modcod x_valcd x_vala
       x_cos x_mcorcat x_provspc x_new_provspc $200. x_revcod $500.;
       
set    controlfile_import;
x_Description = Description;
x_bilcod      = bilcod;
x_poscod      = poscod;
x_svccod      = svccod;
x_modcod      = modcod;
x_revcod      = revcod;
x_valcd       = valcd_01to12;
x_vala        = vala_01to12;
x_cos         = cos;
x_mcorcat     = mcorcat;
x_provspc     = provspc;
x_new_provspc =  new_provspc;
run;

/* Sort the Control file data*/
data controlfile_tmp;
set controlfile_tmp;
key1 = 0;
key2  = 0;

if x_bilcod ne 'NA' then do;
key1+1;
key2= length(x_bilcod);
end;

 if x_poscod ne 'NA' then do;
key1+1;
key2= key2+length(x_poscod);
end;

if x_svccod ne 'NA' then do;
key1+1;
key2= key2+length(x_svccod);
end;

 if x_modcod ne 'NA' then do;
key1+1;
key2= key2+length(x_modcod);
end;

if x_revcod ne 'NA' then do;
key1+1;
key2= key2+length(x_revcod);
end;

 if x_valcd ne 'NA' then do;
key1+1;
key2= key2+length(x_valcd);
end;

 if x_vala ne 'NA' then do;
key1+1;
key2= key2+length(x_vala);
end;

 if x_cos ne 'NA' then do;
key1+1;
key2= key2+length(x_cos);
end;

 if x_mcorcat ne 'NA' then do;
key1+1;
key2= key2+length(x_mcorcat);
end;

if x_provspc ne 'NA' then do;
key1+1;
key2= key2+length(x_provspc);
end;

run;

proc sort data = controlfile_tmp; by key1 key2; run;

/* Sort the Control file data so that the higher degres new prvspcd assigned first */
data controlfile;
set controlfile_tmp;
if x_bilcod ne 'NA' then x_bilcod = "'"||tranwrd(trim(compress(x_bilcod)), ",", "','")||"'";
if x_poscod ne 'NA' then x_poscod = "'"||tranwrd(trim(compress(x_poscod)), ",", "','")||"'";
if x_svccod ne 'NA' then x_svccod = "'"||tranwrd(trim(compress(x_svccod)), ",", "','")||"'";
if x_modcod ne 'NA' then x_modcod = tranwrd("'"||tranwrd(trim(compress(x_modcod)), ",", "','")||"'","Null","");
if x_revcod ne 'NA' then x_revcod = "'"||tranwrd(trim(compress(x_revcod)), ",", "','")||"'";
if x_valcd  ne 'NA' then x_valcd  = "'"||tranwrd(trim(compress(x_valcd)), ",", "','")||"'";
if x_cos    ne 'NA' then x_cos    = "'"||tranwrd(trim(compress(x_cos)), ",", "','")||"'";
if x_vala ne 'NA' then x_vala = (compress(x_vala));
if x_valcd  ne 'NA' then x_valcd  = (compress(x_valcd));
if x_provspc ne 'NA' then x_provspc = "'"||tranwrd(trim(compress(x_provspc)), ",", "','")||"'";
if find(x_mcorcat,'CONTAINS','i')>0 then 
x_mcorcat=compress(tranwrd("'/"||tranwrd(trim(compress(x_mcorcat)), ",", "|")||"/'","Contains",""));
else
x_mcorcat="'"||tranwrd(trim(compress(x_mcorcat)), ",", "','")||"'";
run;

/* Assigning macro variables*/
proc sql noprint;
select count(*)  into :NObs from controlfile;
select x_Description  into :des1-:des%left(&NObs) from controlfile;
select x_Bilcod  into :Bilcod1-:Bilcod%left(&NObs) from controlfile;
select x_POSCOD  into :POSCOD1-:POSCOD%left(&NObs) from controlfile;
select x_svccod  into :svccod1-:svccod%left(&NObs) from controlfile;
select x_modcod  into :modcod1-:modcod%left(&NObs) from controlfile;
select x_Revcod  into :Revcod1-:Revcod%left(&NObs) from controlfile;
select x_Valcd   into :Valcd1-:Valcd%left(&NObs) from controlfile;
select x_vala    into :vala1-:vala%left(&NObs) from controlfile;
select x_COS     into :COS1-:COS%left(&NObs) from controlfile;
select x_mcorcat   into :mcorcat1-:mcorcat%left(&NObs) from controlfile;
select x_Provspc  into :Provspc1-:Provspc%left(&NObs) from controlfile;
select x_New_Provspc  into :New_Provspc1-:New_Provspc%left(&NObs) from controlfile;
quit;

/* Checking each observation for the control file match to assign the new prvspcd*/
%macro loop1;

%do i=1 %to &NObs;

     %if &&new_provspc&i. ne NA  %then %do;
           if 1=1  
     %end;
     
     %if &&Bilcod&i. ne NA  %then %do;
              and Bilcod in ( &&Bilcod&i..) 
     %end;
     
     %if &&poscod&i. ne NA %then %do;
              and  poscod in ( &&POSCOD&i..)  
     %end;
     
      %if &&svccod&i. ne NA %then %do;
              and svccod in ( &&svccod&i..)  
     %end;
     
     %if &&modcod&i. ne NA %then %do;
             and modcod in ( &&modcod&i..)  
     %end;
     
     %if &&revcod&i. ne NA %then %do;
             and  revcod in ( &&revcod&i..)  
     %end;
     
     %if &&COS&i. ne NA %then %do;
             and COS in ( &&COS&i..)  
     %end;
     
     %if &&MCORCAT&i. ne NA and (%sysfunc(find(&&MCORCAT&i..,/)) > 0 ) %then %do;
             and  prxmatch(&&MCORCAT&i..,upcase(MCORCAT)) > 0
     %end;      

     %if &&MCORCAT&i. ne NA and  (%sysfunc(find(&&MCORCAT&i..,/)) = 0 )    %then %do;
              and  MCORCAT in ( &&MCORCAT&i..) 
     %end;     
          
     %if &&Provspc&i. ne NA  %then %do;
          and  prvspcd in ( &&Provspc&i..)  
     %end;   
     

      %if &&vala&i. ne NA %then %do;
      and
     ( vala01 in ( &&vala&i..) or  vala02 in ( &&vala&i..) or vala03 in ( &&vala&i..) or vala04 in ( &&vala&i..) or
       vala05 in ( &&vala&i..) or  vala06 in ( &&vala&i..) or vala07 in ( &&vala&i..) or vala08 in ( &&vala&i..) or
       vala10 in ( &&vala&i..) or  vala11 in ( &&vala&i..) or vala12 in ( &&vala&i..)
     )  %end;
     
     %if &&valcd&i. ne NA %then %do;
       and      
     (    valcd01 in ( &&Valcd&i..) or valcd02 in ( &&Valcd&i..) or  valcd03 in ( &&Valcd&i..) or  valcd04 in ( &&Valcd&i..)
       or valcd05 in ( &&Valcd&i..) or valcd06 in ( &&Valcd&i..) or  valcd07 in ( &&Valcd&i..) or  valcd08 in ( &&Valcd&i..)
	   or valcd09 in ( &&Valcd&i..) or valcd10 in ( &&Valcd&i..) or  valcd11 in ( &&Valcd&i..) or  valcd12 in ( &&Valcd&i..)
     )   %end; 
    
   then do; 
 New_Provspc_tmp = &&New_Provspc&i.; 
  end;

%end;
%mend;


data &RGA191..encountersY5_Prvspc_&Compno._&run_id.(keep=clamno prvspcd New_Provspc_tmp);
set &RGA191..encountersY5_&Compno._&run_id.;
length New_Provspc_tmp $5.;
%loop1;
run;

data  &RGA191..encountersY5_Prvspc_&Compno._&run_id.;
set &RGA191..encountersY5_Prvspc_&Compno._&run_id.;
where missing(New_Provspc_tmp)=0;
run;

proc sort data = &RGA191..encountersY5_Prvspc_&Compno._&run_id.; by clamno ;
proc sort data = &RGA191..encountersY5_&Compno._&run_id.; by clamno ;

data &RGA191..encountersY5_&Compno._&run_id.(drop = New_Provspc_tmp)  ; 
merge &RGA191..encountersY5_&Compno._&run_id.(in=a)
      &RGA191..encountersY5_Prvspc_&Compno._&run_id.(in=b);
by clamno ;
if a ;
if a and b then do;
		prvspcd = compress(New_Provspc_tmp);
end;
run;
%exit: %mend t1;
/*%t1;*//*B-289556*/

/*B-247074  - Provider Speciality Crosswalk control fle logic - End's Here*/

/* B-261072 - introduced new logic to create the 99963 edit instead of Oth_check logic */
data &RGA191..encountersY5_&Compno._&run_id.
		   &RGA191..NDCSVC_Procbill_Code_rej(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode); 
		set  &RGA191..encountersY5_&Compno._&run_id.  ; 
	     if( (kupcase(IN_2400_SV202_2_SL_HCPCS)) in (&NDC_PROC_CD.) and (ksubstr(Bilcod,1,2)) in (&NDC_BILL_CD.) and (klength(NDCSVC)) ne 11 and    
	       typecode in (&enctyp_code_NDCSVC_Procbill_rej) and "&edit_type_NDCSVC_Procbill_rej." eq "H" 
	        )then do;	    	
	    	    edit_code = '99963' ;
						encapdfld =  'IN_2400_SV202_2_SL_HCPCS' ;
						edit_chk_type = 'NDCSVC Code value reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..NDCSVC_Procbill_Code_rej ;	
						delete;					
	 	end;
	 	else output &RGA191..encountersY5_&Compno._&run_id.;
	run;
	
	/* B-261072 - introduced new logic to create the 99963 edit instead of Oth_check logic */
data &RGA191..encountersY5_&Compno._&run_id.
		   &RGA191..NDCSVC_Proc_Code_rej(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode) ; 
		set  &RGA191..encountersY5_&Compno._&run_id.  ; 
	     if( (kupcase(svccod)) in (&NDC_PROC_CD.) and (klength(NDCSVC)) ne 11 and  typecode ='P' and "&edit_type_NDCSVC_Proc_rej." eq "H" 
	        )then do;	    	
	    	    edit_code = '99963' ;
						encapdfld =  'SVCCOD' ;
						edit_chk_type = 'NDCSVC Code value reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..NDCSVC_Proc_Code_rej ;	
						delete;				
	 	end;
	 	else output &RGA191..encountersY5_&Compno._&run_id.;
	run;
	
	/* 263565 NPI City length - Internal Rejection Logic*/
	
	data &RGA191..encountersY5_&Compno._&run_id.
		   &RGA191..NPI_City_Rej(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode) ; 
		set  &RGA191..encountersY5_&Compno._&run_id.  ; 
	     if( (missing(BI_PRV_CITY) =0 and (klength(BI_PRV_CITY)<2 or klength(BI_PRV_CITY)>30)) or
	         (missing(RN_PHY_CITY) =0 and (klength(RN_PHY_CITY)<2 or klength(RN_PHY_CITY)>30)) or
	         (missing(AT_PHY_CITY) =0 and (klength(AT_PHY_CITY)<2 or klength(AT_PHY_CITY)>30))
	      )then do;	    	
	    	    edit_code = '99940' ;
						encapdfld =  'PROVPRACCITY' ;
						edit_chk_type = 'NPI City Length Reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..NPI_City_Rej ;	
						delete;				
	 	end;
	 	else output &RGA191..encountersY5_&Compno._&run_id.;
	run;
	
	/*B-263560 - Incorrect Admission & Discharge time Internal Reject logic */
	
	data &RGA191..encountersY5_&Compno._&run_id.
		   &RGA191..admtime_Rej(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode) ; 
		set  &RGA191..encountersY5_&Compno._&run_id.  ; 
	     if(admtim > '23:59' or admtim < '00:00' )then do;	    	
	    	    edit_code = '99939' ;
						encapdfld =  'admtim' ;
						edit_chk_type = 'Incorrect Admission Time - Reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..admtime_Rej ;	
						delete;				
	 	end;
	 	else output &RGA191..encountersY5_&Compno._&run_id.;
	run;
	
		/*B-263560 - Incorrect  Discharge time Internal Reject logic */
		
	data &RGA191..encountersY5_&Compno._&run_id.
		   &RGA191..distim_Rej(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode) ; 
		set  &RGA191..encountersY5_&Compno._&run_id.  ; 
	     if(distim > '23:59' or distim < '00:00' )then do;	    	
	    	    edit_code = '99938' ;
						encapdfld =  'distim' ;
						edit_chk_type = 'Incorrect Discharge Time - Reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..distim_Rej ;	
						delete;				
	 	end;
	 	else output &RGA191..encountersY5_&Compno._&run_id.;
	run;
	
	/* [B-335998] - provider date based internal rejection*/	

	data &RGA191..encountersY5_&Compno._&run_id.
	      &RGA191..provider_date_reject(keep = clamno lineno edit_chk_type edit_code encapdfld  Recsubind typecode);
	length edit_chk_type $100.;
	set  &RGA191..encountersY5_&Compno._&run_id.  ;	 

if "&edit_type_RFPRVNPI." eq "H" and typecode in (&enctyp_code_RFPRVNPI.) and ((missing(RF_PHY_npiid)=0 and missing(RF_PHY_LST)=1)  
	or (missing(RF_PHY_LST)=0 and (FIRSTDOS lt RF_PROVNUMDT or (FIRSTDOS gt RF_NPIDEACTDT and missing(RF_NPIDEACTDT)=0
	and FIRSTDOS lt RF_NPIREACTDT and missing(RF_NPIREACTDT)=0))))
	then do; 
		edit_code = '99921' ;
		encapdfld =  'RF_PROVNUMDT' ;
		edit_chk_type = 'Referring Provider NPI Date gt firstdos';
		Recsubind = 'N' ;
		output &RGA191..provider_date_reject ;
		delete;
	end;

if "&edit_type_RNPRVNPI." eq "H" and typecode in (&enctyp_code_RNPRVNPI.) and ((missing(RN_PHY_NPiid)=0 and missing(RN_PHY_LST)=1)  
	or (missing(RN_PHY_LST)=0 and (FIRSTDOS lt RN_PROVNUMDT or (FIRSTDOS gt RN_NPIDEACTDT and missing(RN_NPIDEACTDT)=0
	and FIRSTDOS lt RN_NPIREACTDT and missing(RN_NPIREACTDT)=0))))
	then do; 
		edit_code = '99922' ;
		encapdfld =  'RN_PROVNUMDT' ;
		edit_chk_type = 'Rendering Provider NPI Date gt firstdos';
		Recsubind = 'N' ;
		output &RGA191..provider_date_reject ;
		delete;
	end;

if "&edit_type_ATPRVNPI." eq "H" and typecode in ( &enctyp_code_ATPRVNPI.) and ((missing(ATPRVNPI)=0 and missing(AT_PHY_LST)=1) /*B-166926 Edit_type */
	or (missing(AT_PHY_LST)=0 and (FIRSTDOS lt AT_PROVNUMDT or (FIRSTDOS gt AT_NPIDEACTDT and missing(AT_NPIDEACTDT)=0
	and FIRSTDOS lt AT_NPIREACTDT and missing(AT_NPIREACTDT)=0))))
	then do; 
		edit_code = '99923' ;
		encapdfld =  'AT_PROVNUMDT' ;
		edit_chk_type = 'Attending Provider NPI Date gt firstdos';
		Recsubind = 'N' ;
		output &RGA191..provider_date_reject ;
		delete;
	end;

if "&edit_type_OPPRVNPI." eq "H" and typecode in (&enctyp_code_OPPRVNPI.) and ((missing(OPPRVNPI)=0 and missing(OP_PHY_LST)=1)  
	or (missing(OP_PHY_LST)=0 and (FIRSTDOS lt OP_PROVNUMDT or (FIRSTDOS gt OP_NPIDEACTDT and missing(OP_NPIDEACTDT)=0
	and FIRSTDOS lt OP_NPIREACTDT and missing(OP_NPIREACTDT)=0))))
	then do; 
		edit_code = '99924' ;
		encapdfld =  'OP_PROVNUMDT' ;
		edit_chk_type = 'Operating Provider NPI Date gt firstdos';
		Recsubind = 'N' ;
		output &RGA191..provider_date_reject ;
		delete;
	end;

if "&edit_type_BIPRVNPI." eq "H" and typecode in (&enctyp_code_BIPRVNPI.) and ((missing(BIPRVNPI)=0 and missing(BI_PRV_LST)=1)  
	or (missing(BI_PRV_LST)=0 and (FIRSTDOS lt BI_PROVNUMDT or (FIRSTDOS gt BI_NPIDEACTDT and missing(BI_NPIDEACTDT)=0
	and FIRSTDOS lt BI_NPIREACTDT and missing(BI_NPIREACTDT)=0))))
	then do; 
		edit_code = '99925' ;
		encapdfld =  'BI_PROVNUMDT' ;
		edit_chk_type = 'Billing Provider NPI Date gt firstdos';
		Recsubind = 'N' ;
		output &RGA191..provider_date_reject ;
		delete;
	end;

else output &RGA191..encountersY5_&Compno._&run_id.;
run;

	/* provider date reject - ends*/
	
  %Internal_Reject_Dynamic(mcorcat,&RGA191..encountersY5_&Compno._&run_id.); /* B-278970 - DS - Added */

	/* CR# S-22225 - added logic to read values from COSProvider_Specialty_codes_mapping control file 
  if valcode and valamount value matches then populate cos and prvspcd values from control file -- Ends here */

/* B-160659 Diag code mismatch between claims table and control file Diag_xwalk.xlsx - Start */
%macro trans_diag_poa(dsn= );
	
	 proc sort data= &dsn.;by clamno;run;
	
	proc transpose data=&dsn.
	out=&dsn._trns(keep=clamno _NAME_  col1 );
	var &Keepv_ClmDiag.;
	by Clamno;
	run;

	data &dsn._trns1;
	set &dsn._trns;
	label _NAME_="Diagcd_Varible_Name";
	rename _NAME_=Diagcd_Varible_Name   col1=Diagcd_Value  ;
	if missing(col1)=1 then delete;
	run;
%mend;

%macro trans_diag_poa_call;
%nobs(calims_Diagcd);
    %if &nobs gt 0 %then 
    	%do;
				%trans_diag_poa(dsn=calims_Diagcd); 
				%trans_diag_poa(dsn=diag_xwalk1);
			
				proc sort data=calims_Diagcd_trns1;by clamno Diagcd_Value;run;
				proc sort data=diag_xwalk1_trns1;by clamno Diagcd_Value;run;

				data Diag_Poa_mismatch;
				length  comments $300. Diagcd_Varible_Name Diagcd_Value $12. Diagcd_Flag $2. comments $300. ;
  			merge calims_Diagcd_trns1 (in=a) diag_xwalk1_trns1 (in=b);
  			by clamno Diagcd_Value;
  			if  a and not b then do; comments='Diagcode Exist in the Claims table and not exist in the Control file'; Diagcd_Flag='D';end;
  			if  b and not a then do; comments='Diagcode Exist in the Control file and not exist in the Claims table'; Diagcd_Flag='A';end;
  			if missing(comments)=1 then delete;
				run;
		%end;
%mend;
%trans_diag_poa_call;
/* B-160659 Diag code mismatch between claims table and control file Diag_xwalk.xlsx - End */


Proc sql noprint ;
	Select count(clamno) into :Bil_cod_Rej_count  from &RGA191..Reject_Bilcod_&compno._&run_id. ;		
quit; 
Proc sql noprint ;
	Select count(distinct(clamno)) into :Bil_cod_Rej_count1  from &RGA191..Reject_Bilcod_&compno._&run_id. ;		
quit; 
Proc sql noprint ;
	Select count(distinct(clamno)) into :temp1 from &RGA191..svclines2_&Compno.;		
quit;
data _Null_ ;
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('hold_rej_new_Process_clm',kcompress(%eval(&QI_and_clm_count. - &temp1. )));
run;
data _Null_ ;
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('ClmN_SvcY_Count1',kcompress(%eval(&temp1.  - ( &ClmY_SvcY_Count1. + &Bil_cod_Rej_count1.) )));
run;

Proc sql noprint ;
	Select count(clamno) into :Reject_DG_I from &RGA191..Reject_DG_I_&Compno._&run_id. ;		
quit; 

Data _Null_;
/* SAS2AWS2: ReplacedFunctionCompress */
call symput('Reject_DG_I_2',kcompress(%eval(&rec_encY2_1. - &Reject_DG_I_1. )));
Run;

data _null_;
/* SAS2AWS2: ReplacedFunctionCompress */
call symput('Rej_Enc_N_IO_P_1',kcompress(%eval(&ClmY_SvcY_Count1. - &ttt. )));
run;


/* HS 542635 - Added Proc deletestatement */

Proc delete data=svclines_1_&compno.;
run;
Proc delete data=svclines2_&compno.;
run;
/*Proc delete data=en_prov2;
run;*//*B-289556*/
Proc delete data=claims;run;
/*Proc delete data=cos_table_out;run;
Proc delete data=cos_table_pro;run;*//*B-289556*/

/* B-92443 added diag deletes */
Proc delete data=claim_nodiag;run;
Proc delete data=claim_diag;run;
Proc delete data=clm_diag1;run;
Proc delete data=clm_diag2;run;

*rsubmit;
/*
 proc datasets lib= work nolist  ;
			      delete 	
				  svclines_&compno.
					svclines3_&compno.
					providernpi_&compno. 
					encounters_T_&compno._&run_id.
					encountersY2B_&Compno._&run_id.
					encountersY2_&compno._&run_id.
					encountersY_&compno._&run_id.
					encountersY_T_&compno._&run_id.
					encountersY4_&Compno._&run_id.
					encountersY4B0_&Compno._&run_id.
					encountersY4e1_&Compno._&run_id.
					encountersY4e2_&Compno._&run_id.
					encountersY4e3_&Compno._&run_id.
					encountersY4e3_&Compno._&run_id._Temp2
					encountersY4e3_&Compno._&run_id.
					encountersY4e3_&Compno._&run_id.	
									;	
			  title3 ;
			  title4 ;
			  title5 ;
			
quit;
*/
%macro RGA192_tmp_ds_del_1  ;
	%if &temp_ds_del_flag. = Y %then 
	%do; 
			 proc datasets lib= &RGA191. nolist  ;
			      delete 	admxwalk1_&Compno.
						claimcrosswalk_&Compno. mcorcatclmtyp_&Compno. 	mcorcatenccat_&Compno. reject_dup_claim_line_&Compno.
						reject_svclines_&Compno. svclines_&Compno. svcholds1_&Compno._&run_id. 
						svcholds_&Compno._&run_id.  svclines2_&Compno. svclines3_&Compno. svclines_date_&Compno.
						provider2_&Compno. provider3_&Compno. provlc2_&Compno. provlc3_&Compno. provlc_&Compno.
						provmdcaid2_&Compno. provmdcaid3_&Compno. provmdcaid_&Compno. provnpi2_&Compno.
					    provnpi_&Compno. 
						provupi2_&Compno. provupi3_&Compno. provupi_&Compno. members_&Compno.
						clmn_svcy_&Compno._&run_id.   encountersy2_&Compno._&run_id. 
						encountersy3_&Compno._&run_id.  encountersy4_&Compno._&run_id. 							
						 providernpi_&Compno. svclines2_1_&compno.  svclines2_2_&compno. encountersy4a_&Compno._&run_id.
					    encountersY4B_&Compno._&run_id.	&RGA191..encountersY5_Prvspc_&Compno._&run_id.	
						
						

	
					;	
			  title3 ;
			  title4 ;
			  title5 ;
			
			  quit;
		%if &QIfiflag=Y %then %do;
			  proc datasets lib= &RGA191. nolist  ;
			      delete 	/*   finclaims_sort_&Compno.*/  svclines_1_&Compno.
							
					;	
			  title3 ;
			  title4 ;
			  title5 ;
			
			  quit;
		%end;


	%end;
%mend RGA192_tmp_ds_del_1  ;
%RGA192_tmp_ds_del_1 ;

proc sql;
update &R42173..medical_job_mstr
set holdcd_flag = '1'
where run_id = "&run_id."
;
quit;

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19102 Program End execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

 /***********************************************************************************************/  
 /*                                                                                             */
 /*                   End of Program - RGA19201.sas                                             */
 /*                                                                                             */
 /***********************************************************************************************/    
    


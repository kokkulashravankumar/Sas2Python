
/***       
$Header: $ 
***/ 

/********************************************************************************/
/*           Creating table where Val Flag = y 									*/
/********************************************************************************/

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19103 Program Start execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

*Rsubmit;
Data _null_ ;
   format today svcyear mmddyy10. ;
   today = today() ;
   svcyear = intnx('year', today, -2, 'same'); 
   call symput ('today',today);
   call symput ('svcyear',svcyear);
run ;
Data look;
	set &libn2..diagcode(keep=diagcd medsflag);
/*	where medsflag='Y';*/ /* HS 569650 */
run;
data look_blank;
diagcd='     ';
run;
data look;
set look look_blank;
run;
data look_up(keep=fmtname start label);
		set look end=eof;
		fmt_field='diag';
		length label $7.
			   start $15.;
		/* SAS2AWS2: ReplacedFunctionCompress */
		fmtname = kcompress("$"|| fmt_field ) ;
		start = diagcd ;
		label = 'valid';
		output;
		if eof then
		do;
			/* SAS2AWS2: ReplacedFunctionCompress */
			fmtname = kcompress("$"|| fmt_field ) ;
		  	start = 'Other';
		  	label = 'invalid';
		  	output;
		end;
	run;

Proc format cntlin=look_up;
Run;

Data APD_enceditmatrix;
	set APD_enceditmatrix;
	where edittyp='H';
Run;

Proc sort data=APD_enceditmatrix(keep=Dtrgeflag Numflag encapdfld editcode dwhfld)
	out=APD_enceditmatrix1(rename=(encapdfld=var_name editcode=var_value)) nodupkey;
	by encapdfld;
where Dtrgeflag='Y' or  Numflag='Y'  ;
run;

proc sql  noprint;		
   	create table &RGA192..APD_EditValY as
   	select distinct encapdfld, 
					dwhtable,
					costype,
					encTypCode,
					editcode
	from APD_enceditmatrix 
	where valFlag = 'Y' and dwhtable IS NOT MISSING and editTyp = 'H' ;
    
  	select count(*)
   	into :total
	from &RGA192..APD_EditValY ;

	/* CHG0031701 */

/*	select count(distinct(compress(encapdfld || costype || encTypCode)))*/
	/* SAS2AWS2: ReplacedFunctionCompress */
	select count(distinct(kcompress(encapdfld || costype || encTypCode||editcode)))
   	into :total_miss
    from APD_enceditmatrix  
	where missFlag = 'Y' and editTyp = 'H'  ;

	select count(*)
   	into :total_numchk
	from APD_enceditmatrix1
	where Numflag = 'Y'  ;

	select count(*)
   	into :total_datrge
	from APD_enceditmatrix1
	where Dtrgeflag = 'Y' ;

/*	select count(distinct(compress(encapdfld || costype || encTypCode)))*/
	/* SAS2AWS2: ReplacedFunctionCompress */
	select count(distinct(kcompress(encapdfld || costype || encTypCode||editcode)))
   	into :total_Othchk
	from APD_enceditmatrix
	where Othcheckflag = 'Y' and editTyp = 'H' ;
quit;
%let  total			= 	&total			;
%let  total_miss 	= 	&total_miss		;
%let  total_numchk 	= 	&total_numchk	;
%let  total_datrge 	= 	&total_datrge	;
%let  total_Othchk 	= 	&total_Othchk	;
%PUT &TOTAL.;
proc sql noprint ;
    /* SAS2AWS2: ReplacedFunctionCompress */
    select distinct(kcompress(dwhtable ||costype || encTypCode)),
					/* SAS2AWS2: ReplacedFunctionCompress */
					kcompress(costype || encTypCode),
		  			encapdfld,
					dwhtable,
					editcode,
					encTypCode,
					costype
    into :this_dwhtable1-:this_dwhtable&total.,
		 :costyp_enctyp1-:costyp_enctyp&total.,
		 :this_encapdfld1-:this_encapdfld&total.,
		 :ref_table1-:ref_table&total.,
		 :edit_cd1-:edit_cd&total.,
		 :enctyp_cd1-:enctyp_cd&total.,
		 :costyp1-:costyp&total.
    from &RGA192..APD_EditValY 
	order by encTypCode;

	/* SAS2AWS2: ReplacedFunctionCompress */
	select distinct(kcompress(encapdfld || costype || encTypCode)),
					/* SAS2AWS2: ReplacedFunctionCompress */
					kcompress(put(costype,$2.) || encTypCode),
					editcode,
					encapdfld,
					encTypCode,
					costype
    into 	:encapdfld_enct_miss1-:encapdfld_enct_miss&total_miss.,
		 	:costyp_enctyp_miss1-:costyp_enctyp_miss&total_miss.,
		 	:edit_cd_miss1-:edit_cd_miss&total_miss.,
		 	:encapdfld_miss1-:encapdfld_miss&total_miss.,
		 	:enctyp_cd_miss1-:enctyp_cd_miss&total_miss.,
		 	:costyp_miss1-:costyp_miss&total_miss.
    from APD_enceditmatrix  
	where 	missFlag = 'Y' and editTyp = 'H' 

	order by encTypCode;

	/*** Creating tablenames to create format - One table for every dwtable - costype - encouter type combination ***/
	/* SAS2AWS2: ReplacedFunctionCompress */
	select distinct(kcompress(dwhtable || costype ||encTypCode ))
    into 	:all_table separated by ' '
    from 	&RGA192..APD_EditValY  ;

    select 	var_name,
		   	var_value,
			dwhfld
    into 	:var_numchk1-:var_numchk&total_numchk.,            
			:edit_numchk1-:edit_numchk&total_numchk.,
			:var_con1-:var_con&total_numchk.
	from 	APD_enceditmatrix1
	where Numflag = 'Y'  ;

    select 	var_name,var_value
    into 	:var_datrge1-:var_datrge&total_datrge.,:edit_datrge1-:edit_datrge&total_datrge.
	from APD_enceditmatrix1
	where Dtrgeflag = 'Y' ;

	/* SAS2AWS2: ReplacedFunctionCompress */
	select distinct(kcompress(encapdfld || costype || encTypCode)),
					/* SAS2AWS2: ReplacedFunctionCompress */
					kcompress(put(costype,$2.) || encTypCode),
					editcode,
					encapdfld,
					encTypCode,
					costype
    into 	:encapdfld_enct_Othchk1-:encapdfld_enct_Othchk&total_Othchk.,
		 	:costyp_enctyp_Othchk1-:costyp_enctyp_Othchk&total_Othchk.,
		 	:edit_Othchk1-:edit_Othchk&total_Othchk.,
		 	:var_Othchk1-:var_Othchk&total_Othchk.,
		 	:enctyp_cd_Othchk1-:enctyp_cd_Othchk&total_Othchk.,
		 	:costyp_Othchk1-:costyp_Othchk&total_Othchk.
    from APD_enceditmatrix  
	where 	OthcheckFlag = 'Y' and editTyp = 'H' ;

quit ;
/********************************************************************************/
/* 				START OF MACRO num_check										*/
/*				1.To do other Check for Edit Matrix							*/
/********************************************************************************/
*Rsubmit;
%macro Oth_check(var_in,edit_in) ;
%let var_in=&var_in.; 

%if &var_in = MEM_MEMBNO  %then %do;
/******************************************************
   Hardcode missing value and validation check of CIN
*******************************************************/

	%if &compno=20 %then %do ; 	 
	           if klength(ktrim(kleft(&var_in)))= 8 and kverify(ktrim(kleft(&var_in)), '0123456789') = 0 then
				do;
					edit_code = '000';
					encapdfld =  "&var_in";
					edit_chk_type = 'Member number Check';
					output &RGA191..EncounterEditPass_&Compno._&run_id.  &RGA191..encounter837_&Compno._&fi_ds_dt.;
					end;
	           else do;
			
				   	edit_code = "&edit_in" ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'Member number Check';
					editcd_flag = 'OFF';
					Recsubind = 'N' ;
					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
			end; 
	  
	%end;/*ALM#7174 - added company 42 condition, B-145499 add MCR */
	%else %if &compno=30 or &compno=45 or (&compno=42 and "&LOB." eq "EP") %then %do ; 	/* CR# CHG0032747 - Added else if cndition company 45 */
	           /* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionTrim */
	           if ktrim(kleft(&var_in)) ne ' ' then
				do;
					edit_code = '000' ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'Member number Check';
					output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 			end;
	           else do;
			
				   	edit_code = "&edit_in" ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'Member number Check';
					editcd_flag = 'OFF';
					Recsubind = 'N' ;
					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	      		end; 
  
	%end;
	%else %if &compno=42 and "&LOB." eq "QHP" %then %do ; 	/* B-96285 added QHP */
				/* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionTrim */
				if ktrim(kleft(&var_in)) eq ' ' then
				do;
			
				   	edit_code = "&edit_in" ;
					encapdfld =  "&var_in" ;				/* B-97137 replaced enccmsfld */
					edit_chk_type = 'Member number Check';
					editcd_flag = 'OFF';
					Recsubind = 'N' ;
					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	      		end;          	        
				else do;
					edit_code = '000' ;
					encapdfld =  "&var_in" ;				/* B-97137 replaced enccmsfld */
					edit_chk_type = 'Member number Check';
					output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 			end;

	%end;
	
	%else %do;			
				  /* SAS2AWS2: ReplacedSlash */
/* 	  			rx=prxparse("/[A-Z]{2}/d{5}[A-Z]/"); */
/* 	               retain rx; */
						
	           /* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionTrim */
/* 	           if ktrim(kleft(&var_in)) eq ' ' then */
/* 				do; */
/* 			 */
/* 				   	edit_code = "&edit_in" ; */
/* 					encapdfld =  "&var_in" ; */
/* 					edit_chk_type = 'Member number Check'; */
/* 					editcd_flag = 'OFF'; */
/* 					Recsubind = 'N' ; */
/* 					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ; */
/* 	      		end;           */
	           /* SAS2AWS2: ReplacedFunctionTrim */
/* 	           else if prxmatch(rx,ktrim(&var_in))=0 then */
/* 			 	do; */
/* 			 */
/* 				   	edit_code = "&edit_in" ; */
/* 					encapdfld =  "&var_in" ; */
/* 					edit_chk_type = 'Member number Check'; */
/* 					editcd_flag = 'OFF'; */
/* 					Recsubind = 'N' ; */
/* 					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ; */
/* 	      		end; 				 */
/* 				else do; */
/* 					edit_code = '000' ; */
/* 					encapdfld =  "&var_in" ; */
/* 					edit_chk_type = 'Member number Check'; */
/* 					output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.; */
/* 	 			end;	   */
	%end;
%end; 

/* B-114226 start */ 
%if &var_in = membno and &compno=01 and &LOB. NE DSNP %then %do;
		       /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		       if klength(kcompress(&var_in)) > 8 then
				do ; 	
	      			edit_code = "&edit_in" ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'Member number Length > 8';
					editcd_flag = 'OFF';
					Recsubind = 'N' ;
					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
				end;	

			 	else do;
					edit_code = '000' ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'Member number Length > 8';		
					output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
				 end;
%end;	/* B-114226 end */
%if &var_in = dsnp_membno and &compno=01 and &LOB. = DSNP %then %do;
		       if klength(kcompress(&var_in)) > 8 then
				do ; 	
	      			edit_code = "&edit_in" ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'Member number Length > 8';
					editcd_flag = 'OFF';
					Recsubind = 'N' ;
					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
				end;	

			 	else do;
					edit_code = '000' ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'Member number Length > 8';		
					output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
				 end;
%end;	

%if &var_in = BIPRVNPI  %then %do; /* CHG0032056 - added mrg_provno */ 
/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
if (kcompress(&var_in.) ne ' ' and  klength(kcompress(&var_in.))= 10
	 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
	 and kcompress(&var_in.)  NE '0000000000' and kverify(kcompress(&var_in.), '0123456789') = 0 ) or mrg_provno ne ''
	 then do;
					edit_code = '000' ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'NPI number Check';		
					output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
 	end;

 	else do;
					edit_code = "&edit_in" ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'NPI number Check';
					editcd_flag = 'OFF';
					Recsubind = 'N' ;
					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
				
	 end;

%end;


%if &var_in = MEM_MZIPCOD or &var_in = BI_PRV_ZIP or &var_in = /*pzipcod_1*/pzipcod   %then %do;/*CR# CHG0039334*/

	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if klength(kcompress(&var_in))=9 then
	do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'ZIP CODE Check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'ZIP CODE Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
	
%end;

%if &var_in = svcdat %then %do;
/***********************************************************
   Hardcode missing value and validation check of SVCDAT
************************************************************/

/* SAS2AWS2: ReplacedFunctionCompress */
if kcompress(&var_in) < &today.  and kcompress(&var_in) >=  &svcyear. then
	do;

						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service date check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service date check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
%end;

%if &var_in. = enddat %then %do;
/***********************************************************
   Hardcode missing value and validation check of ENDDAT
************************************************************/
if &var_in < &today.   then 
	do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service end date check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service end date check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
%end;
%if &var_in = unitct %then %do;

       if  &var_in > 0 then 
		do ;
          				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service Unit count check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;

		else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service Unit count check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
		end;			


%end;
/* CR# CHG0031183 KV - Commented procedure code indicator and diagnosis code indicator edit check logic */
/*
%if &var_in. = diagvind %then %do;

       if  &var_in. = 'U'  then 
		do ;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Diagnosis code indicator check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
          			
		end;

		else do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Diagnosis code indicator check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;			


%end;

%if &var_in. = procvind %then %do;

       if  &var_in. = 'U' and typecode in ("I") then 
		do ;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Procedure code indicator check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
          			
		end;

		else do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Procedure code indicator check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;	
%end;
*/
%if  &var_in. = prvspcd %then %do;
       /* SAS2AWS2: ReplacedFunctionLength */
       if  typecode in ("P","O")  and   (missing(&var_in) = 1 or klength(&var_in) ne 3) then
		do ;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Specialty code check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
          				
		end;
			
		else do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Specialty code check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;	
%end;

/* HS 567501 - commented */
/*
%if  &var_in. = prvspcd %then %do;
       if  typecode in ("P","O")  and   (missing(&var_in) = 1 or klength(&var_in) ne 3) and COS = '11'  then
		do ;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Specialty code check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
          				
		end;
			
		else do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Specialty code check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;	
%end;
*/

%if &var_in. = dchrgdt %then %do;
if typecode in ('I') then do;
/* B-121242 add HFIC, B-145499 add MCR, B-154145 check non COS first */
		/* SAS2AWS2: ReplacedFunctionCompress */
		if ("&compno." = "30" or "&compno." = "45" or "&LOB." eq "QHP") and (kcompress(&var_in) ne ' ' or kcompress(&var_in) >= admitdt) then
		do ;
          				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Discharge date check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;

        /* SAS2AWS2: ReplacedFunctionCompress */
        else if cos in ('11', '12', '28') and (kcompress(&var_in) ne ' ' or kcompress(&var_in) >= admitdt) then
		do ;
          				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Discharge date check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;

		else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Discharge date check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
					    output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
		end;			

end;
%end;

%if &var_in. = IN_2400_SV203_SL_CHG_AMT or &var_in. = IN_2430_SVD02_SL_PAID_AM 
or &var_in. = IN_2430_CAS03 or &var_in. = IN_2430_CAS06 or 
&var_in. = IN_2430_CAS09 or &var_in. = IN_2430_CAS12 or &var_in. = IN_2430_CAS15
or &var_in. = IN_2430_CAS18 or &var_in. = IN_2430_CAS21 or &var_in. = IN_2430_SVD02_SL_PAID_AM_MCR  %then %do; /*B-222679 Added*/
if typecode in ('I','O') then do;

	if &var_in ge 0 then 
	do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Amount logic negative Check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Amount logic negative Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
end;	
%end;

%if &var_in. =  PR_2400_SV102_S_CHG_AMT or &var_in. = PR_2430_SVD02_SL_PAID_AMT
or &var_in. = PR_2430_CAS03 or &var_in. = PR_2430_CAS06 or 
&var_in. = PR_2430_CAS09 or &var_in. = PR_2430_CAS12 or &var_in. = PR_2430_CAS15
or &var_in. = PR_2430_CAS18 or &var_in. = PR_2430_CAS21  or &var_in. = PR_2430_SVD02_SL_PAID_AMT_MCR %then %do; /*B-222679 Added*/

if typecode in ('P') then do;

	if &var_in ge 0 then 
	do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Amount logic negative Check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Amount logic negative Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;

end;
	
%end;

%if &var_in. = PRDIAGCD  or &var_in. = DIAGD2  or  &var_in. = DIAGD3 or &var_in. = DIAGD4 or
    &var_in. = DIAGD5  or &var_in. = DIAGD6  or  &var_in. = DIAGD7 or &var_in. = DIAGD8 or
	&var_in. = DIAGD9  or &var_in. = DIAG10  or  &var_in. = DIAG11 or &var_in. = DIAG12  or
    &var_in. = DIAG13  or &var_in. = DIAG14  or  &var_in. = DIAG15 or &var_in. = DIAG17 or
	&var_in. = DIAG18  or &var_in. = DIAG19  or  &var_in. = DIAG20 or &var_in. = DIAG21 or
	&var_in. = DIAG22  or &var_in. = DIAG23  or  &var_in. = DIAG24 or  &var_in. = DIAG25 or
	&var_in. = ADMTDIAG  or  &var_in. = DIAGN16 %then %do;

		   /* SAS2AWS2: ReplacedFunctionCompress */
		   var_val100        = kcompress(&var_in.) ;
		   value_check100    = put(var_val100 ,$diag. );

		   if value_check100 = 'invalid' then
		   do;
		       edit_code = "&edit_in" ;
			   encapdfld =  "&var_in" ;
			   edit_chk_type = 'Diagcode reject';
			   editcd_flag = 'OFF';
			   Recsubind = 'N' ; 
			   drop var_val100 value_check100;
			   output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
		   end;
		   else
		   do;
		       edit_code = '000' ;
			   encapdfld =  "&var_in" ;
			   edit_chk_type = 'Diagcode reject';
			   drop var_val100 value_check100;
			   output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		   end;
%end;
	
%if  &var_in. = COS and &compno. ne 45 and &compno. ne 30 and "&LOB." ne "QHP" %then %do; /* B-154145 exclude QHP, HFIC, MCR */	
       if  missing(COS) = 0 then 
		do ;
          				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'COS Missing';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;
			
		else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'COS Missing';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
		end;	
%end;

%if  &var_in. = Balancing_Amount %then %do;
       if  ( (PR_val_2430 eq PR_val_2400) and typecode="P" ) or 
		    ( (IN_val_2430 eq IN_val_2400) and typecode in ("I","O") ) 
		    
             then 
		do ;
          				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Balacing Amount Missing';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;
			
		else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Balacing Amount Missing';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
		end;	
%end;

%if  &var_in. = Balancing_Amount and &compno.=34 %then %do; /*B-222679 Added*/
       if   ((PR_val_MCR_2430 eq PR_val_2400) and typecode="P") 
			or ((IN_val_MCR_2430 eq IN_val_2400) and typecode in ("I","O"))
		    
             then 
		do ;
          				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Balacing Amount Missing';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
		end;
			
		else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Balacing Amount Missing';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
		end;	
%end;

/* 541126 KV - added  edit logic check --  start */
%if &var_in = MEM_socsec %then %do;
/* SAS2AWS2: ReplacedFunctionCompress */
if  kcompress(&var_in.) NE ' ' and "&edit_type_MEM_socsec_rej." eq "H" and typecode in (&enctyp_code_MEM_socsec_rej.) then do;
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if  klength(kcompress(&var_in.))= 9 and kcompress(&var_in.)  not in (&socsec_blnk_out.) and 
(ksubstr(&var_in.,1,3) ne '000' and ksubstr(&var_in.,6,4) ne '0000' and ksubstr(&var_in.,4,2) ne '00'
and ksubstr(&var_in.,1,3) ne '666' and ksubstr(&var_in.,1,1) ne '9')
then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Social security number check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Social security number check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
end;
%end;
%if &var_in = BI_PRV_STATE  %then %do;

	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if klength(kcompress(&var_in.))= 2 and kcompress(&var_in.) in(&US_States.) then 		/* B-108297 US check */
	do;

						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider State check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider State check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
%end;

%if &var_in = BIPRVNPI  %then %do; /* CR# CHG0032056 - added mrg_provno */
/* SAS2AWS2: ReplacedFunctionCompress */
if kcompress(BI_PRV_FST) ne ' '  or  kcompress(BI_PRV_LST) ne ' ' then do;
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if (kcompress(&var_in.) ne ' ' and  klength(kcompress(&var_in.))= 10 and kcompress(&var_in.)  NE '0000000000' and
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
	kverify(kcompress(&var_in.), '0123456789') = 0 ) or mrg_provno ne ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider NPI number Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider NPI number Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;

%if &var_in = ATPRVNPI  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('I','O') and (kcompress(AT_PHY_LST) ne ' ' or kcompress(AT_PHY_FST) ne ' ') then do;

		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if kcompress(&var_in.) ne ' ' and klength(kcompress(&var_in.))= 10 and kcompress(&var_in.)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(&var_in.), '0123456789') = 0
		 then do;

						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Attending Provider NPI number Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Attending Provider NPI number Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;

%if &var_in = OPPRVNPI  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('I','O') and ( kcompress(OP_PHY_LST) ne ' ' or kcompress(OP_PHY_FST) ne ' ' ) then do;

		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if  kcompress(&var_in.) ne ' ' and  klength(kcompress(&var_in.))= 10 and kcompress(&var_in.)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(&var_in.), '0123456789') = 0
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Operating Provider NPI number Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Operating Provider NPI number Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;


%if &var_in = RF_PHY_npiid  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('P') and ( kcompress(RF_PHY_LST) ne ' ' or kcompress(RF_PHY_FST) ne ' ' )  then do;

		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if kcompress(&var_in.) ne ' ' and  klength(kcompress(&var_in.))= 10 and kcompress(&var_in.)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(&var_in.), '0123456789') = 0
		 then do;	
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring Provider NPI number Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring Provider NPI number Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
 end;
%end;


%if &var_in = RN_PHY_Npiid  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('P') and  kcompress(RN_PHY_LST) ne ' ' or kcompress(RN_PHY_FST) ne ' ' then do;
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if kcompress(&var_in.) ne ' ' and  klength(kcompress(&var_in.))= 10 and kcompress(&var_in.)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(&var_in.), '0123456789') = 0
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Rendering Provider NPI number Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Rendering Provider NPI number Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
	 
end;
%end;


%if &var_in = BI_prvnam  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if (kcompress(BIPRVNPI) ne ' ' or  mrg_provno ne ' ')  then do;/* CR# CHG0032056 - added mrg_provno */
		/* SAS2AWS2: ReplacedFunctionCompress */
		if kcompress(BI_PRV_LST) ne ' ' or kcompress(BI_PRV_FST) ne ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider Name Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider Name Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;

%if &var_in = AT_prvnam  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('I','O') and kcompress(ATPRVNPI) ne ' '  then do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		if kcompress(AT_PHY_LST) ne ' ' or kcompress(AT_PHY_FST) ne ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Attending Provider Name Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Attending Provider Name Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;

%if &var_in = OP_prvnam  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('I','O') and kcompress(OPPRVNPI) ne ' '  then do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		if kcompress(OP_PHY_LST) ne ' ' or kcompress(OP_PHY_FST) ne ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Operating Provider Name Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Operating Provider Name Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;



%if &var_in = RN_prvnam  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('P') and  kcompress(RN_PHY_npiid) ne ' '  then do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		if kcompress(RN_PHY_LST) ne ' ' or kcompress(RN_PHY_FST) ne ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Rendering Provider Name Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Rendering Provider Name Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end; 
end;
%end;


%if &var_in = RF_prvnam  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('P') and kcompress(RF_PHY_npiid) ne ' '  then do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		if ( kcompress(RF_PHY_LST) ne ' ' or kcompress(RF_PHY_FST) ne ' ' ) and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionIndex */
		kindex(cat(kcompress(RF_PHY_LST),kcompress(RF_PHY_FST)),'*') eq 0
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring Provider Name Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring Provider Name Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
 end;
%end;


%if &var_in = NPIID_1  %then %do; 

/* SAS2AWS2: ReplacedFunctionCompress */
if kcompress(Plstnam_1) ne ' ' or kcompress(pfstnam_1) ne ' ' then do;
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if (kcompress(&var_in.) ne ' ' and klength(kcompress(&var_in.))= 10 and kcompress(&var_in.)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(&var_in.), '0123456789') = 0 ) or  mrg_provno ne ''
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service Facility Location Provider NPI number Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service Facility Location Provider NPI number Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;


%if &var_in = prvnam  %then %do; 
/* SAS2AWS2: ReplacedFunctionCompress */
if kcompress(NPIID_1) ne ' '  or   mrg_provno ne ''  then do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		if kcompress(Plstnam_1) ne ' ' or kcompress(pfstnam_1) ne ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service Facility Location Provider Name Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service Facility Location Provider Name Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
%end;



%if &var_in = RF_PHY_npiid_1  %then %do; 
if typecode in ('P') and (rpanpi ne pcpnpiid) and (rpanpi ne "") and (pcpnpiid ne "") then do;
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('P') and kcompress(RF_PHY_LST_1) ne ' ' or kcompress(RF_PHY_FST_1) ne ' '  then do;

		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if kcompress(&var_in.) ne ' ' and  klength(kcompress(&var_in.))= 10 and kcompress(&var_in.)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(&var_in.), '0123456789') = 0
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring PCP Provider NPI number Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring PCP Provider NPI number Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end;
end;
%end;


%if &var_in = RF_prvnam_1  %then %do; 
if typecode in ('P') and (rpanpi ne pcpnpiid) and (rpanpi ne "") and (pcpnpiid ne "") then do;
/* SAS2AWS2: ReplacedFunctionCompress */
if typecode in ('P') and kcompress(RF_PHY_npiid_1) ne ' '  then do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		if kcompress(RF_PHY_LST_1) ne ' ' or kcompress(RF_PHY_FST_1) ne ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring PCP Provider Name Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Referring PCP Provider Name Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
 end;
end;
%end;



%if &var_in = NDCSVC  %then %do; 
	/* SAS2AWS2: ReplacedFunctionCompress */
	if kcompress(&var_in.) not in (&NDCSvccodrej.)
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDC Service code Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDC Service code Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
	 
%end;
/* 541126 KV - added  edit logic check --  end */
/* HS 569650 KV - added  edit logic check  -----> start */
%if &var_in = BI_PRV_CITY   %then %do; 
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if kcompress(&var_in.) ne  '' and  klength(kcompress(&var_in.)) ge 2
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider city check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider city check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
	 
%end;
%if &var_in = valcd01   %then %do; 
if typecode in ("I","O") then do;
	/* SAS2AWS2: ReplacedFunctionCompress */
	if kcompress(&var_in.) ne  'A0'
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Value code check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Value code check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
end; 
%end;
%if &var_in = BI_PRV_EIN   %then %do; 

	/* SAS2AWS2: ReplacedFunctionCompress */
	if kcompress(&var_in.) ne  ' '
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider federal number check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Billing Provider federal number check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
	 
%end;



/* HS 569650 KV - added  edit logic check  -----> end */
/* CR# CHG0031183 KV - Added logic to capture NDC service code missing in internal rejections when procedure code start with 'J' ---> start */
/*and UD_flag ne 1*/  /* CHG0032206 */ /*B-222732 UD_flag commneted*/
/* B-261072 - Commenting the Oth_check logic  */
/* 

%if &var_in = IN_2400_SV202_2_SL_HCPCS   %then %do; 
	
	if ( ( kupcase(ksubstr(IN_2400_SV202_2_SL_HCPCS,1,1)) eq 'J' or IN_2400_SV202_2_SL_HCPCS in (&J_HCPCS_PROC_CD.) )and klength(NDCSVC) ne 11 and  typecode='O') 
		 then do;
		 				edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDCSVC Code value reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;					
	 	end;

	 	else do;
					   	edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDCSVC Code value reject ';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
						
					
		 end;
	 
%end;

%if &var_in = SVCCOD   %then %do; 

	
	if  ( kupcase(ksubstr(svccod,1,1)) eq 'J' or svccod in (&J_HCPCS_PROC_CD.))  and klength(NDCSVC) ne 11 and  typecode='P'
		 then do;
		 				edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDCSVC Code value reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;

	 	end;
	 	else do;						
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDCSVC Code value reject ';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
					
		 end;
	 
%end;
*/

/* CR# CHG0031183 KV - Added logic to capture NDC service code missing in internal rejections when procedure code start with 'J' ---> end */

/* CR# CHG0031701 KV - Added logic to capture invalid national drug code unit of measurement  */

%if &var_in = NDCUOM  %then %do; 
	/* SAS2AWS2: ReplacedFunctionCompress */
	if kcompress(&var_in.) not in (&NDC_UOM.)
		 then do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDC Unit of measurment Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
		 else do;
		 				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'NDC Unit of measurment Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 
%end;
/* CR# CHG0034810 - added logic to reject the rate code having less han 4 characters when value code = 24  */
%if &var_in = val_rej  %then %do; 
if typecode in ("I","O") then do;
	/* SAS2AWS2: ReplacedFunctionCompress */
	if kcompress(&var_in.) eq  '1'
		 then do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Value amount Rejection Check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
		 else do;
		 				edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Value amount Rejection Check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;
end;
	 
%end;
/* CR# CHG0035401 - added logic to reject the state code if length is less than 2  */
%if &var_in = pstacod   %then %do; 
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if kcompress(&var_in.) ne  '' and  klength(kcompress(&var_in.)) eq 2 and kcompress(&var_in.) in(&US_States.)	/* B-108297 US check */
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service provider state check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Service provider state check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
	 
%end;
/* CR# CHG0035401 - added logic to reject rate code	if length of rate code is not equal to 5 */
%if &var_in = valA01   %then %do; 
if typecode in ("I","O") and valcd01='A0' then do;
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if klength(kcompress(&var_in.)) ne 5
		 then do;
		 		        edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Rate code length check';		
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;	
		  			
						
		 end;
		 else do;
					
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Rate code length check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;	
	 	end; 	
end; 
%end;
/* CR# CHG0035563 - added logic to reject member city code	if length of city code is not equal to 2 */
%if &var_in = MEM_MCITYCD   %then %do; 
	/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if kcompress(&var_in.) ne  '' and  klength(kcompress(&var_in.)) ge 2
		 then do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Member city length check';		
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 	end;

	 	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Member city length check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
					
		 end;
	 
%end;

%if &var_in = MSTACOD  %then %do;	/* B-108297 Member US check */
	 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
	if klength(kcompress(&var_in.,"@:*`~#"))= 2 and kcompress(&var_in.,"@:*`~#") in(&US_States.) then 
	do;
						edit_code = '000' ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Member state check';
						output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;

	else do;
						edit_code = "&edit_in" ;
						encapdfld =  "&var_in" ;
						edit_chk_type = 'Member state check';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;
						output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
%end;

/* B-96285 added QHP, CHG0037746 - added subscriber check condition */
	%if &var_in = SUB_MEMBNO %then %do;
/******************************************************
   Hardcode missing value and validation check of CIN
*******************************************************/ 			
	           /* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionTrim */
	           if Relcod ne 1 and  ktrim(kleft(&var_in)) eq ' ' then
				do;
			
				   	edit_code = "&edit_in" ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'subscriber number Check';
					editcd_flag = 'OFF';
					Recsubind = 'N' ;
					output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	      		end;          	        
				else do;
					edit_code = '000' ;
					encapdfld =  "&var_in" ;
					edit_chk_type = 'subscriber number Check';
					output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	 			end;	  
	%end;

%mend Oth_check;

/********************************************************************************/
/* 				START OF MACRO num_check										*/
/*				1.To do Numeric Check for Edit Matrix							*/
/********************************************************************************/

%macro num_check(var_in,edit_in,var_con) ;
if missing(&var_in) then
do;
	edit_code = '000' ;
	encapdfld =  "&var_in" ;
	edit_chk_type = 'Missing Numeric Check';
end;
else
do;
    %put &var_in.;
	%put &var_con.;

	/* SAS2AWS2: ReplacedFunctionStrip-ReplacedFunctionVerify */
	chk_flag = kverify(kstrip(&var_in),"&var_con.") ;

	if chk_flag > 0 then 
	do;
		edit_code = "&edit_in" ;
		encapdfld =  "&var_in" ;
		edit_chk_type = 'Numeric Check';
		editcd_flag = 'OFF';
		Recsubind = 'N' ;
		output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;
	end;
	else 
	do;
		edit_code = '000' ;
		encapdfld =  "&var_in" ;
		edit_chk_type = 'Numeric Check';
		Recsubind = 'Y' ;
		output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;
	end;
end;
%mend num_check;


/********************************************************************************/
/* 				END OF MACRO num_check											*/
/********************************************************************************/

/********************************************************************************/
/* 				START OF MACRO dscreate											*/
/*				1.Create one table for each table field in Edit matrix			*/
/*				2.Merge each table with corresponding reference table(M:N)		*/
/*				3.Create format with each table created in step 2				*/
/********************************************************************************/

%macro dscreate(McrEditValY);

data &all_table. ;

	set &RGA192..&McrEditValY. ;
	%let i = 1 ;

	%do %while(&i. <= &total.);
		%let tab_name = &&this_dwhtable&i. ;

		/* SAS2AWS2: ReplacedFunctionCompress */
		if "&tab_name" =  kcompress(dwhtable || costype || encTypCode ) then
		do;
			/* SAS2AWS2: ReplacedFunctionCompress */
			fmt_field = kcompress(encapdfld ||  costype || encTypCode ) ;
			output &&this_dwhtable&i. ;
		end;

		%let i = %eval(&i. + 1);
	%end;     
run;

%let i = 1 ;
%do %while(&i. <= &total.);

	proc sql noprint;
		create table &RGA192..&&this_dwhtable&i. as
		select 	A.*,
				B.&&this_encapdfld&i.,
				/* SAS2AWS2: ReplacedFunctionCompress */
				kcompress( put(A.costype,$3.) || A.encTypCode || B.&&this_encapdfld&i.) as mrgvar
		from 	&&this_dwhtable&i. A , 
				&libn2..&&ref_table&i. B ;
	quit ;

	data look_up(keep=fmtname start label);
		set &RGA192..&&this_dwhtable&i. end=eof;
		length label $7.
			   start $15.;
		/* SAS2AWS2: ReplacedFunctionCompress */
		fmtname = kcompress("$"|| fmt_field ) ;
		start = mrgvar ;
		label = 'valid';
		output;
		if eof then
		do;
			/* SAS2AWS2: ReplacedFunctionCompress */
			fmtname = kcompress("$"|| fmt_field ) ;
		  	start = 'Other';
		  	label = 'invalid';
		  	output;
		end;
	run;

	proc format cntlin=look_up;
	run;
%let i = %eval(&i. + 1);
%end;
 
%mend dscreate;

%dscreate(APD_EditValY);

/********************************************************************************/
/* 				END OF MACRO dscreate											*/
/********************************************************************************/
/********************************************************************************/
/* 				START OF MACRO outfile											*/
/* 				Function : Creates sas program with formats for edit check  	*/
/********************************************************************************/
%macro outfile;	
data _null_;

length 	fmt $15.
		;

file EmOut dropover; 
    


%let i = 1 ;
%let if_flag = xyz ;

	%do %while( &i. <= &total.);
	%let end_flag = &&costyp_enctyp&i ;
		
		%if &if_flag ^= &&costyp_enctyp&i. %then %do ;
		/* SAS2AWS2: ReplacedFunctionStrip */
		put "if kstrip(typecode) = '&&enctyp_cd&i.'  and kstrip(costype) = '&&costyp&i.' then do;" ;
		%end;
		%let if_flag = &&costyp_enctyp&i ;
		/* SAS2AWS2: ReplacedFunctionCompress */
		fmt = kcompress("$&&this_encapdfld&i. "||"&&costyp&i" || "&&enctyp_cd&i."|| ".") 	;
		put / ;
		/* SAS2AWS2: ReplacedFunctionCompress */
		put " var_val&i. 		= kcompress(costype || typecode || &&this_encapdfld&i.) ;" 	;
		put " value_check&i. 	= put( var_val&i. ," fmt ");" / 							;
		
		put "	if value_check&i. = 'invalid' then" 			;
		put "	do;" ;
		put "		edit_code 		= '&&edit_cd&i.';" 			;
		put "		encapdfld 		= '&&this_encapdfld&i.';" 	;
		put "		edit_chk_type 	= 'Validity';" 				;
		put "		editcd_flag 	= 'OFF';" 					;
		put "		Recsubind 		= 'N' ;		" 				;
		put "		drop var_val&i. value_check&i. ;  " 		;
		put "		output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt.;"  			;
		put "	end;"		;
		put "	else" 		;
		put "	do;" 		;
		put "		edit_code 		= '000';" 					;
		put "		encapdfld 		= '&&this_encapdfld&i.';" 	;
		put "		edit_chk_type 	= 'Validity';" 				;
		put "		Recsubind 		= 'Y' ;		" 				;
		put "		drop var_val&i. value_check&i. ;  " 		;
		put "		output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;"  			;
		put "	end;"/		;
			
		%let n = %eval(&i. + 1);
		
		%if &i. = &total. %then %do; 
		put "end;"/;
		%end;
		%else %if &end_flag ^= &&costyp_enctyp&n. %then %do ;
				put "end;"/;
		%let end_flag = &&costyp_enctyp&i ;
		%end;
	%let i = %eval(&i. + 1);
	%end;

%let j = 1 ;
%let if_flag = xyz ;


	%do %while( &j. <= &total_miss.);
	%let end_flag = &&costyp_enctyp_miss&j ;

		%if &if_flag ^= &&costyp_enctyp_miss&j. %then %do ;
		/* SAS2AWS2: ReplacedFunctionStrip */
		put "if kstrip(typecode) = '&&enctyp_cd_miss&j.'  and kstrip(costype) = '&&costyp_miss&j.' then do;" ;
		%end;
		%let if_flag = &&costyp_enctyp_miss&j 			;
	

		put "	if missing(&&encapdfld_miss&j.) then" 	;
		put "	do;" ;
		put "		edit_code 		= '&&edit_cd_miss&j.';" 	;
		put "		encapdfld 		= '&&encapdfld_miss&j.';" 	;
		put "		edit_chk_type 	= 'Missing';" 				;
		put "		editcd_flag 	= 'OFF';" 					;
		put "		Recsubind 		= 'N' ;		" 				;
		put "		output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;"  			;
		put "	end;"/		;	
		put "	else" 		;
		put "	do;" 		;
		put "	edit_code 			= '000';" 					;
		put "	encapdfld 			= '&&encapdfld_miss&j.';" 	;
		put "	edit_chk_type 		= 'Missing';" 				;
		put "	Recsubind 			= 'Y' ;		" 				;
		put "	output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;"  				;
		put "	end;"/		;

		%let n = %eval(&j. + 1)	;
		
		%if &j. = &total_miss. %then %do	; 
				put "end;"/		;
		%end;
		%else %if &end_flag ^= &&costyp_enctyp_miss&n. %then %do ;
				put "end;"/		;
		%let end_flag = &&costyp_enctyp_miss&j ;
		%end;

	%let j = %eval(&j. + 1);
	%end;

%let k = 1 ;
	%do %while( &k. <= &total_numchk.);
		put '%num_check(' 		@ ;
		put	"&&var_numchk&k." 	@ ;
		put ","					@ ;
		put "&&edit_numchk&k." 	@ ;
		put ","					@ ;
        put "&&var_con&k." 	@ ; 
		put ");" /				  ;
	%let k = %eval(&k. + 1);
	%end;

%let m = 1 ;
	%do %while( &m. <= &total_datrge.);
		put "if &&var_datrge&m. then" ;
		put "do;" ;
		put "	edit_code 		= '&&edit_datrge&m.';" 	;
		put "	encapdfld 		= '&&var_datrge&m.';" 	;
		put "	edit_chk_type 	= 'Date Range';" 		;
		put "	editcd_flag 	= 'OFF';" 				;
		put "	Recsubind 		= 'N' ;		" 			;
		put "	output &RGA191..EncounterEditFail_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;"  		;
		put "end;"				;

		put "else" ;
		put "do;" ;
		put "	edit_code 		= '000';" 				;
		put "	encapdfld 		= '&&var_datrge&m.';" 	;
		put "	edit_chk_type 	= 'Date Range';" 		;
		put "	Recsubind 		= 'Y' ;		" 			;
		put "	output &RGA191..EncounterEditPass_&Compno._&run_id. &RGA191..encounter837_&Compno._&fi_ds_dt. ;"  		;
		put "end;"/				;
	%let m = %eval(&m. + 1)		;
	%end;

%let o = 1 ;
%let if_flag = xyz ;


	%do %while( &o. <= &total_Othchk.);
	%let end_flag = &&costyp_enctyp_Othchk&o ;

		%if &if_flag ^= &&costyp_enctyp_Othchk&o. %then %do ;
		/* SAS2AWS2: ReplacedFunctionStrip */
		put "if kstrip(typecode) = '&&enctyp_cd_Othchk&o.'  and kstrip(costype) = '&&costyp_Othchk&o.' then do;" ;
		%end;
		%let if_flag = &&costyp_enctyp_Othchk&o			;
	

		put '%Oth_check(' 		@ ;
		put	"&&var_Othchk&o." 	@ ;
		put ","					@ ;
		put "&&edit_Othchk&o." 	@ ;
		put ");" /		;

		%let n = %eval(&o. + 1)	;
		
		%if &o. = &total_Othchk. %then %do	; 
				put "end;"/		;
		%end;
		%else %if &end_flag ^= &&costyp_enctyp_Othchk&n. %then %do ;
				put "end;"/		;
		%let end_flag = &&costyp_enctyp_Othchk&o ;
		%end;

	%let o = %eval(&o. + 1);
    
	%end;
/*	    put '%Oth_check(' 		@ ;
		put	"COS" 	@ ;
		put ","					@ ;
		put "99994" 	@ ;
		put ");" /		;

		put '%Oth_check(' 		@ ;
		put	"Balancing_Amount" 	@ ;
		put ","					@ ;
		put "99989" 	@ ;
		put ");" /		;

		put '%Oth_check(' 		@ ;
		put	"prvspcd" 	@ ;
		put ","					@ ;
		put "99994" 	@ ;
		put ");" /		;
	*/ /* HS 567501 KV - commented */
run;
%mend ;

/********************************************************************************/
/* 				END OF MACRO outfile											*/
/********************************************************************************/
%outfile; 
data &RGA191..EncounterEditPass_&Compno._&run_id. (keep = clamno lineno /* edit_chk_type edit_code encapdfld editcd_flag typecode costype
								provno mrg_provno svccod svctyp BI_PROVNO BI_PRV_LST MEM_MEMBNO  MEM_BTHDAT  SVCDAT        
								ENDDAT BILCOD ADMTYPE DISTAT SVCCOD ADMSRC OCCCD1 POSCOD CLAAMT PRDIAGCD PIDATE              
								BIPRVTXY ATPRVNPI OPPRVNPI BIPRVNPI REVENUE_CD RCVDAT */ Recsubind )        

	  &RGA191..EncounterEditFail_&Compno._&run_id. (keep = clamno lineno  edit_chk_type encapdfld  editcd_flag edit_code Recsubind typecode svcstat netalwamt)/* HS 567501 - modified */
      &RGA191..EditFailClmLine_&Compno._&run_id.(keep= clamno lineno typecode icn claimfrq COS edit_chk_type encapdfld  editcd_flag edit_code Recsubind  svcstat netalwamt)/* HS 567501 - modified */
	  &RGA191..EditPassClmLine_&Compno._&run_id.
	  &RGA191..encounter837_&Compno._&fi_ds_dt. (keep =  clamno lineno);	 
length 	    edit_chk_type 	$100. 
	 	    clamno 			$20.			
			edit_code 		$5.
			encapdfld		$50.
			editcd_flag		$3.
			Recsubind 		$1.;
set	&RGA191..encountersY5_&compno._&run_id.; 			
	retain 	edit_pass		0
			edit_fail		0
		;
		
	editcd_flag = 'ON';
    costype=cos;
	%include EmOut ;              
	if editcd_flag = 'ON' then 
	do;
		edit_pass + 1;
		output &RGA191..EditPassClmLine_&Compno._&run_id.;
	end;
	else 
	do;
		edit_fail + 1 ;
		output &RGA191..EditFailClmLine_&Compno._&run_id.;
	end;

	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('edit_pass',kcompress(edit_pass));
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('edit_fail',kcompress(edit_fail));

run;

	/* HS 542635 - Added Proc delete statement */
	Proc delete data=encountersy5_&Compno._&run_id.;run;
	run;
/* HS - 569650 - added */
data &RGA191..EncounterEditFail_&Compno._&run_id.(rename=(clamno1=clamno)); 
length clamno1 $20. edit_code  $5.
				edit_chk_type $100.
				encapdfld  $50.

  				;
set &RGA191..EncounterEditFail_&Compno._&run_id.;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;
data &RGA191..EditPassClmLine_&Compno._&run_id.(drop = clamno rename = ( clamno_old = clamno temp = clamno_old)); /* HS 569650 - reassigned clamno for multiple TCN*/
length clamno_old $20. temp $15.;
set &RGA191..EditPassClmLine_&Compno._&run_id.;
temp = clamno;

run;
/* HS - 569650 */
data &RGA191..EditFailClmLine_&Compno._&run_id. (rename=(clamno1=clamno)); 
length clamno1 $20. ;
set &RGA191..EditFailClmLine_&Compno._&run_id. ;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;
/* HS 567501 - added logic to reject Inpatient claims if any one of the paid line is rejected  ----> start */
Data temp(keep=clamno lineno  edit_chk_type encapdfld   edit_code Recsubind typecode);
set   &RGA191..EditFailClmLine_&Compno._&run_id. 
	  &RGA191..encounters_N_IO_P_&compno._&run_id.        	 	  
	  &RGA191..Reject_svccod1;
	  where typecode ="I" and netalwamt > 0 and "&edit_type_Inpatient_Paid_Rej." eq "H" and typecode in (&enctyp_code_Inpatient_Paid_Rej.); /* B-166920 */
Run;
Proc sort data= &RGA191..EditPassClmLine_&Compno._&run_id.;by clamno ;run;
Proc sort data=temp nodupkey;by clamno ;run;
Data reject_1;
merge  &RGA191..EditPassClmLine_&Compno._&run_id.(in=_a keep=clamno lineno Cos) temp(in=_b drop = lineno);
if _a and  _b;
by clamno;
if _a and _b then do;
encapdfld = 'Inpatient_Paid_Rejection';
edit_code = '99965';
edit_chk_type = 'Inpatient Paid Rejection';
Recsubind = 'N' ; /* CR# CHG0031183 KV - added */
end;
Run;
Data  &RGA191..EditPassClmLine_&Compno._&run_id.;
merge  &RGA191..EditPassClmLine_&Compno._&run_id.(in=_a) temp(in=_b keep=clamno);
if _a and not _b;
by clamno;
Run;
Proc sort data= &RGA191..EditFailClmLine_&Compno._&run_id. out = &RGA191..EditFailClmLinett_&Compno._&run_id. nodupkey;by clamno lineno;run;
Proc sort data=reject_1 ;by clamno lineno;run;
Data reject_2;
merge  reject_1 (in=_a ) &RGA191..EditFailClmLinett_&Compno._&run_id. (in=_b keep = clamno lineno);
if _a and not _b;
by clamno lineno;

Run;
Data &RGA191..EditFailClmLine_&Compno._&run_id.;
set &RGA191..EditFailClmLine_&Compno._&run_id. reject_2;
run;

Data &RGA191..EncounterEditFail_&compno._&run_id.;
set &RGA191..EncounterEditFail_&compno._&run_id.	  reject_2;
run;
 
data &RGA191..EditPassClmLine_&Compno._&run_id.(drop = clamno rename = ( clamno_old = clamno temp = clamno_old)); /* HS 569650 - reassigned clamno for multiple TCN*/
length clamno_old $20. temp $13.;
set &RGA191..EditPassClmLine_&Compno._&run_id.;
temp = clamno;
run;
/* HS 567501 - added logic to reject Inpatient claims if any one of the paid line is rejected  ----> end */
proc sql;
		select  count(*) into :total_obs_edit  from &RGA191..EditFailClmLine_&Compno._&run_id. ;
    Quit;
	proc sql;
		select  count(*) into :total_obs_fin  from &RGA191..EditPassClmLine_&Compno._&run_id. ;
    Quit;

	

%macro report;

proc sql;
Create table cos_noblank as
 select cos, count(*) as count from &RGA191..EditPassClmLine_&Compno._&run_id.  group by 1;
quit;


proc sql;
Create table cos_BLANK as
 select count(*) as count from &RGA191..EncounterEditFail_&Compno._&run_id.  
where encapdfld eq 'COS' ;
quit;


data cos ;
set cos_noblank cos_blank  ;
run;

%if &compno. ne 45 and &compno. ne 30 and "&LOB." ne "QHP" %then %do; /* B-96285 exclude QHP, HFIC, MCR B-145499 */
PROC EXPORT DATA= cos	 
	     /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	     OUTFILE= "&drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_C.xls"
	 dbms=xls  replace;sheet='COS';
RUN;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_C.xls &drv72./Encounters_Reporting/APD/Archive/&ddir.  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

%end;
%if &QIfiflag = N %then %do;
Data summary;
length Description $100.;
Description="Total Record count";
Count=&total_obs.;
output;
Description="Revind Reject";
Count=0;
output;
Description="Hold Code Reject D1,32,06";
Count=&total_obs_hold.;
output;
Description="Edit Matrix Rejection";
Count=&total_obs_edit.;
output;
Description="Unknow Reject";
Count=sum(&total_obs., - sum(&total_obs_fin,&total_obs_edit.,&total_obs_hold.,0));
output;
Description="Final Base Record Count";
Count=&total_obs_fin.;
output;
run;

PROC EXPORT DATA= summary	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_A.xls"
		           dbms=xls replace;sheet='SUMMARY';
RUN;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_A.xls &drv72./Encounters_Reporting/APD/Archive/&ddir.  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

%end;
%else %do;
Data summary;
length Description $100.;
Description="Total Record count";
Count=&total_obs.;
output;
Description="Revind Reject";
Count=sum(&total_obs., - &total_obs_post_revind.);
output;
Description="Hold Code Reject D1,32,06";
Count=&total_obs_hold.;
output;
Description="Edit Matrix Rejection";
Count=&total_obs_edit.;
output;
Description="Unknow Reject";
Count=sum(&total_obs., - sum(&total_obs_fin,&total_obs_edit.,&total_obs_hold.,sum(&total_obs., - &total_obs_post_revind.)));
output;
Description="Final Base Record Count";
Count=&total_obs_fin.;
output;
run;

PROC EXPORT DATA= summary	 
		            /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		            OUTFILE= "&drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_A.xlsx"
		           dbms=xlsx replace;
RUN;

data _null_;
cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Current/RGA19101_&run_id._&compno._00_A.xlsx &drv72./Encounters_Reporting/APD/Archive/&ddir.  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

%end;

%mend;
*%report;/* CR# CHG0039138 - commented and called in RGA19401 program */


proc sql  noprint;
	select count(distinct(clamno))
			into :edit_pass_1
			from &RGA191..EditPassClmLine_&Compno._&run_id.
			;
quit;
/* HS - 569650 */
data Rollup_claims(rename=(clamno1=clamno)); 
length clamno1 $20. ;
set Rollup_claims;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;
/* HS 565133 */
data Rollup_claims_HG(rename=(clamno1=clamno)); 
length clamno1 $20. ;
set Rollup_claims_HG;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;
/*CR# CHG0035759 - Added logic to reject the service lines of claim if count is more than 50 ---> start */
proc sort data = &RGA191..EditPassClmLine_&Compno._&run_id. ;
by clamno lineno;												/* B-89828 added lineno to order gt 50 reject */
run;

data &RGA191..EditPassClmLine_&Compno._&run_id. 
		line_reject(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode) ;
set &RGA191..EditPassClmLine_&Compno._&run_id.;
by clamno ;
if first.clamno then do;
temp_lineno = 0;
end;
temp_lineno + 1;

if typecode not in ('I','O') then do;							/* B-89828 Exclude Institutional from reject */
  if temp_lineno gt 50 &line_reject_final.						/* B-168796 _final */
  then do; 
						edit_code = "99953" ;
						encapdfld =  "line_reject" ;
						edit_chk_type = 'claim line greater than 50 reject';
						editcd_flag = 'OFF';
						Recsubind = 'N' ;						
  		output line_reject;
  end;
  else  output &RGA191..EditPassClmLine_&Compno._&run_id.;
end;
else    output &RGA191..EditPassClmLine_&Compno._&run_id.;
run;

data line_reject(rename=(clamno1=clamno)); 
length clamno1 $20. ;
set line_reject;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;
/*CR# CHG0035759 - Added logic to reject the service lines of claim if count is more than 50 ---> end */

/* B-145499 */
%macro elig_checkds(dsn);
%if "&elig_chk." eq "Y" and (&compno=45 or &compno=30) %then %do; 
   %if %sysfunc(exist(&dsn)) %then %do;
      &RGA191..eligibility_reject
   %end;
%end;
%mend elig_checkds;

Data &RGA191..EditFailClmLine_&Compno._&run_id.(drop=COS);
set &RGA191..EditFailClmLine_&Compno._&run_id.
	 Rollup_claims(keep=clamno lineno typecode icn claimfrq )
	 Rollup_claims_HG(keep=clamno lineno typecode icn claimfrq )/* HS 565133 */
	 &RGA191..Reject_svccod1(keep=clamno lineno typecode icn claimfrq )
	 /* CR# CHG0040549 KV - added rejection datasets */
	  line_reject 
	  Reject_DGSVCCOD 
	  &RGA191..Inovalon_delete_reject 
	  APD_Reject_cntrl_file
	  APD_Reject_Claim_cntrl_file	/* B-166920 */
	  &RGA191..procedure_code_date_reject	/* B-114226 */
	  &RGA191..NDCSVC_Procbill_Code_rej  	/*B-261072 - 99963  Internal Reject logic - Update */
	  &RGA191..NDCSVC_Proc_Code_rej	  /*B-261072 - 99963  Internal Reject logic - Update */
	  &RGA191..NPI_City_Rej   /* 263565 - NPI city length Internal Reject Logic */
	  &RGA191..admtime_Rej   /*B-263560 - Incorrect Admission  time Internal Reject logic */
	  &RGA191..distim_Rej    /*B-263560 - Incorrect Discharge time Internal Reject logic */
	  &RGA191..MCORCAT_REJECTION /* B-278970 - DS - ADDED*/
	   %elig_checkds(&RGA191..eligibility_reject) /* B-131960 */
	  &RGA191..hiosid_reject /*B-274606*/
	  &RGA191..provider_date_reject /*[B-335998] */
	  
;
run;

data _null_ ;
		/* SAS2AWS2: ReplacedFunctionCompress */
		call symput('Clmline_Fail',kcompress(&Clmline_Fail.));
		/* SAS2AWS2: ReplacedFunctionCompress */
		call symput('ClmTot_Fail',kcompress(%eval(&Rej_Enc_N_IO_P. + &edit_fail. + &Reject_DG_I. + &Bil_cod_Rej_count. + &QI_duplicate.  )));
		/* SAS2AWS2: ReplacedFunctionCompress */
		call symput('edit_fail_1',kcompress(%eval(&Reject_DG_I_1. - &edit_pass_1. )));
	
	
run;
data _null_;
	/* SAS2AWS2: ReplacedFunctionCompress */
	call symput('ClmTot_Fail_1',kcompress(%eval(&Rej_Enc_N_IO_P_1. + &edit_fail_1. + &Bil_cod_Rej_count1. + &Reject_DG_I_2. )));
run;

/* HS 542635 - Added Proc delete statement */
Proc delete data=encountereditpass_&Compno._&run_id.;run;
%macro RGA192_tmp_ds_del_1  ;
	%if &temp_ds_del_flag. = Y %then 
	%do; 
			 proc datasets lib= &RGA192. nolist  ;
			      delete &all_table. APD_editvaly ;
			  quit;
			 proc datasets lib= &RGA191. nolist  ;
			      delete 	encounter837_&Compno._&fi_ds_dt. 	encountersy5_&compno._&run_id. ;
			  quit;
	%end;  
%mend RGA192_tmp_ds_del_1  ;
%RGA192_tmp_ds_del_1 ;

proc sql;
	update &R42173..medical_job_mstr
	set editmatrix_flag = '1'
	where run_id = "&run_id." ;
quit;

/* VVKR Code Start -- B-75041 */
%macro cas_mc;
	proc sql noprint;
		create table &RGA191..cas_1_pd_n_e as /* Extract records only apsvcstat eq "PD" and revind eq 'N' typcod eq 'E'  */
		select c.clamno,c.lineno,c.typecode,c.capind,c.rollup_line,c.apsvcstat,c.revind,d.holdcd,d.typcod 
		from (select a.clamno,a.lineno,a.typecode,a.capind,a.rollup_line,b.apsvcstat,b.revind from &RGA191..EditPassClmLine_&compno._&run_id. as a left join &RGA191..cas_svclines b on a.clamno eq b.clamno and a.lineno eq b.lineno) as c,
		svcholds as d
		where (c.clamno eq d.clamno) and (c.lineno eq d.lineno) and (c.apsvcstat eq 'PD') and (c.revind eq 'N') and (d.typcod eq 'E')
		order by clamno,lineno ;
	quit;

	data &RGA191..cas_rp_1 &RGA191..cas_nrp_1; /* Split datasets with and without rollup records */
	set &RGA191..cas_1_pd_n_e;
		if missing(rollup_line) ne 1 then output  &RGA191..cas_rp_1;
		else output  &RGA191..cas_nrp_1;
	run;

	proc sql noprint; /* Rollup dataset joining for extracting rollup lineno */
		create table &RGA191..cas_rp_2_all_lineno as 
		select a.clamno,a.lineno,a.typecode,a.capind,a.rollup_line,b.lineno as join_lineno from &RGA191..cas_rp_1 as a left join &RGA191..encountersY_&compno._&run_id. b on a.clamno eq b.clamno;
	quit;

	data &RGA191..cas_rp_3_chk_lineno; /* Keep records only for rollup lineno */
	set &RGA191..cas_rp_2_all_lineno;
		/* SAS2AWS2: ReplacedFunctionIndex-ReplacedFunctionStrip */
		if kindex(kstrip(rollup_line),kstrip(join_lineno)) gt 0 then output;
	run;

	proc sql noprint;
		create table &RGA191..cas_rp_4_svclines as /* Rollup data set join with SVCLINES for apsvcstat & revind variables by using rollup lineno */
			select a.clamno,a.lineno,a.join_lineno,a.typecode,a.capind,a.rollup_line,b.apsvcstat,b.revind from &RGA191..cas_rp_3_chk_lineno as a 
			left join &RGA191..cas_svclines as b on a.clamno eq b.clamno and a.join_lineno eq b.lineno ;
		create table &RGA191..cas_rp_5_svcholds as /* Rollup data set join with SVCHOLDS for holdcd & typcod variables by using rollup lineno */
			select a.*,b.holdcd,b.typcod  from &RGA191..cas_rp_4_svclines as a left join (select clamno,lineno,holdcd,typcod from svcholds where typcod eq 'E') as b 
			on a.clamno eq b.clamno and a.join_lineno eq b.lineno order by clamno,lineno,join_lineno;
		create table &RGA191..cas_rp_6_unique as /* Selected Unique clamno & lineno which are not satisfies CAS Segment condition */
			select distinct clamno,lineno from &RGA191..cas_rp_5_svcholds where (apsvcstat ne 'PD') or (revind ne 'N') or (typcod ne 'E') order by clamno,lineno;
	quit;

	data &RGA191..cas_rp_7_act_claims; /* Excluding clamno & lineno which are not required for CAS Segment */
		merge &RGA191..cas_rp_5_svcholds (in=a) &RGA191..cas_rp_6_unique (in=b);
		by clamno lineno;
		if a eq 1 and b eq 0;
	run;

	data &RGA191..cas_rp_8_first_claims; /* Selecting every clamno first lineno and first rollup lineno (join_lineno) to populate CAS Segment */
		set &RGA191..cas_rp_7_act_claims;
		by clamno lineno join_lineno;
		if first.lineno and first.join_lineno;
	run;
	
	proc sort data=&RGA191..cas_rp_7_act_claims out=&RGA191..cas_rp_9_unique nodupkey dupout=&RGA191..cas_rp_10_duplicates;
		by clamno lineno Join_lineno holdcd;
	run;
	
	data &RGA191..cas_rp_11_fst_clim_holdcd;
		merge &RGA191..cas_rp_8_first_claims (in=a) &RGA191..cas_rp_9_unique (in=b);
		by clamno lineno join_lineno;
		if a;
	run;
	
	%macro cas_09_12(ipds,od,ln_var); /* Macro for XWALK join and cas_flag populate */
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
	%mend cas_09_12;

	data &RGA191..cas_nrp_2_rm_rp_clms; /* Remove claim if control table rollup_lineno not satisfy CAS segment condition */
		merge &RGA191..cas_nrp_1(in=a) &RGA191..cas_arp_9_rp_no_fst_claim_srt(in=b keep=clamno lineno);
		by clamno lineno;
		if a eq 1 and b eq 0;
	run;
	
	data &RGA191..cas_nrp_3_add_rp_clms; /* Update claim if control table rollup_lineno satisfy CAS segment condition */
		merge &RGA191..cas_nrp_2_rm_rp_clms(in=a) &RGA191..cas_arp_8_rp_fst_claim_srt(in=b drop=Rollup_lineno);
		by clamno lineno;
		if a;
	run;

	%cas_09_12(cas_nrp_3_add_rp_clms,cas_nrp,lineno);

	%nobs(&RGA191..cas_rp_11_fst_clim_holdcd);
	%put &=nobs;
	%if &nobs ne 0 %then %do; /* rollup dataset have records then IF block execute*/
		%cas_09_12(cas_rp_11_fst_clim_holdcd,cas_rp,join_lineno); 

		proc sql noprint; /* Append with and without rollup datasets */
		create table &RGA191..cas_final_rp_nrp as
		select clamno,lineno,typecode,capind,rollup_line,apsvcstat,revind,holdcd,typcod,carc,nyscarc,cas_flag from &RGA191..cas_nrp_07_Final 
		  union all
		select clamno,lineno,typecode,capind,rollup_line,apsvcstat,revind,holdcd,typcod,carc,nyscarc,cas_flag from &RGA191..cas_rp_07_Final
		order by clamno,lineno;
		quit;
			
		proc sort data=&RGA191..EditPassClmLine_&compno._&run_id.;
		by clamno lineno;
		run;

		data &RGA191..EditPassClmLine_&compno._&run_id. &RGA191..Cas_not_reject; /* Joined cas_flag column with final datasets */
		merge &RGA191..EditPassClmLine_&compno._&run_id.	(in=_a) &RGA191..cas_final_rp_nrp (in=_b drop=rollup_line revind typecode capind );
		by clamno lineno;
		if _a then output &RGA191..EditPassClmLine_&compno._&run_id.;
		if _a and _b then output &RGA191..Cas_not_reject;
		run;
		
		data &RGA191..EditPassClmLine_&compno._&run_id. &RGA191..Cas_dg_claims;/* Joined cas_flag column with final datasets */
		merge &RGA191..EditPassClmLine_&compno._&run_id.	(in=_a) &RGA191..cas_dg10_final (in=_b keep=clamno lineno cas_flag carc);
		by clamno lineno;
		if _a then output &RGA191..EditPassClmLine_&compno._&run_id.;
		if _a and _b then output &RGA191..Cas_dg_claims;
		run;
	%end;
	%else %do;
		proc sort data=&RGA191..EditPassClmLine_&compno._&run_id.;
		by clamno lineno;
		run;

		data &RGA191..EditPassClmLine_&compno._&run_id. &RGA191..Cas_not_reject; /* Join (only rollup dataset) cas_flag column with final datasets */
		merge &RGA191..EditPassClmLine_&compno._&run_id.	(in=_a) &RGA191..cas_nrp_07_Final (in=_b drop=rollup_line revind typecode capind );
		by clamno lineno;
		if _a then output &RGA191..EditPassClmLine_&compno._&run_id.;
		if _a and _b then output &RGA191..Cas_not_reject;
		run;
		
		data &RGA191..EditPassClmLine_&compno._&run_id. &RGA191..Cas_dg_claims;/* Joined cas_flag column with final datasets */
		merge &RGA191..EditPassClmLine_&compno._&run_id.	(in=_a) &RGA191..cas_dg10_final (in=_b keep=clamno lineno cas_flag carc);
		by clamno lineno;
		if _a then output &RGA191..EditPassClmLine_&compno._&run_id.;
		if _a and _b then output &RGA191..Cas_dg_claims;
		run;
	%end;
%mend cas_mc;
%cas_mc;

	proc sql noprint;
		create table &RGA191..cas_dg_uniq_PA_claims as select distinct clamno,lineno,apsvcstat,edit_chk_type from &RGA191..cas_dg02_TempFlag where apsvcstat eq 'PA' order by clamno,lineno;
		select count(cas_flag) into :cas_flag_cnt_be4_update trimmed from &RGA191..EditPassClmLine_&compno._&run_id. where cas_flag eq 1;
		select count(*) into :in_paid_casflag_cnt_be4_updt trimmed from &RGA191..EditPassClmLine_&compno._&run_id. where IN_2430_SVD02_SL_PAID_AM>0 and cas_flag eq 1;
		select count(*) into :pr_paid_casflag_cnt_be4_updt trimmed from &RGA191..EditPassClmLine_&compno._&run_id. where PR_2430_SVD02_SL_PAID_AMT>0 and cas_flag eq 1;
	quit;

	%put &=cas_flag_cnt_be4_update;
	%put &=in_paid_casflag_cnt_be4_updt;
	%put &=pr_paid_casflag_cnt_be4_updt;
	
	data &RGA191..EditPassClmLine_&compno._&run_id.;
		merge &RGA191..EditPassClmLine_&compno._&run_id.(in=a) &RGA191..cas_dg_uniq_PA_claims(in=b keep=clamno lineno);
		by clamno lineno;
		if (a and b) or (IN_2430_SVD02_SL_PAID_AM > 0)  or (PR_2430_SVD02_SL_PAID_AMT > 0) then do;
			cas_flag=.;
			carc='';
		end;
		if a;
	run;

	proc sql noprint;
		select count(cas_flag) into :cas_flag_cnt_After_update trimmed from &RGA191..EditPassClmLine_&compno._&run_id. where cas_flag eq 1;
		select count(*) into :in_paid_casflag_cnt_aftr_updt trimmed from &RGA191..EditPassClmLine_&compno._&run_id. where IN_2430_SVD02_SL_PAID_AM>0 and cas_flag eq 1;
		select count(*) into :pr_paid_casflag_cnt_aftr_updt trimmed from &RGA191..EditPassClmLine_&compno._&run_id. where PR_2430_SVD02_SL_PAID_AMT>0 and cas_flag eq 1;
	quit;

	%put &=cas_flag_cnt_After_update;
	%put &=in_paid_casflag_cnt_aftr_updt;
	%put &=pr_paid_casflag_cnt_aftr_updt;
	
proc datasets lib=&RGA191. nolist;
 delete cas_: encountersY_&compno._&run_id.;
quit;
run;

/* B-274371 Merge the Rollup line of HG and DG claims to final dataset - Start's Here*/
proc sort data=&RGA191..EditPassClmLine_&compno._&run_id.;by clamno lineno;run;
proc sort data=HG_DG_ROLLUP;by clamno lineno;run;

data &RGA191..EditPassClmLine_&compno._&run_id.;
		merge &RGA191..EditPassClmLine_&compno._&run_id.(in=a) HG_DG_ROLLUP(in=b);
		by clamno lineno;
		if a;
		if a and b then do; rollup_line=catx(',',rollup_line,rollup_line3); end;
if a;
run;
/* B-274371 Merge the Rollup line of HG and DG claims to final dataset - End's Here*/
	
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19103 Program End execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

/*******************************************************************************/
/* 				END OF PROGRAM													*/
/********************************************************************************/




%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
%time(timeparm=s);

/*B-172922 - commented lock macro*/
/*lock &libn1..svclines.data; *//* HS 560569 DS changed the libnx to libn1 */ 

options bufno=400 ibufno=400 ubufno=20 sortsize=512M fullstimer MSGLEVEL=I minoperator ; /*HS# 521992 KS: Adding minoperator options*//* cw #236099 */

/* cw #210588: not include DWA31201.sas as mcor mapping was moved from dwa31201.sas to dwa32101.SAS         */
/* cw #210588: move code for MCORCAT mapping from DWA31201.sas to DWA32101.sas.                             */
/*             DWA31201.sas will only load for MCORxxMyyyy, not for SVCLINES                                */
/* cw #210588: Re-order field update based on their dependency. Field mcormat depends on REVCOD SVCCOD.     */
/*             dwhrefs.pregdiagcod table listing Pregnancy Diagnosis Codes is used for SVCCOD update        */ 
/*             The code to read this table was moved before mcorcat mapping.                                */

/* cw #210588: added %delview(xxx) for each data throughout the program                                     */

/* Remove a view */
%macro delview(DS);																

  %if %sysfunc (exist (&DS, VIEW)) %then %do;				
    proc sql noprint;								
      drop view &DS;								
    quit;											
  %end;
  %if %sysfunc (exist (&DS, DATA)) %then %do;		
    proc sql noprint;								
      drop table &DS;								
    quit;											
  %end;												

%mend delview;

/* cw #367813: utilize index field batdat to improve process  */
/*INC0575244  - commented the existing logic*/
/* data _null_;	 */
	/* SAS2AWS2: ReplacedFunctionCats */
/* 	call symputx('cutoffyr', input(cats('01/01/',year(today())-3),mmddyy10.)); */
/* run; */

/*B_313067_weeklycdc_POC  - adeded new logic for cutoffyr derivation*/
%macro batdat_replace;

%if &DWC32101_bat_ind. = 'A' %then %do;
filename outfl9 "&drv2/FIN/SVCLINES/batdat_cutoffyr.txt";
%inc outfl9;
%end;

%mend batdat_replace;
%batdat_replace;

%put &cutoffyr.;

/*********************************************************************************************/
/* MCORCAT, UTILCT are obtained by passing control= 2 to dwa312.sas that creates a temporary */
/* dataset mcorfinal using the same MCORCAT  mapping logic as monthly mcorxxmyyyy            */  
/*********************************************************************************************/

%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/*%define_variables(&libn1..mcormapping)*/						/* KS - 159341 - 1104009 */
%define_variables1(&libn1..mcormapping,&libn1..umrmap,&libn2..MMCORPROVSPECMAP); /* LZ #209447: add varaiables check from umrmap table */
/* B_294199_CDW_MMCOR_updates_include_New_PROVSPEC_mapping_Logic - ADDED MCORMAPPINGPROVSPEC*/

/* cw #245089 variable name list to be selected for SVCLINES  */
data svctemp2;
 		set svclinesf;
 		length testvalue $ 38;
		if name not in ('revcod','cptcod') then do;
			 /* SAS2AWS2: ReplacedFunctionCompress */
	 		testvalue=kcompress('a.'||name||',');
			output;
		end;
 run;
 
proc sql noprint;
    	select testvalue into :svclinesf separated by ' ' from svctemp2;
quit;
/* B-270231 - PREPARE the variable names to be selected for CLAIMS */
data temp;
 		set claimsf end=eof;
 		length testvalue $ 38;
		testvalue=kcompress('b.'||name||',');
		output;

	if eof then do;
		testvalue=kcompress('b.'||'subdrg'||',');
	  output;

  end;
run;
proc sort data=temp nodupkey;
by testvalue;
run;
/*CREATE the variable name list to be selected for CLAIMS */ 
proc sql noprint;
    	select testvalue into :claimsf separated by ' ' from temp;
quit;
%put &svclinesf;
%put &claimsf;
%put &has_prvtaxcod;

/* cw 210588: list of fields need for aputilct Note: it does not include mosdat mcorcat and subumrgrp.*/
/*             They will be pulled after they are mapped                                              */

%global svclines_all_f_d   aputilct_map_f_d    aputilct_map_f_s  ;

data aputilct_map_fld;
infile datalines dsd missover;
length varname $15.;
input varname;
/* SAS2AWS2: ReplacedFunctionCompress */
varname=kcompress(varname);
datalines;
clamno
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
unitct
extclm  
disdat 
firstdos 
lastdos  
admdat
;

run; 

proc sort data=aputilct_map_fld nodupkey;
by varname;
run;

proc sql noprint;
    	select distinct varname             into :aputilct_map_f_d separated by ' ' from aputilct_map_fld; /* for data step */
		/* SAS2AWS2: ReplacedFunctionTrim */
		select distinct 'a.'||ktrim(varname) into :aputilct_map_f_s separated by ', ' from aputilct_map_fld; /* for sql */
quit;

%put &aputilct_map_f_d;
%put &aputilct_map_f_s;


%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/**B-190005  changed to rsstr from &libn1..***/
proc contents data=rsstr.svclines out=svclines_all_fld(keep=name) noprint; /* HS 560569 DS changed the libnx to libn1 */
run;

proc sql noprint;
    	select distinct name into :svclines_all_f_d separated by ' ' from svclines_all_fld; /* for data step */		
quit;
 
/*****************************************************************************************/
/* cw #210588: get prvtaxcod from new table -- PROVTAXCODES if it exists.                */
/*             Otherwise get from old table -- PROVSPEC                                  */
/*             for NJ LOBs only. for NY create a dummy table                             */                    
/*****************************************************************************************/
%delview(prvtaxcod_tb);
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
%macro getprvtaxcod;
%if &has_prvtaxcod =1 and (&compno = 07 or &compno = 37) %then %do;
	 %if  %sysfunc(exist(&libn2..provtaxcodes)) =1  %then %do;
		proc sql _method;
		  create table prvtaxcod_tb as                                                          
		   select unique provno, prvtaxcod
		   from &libn2..provtaxcodes
		   where prvtaxcod ne ' '
	       ;            
		quit;
		%put From -- PROVTAXCODES;
	%end;
	%else %do;	
		proc sql _method;
		  create table prvtaxcod_tb as                                                          
		   select unique provno, prvtaxcod
		   from &libn2..provspec
		   where prvtaxcod ne ' '
	       ;      
		quit;
		%put From -- PROVSPEC;
	%end;

%end;
%else %do;
	data prvtaxcod_tb;
		length provno prvtaxcod $15.;
		provno=' ';
		prvtaxcod= ' ';
		delete;
	run;
%end;
%mend getprvtaxcod; 
%getprvtaxcod
%time;

/*  B_295356_CDW_SVCLINES_refresh_process_update_to_use_DRGCOD
start - 
This macro will add drgcod to claimsf macro vairable if drgcod is missing */
%macro drogcod_claimsf;

 %if %sysfunc(find("&claimsf.",drgcod)) ge 1 %then %do;   
   %let claimsf = &claimsf.;
    %end; 
   %else %do;
   	%let claimsf = &claimsf. b.drgcod,;
   %end;

%mend drogcod_claimsf;
%drogcod_claimsf;

/*  B_295356_CDW_SVCLINES_refresh_process_update_to_use_DRGCOD
end - 
This macro will add drgcod to claimsf macro vairable if drgcod is missing */

/*  pull from svclines that a batch date greater than or              */
/*             equal to the cutoff year and regardless of the service status	 */		
%delview(svcs_clam);	 
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/* cw #210588 Read claims table once to bring necessary fields for REVCOD, SVCCOD, MCORCAT, IPVALUE */

/* B_295356_CDW_SVCLINES_refresh_process_update_to_use_DRGCOD
start of the program */

proc sql;																	/* KS - 156845 */
create table svcs_clam_1 as													/* KS - 156845 */                   
select &svclinesf &claimsf 													/* KS - 159341 - 1104009 */                                                                                                                           
           case svcstat 													/* KS - 156845 */                                                                                                                                                                                  
                when 'PA' then 1											/* KS - 156845 */                                                                                                                                                                          
	                      else 2 
           end as prior 													/* KS - 156845 */
		   ,b.clatyp as clatyp_clm      /* cw #210588 */
		   ,b.covdys as covdys_clm      /* cw #210588 */
           ,b.clcovdys as clcovdys_clm  /* cw #210588 */
		   ,c.prvtaxcod                 /* cw #210588 */
		   ,a.cptcod
		   ,a.revcod
		/* B-270231 - added new variables  -- start */
		   ,a.patage
		   ,a.cptmd3
		   ,a.cptmd4
		   ,a.batdat
		   ,b.atpnpi
		   ,b.clspclcd
		   ,b.clatyp
		/* B-270231 - added new variables  -- end */
		   ,a.primdiagvind				/*UP -424199- Read  primdiagvind from SVCLINES table*/
		   ,b.diagvind					/*UP -424199- Read  diagvind from CLAIMS table*/
		   ,b.parstat                   /*NN- 461060- Read PARSTAT from Claims*/
		   ,b.rndnpi	/*added new variable - B_294199_CDW_MMCOR_updates_include_New_PROVSPEC_mapping_Logic*/
		   /*
		   ,case  when a.svctyp='CP' and a.cptcod=' ' then a.svccod											                                                                                                                                                                        
	              else a.cptcod 
            end as cptcod length=11
		   ,case  when a.svctyp='RV' and a.revcod=' ' then a.svccod											                                                                                                                                                                        
	              else a.revcod        
            end as revcod length=11 				
            */	
/**B-190005  changed to rsstr from &libn1..***/												
    from rsstr.svclines(where=(batdat>=&cutoffyr.))  a	/* HS 560569 DS changed the libnx to libn1 */ /* cw #367813: utilize index field */
       left join 
/**B-190005  changed to rsstr from &libn1..***/	
         rsstr.claims  (where=(batdat>=&cutoffyr.))  b	/* HS 560569 DS changed the libnx to libn1 */						
    on a.clamno=b.clamno														/* KS - 156845 */
       left join prvtaxcod_tb c                                              /* cw #210588 */
	 on a.provno=c.provno 
  order by clamno
  ;
quit;	
%time;

data svclines_drgcod;
set rsstr.svclines(where=(batdat>=&cutoffyr. and SVCTYP = 'DG'));
length DRGCOD $8;
format DRGCOD $8.;
informat DRGCOD $8.;
label DRGCOD = 'Billed DRG';
DRGCOD = compress(SVCCOD,'','kd');

keep CLAMNO LINENO SVCTYP  DRGCOD;
run;

proc sort data=svclines_drgcod;
by clamno descending DRGCOD;
run;

proc sort data=svclines_drgcod nodupkey;
by clamno;
run;


data svcs_clam;
merge svcs_clam_1(in=A drop=DRGCOD) svclines_drgcod(in=B keep=CLAMNO DRGCOD);
by CLAMNO;
if A=1;
run;

proc sort data=svcs_clam;
by clamno lineno;
run;

%delview(svcs_clam_1);
%delview(svclines_drgcod);

/* B_295356_CDW_SVCLINES_refresh_process_update_to_use_DRGCOD
end of the program */

proc sql;
create table yearcall as 
select distinct year(batdat) as year from svcs_clam;
quit;

%global diagcd cutoffdt;
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries */

/* Collect all the Pregnancy Diagnosis Codes */
%delview(temp);
data temp;												/* KS - 109597 */
 		set dwhrefs.pregdiagcod;						/* KS - 109597 */
 		length testvalue $15.;							/* KS - 109597 */
		 /* SAS2AWS2: ReplacedFunctionCompress */
 		testvalue=kcompress("'"||diagcd||"'");			/* KS - 109597 */
run;													/* KS - 109597 */

/* Create Macro for testing for Pregnancy Diagnosis Codes */ 
proc sql noprint;										/* KS - 109597 */
    	select testvalue into :diagcd separated by ' ' from temp;	/* KS - 109597 */
quit;	

filename icdcutof "&drv2/FIN/ICD10_cutoffdate.csv" ;
	
	/*UP -424199- Read-in ICD10 Cutoffdate file */
	data cutoffdt;
		length  cutoffdt $ 10.;
		infile icdcutof delimiter=',' dsd truncover firstobs=2;
		input cutoffdt $;
		call symput('cutoffdt',put(input(cutoffdt,mmddyy10.),date9.));
	
	run;
	
%put &compno.;

%let comp= &compno.;
%let cmp="%sysfunc(tranwrd(%cmpres(&comp.),%str( ),%str(" ")))";

%put &cmp;

%macro alllob;
%if &cmp. = "02" or &cmp. = "01"  or &cmp. = "20"  or &cmp. = "30" or &cmp. = "34" 
or &cmp. = "42" or &cmp. = "45" %then %do;

	/* B-270231 - Derived Few new variables to populate NEW MCORCAT values --------------------start */;
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/NDC_SVCCODE.xlsx"
		out=NDC_SVCCODE dbms=xlsx
		replace;
	Run;
	
	Data NDC_SVCCODE;
	set NDC_SVCCODE;
	/* SAS2AWS2: ReplacedFunctionCompress */
	NDC_Svccod1=kcompress("'"||NDC_Svccod||"'");
	run;
	Proc sql noprint;
	Select NDC_Svccod1 into :NDC_Svccod SEPARATED
	 by ',' from NDC_SVCCODE;
	quit;
	
	Data Npi_340b;
	set RGA193.Npi_340b;
	/* SAS2AWS2: ReplacedFunctionCompress */
	npiid1=kcompress("'"||npiid||"'");
	run;
	
	Proc sql noprint;
	Select npiid1 into :binpiid SEPARATED
	 by ',' from Npi_340b;
	quit;
	
	%inc "&drv./MACLIB/prvspcd_mapping.sas";
	proc sort data=FINALPROVSPECOUT_1;
	by clamno lineno;
	run;
	
	%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
				data svcs_clam1;
				length provsp $3.;
				merge svcs_clam(in=a )  	FINALPROVSPECOUT_1(in=b keep=clamno lineno npiid specialty_code);
				by clamno lineno;
				if a;
				if a and b then do;
				provsp=specialty_code;
				npiid=npiid;
				end;
				else do;
				provsp= '';
				npiid='';
				end;
				svccod_old=svccod;
				revcod_old=revcod;
				drop specialty_code;
				If formcd = 'U' and ksubstr(bilcod,1,2) in ( '11','12','28','41','65','66' ) Then 
									    Do;							
						        				 typecode = 'I';
						        				 cos ='11';
												
				end;
				Else  If formcd = 'U' Then
										Do;
						             			 typecode = 'O';  
				end;
				Else If formcd = 'H' Then 
										Do;
						             			 typecode = 'P';
				end;
				/* SVCCOD Derivation logic */		
				if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1) then do;
								svccod = svccod_old;
								end;
								else if typecode="O" then do;
											if missing(cptcod)=0 then SVCCOD  = cptcod ;
											else SVCCOD  = svccod_old;
								End;
								else do;
								svccod = svccod_old;
				end;
				
				/* Revenue code derivation logic */
				
				if missing(revcod_old) = 0 then  
						do;
								revcod = revcod_old ;
						end;
						else if svctyp = "RV" then
						do;
								revcod = svccod_old ;
						end;
				modcod_old=modcod;
				modcd2_old=modcd2;
				modcd3_old=modcd3;
				modcd4_old=modcd4;
				if typecode in ('O') and npiid in (&binpiid.) and svccod in (&NDC_Svccod.) then do;
											modcod	='UD';
										   modcd2                 =     cptmd2         ;   
						                   modcd3                 =     cptmd3         ;     
						                   modcd4                 =     cptmd4         ;  	
				end;
										else if typecode in ('O') then do;						  
				                    modcod                 =     cptmd1         ;   
						           		 	modcd2                 =     cptmd2         ;
						                 	modcd3                 =     cptmd3         ;     
						                 	modcd4                 =     cptmd4         ; 	
										end;
										
										else if typecode = "I" then		                   
									do;
									      modcod  = " " ;										
					           		      modcd2  = " " ;    
					                      modcd3  = " " ;      
					                      modcd4  = " " ;    	
					            	end;
					            	else if typecode = "P" then		                   
									do;
									      modcod  = modcod_old ;										
					           		      modcd2  = modcd2_old ;    
					                      modcd3  = modcd3_old ;      
					                      modcd4  = modcd4_old;    	
					            	end;
				vala01=compress(vala01,'.');
				vala02=compress(vala02,'.');
				vala03=compress(vala03,'.');
				vala04=compress(vala04,'.');
				vala05=compress(vala05,'.');
				vala06=compress(vala06,'.');
				vala07=compress(vala07,'.');
				vala08=compress(vala08,'.');
				vala09=compress(vala09,'.');
				vala10=compress(vala10,'.');
				vala11=compress(vala11,'.');
				vala12=compress(vala12,'.');
				drgcod_1=COALESCEC(SUBDRG,DRGCOD);
				drgcod=substr(drgcod_1,1,3);
				drop drgcod_1;
				run;
	
		proc delete data=svcs_clam;run;
		proc delete data=FINALPROVSPECOUT_1;run;
		
	%macro new_mcormapping(year, year1);
		
			
				data svcs_clam1_&year.;
				set svcs_clam1(where =(batdat between "01jan&year."d and "31dec&year1."d));
				run;
        %include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries */
			
				
				/* B-270231 - Derived Few new variables to populate NEW MCORCAT values --------------------end */
				
				/*B-270231 - MCORCAT header level mapping ----------------------------------------------------------start */
				proc sql;
				Create table mcor_head  as 
				select distinct compress(scan(mcorcat,1,'_'),'','ak') as mcorcat1, Header_flag as flag_value 
				from rsstr.mcor_head where table = 'SVCLINES' and compno = "&compno." 
				order by flag_value;
				quit;
				
				%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
				
					data svclines_svccod_head_&year. (drop=i svccod_ind cnvind);																									
					set svcs_clam1_&year.;																									
					length save_svccod $11.  svccod_ind $1. cnvind $2. vind $1.;										
					/* Test diagnosis codes against codes from Pregnancy Table to determine adjudication */
						array diag(15) primdiag diagd2-diagd9 diag10-diag15;
						save_svccod =svccod;			
					svccod_ind=' ';												
					if svccod = '99499' then do;								   
					do i=1 to 15;											    
					if diag(i) in(&diagcd ) then svccod_ind = 'Y';				
					end;													
					end;														
					if svccod_ind = 'Y' then svccod='99213';				
																												
					/* SAS2AWS2: ReplacedFunctionRight */
					svccod=kright(svccod);											
					/* SAS2AWS2: ReplacedFunctionTranslate */
					svccod=ksubstr(ktranslate(svccod,'0',' '),7);/*SAS2AWS2 - added substr*/								
					/* Concatenate diagvind & primdiagvind and create a new Version indicator
					based on Diagvind & primdiagvind from Claims & svclines respectively*/
					cnvind=catt(diagvind,primdiagvind);
					/* SAS2AWS2: ReplacedFunctionCompress */
					if findc(kcompress(cnvind),'U') ne 0  Then  vind ='U';
					/* SAS2AWS2: ReplacedFunctionCompress */
					else if kcompress(cnvind)='99' then vind ='9';
					/* SAS2AWS2: ReplacedFunctionCompress */
					else if kcompress(cnvind)='00' then vind ='0';
					/* SAS2AWS2: ReplacedFunctionCompress */
					else if kcompress(cnvind)='9' then vind ='9';
					/* SAS2AWS2: ReplacedFunctionCompress */
					else if kcompress(cnvind)='0' then vind ='0';
					/* SAS2AWS2: ReplacedFunctionCompress */
					else if findc(kcompress(cnvind),'09') then vind ='C';
					else vind =''; 
					if svcstat = 'PA' then prior = 1;
					else prior =2 ;
					if vind = '0' or (vind in ('C','U','') and svcdat >= "&cutoffdt."d) then output svclines_svccod_head_&year.;
				run;
				 
				
				/* Sort the data based on clamno prior lineno*/
				proc sort data=svclines_svccod_head_&year.;by clamno prior lineno;run;
				
				filename outfile "&drv2/FIN/temp_HEAD_%ktrim(&compno.).txt";
				
				data mcorf_i10_head_&year.(Compress=yes);
				    length  mcorcat $15. origmcorcat $15.  ;
				    set svclines_svccod_head_&year. end=last; 
				    by clamno prior lineno;
					
				    origmcorcat = ' ';
				
				    %inc outfile;																/* KS - 159341 - 1104009 */
					end;																		/* KS - 159341 - 1104009 */
					svccod = save_svccod;
					drop save_svccod;
				mcorcat=compress(scan(mcorcat,1,'_'),'','ak');
				
				run;
				proc delete data=svclines_svccod_head_&year.; run;
				
				PROC SQL;
				CREATE table  mcorf_i10_map_&year. as 
				select a.*, input(b.flag_value,8.2) as flag_value
				from mcorf_i10_head_&year. a left join mcor_head  b
				on a.mcorcat=b.mcorcat1;
				QUIT;
				
				proc delete data=mcorf_i10_head_&year.; run;
				proc sql;
				create table mcorf_i10_map_min_&year. as 
				select clamno, min(flag_value) as flag_value from mcorf_i10_map_&year. 
				 group by clamno;
				quit;
				proc delete data=mcorf_i10_map_&year.; run;
				
				proc sql;
				create table svcs_clam2_&year. (drop=mcorcat) as select a.*, b.flag_value 
				from svcs_clam1_&year. a left join mcorf_i10_map_min_&year. b 
				on a.clamno=b.clamno;
				quit;
				proc delete data=mcorf_i10_map_min_&year.; run;
				proc delete data=svcs_clam1_&year.; run;
				
				
				
				/* B-270231 - MCORCAT header level mapping ----------------------------------------------------------end */
				
				/* B-270231 - Outpatient Pharmacy: hospital based pharmacy - OPHARMHBP ------ START */
				/*OPHARMBP records line count taken   */
				Proc sql;
	              			Create table svc_clam_oppharm_&year. as select *
	              			from svcs_clam2_&year. where &OPPHARMHBP_CON. ;
				quit;
				Proc sql;
	              			Create table svc_clam_oppharm_count_&year. as select clamno,count(clamno) as line_count
	              			from svc_clam_oppharm_&year. group by 1 order by 1,2;
				quit;
				/* line count taken for all claims from SVCLINES table  */
				Proc sql;
				              Create table Line_count_&year. as select clamno,count(clamno) as line_count
				              from svcs_clam2_&year. group by 1 order by 1,2;
				quit;
				/* Line count mapped for all claims  */
				Proc sql;
				              Create table svc_clam_finl_op_&year. as select a.*, b.line_count
				              from svcs_clam2_&year. a left join  Line_count_&year. b on a.clamno=b.clamno 
				              order by clamno, line_count;
				quit;
				/* Line count of claims matches with line count of OPPHARM then MCORCAT assigned with OPHARMHBP */
				Data svcs_clam3_&year.;
				              merge svc_clam_finl_op_&year.(in=_a)  svc_clam_oppharm_count_&year.(in=_b);
				              by clamno line_count;
				              if _a;
				              if _a and _b then LINE_COUNT_MTCH='Y';
				              ELSE LINE_COUNT_MTCH='N';
				              drop  line_count typecode;
				Run;
				proc delete data=svc_clam_finl_op_&year.; run;
				proc delete data=svc_clam_oppharm_count_&year.; run;
				proc delete data=Line_count_&year.; run;
				proc delete data=svcs_clam2_&year.; run;
				
				proc append base=svcs_clam3 data=svcs_clam3_&year.;
				run;
	
				proc delete data=svcs_clam3_&year.; run;
	/* B-270231 - Outpatient Pharmacy: hospital based pharmacy - OPHARMHBP ------ END */
	%mend new_mcormapping;
	
	data _null_;
	set yearcall(where=(year < year(today()))) ;
		year1=year;
			put "Batch date year less than current year";
			call execute('%new_mcormapping('||year||','||Year1||');');
	run;

	proc sql;
		create table yearcall1(where=(year <> .)) as 
		select  min(year) as year, max(year) as year1 from 
		yearcall where year >= year(today());
	quit;
	%nobs(yearcall1);
	%macro year_today;
		%if &nobs. > 0 %then %do;
			data _null_;
				set yearcall1;
				put "batch date year greater current year";
					call execute('%new_mcormapping('||year||','||Year1||');');
			run;
		%end;
		%else %do;
				%put "batch date year not greater current year";
		%end;
	%mend year_today;
	%year_today;
proc delete data=svcs_clam1; run;
%end;

%else %do;
		%nobs(svcs_clam);
		%if &nobs = 0 %then %do;
			data svcs_clam;
				set svcs_clam;
				svccod_old=svccod;
				revcod_old=revcod;
			run;
		proc datasets library=work;
   		change svcs_clam=svcs_clam3 ;
		run;
		%end;
		%else %do;
			data svcs_clam3;
				set svcs_clam;
				svccod_old=svccod;
				revcod_old=revcod;
			run;
			proc delete data=svcs_clam;
			run;
		%end;
	
%end;
%mend alllob;
%alllob;

/*---------------------------------------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------------------------------------------------------------------------------------------------------------*/
                /* cw #210588  below mcormat and utilct mapping logic are from old DWA31201.sas version 22 */
                /* cw #226194  modified mapping: allow use prvtaxcod field                                 */
/*---------------------------------------------------------------------------------------------------------------------------------------------*/
/*---------------------------------------------------------------------------------------------------------------------------------------------*/


%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/

/* B-270231 - added macro for inactive Lob's  --- end */

/* cw #210588: below mcormat and utilct mapping logic are from old DWA31201.sas version 22 */

options lrecl=32767; /* UP #424199 Added LRECL option to handle ICD10 Mcor file*/
%macro mcorcatUtilct;
/*UP -424199- Included ICD10 Cutoffdate file */
/* SAS2AWS2: ReplacedSlash */
filename icdcutof "&drv2/FIN/ICD10_cutoffdate.csv" ;

/*UP -424199- Read-in ICD10 Cutoffdate file */
data cutoffdt;
	length  cutoffdt $ 10.;
	infile icdcutof delimiter=',' dsd truncover firstobs=2;
	input cutoffdt $;
	call symput('cutoffdt',put(input(cutoffdt,mmddyy10.),date9.));

run;

%if (&compno = 07 or &compno = 37) %then %do;					/* KS - 156845 */

%delview(svclines_svccod);	 
data svclines_svccod(drop=cnvind);											/* KS - 159341 - 1104009 */													
	set /*svcs_clam*/  svcs_clam3;	/* B-270231 - modified input dataset name*/									/* KS - 159341 - 1104009 */													
	length save_svccod $11. cnvind $2. vind $1.;									/* KS - 159341 - 1104009 */		
	save_svccod =svccod;										/* KS - 159341 - 1104009 */													
	/* SAS2AWS2: ReplacedFunctionRight */
	svccod=kright(svccod);										/* KS - 159341 - 1104009 */
	/* SAS2AWS2: ReplacedFunctionTranslate */
	svccod=ksubstr(ktranslate(svccod,'0',' '),7);/*SAS2AWS2 - added substr*/							/* KS - 159341 - 1104009 */
	/*UP -424199- Concatenate diagvind & primdiagvind and create a new Version indicator
	based on Diagvind & primdiagvind from Claims & svclines respectively*/
    cnvind=catt(diagvind,primdiagvind);
	/* SAS2AWS2: ReplacedFunctionCompress */
	if findc(kcompress(cnvind),'U') ne 0  Then  vind ='U';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='99' then vind ='9';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='00' then vind ='0';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='9' then vind ='9';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='0' then vind ='0';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if findc(kcompress(cnvind),'09') then vind ='C';
	else vind =''; 									
run;

/*UP -424199-SPLIT ICD9 &10 data based on the Diagnosis indicator value and Cutoff date*/
%delview(svclines_svccod_i9);	 
%delview(svclines_svccod_i10);	 
data svclines_svccod_i9 svclines_svccod_i10;
	set svclines_svccod;
	if vind = '9' or (vind in ('C','U','') and svcdat <  "&cutoffdt."d) then output svclines_svccod_i9;
	if vind = '0' or (vind in ('C','U','') and svcdat >= "&cutoffdt."d) then output svclines_svccod_i10;
run;

/*UP -424199-Sort the tables by clamno  lineno*/
proc sort data=svclines_svccod_i9;by clamno  lineno;run;
proc sort data=svclines_svccod_i10;by clamno  lineno;run;

/*UP -424199- Include ICD9 & 10 mcormapping file*/
/* SAS2AWS2: ReplacedSlash */
filename outfl9 "&drv2/FIN/temp_ICD9_%ktrim(&compno.).txt";
/* SAS2AWS2: ReplacedSlash */
filename outfl10 "&drv2/FIN/temp_ICD10_%ktrim(&compno.).txt";
	
/* cw #210588 file temp&compno..txt is generated by DWA844. No need to generate here. Just use it here  */
/* %define_mcorcat;													KS - 156845 */

/*UP -424199 Include ICD9 Mcormapping file and create mcor tables for ICD9*/

%delview(mcorfinal1_i9);
%delview(mcorfinal2_i9);
/* Define the MCORCATS */
data mcorfinal1_i9  mcorfinal2_i9;								
	length  mcorcat $15. origmcorcat $15. origmedcty $6. umrgrp subumrgrp umrcat subumrcat $10. ;		/* KS - 156845 */
	origmcorcat=' ';														/* KS - 156845 */
	origmedcty=' ';															/* KS - 156845 */
	set svclines_svccod_i9;				 							/* KS - 156845 */
	
	by clamno  lineno;														/* KS - 156845 */

	%inc outfl9;															/* KS - 171281 - 09212009 */
	end;
	svccod = save_svccod;
	drop save_svccod;

/****************************************************************************************/
/* Define UMRGRP, SUBUMRGRP, UMRCAT, SUBUMRCAT, IPVALUE, PERDIEMRATE for mcorfinal      */
/****************************************************************************************/
		
	umrgrp=' ';																/* KS - 156845 */
	subumrgrp=' ';															/* KS - 156845 */
	umrcat=' ';																/* KS - 156845 */
	subumrcat=' ';															/* KS - 156845 */
	if ipopind = 'O' then do;												/* KS - 156845 */
   		utilct = 1;															/* KS - 156845 */
		output mcorfinal1_i9;													/* KS - 156845 */
		end;																/* KS - 156845 */
   	else output mcorfinal2_i9;													/* KS - 156845 */

	/* cw #210588: keep more to reduce the number of time to read claims table */
	/*keep &aputilct_map_f_d. 
				ipopind
				caretype
				alwamt
				mcorcat 
				origmcorcat
				utilct
				clatyp_clm covdys_clm clcovdys_clm prod prvorg
				;  */                                                       /* LZ - 209447 */                     
run;																		/* KS - 156845 */

/*UP 424199 Include ICD10 Mcormapping file and create mcor tables for ICD10*/

%delview(mcorfinal1_i10);
%delview(mcorfinal2_i10);
/* Define the MCORCATS */
data mcorfinal1_i10  mcorfinal2_i10;								
	length  mcorcat $15. origmcorcat $15. origmedcty $6. umrgrp subumrgrp umrcat subumrcat $10. ;	
	origmcorcat=' ';				
	origmedcty=' ';					
	set svclines_svccod_i10;				 						
	by clamno  lineno;											

	%inc outfl10;												
	end;
	svccod = save_svccod;
	drop save_svccod;

/****************************************************************************************/
/* Define UMRGRP, SUBUMRGRP, UMRCAT, SUBUMRCAT, IPVALUE, PERDIEMRATE for mcorfinal      */
/****************************************************************************************/
		
	umrgrp=' ';															
	subumrgrp=' ';														
	umrcat=' ';															
	subumrcat=' ';														
	if ipopind = 'O' then do;											
   		utilct = 1;														
		output mcorfinal1_i10;											
		end;															
   	else output mcorfinal2_i10;											
run;	

%delview(mcorfinal1);
%delview(mcorfinal2);
/*UP 424199 Combine ICD9 & 10 data*/
data mcorfinal1;
set mcorfinal1_i9 mcorfinal1_i10;
run;

data mcorfinal2;
set mcorfinal2_i9 mcorfinal2_i10;
run;

/* Make sure all svcstat of 'PA' are first */
 proc sort data=mcorfinal2;													/* KS - 156845 */							
		by clamno svcstat;													/* KS - 156845 */							
 run;																		/* KS - 156845 */										

/* Populate the UTILCT for inpatient claims */
data mcorfinal2;															/* KS - 156845 */									
		set mcorfinal2;														/* KS - 156845 */								
		by clamno svcstat;													/* KS - 156845 */								
		if  first.clamno and svcstat = 'PA' then utilct = 1; 				/* KS - 156845 */
		else utilct = 0;													/* KS - 156845 */									
run;																		/* KS - 156845 */

/* cw #210588 reduce steps */
%delview(mcorfinal123);
/* Combine Inpatient & Outpatient claims */
data mcorfinal123(drop=primdiagvind diagvind vind);																/* KS - 156845 */
	set mcorfinal1 mcorfinal2;												/* KS - 156845 */
run;																		/* KS - 156845 */	

 /* Resulting data must be CLAMNO, LINENO order */
 proc sort data=mcorfinal123;													/* KS - 156845 */
 	by clamno lineno;														/* KS - 156845 */
 run;																		/* KS - 156845 */

/* Define joiningmcorcat */
data mcorfinal123 ;		                        		                    /*	KS - 100654 */
	set  mcorfinal123;														/* KS - 156845 */
	length save_svccod $11.   ;                                             /* LZ - 209447 */
	by clamno;																/* KS - 156845 */
	format joiningmcorcat $10.;												/* KS - 156845 */
		if origmcorcat = '' then joiningmcorcat = mcorcat; 					/* KS - 156845 */
		else joiningmcorcat = origmcorcat;									/* KS - 156845 */
    save_svccod =/*svccod*/ svccod_old;	/* B-270231 - changed as svccod_old */									            /* LZ - 209447 */												
	/* SAS2AWS2: ReplacedFunctionRight */
	svccod=kright(/*svccod*/svccod_old); /* B-270231 - changed as svccod_old */											        /* LZ - 209447 */
	/* SAS2AWS2: ReplacedFunctionTranslate */
	svccod=ksubstr(ktranslate(svccod,'0',' '),7);/*SAS2AWS2 - added substr*/								        /* LZ - 209447 */
 run;																		/* KS - 156845 */

%end; /* end with %if (&compno = 07 or &compno = 37) %then %do;*/			/* KS - 156845 */


%else  %do;   

%delview(svclines_svccod)	 
 /* Reformat SVCCOD for use in define_mcorcat macro*/
data svclines_svccod (drop=i svccod_ind cnvind);												/* KS - 159341 - 1104009 */													
	set /*svcs_clam*/ svcs_clam3;	/* B-270231 - modified input dataset name*/												/* KS - 159341 - 1104009 */													
	length save_svccod $11.  svccod_ind $1. cnvind $2. vind $1.;									/* KS - 159341 - 1104009 */	
	/* KS - 109597 Test diagnosis codes against codes from Pregnancy Table to determine adjudication */
		array diag(15) primdiag diagd2-diagd9 diag10-diag15;	/* KS - 109597 */
		save_svccod =svccod; /* B-270231 */
		svccod_ind=' ';										/* KS - 109597 */
		if svccod = '99499' then do;						/* KS - 109597 */
		   do i=1 to 15;									/* KS - 109597 */
		    if diag(i) in(&diagcd ) then svccod_ind = 'Y';	/* KS - 109597 */
			end;											/* KS - 109597 */
		end;												/* KS - 109597 */
		if svccod_ind = 'Y' then svccod='99213';			/* KS - 109597 */
	
	/*save_svccod =svccod;*/ /* B-270231 */										/* KS - 159341 - 1104009 */													
	/* SAS2AWS2: ReplacedFunctionRight */
	svccod=kright(svccod);											/* KS - 159341 - 1104009 */
	/* SAS2AWS2: ReplacedFunctionTranslate */
	svccod=ksubstr(ktranslate(svccod,'0',' '),7);/*SAS2AWS2 - added substr*/								/* KS - 159341 - 1104009 */
	/*UP -424199- Concatenate diagvind & primdiagvind and create a new Version indicator
	based on Diagvind & primdiagvind from Claims & svclines respectively*/
	cnvind=catt(diagvind,primdiagvind);
	/* SAS2AWS2: ReplacedFunctionCompress */
	if findc(kcompress(cnvind),'U') ne 0  Then  vind ='U';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='99' then vind ='9';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='00' then vind ='0';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='9' then vind ='9';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if kcompress(cnvind)='0' then vind ='0';
	/* SAS2AWS2: ReplacedFunctionCompress */
	else if findc(kcompress(cnvind),'09') then vind ='C';
	else vind =''; 										
run;


/* Get Specialities */
%delview(provspec);												/* KS - 156845 */
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
proc sql;
  create view provspec as										/* KS - 156845 */
    select a.provno, a.spccod, b.spccod as bspccod, b.nysmeds
    from &libn2..provspec a left join &libn2..spcxwalk b
     on a.spccod=b.spccod;
quit;

/* Remove duplicate provider numbers and specialties*/
proc sort data=provspec out=provspec2 nodupkey;					/* KS - 156845 */
 by provno nysmeds;
run;
%delview(provspec3);												

/* Transpose specialties */
proc transpose data=provspec2 out=provspec3;					/* KS - 156845 */
by provno;
var nysmeds;
quit;

/* Create dummy dataset with 5 columns*/
data provspecdummy;
  length col1 col2 col3 col4 col5 $5.;
  col1='';
  col2='';
  col3='';
  col4='';
  col5='';
run;

/* Concatanate Dummy & Provider Specialty Together*/
%delview(provspec4);											/* KS - 156845 */
data provspec4(keep = provno col1 col2 col3 col4 col5) /view=provspec4;	/* KS - 156845 */
  set provspecdummy(obs=0)
       provspec3;												/* KS - 156845 */
run;
%time;

%delview(mcorprovinfo);												/* KS - 156845 */
/* Merge to get Specialities & Provider Type */

/*MV 445559 , Add LOBCOD field to join provider table, add for company 34 */
/* KG 318914 - Add lobcod to merge */

/*HS 521992 KS: Commenting below logic to implement new logic for reading company number from input file*/

/*%if &compno = 60 or &compno = 34 %then %do;*/
/*	%let lobjoin = and a.lobcod = c.lobcod;*/
/*%end;*/
/*%else %do;*/
/*    %let lobjoin = ;*/
/*%end;*/


/* HS# 521992 KS: Adding Macro to get the company list for which LOBCOD specific logic is required and
get the LOBCOD Join condition*/

%macro lobjoin (condno) / mindelimiter=',';
%global lobjoin;

/*filename to get the company list*/

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
filename complist "&drv2./FIN/lobcodcomplist.csv";

/* Read-in the input file*/
 data readfle;
 	infile complist firstobs=2 dsd dlm=',';
	input condno : $10. compnum : $20.;
	if condno = "&condno.";
	call symput ('lobcdcmp',compnum);
 run;

/* LOBCOD join condition assignment*/

%let lobjoin=; 
%if &compno. in (&lobcdcmp.) %then %do; 
%let lobjoin= and a.lobcod=c.lobcod;
%end;

%mend lobjoin;
*%lobjoin(cond1); /*B-270231 - commented Lobjoin macro */
/* LOBCOD join condition assignment*/
%let lobjoin=; /*B-270231 - added Lobjoin assignment */
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
proc sql;
  create view mcorprovinfo as											/* KS - 156845 */										
    select a.*, b.col1 as nysmeds1, b.col2 as nysmeds2, b.col3 as nysmeds3,
                b.col4 as nysmeds4, b.col5 as nysmeds5, c.prvtyp, c.mdcaid
     from svclines_svccod a left join provspec4 b								/* KS - 156845 */
     on a.provno=b.provno                /* Monthly table -- CW 57622 */
	 left join &libn1..provider c	     /* KS - 156845 */
	  on a.provno=c.provno &lobjoin.				/* KS - 156845 */  
      order by clamno, prior, lineno;	/* KS - 156845 */
quit;
%time;

/*UP -424199-SPLIT ICD9 &10 data based on the Diagnosis indicator value and Cutoff date*/
%delview(svclines_svccod_i9);	 
%delview(svclines_svccod_i10);	

data svclines_svccod_i9(compress=yes) svclines_svccod_i10(compress=yes);
set Mcorprovinfo;
if vind = '9' or (vind in ('C','U','') and svcdat <  "&cutoffdt."d) then output svclines_svccod_i9;
if vind = '0' or (vind in ('C','U','') and svcdat >= "&cutoffdt."d) then output svclines_svccod_i10;
run;

/*UP -424199- Sort the data based on clamno prior lineno*/
proc sort data=svclines_svccod_i9;by clamno prior lineno;run;
proc sort data=svclines_svccod_i10;by clamno prior lineno;run;

/*UP -424199- Include bothe ICD9 & 10 file*/
/* SAS2AWS2: ReplacedSlash */
filename outfl9 "&drv2/FIN/temp_ICD9_%cmpres(&compno.).txt";
/* SAS2AWS2: ReplacedSlash */
filename outfl10 "&drv2/FIN/temp_ICD10_%cmpres(&compno.).txt";

/*UP -424199 Include ICD9 Mcormapping file and create mcor tables for ICD9*/

%delview(mcorf_i9);

data mcorf_i9(Compress=yes);
/*data mcorfinal   /view=mcorfinal ;		 KS - 156845 */
    length  mcorcat $15. origmcorcat $15. origmedcty $9.  ;
    set svclines_svccod_i9 end=last; 
    by clamno prior lineno;
	
    origmcorcat = ' ';

    %inc outfl9;																/* KS - 159341 - 1104009 */
	end;																		/* KS - 159341 - 1104009 */
	svccod = save_svccod;
	drop save_svccod;

	/* -- Reassignment starts-- */

/*******************************************************************************************/
/* CW 57622.Re-assign MEDCTY: MEDCTY should be made AD0006 If the MMCOR category is IPNB   */ 
/* and the MEDCTY is not equal to 'AD0006', and company is '01' or '05' or '42'            */ 
/* The original value of MEDCTY is stored in origmedcty before MEDCTY is re-assigned.      */
/*******************************************************************************************/
    if compno in ('01' '05' '42' ) and mcorcat = 'IPNB' and medcty ^='AD0006'  then do; /* Added company 42 HS# 363644  UP*/ /*UP - HS# 572363 Added 45*/
        origmedcty = medcty;
        medcty = 'AD0006';
        end;
	/*B-248852 - MPPO Changes */	
    else if compno = '45' and prod ^= 'PPO' and mcorcat = 'IPNB' and medcty ^='AD0006'  then do; /* Added company 42 HS# 363644  UP*/ /*UP - HS# 572363 Added 45*/
        origmedcty = medcty;
        medcty = 'AD0006';
        end;
    else 
        origmedcty = ' ';
run;

/*UP -424199 Include ICD10 Mcormapping file and create mcor tables for ICD9*/

%delview(mcorf_i10)
data mcorf_i10(Compress=yes);
/*data mcorfinal   /view=mcorfinal ;		 KS - 156845 */
    length  mcorcat $15. origmcorcat $15. origmedcty $9.  ;
    set svclines_svccod_i10 end=last; 
    by clamno prior lineno;
	
    origmcorcat = ' ';

    %inc outfl10;																/* KS - 159341 - 1104009 */
	end;																		/* KS - 159341 - 1104009 */
	svccod = save_svccod;
	drop save_svccod;

	/* -- Reassignment starts-- */

/*******************************************************************************************/
/* CW 57622.Re-assign MEDCTY: MEDCTY should be made AD0006 If the MMCOR category is IPNB   */ 
/* and the MEDCTY is not equal to 'AD0006', and company is '01' or '05' or '42'            */ 
/* The original value of MEDCTY is stored in origmedcty before MEDCTY is re-assigned.      */
/*******************************************************************************************/
    if compno in ('01' '05' '42') and mcorcat = 'IPNB' and medcty ^='AD0006'  then do; /* Added company 42 HS# 363644  UP*/ /*UP - HS# 572363 Added 45*/
        origmedcty = medcty;
        medcty = 'AD0006';
        end;
	/*B-248852 - MPPO changes */	
    else if compno = '45' and prod ^= 'PPO' and mcorcat = 'IPNB' and medcty ^='AD0006'  then do; /* Added company 42 HS# 363644  UP*/ /*UP - HS# 572363 Added 45*/
        origmedcty = medcty;
        medcty = 'AD0006';
        end;
	else 
        origmedcty = ' ';
run;

/*UP - 424199 Combine  ICD9 & ICD10 data*/
%delview(mcorprovinfo_new)
data mcorprovinfo_new(drop=primdiagvind diagvind vind);
set mcorf_i9 mcorf_i10;
run;

/*UP - 424199 Sort the data*/
proc sort data=mcorprovinfo_new;
by clamno prior lineno;
run;

%delview(mcorfinal1);
%delview(mcorfinal2);
%delview(mcorfinal3);

/*UP - 424199 Assign utilct with Combined MCOR table*/
data mcorfinal1(compress=yes) mcorfinal2(compress=yes) mcorfinal3(compress=yes);
    set mcorprovinfo_new end=last;  /*UP - 424199 Included new mcor table*/

    by clamno prior lineno;
    /****************************/
  /* Create Utilization Count */
  /****************************/

  /* If Inpatient & first claim line then 1
     (data sorted in previous step to assign to first paid line */
  if first.clamno and ipopind = 'I' then utilct=1;

  /* CW & SM 60194: For OPTRER and service code is A0425 then UTILCT is 0 */
  else if ipopind = 'O' and mcorcat = 'OPTRER' and svccod in ('A0425') then utilct=0;

  /* If Outpatient then 1 */
  else if ipopind = 'O' then utilct=1;

  /* Else 0 */
  else utilct=0; 

  /* cw #210588 move code here to reduce one data step */
  select(mcorcat);									        /* KS - 100654 */
			when('OPTRNE') output mcorfinal1;				/* KS - 100654 */
			when('IPSNF')  output mcorfinal3;				/* KS - 100654 */
            otherwise      output mcorfinal2;				/* KS - 100654 */
	end;											        /* KS - 100654 */
run;

/* Sort OPTRNE transactions for special utilct determination */
	proc sort data=mcorfinal1;								/* KS - 100654 */
		by clamno enddat svcstat;							/* KS - 100654 */
	run;													/* KS - 100654 */

/* special utilct determination */
	%delview(mcorfinal1a);									/* KS - 156845 */
	data mcorfinal1a / view=mcorfinal1a;					/* KS - 156845 */										
		set mcorfinal1;										/* KS - 100654 */
		by clamno enddat;									/* KS - 100654 */
		if  first.enddat and svcstat = 'PA' then utilct = 1;/* KS - 100654 */
		else utilct = 0;									/* KS - 100654 */
	run;													/* KS - 100654 */

/* Sort IPSNF transactions for special utilct determination */
	proc sort data=mcorfinal3;								/* KS - 100654 */
		by clamno  svcstat;									/* KS - 100654 */
	run;													/* KS - 100654 */

 /* special utilct determination */
	%delview(mcorfinal3a);									/* KS - 156845 */
	data mcorfinal3a /view=mcorfinal3a;						/* KS - 156845 */										
		set mcorfinal3;										/* KS - 100654 */
		by clamno svcstat;									/* KS - 100654 */
		if  first.clamno and svcstat = 'PA' then utilct = 1; /* KS - 100654 */
		else utilct = 0;									/* KS - 100654 */
	run;													/* KS - 100654 */

/* Merge all transactions together */
	%delview(mcorfinal123);									/* KS - 156845 */
	  data mcorfinal123 ;				                    /* KS - 156845 */	  
		set mcorfinal1a mcorfinal2 mcorfinal3a;				/* KS - 100654 */
		length save_svccod $11.   ;                         /* LZ - 209447 */
		format joiningmcorcat $15.;							/* KS - 156845 */
		if origmcorcat = '' then joiningmcorcat = mcorcat; 	/* KS - 156845 */
		else joiningmcorcat = origmcorcat;					/* KS - 156845 */
		save_svccod =svccod_old;								/* LZ - 209447 */													
	    /* SAS2AWS2: ReplacedFunctionRight */
	    svccod=kright(svccod_old);								/* LZ - 209447 */
	    /* SAS2AWS2: ReplacedFunctionTranslate */
	    svccod=ksubstr(ktranslate(svccod,'0',' '),7);/*SAS2AWS2 - added substr*/					/* LZ - 209447 */
	  run;

	%time;

%end;

%mend mcorcatUtilct;

%mcorcatUtilct;
options lrecl=1000;  /* UP #424199 Re-assigned LRECL option*/

/* cw #210588: clean space */;
%delview(prvtaxcod_tb);
%delview(svcs_clam)
%delview(svclines_norevcod);
%delview(svclines_revcod);
%delview(temp);
%delview(svclines_svccod);
%delview(provspec);
%delview(provspec2);
%delview(provspec3);
%delview(provspec4);
%delview(mcorprovinfo);
%delview(mcorfinal1);
%delview(mcorfinal2);
%delview(mcorfinal3);
%delview(mcorfinal1a);
%delview(mcorfinal3a);

/*UP - 424199 Added new tables*/
%delview(mcorprovinfo_new)
%delview(mcorf_i10);
%delview(mcorf_i9);
%delview(svclines_svccod_i9);	 
%delview(svclines_svccod_i10);	
%delview(mcorfinal1_i10);
%delview(mcorfinal2_i10);
%delview(mcorfinal1_i9);
%delview(mcorfinal2_i9);
%delview(svcs_clam3);

%put &compno.;

%let comp= &compno.;
%let cmp="%sysfunc(tranwrd(%cmpres(&comp.),%str( ),%str(" ")))";

%put &cmp;
%macro alllob;
%if &cmp. = "02" or &cmp. = "01"  or &cmp. = "20"  or &cmp. = "30" or &cmp. = "34" 
or &cmp. = "42" or &cmp. = "45" %then %do;
%macro new_mcormapping_rollup(year, year1);

	/* B-270231 - MCORMAPPING -  Header level - Roll up --------------------- START */
	%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries */
	
	/*Read data from MCORMAPPING TABLE*/
	proc sql;
	create table mcormapping_rs as 
	select *  , compress(mcorcat,'','d') as mcorcat1 from rsstr.mcor_icd10 
	order by hierarchy; 
	quit;
	
	/* Removed duplicates - based on MCORCAT, Hierarchy & Header_flag */
	proc sort data=mcormapping_rs(keep=mcorcat1 hierarchy header_flag ) nodupkey;
	by mcorcat1 hierarchy header_flag  ;
	run;
	
	/* Populated Heirarchy & Header_flag to SVCLINES data based on MCORCAT value */
	proc sql;
	create table svclines2_mcorcat_&year. as select distinct a.*, b.hierarchy, 
	 b.header_flag from mcorfinal123(where =(batdat between "01jan&year."d and "31dec&year1."d)) a 
	 left join mcormapping_rs b 	on a.mcorcat=b.mcorcat1  order by clamno, hierarchy;
	quit;
/* Unique claim number is selected where header_flag is Y*/
	proc sql;
	create table svclines2_mcorcat_header_&year. as select distinct clamno from svclines2_mcorcat_&year. 
	where header_flag = 'Y';
	quit;

		/* Selected  header claims from SVCLINES table */
	proc sql;
	create table svclines2_header_claim_&year. as select distinct a.* from svclines2_mcorcat_&year. a inner join  
	 svclines2_mcorcat_header_&year. b on a.clamno=b.clamno;
	quit;
	/* Selected Non header claims from SVCLINES table */
	
	proc sql;
	create table svclines2_noheader_claim_&year. as select distinct a.* from svclines2_mcorcat_&year. a 
	left join  svclines2_mcorcat_header_&year.  b on a.clamno = b.clamno where b.clamno is null;
	quit;


	/* selected claim level header hierarchy & header MCORCAT */
	proc sql;
	create table svclines2_header_claim_lvl_&year. as select distinct clamno, hierarchy , mcorcat  
	from svclines2_header_claim_&year.
	where header_flag = 'Y' order by clamno, hierarchy;
	quit;
	/*Remove duplicates if one claims have two header mcorcat values*/
	proc sort data=svclines2_header_claim_lvl_&year. nodupkey;
	by clamno ;
	run;
	/* Populate header hierarchy & header MCORCAT to all lines*/
	proc sql;
	create table svclines2_header_claim_fnl_&year. as select distinct a.*, b.hierarchy as header_hierarchy, 
	b.mcorcat as header_mcorcat from svclines2_header_claim_&year. a 
	left join svclines2_header_claim_lvl_&year. b on a.clamno=b.clamno;
	quit;
	/*if header heirarchy is less than actual value. then replaced actual value*/
	data svc_clam_finl_&year.(rename=(hierarchy_new = hierarchy mcorcat_new = mcorcat));
	set svclines2_header_claim_fnl_&year.;
	if header_hierarchy > hierarchy then do;
	hierarchy_new = hierarchy;
	mcorcat_new = mcorcat;
	end;
	else do;
	hierarchy_new = header_hierarchy;
	mcorcat_new = header_mcorcat;
	end;
	drop hierarchy mcorcat header_hierarchy header_mcorcat;
	run;
	/* Combined Header & Non- Header data */
	data svclines2_mcorcat_fnl_&year.;
	set svc_clam_finl_&year.  svclines2_noheader_claim_&year.;
	svccod=svccod_old;
	svccod=ksubstr(ktranslate(kright(svccod),'0',' '),7);
	revcod=revcod_old;
	modcod=modcod_old;
	modcd2=modcd2_old;
	modcd3=modcd3_old;
	modcd4=modcd4_old;
	mcorcat=compress(scan(mcorcat,1,'_'));
	drop svccod_old revcod_old modcod_old modcd2_old modcd3_old modcd4_old provsp;
	run;
	
	proc delete data=mcormapping_rs; run;
	proc delete data=svclines2_mcorcat_&year.; run;
	proc delete data=svclines2_noheader_claim_&year.; run;
	proc delete data=svclines2_header_claim_&year.; run;
	proc delete data=svclines2_header_claim_lvl_&year.; run;
	proc delete data=svclines2_header_claim_fnl_&year.; run;
	proc delete data=svc_clam_finl_&year.; run;
	proc delete data=svclines2_mcorcat_header_&year.; run;
	
	proc append base=svclines2_mcorcat_fnl  data=svclines2_mcorcat_fnl_&year.;
	run;

	proc delete data=svclines2_mcorcat_fnl_&year. ; run;

/* B-270231 - MCORMAPPING -  Header level - Roll up --------------------- END */
%mend new_mcormapping_rollup;

%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/

data _null_;
	set yearcall(where=(year < year(today()))) ;
		year1=year;
			put "batch date year less than current year";
			call execute('%new_mcormapping_rollup('||year||','||Year1||');');
run;

proc sql;
		create table yearcall1(where=(year <> .)) as 
		select  min(year) as year, max(year) as year1 from 
		yearcall where year >= year(today());
	quit;
	%nobs(yearcall1);
	%macro year_today;
		%if &nobs. > 0 %then %do;
			data _null_;
				set yearcall1;
				put "batch date year greater current year";
					call execute('%new_mcormapping_rollup('||year||','||Year1||');');
			run;
		%end;
		%else %do;
				%put "batch date year not greater current year";
		%end;
	%mend year_today;
	%year_today;

%end;

%else %do;
		data mcorfinal123;
		set mcorfinal123;
		mcorcat=scan(mcorcat,1,'_');
		run;
	proc datasets library=work;
   		change mcorfinal123=svclines2_mcorcat_fnl ;
	run;
	
%end;
%mend alllob;
%alllob;
/* B-270231 - added macro for inactive Lob's  --- end */									
/* cw #210588: we no longer use  UMRMAP from hfall. Instead we use it from each Company specific library     */

/* LZ #209447 Start of the new process to populate UMR categories */
%macro umrcat;

/**************************************************************************************/
/* LZ #209447 Update logic to populate UMR categories by using previously created     */
/* (DWAA8101.SAS) select statement                                                    */
/* cw #160524 mapping mosdat and mos                                                  */
/* cw #210588:Also moved mosdat mapping here as it is used for APutilct               */
/* cw #210588:For SHP, mosdat will be assigned with SVCDAT, not admdat because of very*/
/*            long SNF stay                                                           */
/**************************************************************************************/

%macro mos; /* 7434 -Assign SVCDAT to MOSDAT irrespective of Compno,subumrgrp and admdat*/
                                                              /* HS 318914 add company 60 like company 02 RKS */
/*     %if &compno eq 02 or*/
/*         &compno eq 60 %then */
/*         %do;*/
     	   mosdat= svcdat;
/*         %end;*/
/*     %else*/
/*         %do;*/
/*     	    if subumrgrp in ('IP','SNF') and admdat ne . then */
/*                   mosdat= admdat;	*/
/*     	    else mosdat= svcdat;*/
/*         %end;*/
%mend mos;

/* LZ select statement file created in program DWAA8101.SAS */
/* SAS2AWS2: ReplacedSlash */
filename umrfile "&drv2/FIN/UMR/tempumr%cmpres(&compno.).txt";
%delview(mcorumrall);
data mcorumrall /view=mcorumrall;
length umrgrp $10. subumrgrp $10. umrcat $10. subumrcat $10. compno $3. mos $6.;
set /*mcorfinal123*/  svclines2_mcorcat_fnl;/* B-270231 - changed dataset name*/	

	%inc umrfile;												
	end;	
	svccod = save_svccod;  
    drop save_svccod;      
	keep &aputilct_map_f_d.
         /*admdat aprectyp apsubsys aptrndat clamno disdat enddat extclm firstdos formcd lastdos lineno
         membno revcod revind revsto svccod svcdat svcstat svctyp unitct   */                                    
				ipopind caretype alwamt
				mcorcat origmcorcat utilct
                umrgrp subumrgrp umrcat subumrcat	
				joiningmcorcat                                           
				clatyp_clm                 
				covdys_clm                 
				clcovdys_clm               
				prod                       
			    prvorg parstat /*NN #461060 fida: Included PARSTAT*/
                mos mosdat;  
	compno = "&compno";

%mos; 
    mos = put(mosdat,yymmn6.);

	format umrgrp $10. subumrgrp $10. umrcat $10. subumrcat $10.;
run;
%time;													

/* Sort the data by claim and line number*/
proc sort data=mcorumrall out=mcorumrall2 (compress=yes);
 by clamno lineno;
quit;
%time;													

%mend umrcat;
%umrcat
%delview(svclines2_mcorcat_fnl);
%delview(mcorfinal123);
/* LZ #209447 End of the new process to populate UMR categories */

/******************************************************************************/
/* The following macro was pulled from DWA01001.sas.  This macro determines   */
/* the ipvalue, covered days, perdiemdate fields.                             */
/******************************************************************************/
%macro ipvalue;
/* Calling macro to get chunk dataset for assigning covered days, ipvalue, etc. */
/*  cw #210588: imporve the process. No need to create chunk data from claims.   */
/*              fields were pulled from claims and carried along the steps       */
/*              changeed merge to set.             */  

/*******************************************************************************/
/* The first paid line will be assigned COVDYS, IPVALUE, PERDIEMRATE           */
/*******************************************************************************/
%delview(pdlines);
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
	 data pdlines;
	    set mcorumrall2 (where=(enddat ge '01Jan2000'd and svcstat='PA' and umrgrp='HS' and subumrgrp in ('IP', 'SNF'))
		                 rename=(covdys_clm=covdys clcovdys_clm=clcovdys)); /* cw #210588 */
	             
        by clamno;
    
               if first.clamno and (covdys=0 or covdys=.) then covdys=disdat-firstdos;
               if not first.clamno then do;                                      /* Added clcovdys pj 122342 */
                  covdys=.;                                                      /* Added clcovdys pj 122342 */
				  clcovdys=.;                                                    /* Added clcovdys pj 122342 */
			   end;                                                              /* Added clcovdys pj 122342 */
			   output;                                              
		
 run;

  /*  cw #210588 NOTE: IPVALUE is still populated althoght not used since 2006 */

  %delview(mcorumrfinal);

  %if &compno eq 30 or &compno eq 37 or &compno eq 40  %then              
 %do;
    %delview(ipvaluemap);
    data ipvaluemap;
		set &libn1..ipvaluemap;
        if effdat=. then effdat='01Jan2000'd;
    run;

	
    %delview(pdlines1);													/* KS - 156845 */
    /* Merge to ipvaluemap to get vars needed for ipvalue computation. */
    proc sql _method;
        create view pdlines1 as select a.*, b.perdiemrate, b.denominator	/* KS - 156845 */
                from pdlines a left join ipvaluemap b
                on a.caretype=b.caretype and
                a.clatyp_clm=b.clatyp and    /* cw #210588 */
                a.umrcat=b.umrcat and
                a.subumrcat=b.subumrcat and
                a.umrgrp=b.umrgrp and
                a.subumrgrp=b.subumrgrp
                where (b.effdat le a.enddat le b.expdat or (b.effdat le a.enddat and b.expdat=.))
                order by clamno, lineno;
    quit;
	%time;													/* KS - 156845 */

    /* Calculate or default ipvalue and merge back to svclinhes temporary dataset. */
    data mcorumrfinal(sortedby = clamno lineno
                          /*  cw #210588 note two updated fields REVCOD SVCCOD are included in &aputilct_map_f_d */ 
                          keep = &aputilct_map_f_d.  mcorcat origmcorcat utilct umrgrp subumrgrp umrcat subumrcat parstat /*NN #461060  Fida included parstat*/ 
                                 mosdat mos covdys  clcovdys perdiemrate ipvalue prod prvorg); /*lz pf change*/  /* Added clcovdys pj 122342 */ 
	  merge mcorumrall2 (in=m) pdlines1 (in=p keep=clamno lineno covdys clcovdys perdiemrate denominator);  /* cw #215088: added keep to avoid overwritten message */

        by clamno lineno;
        if m then
        do;
            if p then
                  do;
                     if perdiemrate ne . then ipvalue=perdiemrate*covdys;
                     else if perdiemrate = . and denominator ne 0 then ipvalue=alwamt/denominator;
                     else if perdiemrate = . and denominator = 0 then ipvalue=.;

                  end;
            else
                 do;
                    ipvalue=.;
                 end;
            output; 
        end;
    run;
	%time;													/* KS - 156845 */

 %end;
 /*B-248852 - Added company 45 logic to mimic medicare logic for MPPO data -- start */
 %if &compno eq 45 %then
 %do;
	data mcorumrall2_mppo mcorumrall2_nonmppo;
	set mcorumrall2;
	if prod = 'PPO' then output mcorumrall2_mppo;
	else output mcorumrall2_nonmppo;
	run;
	/*B-285445 - added logic to split dataset */
	Data pdlines_mppo  pdlines_nonmppo;
	set pdlines;
	if prod='PPO' then output pdlines_mppo;
	else output pdlines_nonmppo;
	run;
 
 
	%delview(ipvaluemap);
    data ipvaluemap;
		set &libn1..ipvaluemap;
        if effdat=. then effdat='01Jan2000'd;
    run;

	
    %delview(pdlines1);													/* KS - 156845 */
    /* Merge to ipvaluemap to get vars needed for ipvalue computation. */
    proc sql _method;
        create view pdlines1_mppo as select a.*, b.perdiemrate, b.denominator	/* KS - 156845 */
                from pdlines_mppo a left join ipvaluemap b
                on a.caretype=b.caretype and
                a.clatyp_clm=b.clatyp and    /* cw #210588 */
                a.umrcat=b.umrcat and
                a.subumrcat=b.subumrcat and
                a.umrgrp=b.umrgrp and
                a.subumrgrp=b.subumrgrp
                where (b.effdat le a.enddat le b.expdat or (b.effdat le a.enddat and b.expdat=.))
                order by clamno, lineno;
    quit;
	%time;													/* KS - 156845 */

    /* Calculate or default ipvalue and merge back to svclinhes temporary dataset. */
    data mcorumrfinal_mppo(sortedby = clamno lineno
                          /*  cw #210588 note two updated fields REVCOD SVCCOD are included in &aputilct_map_f_d */ 
                          keep = &aputilct_map_f_d.  mcorcat origmcorcat utilct umrgrp subumrgrp umrcat subumrcat parstat /*NN #461060  Fida included parstat*/ 
                                 mosdat mos covdys  clcovdys perdiemrate ipvalue prod prvorg); /*lz pf change*/  /* Added clcovdys pj 122342 */ 
	  merge mcorumrall2_mppo (in=m) pdlines1_mppo (in=p keep=clamno lineno covdys clcovdys perdiemrate denominator );  /* cw #215088: added keep to avoid overwritten message */

        by clamno lineno;
        if m then
        do;
            if p then
                  do;
                     if perdiemrate ne . then ipvalue=perdiemrate*covdys;
                     else if perdiemrate = . and denominator ne 0 then ipvalue=alwamt/denominator;
                     else if perdiemrate = . and denominator = 0 then ipvalue=.;

                  end;
            else
                 do;
                    ipvalue=.;
                 end;
            output; 
        end;
    run;
	%time;
	
	data mcorumrfinal_nonmppo(sortedby = clamno lineno 
	                      /*  cw #210588 note two updated fields REVCOD SVCCOD are included in &aputilct_map_f_d */
                          keep = &aputilct_map_f_d. mcorcat origmcorcat utilct umrgrp subumrgrp umrcat subumrcat parstat /*NN #461060  Fida included parstat*/
                                 mosdat mos covdys  clcovdys perdiemrate ipvalue  prod prvorg); /*lz pf change*/  /* Added clcovdys pj 122342 */ 
        
	merge mcorumrall2_nonmppo  pdlines_nonmppo(keep=clamno lineno covdys clcovdys  ); /* cw #215088: added keep to avoid overwritten message */
        by clamno lineno;
		perdiemrate = .;
		ipvalue = .;

    run;
	
	data mcorumrfinal;
	set mcorumrfinal_mppo mcorumrfinal_nonmppo;
	run;
	%time;
 %end;
 /*B-248852 - Added company 45 logic to mimic medicare logic for MPPO data -- end */
 /* The mhslines created for the umrmacro input*/
 %if &compno ne 30 and
     &compno ne 37 and  /*B-248852 - added company 45 */
     &compno ne 40 and &compno ne 45  %then
 %do;
    data mcorumrfinal(sortedby = clamno lineno 
	                      /*  cw #210588 note two updated fields REVCOD SVCCOD are included in &aputilct_map_f_d */
                          keep = &aputilct_map_f_d. mcorcat origmcorcat utilct umrgrp subumrgrp umrcat subumrcat parstat /*NN #461060  Fida included parstat*/
                                 mosdat mos covdys  clcovdys perdiemrate ipvalue  prod prvorg); /*lz pf change*/  /* Added clcovdys pj 122342 */ 
        merge mcorumrall2 pdlines(keep=clamno lineno covdys clcovdys); /* cw #215088: added keep to avoid overwritten message */
        by clamno lineno;
		perdiemrate = .;
		ipvalue = .;

    run;

 %end;

%mend ipvalue;

%ipvalue;
%time;

/* cw #210588: clean space */
%delview(mcorumrall2);
%delview(pdlines);
%delview(pdlines1);

/*-----------------------------------------------------------------------------------------------------*/ 
/*--------------------   cw #210588: APutilct APcovdys Mapping start          -------------------------*/ 
/*-----------------------------------------------------------------------------------------------------*/ 

%delview(snf);
%delview(ip);
%delview(trne);
%delview(trer);
%delview(op);

/* cw 210588: prepare data for APutilct APcovdys                           */
data snf (keep=clamno lineno membno aptrndat svcstat revind revsto extclm svctyp svccod mosdat disdat enddat svcdat unitct formcd revcod) 
     ip  (keep=clamno lineno membno aptrndat svcstat revind revsto extclm svctyp svccod mosdat disdat enddat svcdat admdat) 
	 trne(keep=clamno lineno membno aptrndat svcstat revind revsto extclm svctyp svccod mosdat disdat enddat svcdat ) 
	 trer(keep=clamno lineno membno aptrndat svcstat revind revsto extclm svctyp svccod mosdat disdat enddat svcdat ) 
     op  (keep=clamno lineno membno aptrndat svcstat revind revsto extclm svctyp svccod mosdat disdat enddat svcdat ) 
;

set mcorumrfinal;

	/* cw 210588: prepare data for APutilct APcovdys  */
    if extclm = ' ' then extclm=clamno;
	if mcorcat in ('OPTRNE') then output trne ;
	else if mcorcat in ('OPTRER') then output trer ;
	else if subumrgrp in('IP')    then output ip ;
	else if subumrgrp in('SNF')  and aprectyp='I' and apsubsys='MD' and svctyp = 'RV' and svccod < '200'  	  
                                  then output snf ;
	else output op;
run;
%time


/*****************************************************************************/
/*********                       SNP                                  ********/
/*****************************************************************************/

%delview(snf_admits)
/* cw #210588: 1. Join lines together to determine if interim bills are consecutive*/
proc sql _method;
	create  view snf_admits as
	select i.membno, i.svcdat, i.enddat, i.svcstat 
		   ,i.apmin format mmddyy10. as apmin
	       ,i2.svcdat as svcdat2
	       ,i2.enddat as enddat2
           ,i2.svcstat as svcstat2
	from 
		(select membno, svcdat
			   ,case when svcdat=enddat and abs(unitct)>1 then enddat+(abs(unitct)-1)
			   		 else enddat 
				 end format mmddyy10. as enddat
			   , min(svcstat) as svcstat
			   , min(case when svcstat='PA' then aptrndat
						  when svcstat='PD' and revsto ne '' then aptrndat
						  else . 
					 end) as apmin
			from snf 
			group by membno, svcdat, calculated enddat) i 
	left join 
		(select membno, svcdat
				,case when svcdat=enddat and abs(unitct)>1 then enddat+(abs(unitct)-1)
		   			   else enddat 
				  end format mmddyy10. as enddat
				,min(svcstat) as svcstat
			from snf 
			group by membno, svcdat, calculated enddat) i2
	on  i.membno=i2.membno
	and (  (i2.enddat>i.enddat  and i2.svcdat-i.enddat between 0 and 1 )
		 or(i2.enddat>i.enddat  and i2.svcdat between i.svcdat and i.enddat  ))
	order by membno, svcdat, enddat
;
quit;
%time
/* cw #210588: 2. Establish admission and discharge dates */
/*data snf_unique1 /view=snf_unique1;*/
%delview(snf_unique1)
data snf_unique1 /view=snf_unique1;
set snf_admits;
 	by membno;
	format admdat dschdat holdsvc2 holddsch mmddyy10.;
	retain admdat dschdat holdsvc2 holddsch;

	if (first.membno and last.membno) then do;
		admdat=svcdat;
	 	dschdat=enddat;
	 	output;
	end;
	else do;
	  if first.membno=1 then admdat=svcdat;
	  if first.membno=1 and last.membno ne 1 and enddat2=. then dschdat=enddat;
	  if svcdat2 ne . then dschdat=enddat2;
	  if enddat2=. and enddat>holddsch and holddsch ne . then dschdat=enddat;
	  if holdsvc2=. and first.membno ne 1  and enddat>holddsch then admdat=svcdat;
	
	  output;
	end;
	holdsvc2=svcdat2;
	holddsch=dschdat;
run;
%time

/* cw #210588: 3. Establish admission and discharge dates */
%delview(snf_unique2)
proc sql _method;
create view snf_unique2  as
	select membno, admdat, max(dschdat) format mmddyy10. as dschdat

	                     , min(svcstat) as svcstat
						 , min(apmin) format mmddyy10. as apmin
	from snf_unique1
	group by membno, admdat
;
quit;
%time

/* cw #210588: (Step 1) Determine  which record gets aputilct = 1  */
/* getting svcdat associated with minimum transaction date*/
%delview(snf_unique3)
proc sql _method;
create view snf_unique3 as
	select u2.membno, u2.admdat, u2.dschdat, u2.svcstat, u2.apmin
	       , min(u.svcdat) format mmddyy10. as svcdat
	from snf_unique2 u2 ,snf_unique1 u
		where  u2.membno=u.membno
		and u2.admdat=u.admdat
		and u2.apmin=u.apmin
	group by u2.membno, u2.admdat, u2.dschdat, u2.svcstat, u2.apmin
	;
quit;
%time


/* cw #210588:(Step 2) Determine  which record gets aputilct = 1  */
/* getting claim and line numbers for records with service date and min tranaction date*/
/* cw #245089: include PD line in APUTILCT assignment */
%delview(snf_unique4)
proc sql _method;
create view snf_unique4 as 
	select u.*, a.clamno, a.lineno, a.svcstat as svcstat_orig
	from snf_unique3 u  join snf a
		on a.membno=u.membno
		  and a.svcdat=u.svcdat
		  and a.aptrndat=u.apmin
		where u.svcstat = 'PA'
	order by membno, admdat, dschdat, svcstat, svcstat_orig
;
quit;
%time

/* cw #210588: (Step 3) Determine  which record gets aputilct = 1                 */
/*taking first line for each admission to prevent duplicate counting*/
%delview(snf_unique5)
data snf_unique5 ;
	set snf_unique4;
	by membno admdat dschdat;
	if first.admdat;	
run;
%time

/* cw #210588: Assign aputilct =1 for original file based on admission*/
/* cw #245089: sorting and pulling calculated admdat and discharge date are moved to next step */
%delview(snf_util)
proc sql _method;
create table snf_util as 
select a.*,case when u.admdat is not null then 1
	            else 0 
	        end as aputilct
          ,case when a.svcdat=a.enddat and abs(unitct)>1 then a.enddat+(abs(a.unitct)-1)
	            else a.enddat
            end as enddat_calculated format mmddyy10. 

from snf a left join snf_unique5 u
	on a.clamno=u.clamno
	and a.lineno=u.lineno	
order by membno, svcdat, enddat
;
quit;
%time

/* cw #245089: pulling calculated admdat and discharge date */
%delview(snf_util_covdys_0)
proc sql _method;
create view snf_util_covdys_0 as 
select
case when a.enddat_calculated ne u.dschdat_calculated                                     then sum(a.enddat_calculated, - a.svcdat, 1)
     when a.enddat_calculated =  u.dschdat_calculated and a.enddat_calculated ne a.svcdat then sum(a.enddat_calculated, - a.svcdat) 
     when a.enddat_calculated =  u.dschdat_calculated and a.enddat_calculated =  a.svcdat then 1
     else .
end as apcovdys_temp
,u.admdat_calculated
,u.dschdat_calculated
,u.apmin
,a.*
from snf_util a left join snf_unique5(rename=(admdat= admdat_calculated
		                                      dschdat=dschdat_calculated)) u
	on a.membno=u.membno
	and  u.admdat_calculated<=a.svcdat<=u.dschdat_calculated 
    and (a.enddat_calculated<=u.dschdat_calculated or u.dschdat_calculated=.)
order by clamno, lineno, dschdat_calculated
;
quit;
%time

/* cw #245089: get the greatest discharge date in case there is overlapping between re-calculated admdat and dschdat*/
%delview(snf_util_covdys_1)
data snf_util_covdys_1;
set  snf_util_covdys_0;
by clamno lineno dschdat_calculated;
if last.lineno;
run;
%time

/*  cw #210588: APCOVDSY calculation For SNF   */

/*****************************************************************************************************************/
/* cw #245089: re-calculated apcovdys for SNF                                                       -----START--- */
/*****************************************************************************************************************/

/*1.	Identify PA records from admits table that are contiguous records*/
%delview(cc)
proc sql _method;
create view cc as
	select distinct a.membno, a.svcdat, a.enddat
	from snf_admits a 
	 where a.svcstat='PA'                                                                /* RKS  change svcstat2 to svcstat */   
;
quit;

/*2.	Join the contiguous PA records to original analysis file to get claim number and AP Transaction date*/
%delview(cc2)
proc sql _method;
create view  cc2 as
	select c.*, a.clamno, a.lineno, a.aptrndat
	from cc c left join snf_util_covdys_1 a
	     on c.membno= a.membno
	    and c.svcdat=a.svcdat
		where a.aputilct ne 1 
          and a.svcstat='PA'  
	order by membno, svcdat, enddat, aptrndat, clamno
;
quit;
 %time

/*3.	Dedupe */
 %delview(cc2a)
data cc2a  ;
	set cc2;
	by membno svcdat;
	if first.svcdat;
run;
%time

/*4.	Join the records to themselves to see if there are any records that overlap */
 %delview(cc2b)
proc sql _method;
create view cc2b as
	select c.membno, c.svcdat, c.enddat, c.clamno, c.lineno
          , c2.svcdat as svcdat2
          , c2.enddat as enddat2
		  , c2.clamno as clam
		  , c2.lineno as line
	from cc2a c join cc2a c2
	  on c.membno=c2.membno
     and (   (c2.svcdat>=c.svcdat and c2.svcdat<c.enddat)
          or (c2.enddat>c.svcdat and c2.enddat<c.enddat) )
     and c2.clamno || c2.lineno ne c.clamno || c.lineno
     and c.enddat-c.svcdat    >  c2.enddat-c2.svcdat
;
quit;
 %time 


/*5.	Delete overlapping records  (1) */
  %delview(cc2a2)
proc sql _method;
create table cc2a2 as
	select c.*
	from cc2a c left join cc2b c2
	     on c2.clam=c.clamno
	    and c2.line=c.lineno
	where c2.clam=''
	;
quit;
%time

/*6.	Determine if remaining records overlap with records that have APUTILCT=1*/

 %delview(cc2c)
proc sql _method;
create table cc2c as
select c.membno, c.svcdat, c.enddat, c.clamno, c.lineno
, a.svcdat AS svcdat2
, a.enddat as enddat2
, a.unitct
, case when a.enddat=a.svcdat then intnx("day",a.enddat, a.unitct)
       else                        sum(a.enddat,1) 
  end as enddat2_recalc format=mmddyy10.
from cc2a2 c join snf_util_covdys_1 a
     on c.membno=a.membno
     and (    a.svcdat between c.svcdat and c.enddat
           or a.enddat between c.svcdat and c.enddat)
     and c.clamno || c.lineno ne a.clamno || a.lineno
where a.aputilct=1
and (   (a.enddat ne a.svcdat and a.enddat ne c.svcdat)
     or (a.enddat=a.svcdat    and a.svcdat =  c.svcdat))
;
quit;
 %time

 %delview(cc2c2)
proc sql _method;
create table cc2c2 as
	select c.* 
	from cc2c c
	where svcdat>= svcdat2 
	  and enddat<= enddat2_recalc
	;
quit;
 %time

/*7.	Delete overlapping records*/
 %delview(cc3)
proc sql _method;
create table cc3 as
	select c.*, 'Y' as cal_apcovdys_0 length=1
	from cc2a2 c left join cc2c2 c2
	       on c2.clamno=c.clamno
	      and c2.lineno=c.lineno
	where c2.clamno=''
	;
quit;
 %time

%delview(snf_util_covdys)
proc sql _method;                                 /* change case from a.svccod in ('191', '192', '193', '194', '120')  */
                                                  /* a.revcod in ('191', '192', '193', '194', '120')     rks           */
create table snf_util_covdys as
select a.*, b.cal_apcovdys_0
, case when a.aputilct=1  then a.apcovdys_temp

       when  b.cal_apcovdys_0='Y' 
         and a.svcstat = 'PA' 
         and a.svccod < '200'
         and a.formcd='U' 
         and a.svctyp='RV' then a.apcovdys_temp

	   when  b.cal_apcovdys_0='Y' 
         and a.svcstat = 'PA' 
         and a.revcod < '200' 
            then a.apcovdys_temp
	   else .
  end as apcovdys 
from snf_util_covdys_1 a left join  cc3 b
	     on a.clamno=b.clamno
	    and a.lineno=b.lineno
;
quit;

/*****************************************************************************************************************/
/* cw #245089: re-calculated apcovdys for SNF                                                       -----END---  */
/*****************************************************************************************************************/

/*  cw #245089: move sort here */
/*proc sort nodupkey data=snf_util_covdys ;*/
/*by clamno lineno;*/
/*run;*/
/*%time*/

/* cw #210588: clean space */
%delview(snf)
%delview(snf_admits)
%delview(snf_unique1)
%delview(snf_unique2)
%delview(snf_unique3)
%delview(snf_unique4)
%delview(snf_unique5)
%delview(snf_util)
%delview(snf_util_covdys_0)
 %delview(cc)
 %delview(cc2)
 %delview(cc2a)
 %delview(cc2b)
 %delview(cc2a2)
 %delview(cc2c)
 %delview(cc2c2)
 %delview(cc3)



/*****************************************************************************/
/*********                       IP                                  ********/
/*****************************************************************************/

%delview(ip_min)
proc sql _method;
create view ip_min as 
	select a.*
			,case when a.svctyp in ('DG','RV') then svctyp /*Force DG and RV to top of sort*/
			      else 'ZZ' 
	         end as typsort length=2
			,b.clmstat
		    ,b.apmin
		    ,b.disdatmax
	from ip a 
	join (select membno,mosdat
	          /*admissions with min(svcstat) = 'PD' are complete denials*/
			  ,min(svcstat)as clmstat  

              /*the earliest date on which a claim was paid - - including reversals*/ 
			  ,min(case when revind = 'Y' and revsto ne '' then aptrndat
			  		    when svcstat='PA' then aptrndat 
						else .
					end) format mmddyy10. as apmin   

  			  /*Max discharge date for apcovdys calculation*//*Max discharge date for apcovdys calculation*/ 
			  ,max(case when revind = 'Y' and revsto ne '' then disdat
		  		        when svcstat='PA' then disdat 
					    else .
				   end) format mmddyy10. as disdatmax  
 
		  from ip	  
		   /*unique admissions are identified using only membno and mosdat across all records*/	
		  group by membno, mosdat
	     ) b
	on  a.membno=b.membno
	and a.mosdat=b.mosdat
	/*sort so that earliest transactions, PA records, reversals come to the top*/
	order by membno, mosdat, aptrndat, svcstat,  revind descending, typsort, lineno,clamno
;
quit;
%time


/* cw #210588: Assign aputilct and  Calculate apcovdys */
%delview(ip_util)	
data ip_util ;

	set ip_min;
	by membno mosdat aptrndat  ;
	aputilct=0;	
	apcovdys=.;

	if first.mosdat=1 and clmstat='PA' and aptrndat=apmin and (svcstat='PA' or 
                                                              (svcstat='PD' and revind='Y' and revsto ne '')) then 
		aputilct=1;
	if first.mosdat=0                  and aptrndat=apmin and first.aptrndat=1 and svcstat='PA' and typsort ne 'ZZ' then 
		aputilct=1;
	if first.mosdat=0 and clmstat='PA' and aptrndat=apmin and first.aptrndat=1 and svcstat='PD' and revind='Y' and revsto ne ''  then 
		aputilct=1;

	if aputilct=1 and ( svcstat = 'PA' or (svcstat='PD' and revind = 'Y' and revsto ne '' )) then do;
		if disdatmax ne admdat then apcovdys= sum(disdatmax, - admdat);
                       else apcovdys=1;	
	
	end;
run;
%time

/* cw #210588: clean space */
%delview(ip)
%delview(ip_min)

/*****************************************************************************************/
/*********    mcorcat ='OPTRNE' - Non-emergency transportation claims             ********/
/*****************************************************************************************/
%delview(trne_min)
proc sql _method;
create view trne_min as 
	select a.*, b.clmstat, b.apmin
	    from trne a 
	join (select extclm, membno, enddat
                , min(svcstat)as clmstat     /* groups with min(svcstat) = 'PD' are complete denials */
			   	, min(case when svcstat='PD' and revind = 'Y' and revsto ne '' then aptrndat
			  		       when svcstat='PA' then aptrndat 
						   else .
					  end) format mmddyy10. as apmin
		  from trne
		  group by extclm, membno, enddat    /* unique services identified by using extclm, membno, enddat across records*/
         ) b
		on  a.extclm=b.extclm
		and a.membno=b.membno
		and a.enddat=b.enddat
	order by extclm, membno, enddat, aptrndat, svcstat, revind descending, lineno
;
quit;
%time

%delview(trne_util)
data trne_util  ;
	set  trne_min;
	by extclm membno enddat aptrndat;
	aputilct=0;
	if first.enddat=1 and clmstat='PA' and aptrndat=apmin and (svcstat='PA' or 
           													  (svcstat='PD' and revind='Y' and revsto ne '')) then
		aputilct=1;;
	if first.enddat=0                  and first.aptrndat=1 and aptrndat=apmin and svcstat='PA'  then 
		aputilct=1;
	if first.enddat=0 and clmstat='PA' and first.aptrndat=1 and aptrndat=apmin and svcstat='PD' and revind='Y' and revsto ne ''  then 
		aputilct=1;
run;

%time

/* cw #210588: clean space */
%delview(trne)
%delview(trne_min)

/*****************************************************************************************/
/*********        mcorcat ='OPTRER' - Emergency transportation claims             ********/
/*****************************************************************************************/
%delview(trer_min)
proc sql _method;
create view trer_min as 
	select a.*,b.clmstat,b.apmin

	from trer a 
	join (select extclm, membno, mosdat, svccod 
                 , min(svcstat) as clmstat
			     , min(case when svcstat='PD' and revind = 'Y' and revsto ne '' then aptrndat
				  		    when svcstat='PA' then aptrndat 
							else . 
						end) format mmddyy10. as apmin 
			  from trer			
			  group by extclm, membno, mosdat, svccod
		 ) b
		on  a.extclm=b.extclm
		and a.membno=b.membno
		and a.mosdat=b.mosdat
		and a.svccod=b.svccod
	order by extclm, membno, mosdat, svccod, aptrndat,  svcstat,  revind descending, lineno
;
quit;
%time

%delview(trer_util)
data trer_util;
	set trer_min;
	by extclm membno mosdat svccod aptrndat;
	aputilct=0;
	/* do not assign utilization to claim lines with A0425 HCPS code */
	if first.mosdat=1 and svccod ne 'A0425' and clmstat='PA' and aptrndat=apmin and (svcstat='PA' or 
																					(svcstat='PD' and revind='Y' and revsto ne '')) then 
		aputilct=1;
	if first.mosdat=0 and svccod ne 'A0425'                  and first.aptrndat=1 and aptrndat=apmin and svcstat='PA'  then
		aputilct=1;
	if first.mosdat=0 and svccod ne 'A0425' and clmstat='PA' and first.aptrndat=1 and aptrndat=apmin  and svcstat='PD' and revind='Y' and revsto ne '' then 
		aputilct=1;
run;
%time
/* cw #210588: clean space */
%delview(trer)
%delview(trer_min)
/*****************************************************************************/
/********* OP  All other outpatient and professional claims           ********/
/*****************************************************************************/
%delview(op_min)	
proc sql _method;
create view op_min as
	select  a.*
	,b.clmstat /* PA - payment for group of records; PD - complete denial */
	,b.apmin   /* earliest date of payment for group of records including reversals */
	,b.cnt     /* number of PA (paid lines in group of records i.e. total utilization to be assigned */
	,b.pd_cnt  /* number of reversals */
	from op a 
	join (select extclm, membno, mosdat, svccod 					
				  ,min(svcstat) as clmstat 
				  ,min(case when svcstat='PD' and revind = 'Y' and revsto ne '' then aptrndat
				  		    when svcstat='PA' then aptrndat 
							else . 
						end) format mmddyy10. as apmin
				  ,sum(case when svcstat='PA' then 1 
		                    else 0 
		               end) as cnt       /*# of PA lines in group */
				  ,sum(case when svcstat='PD' and revind='Y' and revsto ne '' then 1 
							else 0 
					   end) as pd_cnt
			  from op
			  /*unique groups are identified identified by using external claim id, membno and mosdat and svccod across records*/
			  group by extclm, membno, mosdat, svccod
         ) b
		on a.extclm=b.extclm
		and a.membno=b.membno
		and a.mosdat=b.mosdat
		and a.svccod=b.svccod
;
quit;
%time

%delview(op_pas)
%delview(op_pds)	
%delview(op_util)
data op_pas(keep=extclm membno clamno lineno mosdat svccod aptrndat pd_cnt cnt )
	 op_pds(keep=extclm membno clamno lineno mosdat svccod aptrndat pd_cnt)
	 op_util;
	set op_min;

	aputilct=0;

	/*assign aputilct=1 to lines with PA svcstat and no reversed lines in the group of records to which it belongs*/
	if svcstat='PA' and pd_cnt = 0 then aputilct=1;
	
	/*create table of PA records that belong to record groups with reversals*/
	else if  svcstat='PA' and pd_cnt ne 0 then 
		output op_pas;

	/*Create table of PD reversal records that belong to group with at least one PA record*/
	else if  svcstat='PD' and pd_cnt ne 0 and revind='Y' and revsto ne '' and clmstat = 'PA'  then 
		output op_pds;

	/* keep all records in op_util. They will be updated for reversal */
	output op_util;
run;
%time


%delview(op_pas_n)
/*cw #210588: Number the rows in each group of records in the pas table*/
proc sql _method;
create view op_pas_n as 
	select m.extclm, m.membno,m.mosdat, m.svccod, m.aptrndat, m.cnt,    
		   m.clamno, m.lineno, count(*) as row
	  from op_pas m join op_pas m2
		on  m.extclm=m2.extclm
		and m.membno=m2.membno
		and m.mosdat=m2.mosdat
		and m.svccod=m2.svccod
		and ((m.clamno=m2.clamno    and m.lineno>=m2.lineno) or 
		     (m.clamno>m2.clamno    and m.aptrndat=m2.aptrndat) or 
		     (m.clamno ne m2.clamno and m.aptrndat>m2.aptrndat))	
	group by m.extclm, m.membno, m.mosdat, m.svccod, m.aptrndat, m.cnt, 
            m.clamno, m.lineno  
;
quit;
%time


%delview(op_pds_n)
/* cw #210588: Number the rows in each group of records in the pds table*/
proc sql _method;
create view op_pds_n as 
	select m.extclm, m.membno,m.mosdat, m.svccod, m.aptrndat, m.pd_cnt, 
		   m.clamno, m.lineno, count(*) as row
	from op_pds m join op_pds m2
		on  m.extclm=m2.extclm
		and m.membno=m2.membno
		and m.mosdat=m2.mosdat
		and m.svccod=m2.svccod
		and ((m.clamno=m2.clamno    and m.lineno>=m2.lineno) or  
			 (m.clamno>m2.clamno    and m.aptrndat=m2.aptrndat) or 
			 (m.clamno ne m2.clamno and m.aptrndat>m2.aptrndat))
	group by m.extclm, m.membno, m.mosdat, m.svccod, m.aptrndat, m.pd_cnt, 	
             m.clamno, m.lineno  
;
quit;
%time

%delview(op_joined)
/* cw #210588: Joint the records from pas table with associated records from pds table (i.e. join the PA and reversals from same record group)*/
/*where the transaction date on the PA record is greater than the tranaction date on the PD record*/
/*idenify claim and line number from the PA and PD record*/
proc sql _method;
create view op_joined as 
	select pa.*, pd.aptrndat as pd_apdt format=mmddyy10., 
				 pd.clamno   as pd_clam length=13, 
				 pd.lineno   as pd_line length=3, 
				 pd.pd_cnt, 
                 pd.row as pd_row
	from op_pas_n pa left join op_pds_n pd
		on  pa.extclm=pd.extclm
		and pa.membno=pd.membno
		and pa.mosdat=pd.mosdat
		and pa.svccod=pd.svccod
		and pa.aptrndat>=pd.aptrndat
;
quit;
%time


%delview(op_joined_ct)
/* cw #210588: Determine which records (by claim and line number) in the op_util table will be update so aputilct=1*/
proc sql _method;
create table op_joined_ct   as 
	select extclm, membno, mosdat, svccod
			,case  when pd_cnt = .  then clamno
				   when pd_cnt ne . and row=pd_row then pd_clam
				   when pd_cnt<cnt  and row>pd_cnt then clamno
				   else 'XXXXXXXXXXXXX' 
			end as clamno length=13

			,case when pd_cnt= . then lineno 
				  when pd_cnt ne . and row=pd_row then pd_line
				  when pd_cnt<cnt  and row>pd_cnt then lineno
				  else 'XXX' 
			 end as lineno length=3

			,case when pd_cnt = .   then aptrndat
			      when pd_cnt ne .  and row=pd_row then pd_apdt
				  when pd_cnt<cnt   and row>pd_cnt then aptrndat
				  else . 
             end as aptrndat format=mmddyy10. 
	from op_joined 
	where   pd_cnt=. 
		or  pd_row=row
		or (pd_cnt<cnt 
			 and pd_row=1
			 and row>pd_cnt)
	order by clamno, lineno
;
quit;
%time

%delview(op_joined_ct_uniq)
proc sort nodupkey data=op_joined_ct out= op_joined_ct_uniq(keep=clamno lineno);
by clamno lineno;
run;
%time

%delview(op_util_uniq)
proc sort nodupkey data=op_util out= op_util_uniq;
by clamno lineno;
run;
%time

%delview(op_util_update)
data op_util_update;
	merge op_util_uniq (in=ina) op_joined_ct_uniq (in=inb);
	by clamno lineno;
	if ina and inb then aputilct=1;
	if ina;
run;
%time

/* cw #210588: clean space */
%delview(op)
%delview(op_min)
%delview(op_pas)
%delview(op_pds)	
%delview(op_util)
%delview(op_pas_n)
%delview(op_pds_n)
%delview(op_joined)
%delview(op_joined_ct)
%delview(op_joined_ct_uniq)
%delview(op_util_uniq)


/* /* cw #210588: combine data*/
%delview(aputilct)
proc sql _method;
create view aputilct as
	select a.clamno, a.lineno, a.aputilct,    a.apcovdys from snf_util_covdys a
		union
	select b.clamno, b.lineno, b.aputilct,    b.apcovdys from ip_util b
		union
	select c.clamno, c.lineno, c.aputilct, . as apcovdys from trne_util c
		union
	select d.clamno, d.lineno, d.aputilct, . as apcovdys from trer_util d
		union
	select e.clamno, e.lineno, e.aputilct, . as apcovdys from op_util_update e
	;
quit;

%delview(svclines_aputilct)
proc sql _method;
create table svclines_aputilct as
	select a.*, b.aputilct, b.apcovdys 
         from mcorumrfinal a left join aputilct b
		   on a.clamno=b.clamno
          and a.lineno=b.lineno
	;
quit;

/* cw #210588: clean space */
%delview(mcorumrfinal);

%delview(snf_util_covdys)
%delview(ip_util)	
%delview(trne_util);
%delview(trer_util);
%delview(op_util_update)

%delview(aputilct)

/*-----------------------------------------------------------------------------------------------------*/ 
/*-----------------   cw #210588: APutilct APcovdys Mapping end    ------------------------------------*/ 
/*-----------------------------------------------------------------------------------------------------*/ 




/*****************************************************************************************/
/* cw #160524: get hfatrisk from hospaff. To avoid error due to blank hospaff, check    */
/*             record first. If blank then avoid pulling hfatrisk from table            */
/*             hfatrisk from  hospaff table based on key field: prod prvorg mosdat      */
/*             hfatrisk='Y' by default.                                                  */
/* cw #210588 step for hfatrisk to here                                                  */
/*****************************************************************************************/

%let nobs=0;
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
%nobs(&libn1..hospaff)

%delview(svclines_hfatrisk);
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
%macro gethfatrisk; 
%if &nobs=0 %then %do;
	proc sql _method;
	  create view svclines_hfatrisk as                                                          
	   select *, 'Y' as  hfatrisk length=1
	   from svclines_aputilct 
	   ;      
	quit;
%end;
%else %do;	
	proc sql _method;
	  create view svclines_hfatrisk as                                                          
	   select a.*, 
				   case b.hfatrisk
					    when ' '  then 'Y'			                       
			            else      b.hfatrisk
			        end as hfatrisk length=1
	   from svclines_aputilct a 
	   left join &libn1..hospaff b
	      on a.prod=b.prod 
	      and a.prvorg=b.prvorg 
	      and b.expdat >=a.mosdat>=b.effdat;      
	quit;
%end;
%mend gethfatrisk; 
%gethfatrisk
%time;


/* cw #210588 In the end, 21 fields will be updated. Note: apsvcstat was added in DWA010
revcod 
svccod
mcorcat
origmcorcat
utilct
umrgrp
subumrgrp
umrcat
subumrcat
mosdat
mos
hfatrisk
covdys
clcovdys
perdiemrate
ipvalue
aputilct
apcovdys
dwloaddt
poolcat
subpool
*/

/* cw #245089 improve process. */
%delview(svclines0)	
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
proc sql _method;																			
  create table svclinesupdate as																	
    select  a.*, b.revcod, b.svccod, b.mcorcat, b.origmcorcat, b.utilct, b.umrgrp, b.subumrgrp, b.umrcat, b.subumrcat, b.mosdat, /* KS - 159267 - 08032010 */  /* cw #210588 */
			     b.mos, b.hfatrisk, b.covdys, b.clcovdys, b.perdiemrate, b.ipvalue, b.aputilct, b.apcovdys, today() as dwloaddt, b.parstat /*NN #461060  Fida included parstat*/  
/**B-190005  changed to rsstr from &libn1..***/	
	 from rsstr.svclines(where=(batdat>=&cutoffyr.)	/* HS 560569 DS changed the libnx to libn1 */							  /* KS - 159267 - 08032010 *//* cw #367813 */ 	  					   
                   drop =revcod svccod mcorcat origmcorcat utilct umrgrp subumrgrp umrcat subumrcat mosdat
						 mos hfatrisk covdys clcovdys perdiemrate ipvalue aputilct apcovdys dwloaddt parstat /*NN #461060  Fida included parstat*/
                           ) a 	 
     left join svclines_hfatrisk b															
	   on a.clamno=b.clamno and																
	      a.lineno=b.lineno		
		  ;
quit;
%nobs(svclinesupdate);									/* cw #245089 */
%let numsvcupd = &nobs;									/* cw #245089 */
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/* cw #245089 improve process. split data  in order to pull poolcat */
%macro poolcat;

%if &compno = 07 or &compno = 37 %then %do;
%delview(svclines1)																			/* KS - 159267 - 08032010 */
proc sql _method;																			/* KS - 159267 - 08032010 */
  create view svclines1 as																	/* KS - 159267 - 08032010 */
    select  a.*, c.poolcat as poolcat_c, c.subpool as subpool_c,                            /* KS - 159267 - 08032010 */  /* cw #210588 */
			     c.fullvalper, c.advalstart, c.advalend                                     /* KS - 159267 - 08032010 */
	 from svclinesupdate(  rename=(poolcat=poolcat_a subpool=subpool_a) ) a 	            /* KS - 159267 - 08032010 */
    left join &libn1..poolcategory c														/* KS - 159267 - 08032010 */
     on a.clatyp=c.clatyp																	/* KS - 159267 - 08032010 */
	     order by clamno, lineno, advalstart desc;											/* KS - 159267 - 08032010 */	
quit;																						/* KS - 159267 - 08032010 */
%time;																						/* KS - 159267 - 08032010 */

%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/* Obtain necessary formulas */
	data _null_;																			/* KS - 159267 - 08032010 */
 	set &libn1..poolcategory;																/* KS - 159267 - 08032010 */
	if _N_ = 1 then do;																		/* KS - 159267 - 08032010 */
		call symput('fullvalper', put(fullvalper,3.0));										/* KS - 159267 - 08032010 */
		call symput('numerator', numerator);												/* KS - 159267 - 08032010 */
		call symput('denominator', denominator);											/* KS - 159267 - 08032010 */
		stop;																				/* KS - 159267 - 08032010 */
		end;																				/* KS - 159267 - 08032010 */
	 run;																					/* KS - 159267 - 08032010 */

%delview(svclines2)	
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/																	/* KS - 159267 - 08032010 */
	/*Write out the correct updated SVCLINES*/
	data svclines2(rename=(subpool_c=subpool poolcat_c=poolcat)) / view=svclines2;	/* cw #210588  */
		set svclines1;																				/* KS - 159267 - 08032010 */															
		by clamno lineno;																			/* KS - 159267 - 08032010 */
        length outind $1.;																			/* KS - 159267 - 08032010 */														
		retain outind;																				/* KS - 159267 - 08032010 */		

		if _N_ = 1 then outind='N';																	/* KS - 159267 - 08032010 */											
		if first.lineno then outind='N';															/* KS - 159267 - 08032010 */										

		num = &numerator;																			/* KS - 159267 - 08032010 */
		den = &denominator;																			/* KS - 159267 - 08032010 */
		full = &fullvalper;																			/* KS - 159267 - 08032010 */ 

		if den = 0 then abs=0;																		/* KS - 159267 - 08032010 */
		     else abs = num * full / den;															/* KS - 159267 - 08032010 */ 

		if last.lineno and outind='N' then do;														/* KS - 159267 - 08032010 */									
			output;																					/* KS - 159267 - 08032010 */																
			return;																					/* KS - 159267 - 08032010 */																
			end;																					/* KS - 159267 - 08032010 */																
		if last.lineno and outind='Y' then return;													/* KS - 159267 - 08032010 */								

		if advalstart <= abs <=advalend then do;													/* KS - 159267 - 08032010 */								
		    output;																					/* KS - 159267 - 08032010 */																
			outind='Y';																				/* KS - 159267 - 08032010 */															
			end;																					/* KS - 159267 - 08032010 */																
		drop outind num den full abs advalstart advalend  fullvalper poolcat_a subpool_a  ;	        /* KS - 159267 - 08032010 */ /* cw 3210588 */
      																		
	run;																					    	/* KS - 159267 - 08032010 */																													

%end;    /* End with  %if &compno = 07 or &compno = 37 %then %do;*/                                /* KS - 159267 - 08032010 */
%else %do;																					       /* KS - 159267 - 08032010 */

data poolm poolb;
set &libn1..poolcategory;
if mcorcat ne ' ' then output poolm;
else output poolb;
run;

proc sort data=poolb nodupkey ;
	by effdat expdat bencod  formcd prod subprod;
run;

proc sql;
	select max(expdat) into: poolmaxexpold  from poolb;
quit;

proc sort data=poolm nodupkey ;
	by effdat expdat mcorcat prod subprod;
run;

%put &poolmaxexpold;

%delview(svclines2)
 proc sql _method;
  create table svclines2 as  
    select a.*, coalesce(c.poolcat,a.poolcat_a) as poolcat, coalesce(c.subpool,a.subpool_a) as subpool 
	 from svclinesupdate(where=( enddat<=&poolmaxexpold.) 
	                       rename=(poolcat=poolcat_a subpool=subpool_a) 	                       	
						  ) a 	left join poolb c
     on (a.bencod ne ''      and
	     a.formcd ne ''      and
         a.bencod =c.bencod  and
         a.formcd =c.formcd  and
         a.prod   =c.prod    and
         a.subprod=c.subprod and
        ((c.effdat le a.enddat le c.expdat) or
         (c.effdat le a.enddat and c.expdat=.)))

union all

  select a.*,  coalesce(b.poolcat,a.poolcat_a) as poolcat, coalesce(b.subpool,a.subpool_a) as subpool         
	from svclinesupdate(where=( enddat>&poolmaxexpold.) 
	                       rename=(poolcat=poolcat_a subpool=subpool_a) 	                       	
						  ) a   left join poolm b
     on (a.mcorcat ne ''     and
         a.mcorcat=b.mcorcat and
         a.prod   =b.prod    and
         a.subprod=b.subprod and
        ((b.effdat le a.enddat le b.expdat) or
         (b.effdat le a.enddat and b.expdat=.)))
   order by clamno, lineno;

quit;
%time;

%end;

%mend poolcat;
%poolcat
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/* #210588 utilize field atributes defined in DWA010 for e.g length, format, label */
data dummy_svclines(keep=&svclines_all_f_d);
/**B-190005  changed to rsstr from &libn1..***/
set rsstr.svclines(obs=0); /* HS 560569 DS changed the libnx to libn1 */
run;

/*%createindex("svclines");   /* HS 560569 removed the index*/                        /* KM #364804 */*/;
%time;


/* Set Service Lines to avoid SAS9 Warning*/
/* #210588 Note poolcat_a subpool_a are extra fields in svclines2 from NY lob. add keep statement */
/*%delview(svclines3);									 KS - 156845 */
/*data svclines3 / view=svclines3;						*/
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/* SAS2AWS2: ReplacedSASDatasetLibNameWithWork */
data work.svclines_new (/*compress = yes  */
						    ); /* KM #364804 */ /* HS 560569 removed the index*/ 
  set dummy_svclines 
/**B-190005  changed to rsstr from &libn1..***/     
      rsstr.svclines(where=(batdat< &cutoffyr.) ) /* cw #367813 */ /* HS 560569 DS changed the libnx to libn1 */
	  svclines2(keep=&svclines_all_f_d)
	  ;
  by clamno lineno;   /* cw #245089 Interleave records, no sort required */ 
run;
/* SAS2AWS2: WriteDataToRedshift - Remove Duplicates (Verify and Replace the placeholder <> with the unique index field if needed) */
/**B-190005 moved to rsstr instead of work**/
%if &syscc<&SYSCC_DWC32101. %then %do; /***Start of IF AG1***/
proc sort data=svclines_new out=rsstr.svclines_new_nodup dupout=svclines_new_dup nodupkey;
	by clamno lineno;
run;
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/**B-190005  commented out redshift load and moved to rsstr SAS library in above SORT***/
/*%sas2rs_dataload_bul(loadtech=delete,srclib=work,srctbl=svclines_new_nodup,trgtlib=&libn1.,trgttbl=svclines);*/

%time;

/*proc sort in=svclines3 out=&libn1..svclines_new (compress = yes);		*/
/*by clamno lineno;									*/
/*run;												*/
/*%time;*/
/*B-172922 - rename svclines_new to svclines */
%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries - Mohan*/
/**B-190005  changed to rsstr from &libn1..***/
%nobs(rsstr.svclines);	/* HS 560569 DS changed the libnx to libn1 *//* KS - 156845 */
%let numrecs = &nobs;									/* KS - 156845 */

/**B-190005 uncommented some old code and changed svclines_new to svclines_new_nodup due to views are not leeting overwrite table**/
proc datasets lib=rsstr nolist; /* HS 560569 DS changed the libnx to libn1 */
     title1 "&rp_title";
     title2 'Claim Service Lines';
     /*B-172922 - commented delete & change statement */
	 delete svclines;									
	 change  svclines_new_nodup = svclines;					
/* SAS2AWS2: CommentedModify */
/*	 modify svclines(label="Claim Service Lines (&libn1.)") ;		*//* KS - 156845 */
/*      contents data=svclines; */
quit;

/*B-172922 - commented lock macro*/
/*lock &libn1..svclines clear;*/ /* HS 560569 DS changed the libnx to libn1 */

 
/* Create Summary Totals */
%delview(summary);
data summary;											/* KS - 156845 */
	recsin ="&numrecs";									/* KS - 156845 */
	recsout="&numsvcupd";								/* KS - 156845 */
    mhsrecs  = &mhsrecs.  ;  					        /*************     RKS - 382910   ****************/
    cdcrecs  = &cdcrecs.  ;					            /*************     RKS - 382910   ****************/
    apndrecs = &mhsrecs.  ; 					        /*************     RKS - 382910   ****************/
    totrecs  = &numrecs.  ; 					        /*************     RKS - 382910   ****************/
	delrecs  = &delexprecs.; 					        /*************     RKS - 382910   ****************/
run;



%global datein nobs ;				/* KS - 156845 */

/* Capture the current date */
data _null_;									/* KS - 156845 */																								
call symput('datein',put(today(),mmddyyd10.));	/* KS - 156845 */	  
run;											/* KS - 156845 */


%let stateind = ALL;									/* KS - 156845 */
%stateind
%macro summary;											/* KS - 156845 */
											
%if  %sysfunc(exist(summary)) =1    %then %do; /* cw #210588 */
%let nobs=0;
%nobs(summary)
	%if &nobs >0 %then %do;

	/* SAS2AWS2: ReplacedSlash */
	%pdf_head(1,n,9,&drv7./DWA32101/current,DWC32101_%cmpres(&compno)_00_A);	/* KS - 156845 */

	/* Generate the EDR report *//*7434-NN Updated labels from SVCHOLDS to SVCLINES*/
	proc print data = summary noobs label split = '%';		/* KS - 156845 */			  			 
	  	var recsin  recsout mhsrecs cdcrecs apndrecs totrecs delrecs;	/* KS - 156845 */							
	    label												/* KS - 156845 */ 
		      recsin='Total Number of%Recs Read in from SVCLINES'	/* KS - 156845 */
	          recsout='Total Number of%Updated SVCLINES'				/* KS - 156845 */
			  mhsrecs='Total Number of%Recs Expected%from MHS'              /*************     RKS - 382910   ****************/
              cdcrecs='Total Number of%Recs from%CDC Input File'            /*************     RKS - 382910   ****************/
              apndrecs='Total Number of%Recs appended%To SVCLINES'          /*************     RKS - 382910   ****************/
              totrecs='Total Number of%Recs on the%SVCLINES Table'          /*************     RKS - 382910   ****************/
              delrecs='Total Number of%Recs deleted on the%SVCLINES Table'  /*************     RKS - 382910   ****************/
		  ;												        /* KS - 156845 */	
	    
		title1 "Healthfirst Inc.";								/* KS - 156845 */	
	    title2 "Summary Report for SVCLINES Table";				/* KS - 156845 */
		title3 "(Program ID: DWC32101.SAS)";					/* KS - 156845 */	
	    title4 "Load process date: &datein";					/* KS - 156845 */						 	
		
		footnote "Report: DWC32101_%cmpres(&compno)_00_A";		/* KS - 156845 */						
	    		
	run;															/* KS - 156845 */																																

	 options byline;											/* KS - 156845 */																										
	 %pdf_tail();												/* KS - 156845 */
   %end; 
%end; 
%mend summary;												/* KS - 156845 */ 
%summary 													/* KS - 156845 */


%time(timeparm=e);												
%timestamp;

/* clear both table and view in work */
proc sql;
create table list_data as
select memname,memtype
from dictionary.members
where libname = "WORK" 
 and memtype in ("DATA")
;

create table list_view as
select memname,memtype 
from dictionary.members
where libname = "WORK" 
 and  memtype in ("VIEW") 
;
quit;





proc sql noprint;
	select count(*) into :ct_list_data from list_data ;
	select count(*) into :ct_list_view from list_view ;
quit;

%macro clearwrkall;
   %if &ct_list_data > 0 %then
   %do;
    proc datasets 
       			library = work
        		memtype = data
      			KILL;
       run;
     quit;
   %end;
   %if &ct_list_view > 0 %then
   %do;
    	proc datasets 
       			library = work
        		memtype = view
      			KILL;
       run;
       quit;
   %end;
%mend clearwrkall;
%clearwrkall; 



%initvar;
quit;
%end; /**End of IF AG1***/
%else %do;/**Start of ELSE AG1***/
%put "DWAC32101 ERROR in loading SVCLINES dataset in SAS RS Storage";
%macro abort_program32101;
  %ABORT RETURN 3000;
%mend abort_program32101;
%abort_program32101;
%end; /**End of ELSE AG1***/

/**B-190005 - Kasi Commented CLAIMS3YR data load in DWC32101 program ----------> START */


/**B-190005 added claims3yr upload from rsstr instead of work along with reconnect*/
/**B-190005 added error check on bulk load macro and used new macro instead of old*/
/* %include "&drv/AUTOEXEC/reconnect.sas";  */
/* %let claims3yr=0; */
/* %nobs(rsstr.claims3yr); */
/* %let claims3yr=&nobs; */
/* *B-190005 shifted claims3yr load only if there are rows in the SAS dataset claims3yr and added parition load*** */
/* %if  &syscc<&SYSCC_DWC32101. and &claims3yr gt 0  %then %do; */
  /*%sas2rs_dataload_lrgfiles_bul(loadtech=delete,srclib=rsstr,srctbl=claims3yr,trgtlib=&libn1.,trgttbl=claims3yr);*/
/*   %load_sas2rs_hugefiles_as_parts(parts=&CLAIMS3YR_PARTS.,library=&libn1,table=claims3yr,saslib=rsstr,sasdataset=claims3yr); */
/* %end; */
/* %else %do; */
/*   %put "DWC32101 ERROR CLAIMS3YR SAS RS Storage data not loaded so please verify logs"; */
/* %macro abort_program32101s; */
/*   %ABORT RETURN 3000; */
/* %mend abort_program32101s; */
/* %abort_program32101s; */
/* %end; */
/* %time; */
/*  */
/* %nobs(&libn1..claims3yr); */
/* %let claims3yr_rs=&nobs; */
/* %if &overall_rs_load_status eq 0 and (&claims3yr eq &claims3yr_rs) and (&claims3yr gt 0 and &claims3yr_rs gt 0) %then %do; */
/*   %put "DWC32101 CLAIMS3YR RS DB data loaded successfully"; */
/* %end; */
/* %else %do; */
/*   %put "DWC32101 ERROR CLAIMS3YR data not loaded into RS DB so please verify logs"; */
/* %macro abort_program32101c; */
/*   %ABORT RETURN 3000; */
/* %mend abort_program32101c; */
/* %abort_program32101c; */
/*  */
/* %end; */
/* %include "&drv/AUTOEXEC/reconnect.sas";  */;

/**B-190005 - Kasi Commented CLAIMS3YR data load in DWC32101 program ----------> END */

%put "DWC32101 &=syscc. &syserr. ";

%time(timeparm=s); /* VVKR Added -- B-75041 */

data _null_;
call symputx('compno', symget('compno'));
run; /* Please Cheack*/
/*B-244088 Starts here*/
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/RGA191_jobmaster_extn.txt  &drv2./Encounters_Reporting/APD/Control_Table/  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

filename filere "&drv2./Encounters_Reporting/APD/Control_Table/RGA191_jobmaster_extn.txt";
        data extn;    
            infile filere truncover firstobs =1 obs=1;                                                                                                       
            input extn_type $4.;
            call symput('extn_type',extn_type);    
        run;
/*B-244088 Ends here*/
/* Rsubmit; */
%let masking=N;
/* Endrsubmit; */

%let temp_ds_del_flag = N;  
/*%let masking=N;*/ /* HS 542635 - commented */
%let jobname = 'RGA191'; /* HS 542635 - commented */
%macro send_mail;

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/Mailing_List.txt  &drv2./Encounters_Reporting/APD/Control_Table/  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

data Mailing(keep = mail);
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	infile "&drv2./Encounters_Reporting/APD/Control_Table/Mailing_List.txt"  firstobs=2;
	length mail1 $32.;
	input Mail1: $32.;
	/* SAS2AWS2: ReplacedFunctionCompress */
	Mail= kcompress(mail1);

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

run;
proc sql;
	/* SAS2AWS2: ReplacedFunctionCompress */
	select quote(kcompress(mail)) into: Emailid separated by " "
	from mailing;
quit;
Data _null_;
	set mail_file;
	call symput("status",status);
run;
%if &status.=PROCESSED %then %do;
	%let subj = %str(/*APD*/&lib. Encounter Process &run_id. completed successfully); /* HS# 542635 */
%end;
%else %if &status.=ERROR  %then %do;
    %let subj = %str(/*APD*/&lib. Encounter Process &run_id. completed with error); /* HS# 542635 */
%end;
%else %if &status.=Invalid  %then %do;
	%let subj = %str(/*APD*/&lib. Encounter Process was not processed Invalid file); /* HS# 542635 */
%end;
%put &subj.;
/* Sending mail to the list managers from the control table */
FileName SENDMAIL email  To=(&emailid) Subject="&subj.";
data _null_;
			file  SENDMAIL lrecl=200;
			set mail_file;
			put " ";
			put " ";
			%if &status.=Invalid  %then %do;
			Put "File Name" "  " ":" " " " &fname."; 
			Put "Run  ID"  "    "  "   "  " :" " "  "  ";
			%end;
			%else %do;		
			Put "File Name" "  " ":" " " " &fname..txt"; 	
			put "Run  ID"  "    "  "   "  " :" " "  " &run_id."; 
			%end;
            put "Status "  "    "  "   "  "  :" " "   STATUS; 		
			put " ";
			put " ";
run;
%if &status.=ERROR or &status.=Invalid  %then %abort ;/* HS 569650 KV */
%mend send_mail;

x "rm -f &drv2./Encounters_Reporting/APD/Import/*.*";

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/medical_job_mstr.sas7bdat &drv2./Encounters_Reporting/APD/Control_Table/     --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;
cmd=" aws s3 mv &drv72./Encounters_Reporting/APD/Import &drv2./Encounters_Reporting/APD/Import/  --recursive --include '*.TXT' --exclude 'Trigger/*' --exclude 'Release/*' --exclude 'Error/*' --exclude 'Archive/*' --sse --acl bucket-owner-full-control";
call system(cmd);
run;

data _null_;/*B-244088 Extension Change*/
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Report/Medical_Job_Mstr_Report.&extn_type. &drv2./Encounters_Reporting/APD/Control_Report/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;

/*ALM# 8377 kv - added logic to split SHPCAID files based on service date ----> start */
%macro shpcaid_filesplit;
/* Reading the input file names from the Import Folder */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
%let dirname = &drv2./Encounters_Reporting/APD/Import;

/* SAS2AWS2: ReplacedSlash-UpdatedOSCommands */
/* filename DIRLIST pipe "ls  &dirname/*.TXT"; */
filename DIRLIST pipe "ls  &dirname/*.TXT | sed 's:.*/::'";

Data  shpcaid_validfile;
     length fname $256  filename_start compno $2.;
     infile dirlist length=reclen ;
     input fname $varying256. reclen ;	 
	 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
	 filename_start= kcompress((kupcase(kscan(fname,1,'_'))));
	 /* SAS2AWS2: ReplacedFunctionScan-ReplacedFunctionUpcase */
	 compno=	kcompress((kupcase(kscan(fname,2,'_'))));
     
	if compno = "02" then output shpcaid_validfile;
  			
Run;
%nobs(shpcaid_validfile);
%if &nobs.= 0 %then %do;

%end;
%else %do;
	/* valid file processing */
	
	Proc sql noprint;
		select count(fname) into :count_valid from shpcaid_validfile;
	quit;
	%put &count_valid.;
	%let count_valid=&count_valid.;
	Proc sql noprint;
	   /* SAS2AWS2: ReplacedFunctionScan */
	   select (fname), kscan(fname,1,'.')
	    into :fnamevalid1-:fnamevalid&count_valid. , 	:fname_new1-:fname_new&count_valid. 
	    from shpcaid_validfile;	
	quit;
	%PUT &fname_new1. &fnamevalid1.;
	%let KK = 1 ;
	%do %while(&KK. <= &count_valid.);
		%let fname=%str(&&fnamevalid&KK.);
		%let fname_new=%str(&&fname_new&KK.);
		%let compno =02;

		Data test  ;
			    /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
			    infile "&drv2./Encounters_Reporting/APD/Import/&fname." truncover delimiter='09'x DSD lrecl=32767
			                                                 firstobs = 2 ;  
			    informat clamno 	$20.
						 lineno 	$05.
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

			run;
			
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/HIOSID_Xwalk.xlsx  &drv2./Encounters_Reporting/APD/Control_Table/  --sse   --acl bucket-owner-full-control";
call system(cmd);
run;

Data _null_;
	 a=sleep(30,1);
	 run;			
			
			/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
			Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/HIOSID_Xwalk.xlsx"
					out=HIOSID (drop=comments) dbms=xlsx  /*B-274606*/
					replace;
			Run;

			proc sql;
				create table svcdat_map_shp as 
				select a.*, b.svcdat  from test a left join shpcaid.svclines b
				on a.clamno = b.clamno and a.lineno = b.lineno ;
			quit;
			data _null_;
				set HIOSID(WHERE = (compno="&compno."  and  missing(svcdat) ne 1));
				call symput('svcdat',svcdat);
			run;
			data svcdat_map_shp;
				set svcdat_map_shp;
				if svcdat ge "&svcdat." then svc_shp_flag = 'Y';
				else svc_shp_flag = 'N';
			run;
				
			%macro shp_split(shp_flag);
					proc sort data = svcdat_map_shp(where = (svc_shp_flag="&shp_flag.")) 
						out=svcdat_map_shp_&shp_flag.(drop= svcdat svc_shp_flag );
						by clamno lineno;
					run;
					
					data _null_;
						%if "&shp_flag." eq "Y" %then %do;
							set HIOSID(WHERE = (compno="&compno."  and  missing(svcdat) ne 1));
						%end;
						%else %do;
							set HIOSID(WHERE = (compno="&compno."  and  missing(svcdat) eq 1));
						%end;
						/* SAS2AWS2: ReplacedFunctionCompress */
						call symput('HIOSID',kcompress(HIOSID));	 /* B-146417 add compress */
					run;

					%nobs(svcdat_map_shp_&shp_flag.);

					%if &nobs. gt 0   %then %do;

						proc export data =  svcdat_map_shp_&shp_flag.
							/* SAS2AWS2: ReplacedSlash-ChangedFolderName-ReplacedSpaceWithUnderscore */
							outfile = "&drv2./Encounters_Reporting/APD/Import/&&fname_new._&HIOSID..TXT"
							dbms = tab replace;
						run;
					%end;
			%mend shp_split;
			*%nobs(svcdat_cnt); /*CR# CHG0040374 - commented */
				%shp_split(Y);
				%shp_split(N);	
	

				/* delete existing file  ------- */
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		%let path1 = &drv2./Encounters_Reporting/APD/Import/Archive ;

		%let dirid2=%sysfunc(dopen(&path1));

				 data _null_;
                    /* SAS2AWS2: ReplacedFunctionTranslate */
                    ddir=ktranslate(put(today(),mmddyy10.),'-','/');
                               call symput('ddir',ddir);
                run;
%put &ddir.;
                 /* SAS2AWS2: ReplacedSlash */
                 %let destpath = &path1./&ddir;


                %let dirid2=%sysfunc(dopen(&destpath));
                data _null_ ;
                               /* SAS2AWS2: ReplacedSlash-UpdatedOSCommands */
                               commd="mkdir &path1./&ddir";
                               call system(commd);
                run;
                %let rc=%sysfunc(dclose(&dirid2));

		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 		%if (%sysfunc(system(move "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/&fname."    "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/archive/&ddir/&fname.")) gt 0 ) */
/* 				%then %do; */
/* 				%put &fname..TXT was not moved to Archive folder ; */
/* 			%end;  */
/* 				%else %do; */
/* 					%put &fname..TXT was  moved to Archive folder ; */
/* 				%end; */
		data _null_;
         cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Import/&fname. &drv72./Encounters_Reporting/APD/Import/Archive/&ddir/&fname. --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;

         data _null_;
         cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Import/&fname. &drv72./Encounters_Reporting/APD/Import/Archive/&fname. --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;	
		%put &fname. was  moved to Archive folder ;		
						
						%let KK = %eval(&KK. + 1);

	%end;

%end;
%mend shpcaid_filesplit;
%shpcaid_filesplit;
/*ALM# 8377 kv - added logic to split SHPCAID files based on service date ----> end	 */

%macro APD_Process(); 
/*  HS# 542635 - commented */
/*
data svc_date;
	format svcdat date9.;
	svcdat=today();
	 call symput('svcdat',svcdat);		
run; */

%global compno filename_start fname Begf Endf flag chartind; /* HS 569650 KV - added flag */ /*B-336603*/
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
%let dirname = &drv2./Encounters_Reporting/APD/Import;

/* Reading the Company numbers from Auto Ref table *//*  HS# 542635 - commented */

proc sql ;
      select "'"||company||"'" into : LOBLIST1 separated by ','
      from DWHREFS.autoref
      /* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionSubstr-ReplacedFunctionTrim-ReplacedFunctionUpcase */
      where (&jobname eq kupcase(ksubstr(ktrim(kleft(pgmname)),1,6))) and kupcase(Active) = "Y";
quit; 

/* Reading the input file names from the Import Folder */

/* SAS2AWS2: ReplacedSlash-UpdatedOSCommands */

/* filename DIRLIST pipe "ls  &dirname/*.TXT"; */
filename DIRLIST pipe "ls  &dirname/*.TXT | sed 's:.*/::'";

Data validfilelist invalidfilelist;
     length fname $256  filename_start compno $2.;
     infile dirlist length=reclen ;
     input fname $varying256. reclen ;	 
	 /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
	 filename_start= kcompress((kupcase(kscan(fname,1,'_'))));
	 /* SAS2AWS2: ReplacedFunctionScan-ReplacedFunctionUpcase */
	 compno=	kcompress((kupcase(kscan(fname,2,'_'))));
     if (filename_start  in ("QI","DT")  and compno in ("01","02","20","25","30","45")) or /* CR# CHG0032747 - added 45 */
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
		(filename_start  in ("QI","DT")  and compno="60" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("MN1","NSL","NSF")) or
        /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
        (filename_start  in ("QI","DT")  and compno="34" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("CCC","FDA")) or
			    /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
		 (filename_start  in ("QI","DT")  and compno="42" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("QHP","EP")) then do;	   
			/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
			if (filename_start  in ("QI","DT")  and compno="60" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("MN1","NSL","NSF")) or
			       /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
       			(filename_start  in ("QI","DT")  and compno="34" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("CCC","FDA")) or
				(filename_start  in ("QI","DT")  and compno="01" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("DSNP")) or /*B-315473*/
						       /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
		 		(filename_start  in ("QI","DT")  and compno="42" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("QHP","EP")) 
		 		or (filename_start  in ("QI","DT")  and compno="45" and kcompress(kscan(kupcase(fname),3,'_'),".TXT") in ("MPPO"))  /*B-248997*/
		 		then do;	   	   
				/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionScan-ReplacedFunctionUpcase */
				Lobcd=kcompress(kscan(kupcase(fname),3,'_'),".TXT");
			end;
			
			Lobcd_=kcompress(kscan(kupcase(fname),3,'_'),".TXT"); /*B-289556*/
			chartind =	kcompress((kupcase(kscan(fname,4,'_'))));
			
  			output   validfilelist;	
	end; 	
	else output invalidfilelist ;	
 	
	call symput('compno',compno);
	call symput('chartind',chartind); /*B-336603*/
Run;

%nobs(invalidfilelist);

%if &nobs.= 0 %then %do;
%end;
%else %do;
/* Invalid file processing */
Proc sql noprint;
	select count(fname) into :count_invalid from invalidfilelist;
quit;
%let count_invalid=&count_invalid.;
Proc sql;
   select (fname)					
    into :fnameinvalid1-:fnameinvalid&count_invalid.		 
    from invalidfilelist;	
	select (compno)			/* HS# 542635 - added compno */ 			
    into :compnoinvalid1-:compnoinvalid&count_invalid.		 
    from invalidfilelist
quit;
/* Moving the error file in the Error folder  */
%let j = 1 ;
%do %while(&j. <= &count_invalid.);
	%let fname=%str(&&fnameinvalid&j.);	
	%let compno=%str(&&compnoinvalid&j.);	/* HS# 542635 - added compno */ 
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	%if %sysfunc(fileexist("&drv2./Encounters_Reporting/APD/Import/&fname.")) =1 %then
	 %do;
			/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 			%if (%sysfunc(system(move "&drv2./Encounters_Reporting/APD/Import/&fname." "&drv2./Encounters_Reporting/APD/Import/Error/&fname.")) gt 0) %then %do; */
/* 				%put &fname. was not moved to ERROR folder ; */
/* 			%end;  */
/* 			%else %do; */
			
		data _null_;
     cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Import/&fname. &drv72./Encounters_Reporting/APD/Import/Error/&fname. --sse --acl bucket-owner-full-control";
     call system(cmd);
     run;	
     
				%put &fname. was  moved to ERROR folder ;
				data mail_file;				  		
					status = "Invalid";
				RUN;
				%if &compno. eq 42 and "&LOB." eq "QHP" %then %do;/*ALM#7174 - added lob condition*/ 	/* HS# 542635 - added if else condition */ 
					%let lib = HIX;
				%end;
				%else %do;
					%let lib = APD;
				%end;
				%send_mail; 
/* 			%end;    */

	%end;
	%else %do;
		%put &fname. does not exist ;
	%end;
		%let j = %eval(&j. + 1);
	%end;
%end;

%nobs(validfilelist);
%if &nobs.= 0 %then %do;
	%goto finis;
%end;
%else %do;
/* valid file processing */
Proc sort data=validfilelist;
	by descending filename_start ;
Run;
Proc sql noprint;
	select count(fname) into :count_valid from validfilelist;
quit;
%let count_valid=&count_valid.;
Proc sql noprint;
   /* SAS2AWS2: ReplacedFunctionCompress */ /*B-289556 added Lobcd_ */
   select (fname),kcompress(filename_start),kcompress(compno),Compress(LOBCD),Compress(LOBCD_),compress(chartind) /*B-336603*/
    into :fnamevalid1-:fnamevalid&count_valid. ,:filename_start1-:filename_start&count_valid. ,
		 :compno1-:compno&count_valid.,:LOBCD1-:LOBCD&count_valid.,:LOBCD_1-:LOBCD_&count_valid. ,:chartind1-:chartind&count_valid. 
    from validfilelist;	
quit;
%let KK = 1 ;
%do %while(&KK. <= &count_valid.);
	%let fname=%str(&&fnamevalid&KK.);
	/*checking the file exist or not  */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
%if %sysfunc(fileexist("&drv2./Encounters_Reporting/APD/Import/&fname.")) =1 %then %do;
	%let compno=%str(&&compno&KK.) ;	
	%let filename_start=%str(&&filename_start&KK.) ;
	%let LOB=%str(&&LOBCD&KK.) ;
	%let LOB_=%str(&&LOBCD_&KK.) ; /*B-289556*/
	%let chartind=%str(&&chartind&KK.) ; /*B-336603*/
	  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	%inc "&drv/Programs/corpinfo.sas";  
	%Put "TESTING";
	%Put &libn1.;
	%let libdsnp=medicare;		/*B-315473*/
	%if "&filename_start." ="QI" %then %do;
	   %let QIfiflag=Y;	
	%end;
	%else %if "&filename_start." ="DT" %then %do;
		%let QIfiflag=N;
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		filename inp "&drv2./Encounters_Reporting/APD/Import/&fname.";
		data daterange;
			 infile inp recfm=v dlm='09'x firstobs=2;
			 informat startdt mmddyy10.
                        enddt mmddyy10.;
					 
			   ;
			 input startdt 
			  enddt 			
			  ;		
			  format startdt enddt date9.; 
			call symput('Begf',startdt);
			call symput('Endf',enddt);	

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

		run;		
	%end;
	/* HS 569650 - added logic to validate QI file input data and assign flag value ---> start  */
%macro multipletcn;
		Data test  ;
		    /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		    infile "&drv2./Encounters_Reporting/APD/Import/&fname." truncover delimiter='09'x DSD lrecl=32767
		                                                 firstobs = 2 ;  
		    informat clamno 	$20.
					 lineno 	$05.
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

		run;	
		data test_sample;
			set test;
			if claimfrq eq '1' and icn eq '' then flag = 'Criteria 1';
			else if claimfrq in ('8') and icn ne '' then flag = 'Criteria 2';
			else if claimfrq eq '1' and icn ne '' then flag = 'Criteria 3';
			else if claimfrq eq '7' and icn ne '' then flag = 'Criteria 5';/* CR CHG0034118 - added */
		run;
		proc sql;
			create table clam as
			select clamno, count(*)  as ccnt
			from test_sample
			group by clamno;
			 
			create table line as 
			select clamno, icn, count(*) as icnt
			from test_sample
			group by clamno, icn;

			create table fin as
			select distinct a.clamno
			from clam as a join line as b on a.clamno = b.clamno
			where ccnt ne icnt;

			update test_sample
			set flag = 'Criteria 4'
			where clamno in (select clamno from fin);
		quit;


		proc freq data = test_sample noprint;
		tables flag / out = test_sample_flagfreq;
		run;
		proc sql;
				select  count(*) into :total_obs  from test_sample_flagfreq ;
		Quit;

		%if &total_obs. gt 1 %then %do;
			%put "QI file Error " ;
			 data mail_file;
			  status = 'Invalid' ;
			  run;
				/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 				%if (%sysfunc(system(move "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/&fname."  "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/error/&fname.")) eq 0 ) */
/* 				%then %do; */
/* 					%put &fname. was  moved to error folder ; */
/* 				%end; */
				
				
				data _null_;
         cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Import/&fname. &drv72./Encounters_Reporting/APD/Import/Error/To_Onprem --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;
         
		data _null_;
         cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Import/&fname. &drv72./Encounters_Reporting/APD/Import/Error/&fname. --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;
        
         %put &fname. was  moved to error folder ;
                  
			 %send_mail;
		%end;
		%else %do;
		data _null_;
			set test_sample;
			call symput("flag",flag);
		run;
		%end;
%mend  multipletcn;
%macro multcn_dsnp;		/*B-315473 start*/
		Data test  ;
		    /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		    infile "&drv2./Encounters_Reporting/APD/Import/&fname." truncover delimiter='09'x DSD lrecl=32767
		                                                 firstobs = 2 ;  
		    informat clamno_mcd 	$20.
					 lineno_mcd		$05.
					 clamno_mcr 	$20.
					 lineno_mcr 	$05.
					 extclm			$25.
		             claimfrq 		$01.
					 icn 			$22. /* 541126 - ICN length increased from 15 to 20, B-136184 from 20 to 22 */
				;
		    input    clamno_mcd 	$ 
					 lineno_mcd		$
					 clamno_mcr 	$ 
					 lineno_mcr		$
					 extclm			$
					 claimfrq 		$ 
					 icn			$
				;

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

		run;	
		data test_sample;
			set test;
			if claimfrq eq '1' and icn eq '' then flag = 'Criteria 1';
			else if claimfrq in ('8') and icn ne '' then flag = 'Criteria 2';
			else if claimfrq eq '1' and icn ne '' then flag = 'Criteria 3';
			else if claimfrq eq '7' and icn ne '' then flag = 'Criteria 5';/* CR CHG0034118 - added */
		run;
		proc sql;
			create table clam as
			select extclm, count(*)  as ccnt
			from test_sample
			group by extclm;
			 
			create table line as 
			select extclm, icn, count(*) as icnt
			from test_sample
			group by extclm, icn;

			create table fin as
			select distinct a.extclm
			from clam as a join line as b on a.extclm = b.extclm
			where ccnt ne icnt;

			update test_sample
			set flag = 'Criteria 4'
			where extclm in (select extclm from fin);
		quit;


		proc freq data = test_sample noprint;
		tables flag / out = test_sample_flagfreq;
		run;
		proc sql;
				select  count(*) into :total_obs  from test_sample_flagfreq ;
		Quit;

		%if &total_obs. gt 1 %then %do;
			%put "QI file Error " ;
			 data mail_file;
			  status = 'Invalid' ;
			  run;
				/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
/* 				%if (%sysfunc(system(move "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/&fname."  "&drv2./Encounters/NYSDOH/MEDS3toAPD/Extracts/import/error/&fname.")) eq 0 ) */
/* 				%then %do; */
/* 					%put &fname. was  moved to error folder ; */
/* 				%end; */
				
				
				data _null_;
         cmd=" aws s3 cp &drv2./Encounters_Reporting/APD/Import/&fname. &drv72./Encounters_Reporting/APD/Import/Error/To_Onprem --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;
         
		data _null_;
         cmd=" aws s3 mv &drv2./Encounters_Reporting/APD/Import/&fname. &drv72./Encounters_Reporting/APD/Import/Error/&fname. --sse --acl bucket-owner-full-control";
         call system(cmd);
         run;
        
         %put &fname. was  moved to error folder ;
                  
			 %send_mail;
		%end;
		%else %do;
		data _null_;
			set test_sample;
			call symput("flag",flag);
		run;
		%end;
%mend  multcn_dsnp; /*B-315473-end*/
	/* HS 569650 - added logic to validate QI file input data and assign flag value ---> end  */

	/* HS# 542635 - added logic to integrate HIX sas job into APD --> START */ 
	%if &compno. eq 42 and "&LOB." eq "QHP" %then %do;/*ALM#7174 - added lob condition*/ 
	
data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/SVC_Cutoff_date.TXT &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run;
	
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		filename inp "&drv2./Encounters_Reporting/APD/Control_Table/SVC_Cutoff_date.TXT";
			data svc_date;
				 infile inp recfm=v dlm='09'x firstobs=2;
				 informat svcdat mmddyy10.             
					;
				 input svcdat 			 		
				  ;		
				  format svcdat  date9.; 
				  call symput('svcdat',svcdat);			

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

			run;
	  /*%let pgmname = R4217101;	   B-96285 */
	  /*%let jobname = R42171;		   B-96285 */
	 %if "&chartind" = "ADHOC" %then %do;
		%let pgmname = RGA19701;	/* B-336603 */		
		%let jobname = RGA191;		/* B-336603 */
	 %end;
	  %else %do;
		%let pgmname = RGA19101;	/* B-96285 */		
		%let jobname = RGA191;		/* B-96285 */
	  %end;	
		%let masking=N;				/* B-96285 */
		%let lib = HIX;
		%multipletcn; /* HS 569650 */
	%end;
	%else %do;

		data svc_date;
			format svcdat date9.;
			svcdat=today();
			call symput('svcdat',svcdat);		
		run;
		 %if "&chartind" = "ADHOC" %then %do;
		%let pgmname = RGA19701;	/* B-336603 */		
		%let jobname = RGA191;		/* B-336603 */
	 %end;
	  %else %do;
		%let pgmname = RGA19101;	/* B-96285 */		
		%let jobname = RGA191;		/* B-96285 */
	  %end;		
		%let masking=N;
		%let lib = APD;
		%if &compno.=01 and &LOB. = DSNP %then %do; /*B-315473*/
		%multcn_dsnp;
		%end;
		%else %do;	/*B-315473*/
		%multipletcn; /* HS 569650 */
		%end;
	%end;
	/* HS# 542635 - added logic to integrate HIX sas job into APD --> end */ 
	/* CHG0038264 -  start time calculation*/
	%nobs(test);
	 %let recordcount =&nobs.;
	Data time;
			format start_time datetime20.;
		    start_time = datetime();
			call symput('start_time',start_time);		
	run;
         Data _null_;
	 a=sleep(180,1);
	 run;
	/*%inc "&drv.\programs\RGA19101.sas";*/ /* HS# 542635 */ 	
	 /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	 %include "&drv/AUTOEXEC/reconnect.sas";  /*Reconnecting Redshift Libraries - B-241901 */
	%inc "&drv./Programs/&pgmname..sas"; 
	 
%end;	
%else %do;
		%put &fname. does not exist ;
	%end;
		%let KK = %eval(&KK. + 1);
	%end;
%end;

%finis:


%mend;
%APD_Process();

/* VVKR Code Start -- B-75041 */
%time(timeparm=e);
%timestamp;
%initvar;
quit;
/* VVKR Code End -- B-75041 */
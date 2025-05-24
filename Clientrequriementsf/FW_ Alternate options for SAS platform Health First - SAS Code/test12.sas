
%macro qidsnp;
Data  &RGA191..finclaims_&compno._1
extdsnp1(keep=extclm clamno_mcd)
extdsnp2(keep=extclm clamno_mcr);
	
    infile "&drv2./Encounters_Reporting/APD/Import/&fname." truncover delimiter='09'x DSD lrecl=32767
                                                 firstobs = 2 ;  
    informat clamno_mcd		$20.  
			 lineno_mcd		$05.  
			 clamno_mcr 	$20.  
			 lineno_mcr 	$05.  
			 extclm			$30.
             claimfrq 		$01.
			 icn 			$22. 
		;
    input    clamno_mcd		$ 
			 lineno_mcd		$
			 clamno_mcr 	$ 
			 lineno_mcr		$
			 extclm			$
			 claimfrq 		$ 
			 icn			$
		;
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;
		
		if clamno_mcd<>'' then output extdsnp1;
		if clamno_mcd='' and clamno_mcr<>'' then output extdsnp2; 
		format lineno $05.;
		lineno=coalescec(lineno_mcd,lineno_mcr);
		if missing(clamno_mcd)=1 and missing(lineno_mcd)=1 then mcd_lin_missfl=1; else mcd_lin_missfl=0;
		if missing(clamno_mcr)=1 and missing(lineno_mcr)=1 then mcr_lin_missfl=1; else mcr_lin_missfl=0;
		output &RGA191..finclaims_&compno._1;
run;

proc sort data=extdsnp1 nodups;by extclm clamno_mcd ; run;
proc sort data=extdsnp2 nodups;by extclm clamno_mcr ; run;

proc sql;
	create table X1 as 
	select a.*, b.clamno_mcd as clamno1
	from &RGA191..finclaims_&compno._1 a
	left join extdsnp1 b 
	on a.extclm=b.extclm
	and a.clamno_mcd=''
	;
	create table X2 as 
	select a.*, b.clamno_mcr as clamno2
	from X1 a
	left join extdsnp2 b 
	on a.extclm=b.extclm
	and a.clamno_mcr=''
	;
quit;
Data &RGA191..finclaims_&compno.(drop=clamno1 clamno2);
	format clamno $20.;
	set X2;
	clamno=coalescec(clamno_mcd,clamno1,clamno_mcr,clamno2);
Run;
proc sort data=&RGA191..finclaims_&compno. nodups; by extclm clamno lineno; run;
%mend qidsnp;

%macro datdsnp;

 	Proc sql;	
	create table tt1 as 
		select distinct clamno_mcd as clamno from &RGA191..finclaims_&compno. where clamno_mcd<>'' order by clamno;
	create table tt2 as 
		select distinct clamno_mcr as clamno from &RGA191..finclaims_&compno. where clamno_mcr<>'' order by clamno;
	quit;
	
	Proc sort data=tt1 nodupkey; by clamno; run;
	Proc sort data=tt2 nodupkey; by clamno; run;
	
	%nobs(tt1);
	%let mcdnobs=&nobs;
	%nobs(tt2);
	%let mcrnobs=&nobs;	
    
	%if &mcdnobs = 0 and &mcrnobs = 0 %then %do;
		Data svclines9 &RGA191..cas_svclines(keep=clamno lineno apsvcstat revind);  
		set &libn..svclines(Keep=&Keepv_svc. obs=0) ;
		run;
		Data svcholds9;
		set &libn..svcholds(keep=clamno lineno holdcd typcod obs=0);	
		run;
		Data claims9;
		set &libn..claims(Keep=&Keepv_Claim. obs=0);
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

	%if &mcdnobs = 0 %then %do;
		Data svclines_01(rename=(clamno=clamno_mcd lineno=lineno_mcd)) svclines_01_mcr1;
		set &libn..svclines(Keep=&Keepv_svc. obs=0) ;
		run;
		Data svcholds_01(rename=(clamno=clamno_mcd lineno=lineno_mcd))  svcholds_01_mcr1;
		set &libn..svcholds(keep=clamno lineno holdcd typcod obs=0);	
		run;
		Data claims_01 claims_01_mcr1;
		set &libn..claims(Keep=&Keepv_Claim. obs=0);
		run;
		%vars(svclines_01_mcr1,_mcr,svclines_01_mcr)   ; 
		%vars(svcholds_01_mcr1,_mcr,svcholds_01_mcr)   ; 
		%vars(claims_01_mcr1,_mcr,claims_01_mcr)   ; 		
	%end;
	%else %do;
		Data test;
			a = ceil(&mcdnobs/2000);
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
				b = "Where clamno in (%str(&)clamno_"||kstrip(put(i,8.))||".)";
			end;
			else do;
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
			else value=ktrim(value)||Trim(b);
			call symput ('cond1',value);
		Run;	
		Proc sql ;
				select kstrip(put(count(stvl),8.)) into :cnt from test;
				select kstrip(put(stvl,8.)) into :stvl_1 -:stvl_&cnt from test;
				select kstrip(put(endvl,8.)) into :endvl_1 -:endvl_&cnt from test;
		quit;
		%do i = 1 %to &cnt;
			Proc sql noprint;
			select  quote(clamno) into :clamno_&i SEPARATED  by "," from tt1 where monotonic() between &&stvl_&i. and &&endvl_&i.;
			quit;
		%end;	
		Data svclines_01(rename=(clamno=clamno_mcd lineno=lineno_mcd)) svclines_01_mcr1;
			set &libn..svclines(Keep=&Keepv_svc.); &cond1.;
		Run;
		Data svcholds_01(rename=(clamno=clamno_mcd lineno=lineno_mcd))  svcholds_01_mcr1;
			set &libn..svcholds(keep=clamno lineno holdcd typcod); &cond1.;
		Run;
		Data claims_01 claims_01_mcr1;
			set &libn..claims(keep=&Keepv_Claim.); &cond1.;
			extclm=compress(extclm,'D');
		Run;		
		%vars(svclines_01_mcr1,_mcr,svclines_01_mcr)   ; 
		%vars(svcholds_01_mcr1,_mcr,svcholds_01_mcr)   ; 
		%vars(claims_01_mcr1,_mcr,claims_01_mcr)   ; 		
	%end;
	%if &mcrnobs = 0 %then %do;
		Data  svclines_30(rename=(clamno=clamno_mcd lineno=lineno_mcd))  svclines_30_mcr1;
		set &libn..svclines(Keep=&Keepv_svc. obs=0) ;
		run;
		Data svcholds_30(rename=(clamno=clamno_mcd lineno=lineno_mcd))  svcholds_30_mcr1;
		set &libn..svcholds(keep=clamno lineno holdcd typcod obs=0);	
		run;
		Data claims_30 claims_30_mcr1;
		set &libn..claims(Keep=&Keepv_Claim. obs=0);
		run;
		%vars(svclines_30_mcr1,_mcr,svclines_30_mcr)   ; 
		%vars(svcholds_30_mcr1,_mcr,svcholds_30_mcr)   ; 
		%vars(claims_30_mcr1,_mcr,claims_30_mcr)   ; 
	%end;
	%else %do;
		Data test4;
			a = ceil(&mcrnobs/2000);
			do i = 1 to a;
				stvl = i*2000-1999;
				endvl = i*2000;
				output;
			end;
		Run;
		data test5;
			set test4;
			length b $100;
			if _n_ = 1 then do;
				b = "Where clamno in (%str(&)clamno1_"||kstrip(put(i,8.))||".)";
			end;
			else do;
				b = " or clamno in (%str(&)clamno1_"||kstrip(put(i,8.))||".)";
			end;
			temp=1;
		Run;
		Proc sort data=test5;by temp;run;
		Data test6;
			set test5;
			by temp;
			Length value $10000.;
			retain value;
			if first.temp then value=Trim(b);
			else value=ktrim(value)||Trim(b);
			call symput ('cond2',value);
		Run;  	
		Proc sql ;
				select  kstrip(put(count(stvl),8.)) into :cnt1 from test4;
				select kstrip(put(stvl,8.)) into :stvl1_1 -:stvl1_&cnt1 from test4;
				select kstrip(put(endvl,8.)) into :endvl1_1 -:endvl1_&cnt1 from test4;
		quit;	
		%do i = 1 %to &cnt1;
			Proc sql noprint;
			select  quote(clamno) into :clamno1_&i SEPARATED  by "," from tt2 where monotonic() between &&stvl1_&i. and &&endvl1_&i.;
			quit;
		%end;	
		Data  svclines_30(rename=(clamno=clamno_mcd lineno=lineno_mcd))  svclines_30_mcr1;
			set &libdsnp..svclines(Keep=&Keepv_svc.); &cond2.;
		Run;	
		Data svcholds_30(rename=(clamno=clamno_mcd lineno=lineno_mcd))  svcholds_30_mcr1;
			set &libdsnp..svcholds(keep=clamno lineno holdcd typcod); &cond2.;
		Run;
		Data claims_30 claims_30_mcr1;
			set &libdsnp..claims(keep=&Keepv_Claim.); &cond2.;
		Run;	
		%vars(svclines_30_mcr1,_mcr,svclines_30_mcr)   ; 
		%vars(svcholds_30_mcr1,_mcr,svcholds_30_mcr)   ; 
		%vars(claims_30_mcr1,_mcr,claims_30_mcr)   ; 		
	%end;
		Data &RGA191..finclaims_&compno._2 &RGA191..finclaims_&compno.;
			set &RGA191..finclaims_&compno.;
			if mcd_lin_missfl=1 then do;
				clamno_mcd=clamno_mcr;
				lineno_mcd=lineno_mcr;
			end;
			if mcr_lin_missfl=1 then do;
				clamno_mcr=clamno_mcd;
				lineno_mcr=lineno_mcd;				
			end;
		Run;

		proc sort data=&RGA191..finclaims_&compno._2 nodups; by extclm clamno; run;
		proc sort data=claims_01 nodups; by extclm clamno; run;
		proc sort data=claims_30 nodups; by extclm clamno; run;
		
		Data claims
			 clmmemb(keep=clamno membno);
			Merge	&RGA191..finclaims_&compno._2(in=a)
					claims_01(in=b)
					claims_30(in=c)
					; by extclm clamno;
						
						%let i = 1 ;
						%do %while(&i. <= &Rate_code_total.); 
							if valA01=&&Rate_Code&i.  then valA01=&&New_Rate_Code&i.;
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
					if a then output;    
		Run;
		
		proc sort data=&RGA191..finclaims_&compno._2 nodups; by clamno_mcd lineno_mcd; run;
		proc sort data=svclines_01 nodups; by clamno_mcd lineno_mcd; run;
		proc sort data=svclines_30 nodups; by clamno_mcd lineno_mcd; run;
	
		Data &RGA191..finclaims_&compno._31(drop=membno);
			Merge	&RGA191..finclaims_&compno._2(in=a)
					svclines_01(in=b)
					svclines_30(in=c)				
					; by clamno_mcd lineno_mcd;
					if a then output;
		Run;
		
		proc sort data=clmmemb nodups; by clamno; run;
		proc sort data=&RGA191..finclaims_&compno._31 nodups; by clamno; run;
		
		Data &RGA191..finclaims_&compno._3;
			Merge	&RGA191..finclaims_&compno._31(in=a)
					clmmemb(in=b)			
					; by clamno;
					if a then output;
		Run;			
	
		proc sort data=&RGA191..finclaims_&compno._3 nodups; by clamno_mcr lineno_mcr; run;
		proc sort data=svclines_01_mcr nodups; by clamno_mcr lineno_mcr; run;
		proc sort data=svclines_30_mcr nodups; by clamno_mcr lineno_mcr; run;
		
		Data svclines
			&RGA191..cas_svclines(keep=clamno lineno apsvcstat revind extclm clamno_mcr lineno_mcr apsvcstat_mcr revind_mcr);
			Merge	&RGA191..finclaims_&compno._3(in=a)
					svclines_01_mcr(in=b)
					svclines_30_mcr(in=c)				
					; by clamno_mcr lineno_mcr;
			if mcd_lin_missfl=1 then do;
				netalwamt=0;
				topamt=0;
			/*	UNITCT=0;*/
				COVUNT=0;
			/*	claamt=0;*/
				ALWUNT=0;
				NETALWAMT_old=0;				
			end;
			if mcr_lin_missfl=1 then do;
				netalwamt_mcr=0;
				topamt_mcr=0;	
				UNITCT_mcr=0;
				COVUNT_mcr=0;
				claamt_mcr=0;
				ALWUNT_mcr=0;
				NETALWAMT_old_mcr=0;				
			end;		
			if a then output;
		Run;		
		
		proc sort data=&RGA191..finclaims_&compno._2 nodups; by clamno_mcd lineno_mcd; run;
		proc sort data=svcholds_01 nodups; by clamno_mcd lineno_mcd; run;
		proc sort data=svcholds_30 nodups; by clamno_mcd lineno_mcd; run;
		
		Data &RGA191..finclaims_&compno._4;
			Merge	&RGA191..finclaims_&compno._2(in=a)
					svcholds_01(in=b)
					svcholds_30(in=c)				
					; by clamno_mcd lineno_mcd;
					if a then output;
		Run;
		
		proc sort data=&RGA191..finclaims_&compno._4 nodups; by clamno_mcr lineno_mcr; run;			
		proc sort data=svcholds_01_mcr nodups; by clamno_mcr lineno_mcr; run;
		proc sort data=svcholds_30_mcr nodups; by clamno_mcr lineno_mcr; run;
		
		Data svcholds;
			Merge	&RGA191..finclaims_&compno._4(in=a)
					svcholds_01_mcr(in=b)
					svcholds_30_mcr(in=c)				
					; by clamno_mcr lineno_mcr;
					if a then output;
		Run;
	%end;
%mend datdsnp;
%macro dsnpvar;

	PR_2430_SVD02_SL_PAID_AMT=TOPAMT;
	PR_2430_SVD02_SL_PAID_AMT_MCR=TOPAMT_mcr ;		

	if typecode in ('P') then do;
		if capind = 'F' then do;
			PR_2400_SV104_S_LINE_CNT=UNITCT;
			PR_2430_SVD05_PD_UNITS=COVUNT;
			PR_2400_SV102_S_CHG_AMT=CLAAMT;	
			PR_2430_CAS03_Var=sum(PR_2400_SV102_S_CHG_AMT,-PR_2430_SVD02_SL_PAID_AMT);
			PR_2430_CAS04_Var=SUM(PR_2400_SV104_S_LINE_CNT,-PR_2430_SVD05_PD_UNITS);
			PR_2430_CAS07_Var=0;
			PR_2430_CAS08_Var=0;
			
			PR_2430_SVD05_PD_UNITS_MCR=COVUNT_mcr;

			PR_2430_CAS03_MCR_Var=SUM(PR_2400_SV102_S_CHG_AMT,-PR_2430_SVD02_SL_PAID_AMT_MCR);
			PR_2430_CAS04_MCR_Var=COVUNT_mcr;
			PR_2430_CAS07_MCR_Var=0;
			PR_2430_CAS08_MCR_Var=0;
		end;
		else if capind = 'C' then do;
			PR_2400_SV104_S_LINE_CNT=UNITCT;
			PR_2430_SVD05_PD_UNITS=COVUNT;
			PR_2400_SV102_S_CHG_AMT=CLAAMT;	
			PR_2430_CAS03_Var=NETALWAMT_old;
			PR_2430_CAS04_Var=ALWUNT;
			/*PR_2430_CAS07_Var=sum(PR_2400_SV102_S_CHG_AMT,-PR_2430_CAS03_Var);*/  /* B-329832 - DS - COMMENTED*/
			PR_2430_CAS08_Var=SUM(PR_2400_SV104_S_LINE_CNT,-PR_2430_CAS04_Var);
			
			PR_2430_SVD05_PD_UNITS_MCR=COVUNT_mcr;
			PR_2430_CAS03_MCR_Var=netalwamt_old_mcr;
			PR_2430_CAS04_MCR_Var=COVUNT_mcr;
			/*PR_2430_CAS07_MCR_Var=SUM(claamt_mcr-netalwamt_old_mcr);*/  /* B-329832 - DS - COMMENTED*/
			PR_2430_CAS08_MCR_Var=COVUNT_mcr;
			if topamt > 0 then do; /*B-329832 - DS - only for cap c and topamt > 0 PR_2430_CAS07_Var updated with new logic*/
				PR_2430_CAS07_Var=sum(PR_2400_SV102_S_CHG_AMT,-(sum(PR_2430_CAS03_Var,PR_2430_SVD02_SL_PAID_AMT)));
			end;
			else do;
				PR_2430_CAS07_Var=sum(PR_2400_SV102_S_CHG_AMT,-PR_2430_CAS03_Var);
			end;
			if topamt_mcr > 0 then do; /*B-329832 - DS - only for cap c and topamt > 0 PR_2430_CAS07_Var updated with new logic*/
				PR_2430_CAS07_MCR_Var=sum(PR_2400_SV102_S_CHG_AMT,-(sum(PR_2430_CAS03_MCR_Var,PR_2430_SVD02_SL_PAID_AMT_MCR)));
			end;
			else do;
				PR_2430_CAS07_MCR_Var=sum(PR_2400_SV102_S_CHG_AMT,-PR_2430_CAS03_Var);
			end;			
		end;
	end;
	IN_2400_SV203_SL_CHG_AMT =CLAAMT;
	IN_2430_SVD02_SL_PAID_AM=NETALWAMT;
	IN_2430_SVD02_SL_PAID_AM_MCR=NETALWAMT_mcr;	
	IN_2430_SVD05_PD_UNITS_MCR=COVUNT_mcr;
	if typecode in ('I') then do;
		if first.clamno and Inp_dg_line_flag='Y' then Temp_flag=1;
		if capind = 'F' then do;
						IN_2400_SV205_SL_COUNT=unitct;
			 			IN_2430_SVD05_PD_UNITS=COVUNT;
						IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
						IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;
				
						
						IN_2430_CAS03_MCR_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM_MCR);
						IN_2430_CAS04_MCR_Var=sum(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;
		end;
		if capind = 'C' then do;
						IN_2400_SV205_SL_COUNT=unitct;
			 			IN_2430_SVD05_PD_UNITS=covunt;
						IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM); 
						IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_Var=0;
						IN_2430_CAS08_Var=0;
						

						IN_2430_CAS03_MCR_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM_MCR);
						IN_2430_CAS04_MCR_Var=sum(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
						IN_2430_CAS07_MCR_Var=0;
						IN_2430_CAS08_MCR_Var=0;
		end;
	end;
	if typecode in ('O') then do;
			
		if capind = 'F' then do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
					IN_2430_CAS03_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM);
					IN_2430_CAS04_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_SVD05_PD_UNITS);
					IN_2430_CAS07_Var=0;
					IN_2430_CAS08_Var=0;
					
					IN_2430_SVD05_PD_UNITS_MCR=COVUNT_mcr;
					IN_2430_CAS03_MCR_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_SVD02_SL_PAID_AM_MCR);
					IN_2430_CAS04_MCR_Var=0;
					IN_2430_CAS07_MCR_Var=0;
					IN_2430_CAS08_MCR_Var=0;
		end;
		if capind = 'C' then do;
					IN_2400_SV205_SL_COUNT=UNITCT;
			 		IN_2430_SVD05_PD_UNITS=COVUNT;
					IN_2430_CAS03_Var=NETALWAMT_old;
					IN_2430_CAS04_Var=ALWUNT;
					/*IN_2430_CAS07_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_CAS03_Var);*//* preamt update*/
					IN_2430_CAS08_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_CAS04_Var);
					
					IN_2430_CAS03_MCR_Var=netalwamt_old_mcr;
					IN_2430_CAS04_MCR_Var=alwunt_mcr;
					/*IN_2430_CAS07_MCR_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_CAS03_MCR_Var);*//* preamt update*/
					IN_2430_CAS08_MCR_Var=SUM(IN_2400_SV205_SL_COUNT,-IN_2430_CAS04_MCR_Var);
					
			if topamt > 0 then do; /*B-329832 - DS - only for cap c and topamt > 0 IN_2430_CAS07_Var updated with new logic*/
				IN_2430_CAS07_Var=sum(IN_2400_SV203_SL_CHG_AMT,-(sum(IN_2430_CAS03_Var,IN_2430_SVD02_SL_PAID_AM)));
			end;
			else do;
				IN_2430_CAS07_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_CAS03_Var);
			end;
			if topamt_mcr > 0 then do; /*B-329832 - DS - only for cap c and topamt > 0 IN_2430_CAS07_Var updated with new logic*/
				IN_2430_CAS07_MCR_Var=sum(IN_2400_SV203_SL_CHG_AMT,-(sum(IN_2430_CAS07_MCR_Var,IN_2430_SVD02_SL_PAID_AM_MCR)));
			end;
			else do;
				IN_2430_CAS07_MCR_Var=sum(IN_2400_SV203_SL_CHG_AMT,-IN_2430_CAS07_MCR_Var);
			end;
		end;
	end;

%mend dsnpvar;
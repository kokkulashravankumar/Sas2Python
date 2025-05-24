options bufno=400 ibufno=400 ubufno=20 sortsize=512M fullstimer MSGLEVEL=I;

%time(timeparm=s);

/*spltlogic macro to read svclines and update mcr,mcd and cost sharing fields based on grid file */
/* read source grid file */

data ccmcrbenpkg (rename=(benefit_period=benefitperiod));
	/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	infile "&drv2./FIN/SVCLINES/Complete_Care_Benefit_Grid.csv"
	delimiter = ',' missover
	dsd lrecl=32767 firstobs=2 ;
	length
		days_trigger	 $1.
		benefit_period   8.
		mcd				 $1.
		mcr 			 $1.
		benpkg           $8.	/*ALM 5682 BC updated the length from 5 to 8*/
		ratecode		$100.		
		bencod           $4.
        descrp           $100. 	/*ALM 5682 BC updated the length */
        descsh           $100.	/*ALM 5682 BC updated the length */
        untper             8.
        untsmx             8.
        untfmx             8.
        untslf             8.
        covsmx             8.
        covfmx             8.
        covslf             8.
        covflf             8.
        coptyp            $2.
        copsmx             8.
        copfmx             8.
        copgrp            $1.
        coityp            $1.
        coismx             8.
        coifmx             8.
        coigrp            $1.
        dedsmx             8.
        dedfmx             8.
        dedgrp            $5.
        oopsmx             8.
        oopfmx             8.
        oopslf             8.
        oopflf             8.
        oopgrp            $3.
        effdat             8.
        expdat             8.
        coppct             8.
        copamt             8.
        coipct             8.
        coiamt             8. ;
    format
		days_trigger	$char1.
		benefit_period  8.
		mcd				$char1. 
		mcr				$char1.
		benpkg           $char8. /*ALM 5682 BC updated the length to 8*/
		ratecode		 $char100.
		bencod           $char4.
        descrp           $char100. /*ALM 5682 BC updated the length */
        descsh           $char100. /*ALM 5682 BC updated the length */
        untper           f12.
        untsmx           f12.
        untfmx           f12.
        untslf           f12.
        covsmx           f12.
        covfmx           f12.
        covslf           f12.
        covflf           f12.
        coptyp           $char2.
        copsmx           f12.
        copfmx           f12.
        copgrp           $char1.
        coityp           $char1.
        coismx           f12.
        coifmx           f12.
        coigrp           $char1.
        dedsmx           f12.
        dedfmx           f12.
        dedgrp           $char5.
        oopsmx           f12.
        oopfmx           f12.
        oopslf           f12.
        oopflf           f12.
        oopgrp           $char3.
        effdat           mmddyy10.
        expdat           mmddyy10.
        coppct           f12.2
        copamt           f12.2
        coipct           f12.2
        coiamt           f12.2 ;
    informat
		days_trigger	 $char1.
		benefit_period   8.
		mcd				 $char1.
		mcr				 $char1.
		benpkg           $char8.	/*ALM 5682 BC updated the length to 8*/
		ratecode		 $char100.
		bencod           $char4.
        descrp           $char100.	/*ALM 5682 BC updated the length */
        descsh           $char100. 	/*ALM 5682 BC updated the length */
        untper           best12.
        untsmx           best12.
        untfmx           best12.
        untslf           best12.
        covsmx           best12.
        covfmx           best12.
        covslf           best12.
        covflf           best12.
        coptyp           $char2.
        copsmx           best12.
        copfmx           best12.
        copgrp           $char1.
        coityp           $char1.
        coismx           best12.
        coifmx           best12.
        coigrp           $char1.
        dedsmx           best12.
        dedfmx           best12.
        dedgrp           $char5.
        oopsmx           best12.
        oopfmx           best12.
        oopslf           best12.
        oopflf           best12.
        oopgrp           $char3.
        effdat           mmddyy10.
        expdat           mmddyy10.
        coppct           best12.
        copamt           best12.
        coipct           best12.
        coiamt           best12. ;
	input
		days_trigger	 : $char1. 
		benefit_period   : 8.
		mcd				 : $char1.
		mcr				 : $char1.
		benpkg           : $char8.		/*ALM 5682 BC updated the length */
		ratecode		 : $char100.
		bencod           : $char4.
        descrp           : $char100.	/*ALM 5682 BC updated the length */	
        descsh           : $char100.	/*ALM 5682 BC updated the length */
        untper           : best32.
        untsmx           : best32.
        untfmx           : best32.
        untslf           : best32.
        covsmx           : best32.
        covfmx           : best32.
        covslf           : best32.
        covflf           : best32.
        coptyp           : $char2.
        copsmx           : best32.
        copfmx           : best32.
        copgrp           : $char1.
        coityp           : $char1.
        coismx           : best32.
        coifmx           : best32.
        coigrp           : $char1.
        dedsmx           : best32.
        dedfmx           : best32.
        dedgrp           : $char5.
        oopsmx           : best32.
        oopfmx           : best32.
        oopslf           : best32.
        oopflf           : best32.
        oopgrp           : $char3.
        effdat           : mmddyy10.
        expdat           : mmddyy10.
        coppct           : best32.
        copamt           : best32.
        coipct           : best32.
        coiamt           : best32. 
;
if missing(ratecode) then do;
if missing(days_trigger) or missing(mcd) or missing(mcr) or missing(bencod) or missing(effdat) then delete;
/*ALM 5682 BC updated the benpkg condition*/
if &compno. = 34 then do;
	if benpkg not in ("MCRCCARE","FIDA") then delete;
	end;
else if &compno. = 60 then do;
	if benpkg not in ("MSO") then delete;
end;
end;

/*ALM 5682 BC updated the benpkg condition*/
run;

%global bencodes1 coiamt1 effdat1;

/* Fetching the bencodes & coiamt from benefit grid where the maxcopay limit needs to be calculated */
proc sql;
    select bencod,coiamt,year(effdat) as year into 
    : bencodes1 separated by ' ',
    : coiamt1 separated by ' ',
    : effdat1 separated by ' '
    from work.ccmcrbenpkg  where coiamt ^= 0;
quit;

/* map expdat to future date if any expdat missing or else expdat and effdat are same */
proc sql;
	create table bencode as
	select * ,
			case 
				when expdat is null then '31dec2999'd
				when expdat eq effdat then '31dec2999'd
				else expdat
			end as exp_dat format = mmddyy10.
		   ,case 
				/* SAS2AWS2: ReplacedFunctionSubstr */
				when anydigit(ksubstr(bencod,3,1)) then ksubstr(bencod,1,2)
			    else bencod 
		    end as grp
	from ccmcrbenpkg 
;quit;

data bencodes(rename=(bencod=gridbencod benpkg=gridbenpkg coiamt=gridcoiamt copamt=copamtb exp_dat=expdat));
	set bencode(drop=expdat);
	format effdat mmddyy10.;
run;

proc sql; 
	create table bencodmax as
	/* SAS2AWS2: ReplacedFunctionSubstr */
	select effdat, expdat,gridbenpkg, case when anydigit(ksubstr(gridbencod,3,1)) /*ALM 5682 - BC added gridbenpkg*/
									   /* SAS2AWS2: ReplacedFunctionSubstr */
								 	  then ksubstr(gridbencod,1,2)
								 	  else gridbencod 
								 end as grp
		  ,sum(untsmx) as grpmax
	from bencodes
	group by 1, 2, 3,4
	order by 1, 2, 3, 4,5	;
quit;

proc sql; 
create table bencodshort as
/* SAS2AWS2: ReplacedFunctionSubstr */
select days_trigger, mcd, mcr, effdat, expdat,gridbenpkg, case when anydigit(ksubstr(gridbencod,3,1)) /*ALM 5682 - BC Added gridbenpkg*/
								   /* SAS2AWS2: ReplacedFunctionSubstr */
							 	  then ksubstr(gridbencod,1,2)
							 	  else gridbencod 
							 end as grp
	   ,gridbencod, untsmx, coppct, copamtb, descsh
/* SAS2AWS2: ReplacedFunctionUpcase */
from bencodes where kupcase(days_trigger) eq "Y"
order by effdat, expdat, grp, gridbencod
;quit; 

proc sort data=bencodshort;by grp gridbencod;run;

/*TRANSPOSE THE BENEFIT GRID INTO ONE ARRAY PER BENEFIT GROUP */
proc sort data=bencodshort;
	by gridbenpkg effdat gridbencod untsmx ;	/*ALM 5682 - BC Added gridbenpkg*/
run;
/*BC Updated the Array size from 5 to 6*/
data bencodtranspose(keep = days_trigger gridbenpkg effdat expdat grp bencodarray1-bencodarray5 untsmxarray1-untsmxarray5 
					coppctarray1-coppctarray5 copamtarray1-copamtarray5 mcrarray1-mcrarray5 mcdarray1-mcdarray5);
	do until (last.effdat); 
		set bencodshort; 
		by gridbenpkg effdat gridbencod untsmx; 	/*ALM 5682 - BC Added gridbenpkg*/

		array bencodarray(5) $3; 
		array untsmxarray(5) 8.; 
		array coppctarray(5) 8.; 
		array copamtarray(5) 8.; 
		array mcdarray(5)    $1.; 
		array mcrarray(5)    $1.;  

		format coppctarray1-coppctarray5 copamtarray1-copamtarray5 f12.2;
			if(first.effdat) then 
				do; 
					t = 1; 
					bencodarray(t) = gridbencod;
					untsmxarray(t) = untsmx; 
					coppctarray(t) = coppct; 
					copamtarray(t) = copamtb;  
					mcdarray(t)    = mcd;
					mcrarray(t)	   = mcr;
				end; 
			else	
				do; 
					t = t + 1; 
					bencodarray(t) = gridbencod; 
					untsmxarray(t) = untsmx; 
					coppctarray(t) = coppct; 
					copamtarray(t) = copamtb; 
					mcdarray(t)    = mcd;
					mcrarray(t)	   = mcr;
				end; 

		if last.effdat then
			do; 
				output; 
			end; 
	end; 
run; 

/* PULL MATCHING RECORDS FROM CLAIMS AND SVCLINES  */
proc sql;
create table csrdataclaims as
select
t1.clamno,t1.lineno,kstrcat(t1.clamno,t1.lineno) as primarykey,t1.membno,t1.bencod,t1.benpkg,t1.grpnum,t1.revind, t1.aptrndat,
	case when t1.revsfr is not null then kstrcat(t1.clamno,t1.revsfr)
	else ""
	end as revsfrkey,
case when t1.revsto is not null then kstrcat(t1.clamno,t1.revsto)
	else ""
end as revstokey,
t1.provno,t1.apsvcstat,t1.edtdat,t1.svcdat,t2.admdat,t1.alwamt,t1.netalwamt,t1.topamt,t1.claamt,t1.dedamt,t1.coiamt,t1.copamt,
t1.rcvamt,t1.dscamt,t1.mcralwamt ,t1.mcrnetalwamt ,t1.mcrtopamt ,t1.mcdalwamt ,t1.mcdnetalwamt ,t1.mcdtopamt ,t1.cstshrnetalcoiamt,
t1.cstshrnetalcopamt,t1.cstshrnetaldedamt,t1.cstshrtopaycoiamt,t1.cstshrtopaycopamt,t1.cstshrtopaydedamt,t1.nonamt,
case when t1.unitct eq 0 then 1
	when t1.unitct gt 0 then ceil(t1.unitct)
	when t1.unitct lt 0 then floor(t1.unitct)
	else 0 end as unitct,
case when t1.covunt eq 0 then 1
	when t1.covunt gt 0 then ceil(t1.covunt)
	when t1.covunt lt 0 then floor(t1.covunt)
	else 0 end as covunt,
case when t1.alwunt eq 0 then 1
	when t1.alwunt gt 0 then ceil(t1.alwunt)
	when t1.alwunt lt 0 then floor(t1.alwunt)
	else 0 end as alwunt,
t1.mcralwunt ,t1.mcrcovunt ,t1.mcrcovdys ,t1.mcrapcovdys ,t1.mcrutilct ,t1.mcraputilct,t1.mcdalwunt ,t1.mcdcovunt ,t1.mcdcovdys,
t1.mcdapcovdys ,t1.mcdutilct ,t1.mcdaputilct,t1.compno,t1.covdys,t1.aputilct,t1.utilct,t1.aptrnamt,t1.apcovdys,t1.mcraptrnamt,
t1.mcdaptrnamt
,CASE WHEN T1.COMPNO EQ "34" THEN T1.SUBPROD 	/*ALM 5682 - BC Added SUBPRODUCT field*/
      WHEN T1.COMPNO EQ "60" THEN "MSO" 
ELSE "MCRCCARE"
END AS SUBPRODUCT,
t2.valcd01, t2.valcd02, t2.valcd03, t2.valcd04, t2.valcd05, t2.valcd06, t2.valcd07, t2.valcd08, t2.valcd09, t2.valcd10, t2.valcd11, 
t2.valcd12, t2.vala01, t2.vala02, t2.vala03, t2.vala04, t2.vala05, t2.vala06, t2.vala07, t2.vala08, t2.vala09, t2.vala10, t2.vala11, 
t2.vala12 
from /*&libn1.*/rsstr.svclines t1 left join /*&libn1.*/rsstr.claims t2 on t1.clamno eq t2.clamno;
quit;

data bencod_rate( rename=ratecode_new=ratecode);
format ratecode_new 9.2;
length ratecode_new 8.;
set bencodes;
do i=1 by 1 while(scan(ratecode,i,', ') ^=' ');
ratecode_new=scan(ratecode,i,', ');
output;
end;
drop ratecode  i;
run;
proc sql;
create table csrdataclaims_rate as select * from csrdataclaims  t1, bencod_rate t2 
where  (t1.VALCD01='24' or t1. VALCD02='24' or t1.VALCD03='24' or  
t1.VALCD04='24' or  t1.VALCD05='24' or t1.VALCD06='24' or t1.VALCD07='24' or  t1.VALCD08='24' 
or t1.VALCD09='24' or t1.VALCD10='24' or  t1.VALCD11='24' or t1.VALCD12='24') 
and ((t1.vala01 = t2.ratecode) or (t1.vala02 = t2.ratecode) or (t1.vala03 = t2.ratecode)
 or (t1.vala04 = t2.ratecode) or (t1.vala05 = t2.ratecode) or (t1.vala06 = t2.ratecode) or (t1.vala07 = t2.ratecode) 
 or (t1.vala08 = t2.ratecode) or (t1.vala09 = t2.ratecode) or (t1.vala10 = t2.ratecode) or (t1.vala11 = t2.ratecode) or 
(t1.vala12 = t2.ratecode));

quit;
proc sql;
create table csrdataclaims_ben as select * from csrdataclaims 
where clamno not in (select distinct clamno from csrdataclaims_rate) ;
quit;

proc sql;
create table csrdataclaims_rat as select * from csrdataclaims 
where clamno  in (select distinct clamno from csrdataclaims_rate) ;
quit;


proc sql;
create table csrdata_1 as
select t1.*,t2.*
		   ,case when t1.admdat is not null then t1.admdat
                     else t1.svcdat 
                end as episdate format = mmddyy10. /*episode date */ 
		   ,case when t1.revsfrkey is not null then 0
				when t1.revsfrkey is null then 1 
				else . 
		   end as negation /* indicates if the record was a reversal */ 
		 ,case 	when days_trigger = "N" then t1.alwunt 
	  			when days_trigger = "Y" then t1.unitct 
				end as bendays /* benefit days: holds the days value used to calculate standard amounts */
from csrdataclaims_rat  t1, bencod_rate t2  where ((t1.svcdat between t2.effdat and t2.expdat) AND
 T1.SUBPRODUCT EQ T2.gridbenpkg	and ((t1.vala01 = t2.ratecode) or (t1.vala02 = t2.ratecode) or (t1.vala03 = t2.ratecode)
 or (t1.vala04 = t2.ratecode) or (t1.vala05 = t2.ratecode) or (t1.vala06 = t2.ratecode) or (t1.vala07 = t2.ratecode) 
 or (t1.vala08 = t2.ratecode) or (t1.vala09 = t2.ratecode) or (t1.vala10 = t2.ratecode) or (t1.vala11 = t2.ratecode) or 
(t1.vala12 = t2.ratecode)))
/*ALM 5682 - BC Added SUBPRODUCT condition*/
order by membno, edtdat;
quit;

/* B-297306- Create data set to remove missing bencode values from benefit grid file -START  */
data bencod_ben;
set bencodes;
where compress(gridbencod) is not missing;
run;
/* B-297306- Create data set to remove missing bencode values from benefit grid file -END  */

/* PULL SVCLINES DATA AND JOIN WITH GRID */
proc sql;
create table csrdata_2 as
select t1.*,t2.*, . as ratecode 
		   ,case when t1.admdat is not null then t1.admdat
                     else t1.svcdat 
                end as episdate format = mmddyy10. /*episode date */ 
		   ,case when t1.revsfrkey is not null then 0
				when t1.revsfrkey is null then 1 
				else . 
		   end as negation /* indicates if the record was a reversal */ 
		 ,case 	when days_trigger = "N" then t1.alwunt 
	  			when days_trigger = "Y" then t1.unitct 
				end as bendays /* benefit days: holds the days value used to calculate standard amounts */
from csrdataclaims_ben  t1, /*bencodes*/ bencod_ben (drop=ratecode) t2  /* B-297306 updated bencod_ben table from bencodes*/
where ((t1.svcdat between t2.effdat and t2.expdat) and 
(t1.bencod =t2.gridbencod)) AND T1.SUBPRODUCT EQ T2.gridbenpkg	/*ALM 5682 - BC Added SUBPRODUCT condition*/
order by membno, edtdat;
quit;

data csrdata;
set csrdata_1 csrdata_2;
run;
/* pull cost share requires data */
data csrinput1 ;
	set csrdata;
	if missing(coppct)=1 then coppct=0.00;
	if missing(copamtb)=1 then copamtb=0.00;
	where ((apsvcstat eq "PA") or (apsvcstat eq "PD" and revind eq "Y")) ;
	year=year(svcdat);
run;

proc sql; 
	create table csrdata1 as
	select t1.*, t2.grpmax
	from csrinput1 t1
	inner join bencodmax t2 on (t1.grp eq t2.grp) and (t1.svcdat between t2.effdat and t2.expdat) AND T1.SUBPRODUCT EQ T2.gridbenpkg	/*ALM 5682 - BC Added SUBPRODUCT Condition*/
	order by membno, edtdat;
quit;

data csrdata0;
	set csrdata1;
run;

proc sort data=csrdata1 out=yearcall(keep=year) nodupkey;by year;run;

data yearcall;
set yearcall;
where year>=2015;
run;

%macro spltlogic (year);

data csrdata1_&year.;
	set csrdata0;
	where (svcdat between "01jan&year."d and "31dec&year."d);* and (aptrndat between "01jan&year."d and "31dec&year."d);
run;

/* create base table for member sequence data step  */ 
proc sql; 
	create table memseq_&year. as 
	select distinct membno, SUBPRODUCT /*ALM 5682 - BC Added SUBPRODUCT*/
	from csrdata1_&year.
	order by membno, SUBPRODUCT; 
quit; 

/* number each subscribers' member  */ 
data memseq2_&year.; 
	set memseq_&year.; 

	retain membseq 0; 
	by membno SUBPRODUCT; 	/*ALM 5682 - BC Added SUBPRODUCT*/

	if (first.membno) then
		do; 
			membseq = 1;
		end;  
	else 
		do; 
			membseq = membseq+1; 
		end; 
	output; 
run; 

/* pull all the episode dates, and count how many claims occured on that date */ 
proc sql; 
	create table episodseq_&year. as	
		select membno, SUBPRODUCT, episdate, provno, count(*) as count	/*ALM 5682 - BC Added SUBPRODUCT*/
	from csrdata1_&year.
	where (provno is not null) and (episdate is not null) and (membno is not null)
	group by 1,2,3, 4
	having count gt 1; 
quit;

/*  create episode sequence numbers from episode dates count */
data episodseq2_&year.; 
	set episodseq_&year.; 
	retain episid 0; 
	by membno SUBPRODUCT;	/*ALM 5682 - BC Added SUBPRODUCT*/ 

	if (first.membno) then
		do; 
			episid = 1;
		end;  
	else 
		do; 
			episid = episid+1; 
		end; 
	output; 
run; 

proc sql; 
	create table maxepis_&year. as	
	select unique membno, SUBPRODUCT, max(episid) as maxepisid 	/*ALM 5682 - BC Added SUBPRODUCT*/
	from episodseq2_&year. 
	group by membno, SUBPRODUCT;
quit;

/* join the member sequence values to the csr input table for the csr data step. apply final order logic. */ 
proc sql; 
	create table csrinput2_&year. as 
		select  t1.*
				,t2.membseq 
				,case when t3.episid is null then t3.episid eq 0 
					else t3.episid
				end as episid
				,t4.maxepisid
				/* SAS2AWS2: ReplacedFunctionSubstr */
				,case when anydigit(ksubstr(bencod,3,1)) then ksubstr(bencod,1,2)
			         else bencod 
		        end as grp 
	from csrdata1_&year.(drop=grp) t1
	left join memseq2_&year. t2 on t1.membno eq t2.membno and t1.subproduct eq t2.subproduct	/*ALM 5682 - BC Added SUBPRODUCT condition*/
	left join episodseq2_&year. t3 on t1.membno eq t3.membno and t1.episdate eq t3.episdate and t1.provno eq t3.provno and t1.subproduct eq t3.subproduct
	left join maxepis_&year. t4 on t1.membno eq t4.membno and t1.subproduct eq t4.subproduct
 	order by t1.membno, t1.episdate, t1.clamno, t1.lineno;
quit; 

/* for benefit codes with days_trigger eq "y" check the epis_id and their episode_dates. 
   if their dates are le benefit_period, then the previous episode id remains*/ 
data csrinput3pre2_&year.
(keep = primarykey membno episdate  bendays maxepisid episid benefitperiod bencod days_trigger grp perioddays episidupdated);
do until(last.membno); 
     array periodgrp{9999} $ 5.  periodgrp1-periodgrp9999;
     array periodedt{9999}      periodedt1-periodedt9999;
     array periodeid{9999}      periodeid1-periodeid9999;
	 array periodcount{9999}    periodcount1-periodcount9999; 

	 set csrinput2_&year.;

	 by membno episdate;
	 format var2 mmddyy10.; 
	 format var5 mmddyy10.; 

	 retain l; 
	 retain ll; 
	
			if missing(maxepisid)=1 then 
				do; 
					maxepisid = episid; 
				end; 
		 
			if first.membno then 
				do;
			        l = 1;
					periodgrp(l)   = grp;
		    		periodedt(l)   = episdate;
		     		periodeid(l)   = episid;

			          do ll = 1 to 9999;
			              periodgrp(ll) = '';  periodedt(ll) = 0;  periodeid(ll) = 0;
			          end;

					  perioddays = 0; 
		     	end;
			else 
				do; 
					l = l + 1;

					periodgrp(l)   = grp;
		    		periodedt(l)   = episdate;
		     		periodeid(l)   = episid;
				end; 

		/* SAS2AWS2: ReplacedFunctionUpcase */
		if kupcase(days_trigger) eq "Y" then
			do; 
				if first.membno and last.membno and episid gt 0 then 
					do; 
						episidupdated = episid; 
					end; 
				else if first.membno and last.membno and episid eq 0 then 
					do; 
						episidupdated = episid; 
					end;
				else if first.membno and episid gt 0 then 
					do; 
						episid = episid; 
						episidupdated = episid; 
					end; 
				else if first.membno and episid eq 0 then 
					do; 
						episidupdated = sum(maxepisid, 1);
					end; 
	  	 
				do; 
			          do j = 2 to l;
					  	  	
			              do k = j to 1 by -1;				  		
										  		
			                   if periodgrp(j) = periodgrp(k) then 
								do;
									
									var1 = periodgrp(j); 
									var2 = periodedt(j);
									var3 = periodeid(j); 

			 						var4 = periodgrp(k); 
									var5 = periodedt(k); 
									var6 = periodeid(k); 
							   		
									periodcount(j) = intck('day', var2, var5);
									perioddays = abs(periodcount(j)); 
			 	
			                        if perioddays le benefitperiod then 
										do;
			                             	periodeid(j) = periodeid(k);
											episidupdated = periodeid(j); 

											if episidupdated = 0 then 
												do; 
													episidupdated = sum(maxepisid, 1);
												end; 
											else 
												do; 
													episidupdated = episidupdated; 
												end; 
								    	end;
									
			                   end;
										
			              end;
			          end;
				end; 
		end; 
	/* SAS2AWS2: ReplacedFunctionUpcase */
	else if kupcase(days_trigger) eq "N" then
		do; 
			episidupdated = episid; 
		end; 	

		episid = episidupdated; 
		if missing(episid)=1 then episid=0;
		output;
end;
run;

proc sql; 
	create table csrinput3_&year. as
	select t1.*, t2.episid
	from csrinput2_&year. (drop = episid) t1
	left join csrinput3pre2_&year. t2 on t1.primarykey eq t2.primarykey
	order by membno, episid, grp, primarykey;
quit; 


data csrinput4pre1_&year. (keep = primarykey revsfrkey revstokey episid clamno lineno membno netalwamt topamt effdat expdat episdate bencod untsmx bendays 
			grp grptotal bendaysepis grpmax days_trigger z xx); 

do until(last.membno); 

		array episdaysaccum(99999) 8.; /*array accumulator for benefit days per episode */ 
		array bengrpaccum(99999) 8.; /*array accumulator for benefit days per episode for benefit group*/ 

		
		set csrinput3_&year.; 
		by membno episid grp primarykey;

/*		where days_trigger eq "Y";*/
	 
		retain z; 
		retain xx 0; 

		if revsfrkey eq "" then bendays = abs(bendays); 
		else bendays = -abs(bendays); 

		if episid gt 0 then  
			do; 
				if first.episid then	
					do; 
						z = 1; 
								episdaysaccum(z) = bendays; 
								bendaysepis = episdaysaccum(z); 
					end; 
				else do; 
						z = sum(z,1); 
								episdaysaccum(z) = sum(bendaysepis,bendays); 
					    		bendaysepis = episdaysaccum(z);

					end; 
			end;

		if first.grp then 
			do; 
				xx = 1; 
					bengrpaccum(xx) = bendays; 
					grptotal = bengrpaccum(xx); 
			end; 
			else do; 
				xx = sum(xx,1);
					bengrpaccum(xx) = sum(grptotal,bendays); 
		    		grptotal = bengrpaccum(xx);
		end; 

		if episid eq 0 or (netalwamt eq 0 and revsfrkey ne "") then 	
			do; 
				bendaysepis = 0;
				grptotal = grptotal;  
			end; 
		output;
	end; 
run; 

proc sql;
	create table csrinput4_&year. as
	select t1.*, t2.grptotal, t2.bendaysepis
	from csrinput3_&year. t1
	left join csrinput4pre1_&year. t2 on t1.primarykey eq t2.primarykey
	order by membno, episid, grp, primarykey;		/*ALM 5682 - BC Added episid grp primarykey*/
quit; 

proc sql; 
create table csrinput5_&year. as
	select t1.*, t2.bencodarray1,t2.bencodarray2,t2.bencodarray3,t2.bencodarray4,t2.bencodarray5,
	t2.copamtarray1,t2.copamtarray2,t2.copamtarray3,t2.copamtarray4,t2.copamtarray5,
	t2.coppctarray1,t2.coppctarray2,t2.coppctarray3,t2.coppctarray4,t2.coppctarray5,
	t2.mcdarray1,t2.mcdarray2,t2.mcdarray3,t2.mcdarray4,t2.mcdarray5,
	t2.mcrarray1,t2.mcrarray2,t2.mcrarray3,t2.mcrarray4,t2.mcrarray5,
	t2.untsmxarray1,t2.untsmxarray2,t2.untsmxarray3,t2.untsmxarray4,t2.untsmxarray5
	from csrinput4_&year. t1 left join bencodtranspose t2 
	on (t1.grp eq t2.grp) and (t1.effdat eq t2.effdat) and (t1.expdat eq t2.expdat) and (t1.subproduct eq t2.gridbenpkg)	/*ALM 5682 - BC Added subproduct condition*/
	order by t1.membno, t1.primarykey;
quit;

/*DAYS TRIGGER LOGIC FOR DAYS_TRIGGER EQ "Y" ONLY*/
data csrinput6_&year.
	(keep = primarykey membno netalwamt topamt coppct copamtb unitct covunt
			clamno lineno episid revsfrkey days_trigger mcd mcr bencod grp  bendays grpmax grptotal  episdate 
			maxcopaysplit maxcopaymcr maxcopaymcd maxcopay 
			var1 var2 var3 var4 var5
			apcovdys mcrapcovdys mcdapcovdys	/*ALM 5682 - BC Added apcovdys mcrapcovdys mcdapcovdys fields*/
	);
	do until(last.membno); 
		set csrinput5_&year.; 
		by membno; 
		where days_trigger eq "Y"; 
			array bencoda (5) bencodarray1-bencodarray5; 
			array untsmxa (5) untsmxarray1-untsmxarray5; 
			array coppcta (5) coppctarray1-coppctarray5; 
			array copamta (5) copamtarray1-copamtarray5; 
			array mcdarraya (5) mcdarray1-mcdarray5; 
			array mcrarraya (5) mcrarray1-mcrarray5; 

			retain var1 0; 
			retain var2 0; 
			retain var3 0; 
			retain var4 0; 
			retain var5 0; 

			grpmax = sum(untsmxa{1},untsmxa{2},untsmxa{3}); 

			if episid eq 0 then do; 
					bendays_epis = bendays; 
					grptotal = bendays; 
			end;

			if revsfrkey eq "" and topamt ne 0 and netalwamt ne 0 then 
				do; 
						apcovdys=bendays;	/*ALM 5682 - BC Added apcovdys*/
					if grptotal le untsmxa{1} then 
						do; 

							maxcopay = bendays * coppcta{1}; 
							maxcopaysplit = maxcopay; 
							maxcopaymcd = maxcopaysplit; 
							maxcopaymcr = sum(netalwamt, -maxcopaysplit); 
							mcrapcovdys = bendays; 	/*ALM 5682 - BC Added mcrapcovdys*/
							mcdapcovdys = 0;	/*ALM 5682 - BC Added mcdapcovdys*/
						end; 
					else if grptotal gt untsmxa{1} and (grptotal le sum(untsmxa{1},untsmxa{2})) then 	
						do; 
							var1 = sum(grptotal, -bendays); /*value of grp_total in previous line */ 
							if var1 le untsmxa{1} then
								do; 
									var2 = sum(untsmxa{1},-var1); /* number of benefit days left over after first unit max is hit */ 	
									var3 = sum(grptotal,-untsmxa{1}); /* number of benefit days above untsmxa{1} */ 
									maxcopay = sum((var2 * coppcta{1}),(var3 * coppcta{2}));  
									maxcopaysplit = maxcopay; 
									maxcopaymcd = maxcopaysplit; 
									maxcopaymcr = sum(netalwamt, -maxcopaysplit);
									mcrapcovdys = bendays;  /*ALM 5682 - BC Added mcrapcovdys*/
									mcdapcovdys = 0; /*ALM 5682 - BC Added mcdapcovdys*/
								end; 
							else if var1 gt untsmxa{1} then 
								do;
									var2 = bendays; /* number of benefit days left over after first unit max is hit */ 	
									var3 = 0; 
									maxcopay = (var2 * coppcta{2});  
									maxcopaysplit = maxcopay; 
									maxcopaymcd = maxcopaysplit; 
									maxcopaymcr = sum(netalwamt, -maxcopaysplit);
									mcrapcovdys = bendays; 	/*ALM 5682 - BC Added mcrapcovdys*/
									mcdapcovdys = 0;		/*ALM 5682 - BC Added mcdapcovdys*/
								end;  	
							end; 
					else if (grptotal gt grpmax) then 
						do; 
							var1 = sum(grptotal, -bendays); /*value of grptotal in previous line */ 
							if var1 le grpmax then 
								do; 
									var2 = sum(grpmax, -var1); /* number of unit max days left to reach grp_max*/ 
									var4=round(netalwamt/abs(bendays),.01); /*COST PER DAY FOR THAT CLAIM */ 
									var5=(var2*var4); /*TOTAL COST FOR THE DAYS REMAINING DAYS*/ 

									maxcopay = (var2*coppcta{2}); 
									maxcopaysplit = maxcopay; 

									maxcopaymcr = sum(var5, -maxcopaysplit);
									maxco=sum(maxcopaymcr,maxcopaysplit);
									maxcopaymcd = sum(netalwamt,-maxco); 
									mcdapcovdys = sum(bendays, -var2);	/*ALM 5682 - BC Added mcdapcovdys*/ 
									mcrapcovdys = var2; 	/*ALM 5682 - BC Added mcrapcovdys*/
								end; 
							else
								do; 
									maxcopay = 0; 
									maxcopaysplit = maxcopay; 
									maxcopaymcd = sum(netalwamt, -maxcopaysplit); 
									maxcopaymcr =  maxcopaysplit;
									var1 = 0; 
									var2 = 0; 
									var3 = 0;
									var4 = 0;
									var5 = 0;
									mcrapcovdys = 0; /*ALM 5682 - BC Added mcrapcovdys*/
									mcdapcovdys = bendays;	/*ALM 5682 - BC Added mcdapcovdys*/
								end; 
						end; 			
				end; 
			else 
				do; 
					maxcopay = 0; 
					maxcopaysplit = 0; 
					maxcopaymcd = 0; 
					maxcopaymcr = 0; 
					var1 = 0; 
				end; 

		output; 
	end; 
run; 

proc sql; 
	create table csrinput7pre1_&year. as
	select t1.*, t2.maxcopay, t2.maxcopaysplit, t2.maxcopaymcd, t2.maxcopaymcr,	t2.grpmax,t2.grptotal
		  ,case when t2.apcovdys ne . then t2.apcovdys else t1.apcovdys end as apcovdys2 /*ALM 5682 - BC Updated the condition with apcovdys, mcdapcovdys, mcrapcovdys*/
		  ,case when t2.mcrapcovdys ne . then t2.mcrapcovdys else t1.mcrapcovdys end as mcrapcovdys2
    	  ,case when t2.mcdapcovdys ne . then t2.mcdapcovdys else t1.mcdapcovdys end as mcdapcovdys2
	from csrinput3_&year. (drop=grpmax) t1
	left join csrinput6_&year. t2 on t1.primarykey eq t2.primarykey;
quit;

/*SET THE MAX_COPAY FOR ALL DAYS_TRIGGER EQ "N" RECORDS */ 
data csrinput7_&year.; 
	set csrinput7pre1_&year.; 
 
	apcovdys = apcovdys2;			/*ALM 5682 - BC Added apcovdys*/ 
	mcdapcovdys = mcdapcovdys2;		/*ALM 5682 - BC Added mcdapcovdys*/
	mcrapcovdys = mcrapcovdys2;		/*ALM 5682 - BC Added mcrapcovdys*/

	if days_trigger = "N" and revsfrkey eq "" /*and topamt ne 0 */ and netalwamt ne 0  then 
			do; 
				maxcopay = min(abs((coppct*bendays)),(copamtb));  /* THE MAXIMUM CO-PAYMENT AMOUNT*/
				copamtb = copamtb; 

				if mcd = "Y" and mcr = "Y" then 
					do; 
						maxcopaysplit = maxcopay;
						maxcopaymcd = maxcopaysplit;
						maxcopaymcr = 0;  
					end;
				else 
					do; 
						maxcopaysplit = 0;
						maxcopaymcd = 0;
						maxcopaymcr = 0;
					end; 						 
			end; 
		else if days_trigger = "Y" and revsfrkey eq "" /*and topamt ne 0*/ and netalwamt ne 0 then
			do; 
			copamtb = netalwamt; 
			maxcopay = maxcopay; 
			maxcopaysplit = maxcopaysplit; 
			maxcopaymcd = maxcopaymcd; 
			maxcopaymcr = maxcopaymcr; 
		end; 
run; 

/*final sort order logic for rewrite*/ 
proc sort data = csrinput7_&year.; 
	by membno edtdat negation clamno descending maxcopay lineno; 
run; 

/*  component splits */ 
data finalsnf_&year. (keep=clamno lineno mcdalwunt mcralwunt  mcdtopamt mcrtopamt mcdnetalwamt mcrnetalwamt mcdalwamt mcralwamt   
   mcdcovunt mcrcovunt mcdcovdys mcrcovdys mcdapcovdys mcrapcovdys mcdutilct mcrutilct mcdaputilct mcraputilct 
   mcdaptrnamt mcraptrnamt cstshrnetalcoiamt cstshrnetalcopamt cstshrnetaldedamt cstshrtopaycoiamt cstshrtopaycopamt cstshrtopaydedamt
   mcr mcd topamt netalwamt days_trigger coipct revsfrkey revstokey primarykey bencod revind gridcoiamt
/*   mcdapcovdys mcrapcovdys*/
/*membno clamno lineno bencod bendays svcdat maxcopay netalwamt topamt revsfrkey  oopgrp*/
/*cstshrnetalcoiamt cstshrnetalcopamt cstshrnetaldedamt cstshrtopaycoiamt cstshrtopaycopamt cstshrtopaydedamt*/
/*mcd mcdalwamt mcdalwunt mcdapcovdys mcdaptrnamt mcdaputilct mcdcovdys mcdcovunt mcdnetalwamt mcdtopamt*/
/*mcdutilct mcr mcralwamt mcralwunt mcrapcovdys mcraptrnamt mcraputilct mcrcovdys mcrcovunt*/
/*mcrnetalwamt mcrtopamt mcrutilct unitct coppct episdate episid days_trigger*/
/*gridbenpkg subproduct		*/
/*dedamtstdfinal copayamtstdfinal coinsamtstdfinal copayamtstdfinal maxcopay copamtb remainalwaftdedstd edgecond episcopaydiff*/
/*memsharestdfinal epismaxcopay episaccumend*/
);

/* set arrays, inititate loop until last member is met, and sequence input data by member number */ 	
	do until(last.membno);	
		array mdedaccum(999) 8; /* array for member deductible */ 
		array moopaccum(999) 8;  /* array for member oop */ 
		array reversals(99999,21) $ 23.; /*array for reversals */ 
		array episodes(99999,3) $ 23.; /*array for episodes. counts the number of episodes per subscriber per member. */ 
		array episaccum(99999) 8; /*array for episode accumulator */ 
		array annualdays(99999) 8.; /*array for annual accumulator of benefit days */ 
		array episdays(99999,4) 8.; /* array for benefit days for the current episode */ 
		array episdaysaccum(99999) 8.; /*array accumulator for benefit days per episode */ 

		retain orderseq 0;

		set csrinput7_&year.;
		by membno;

			i = membseq;  
			c = episid;
			r = 0; 
			w = 0; 
			orderseq = orderseq+1;

			format moopaccumend 8.2; 
			format dedamtstdfinal 8.2; 
			format copayamtstdfinal 8.2; 
			format coinsamtstdfinal 8.2;

			retain d; 
			retain z;  

    /* episodes logic: variable b will increment for each member episode. 
						 if an event is not an episode, it will have an episode id of zero (0). 
						 dollar values of episodes are zeroed out when there is a reversal. 
						 the episode accumulator will track copayments made for an episode. 
						 an episode maximum copayment value will track the highest value maximum copayment for an episode. */ 

	/* set episodes accumulator array, initiate variable b and set the episodes array values */ 
		if ((first.membno) and (episid gt 0))then 
			do; 
				b = 1; 
			end; 
		else if ((first.membno) and (episid eq 0)) then 
			do; 
				b = 1;
				qq = 0; 
			end; 

		if ((episid gt 0) and (b gt 1)) then 
			do ; 
				episodes(b,1) = primarykey;
		       	episodes(b,2) = put(episid,8.);
		        episodes(b,3) = put(maxcopay,12.2);
			end;
		else if (episid eq 0) then 
			do; 
				episaccumend = 0;
				epismaxcopay = maxcopay;								
			end; 
					
	/* for each episode that has a reversal, set the max copayment to zero (0) */ 
		if ((episid gt 0) and (revsfrkey ne "")) then 
			do ww = 1 by 1 while(ww le 99999);  
				if episodes(ww,1) eq revsfrkey then 
						do; 
							rr = ww;
						 	episodes(ww,3) = "0";
							maxcopay = input(episodes(ww,3),12.2);  
						leave; 
					end; 
			end; 

	/* episode maximum copayment comparison logic: 
			for each member episode, evaluate the max copayment value from the episodes array
			store and assign the highest max copay for that episode. 
			update the episode copay difference. */

		if episid gt 0 then 
			do; 
				epismaxcopay = maxcopay; 
				format epismaxcopay 12.2;
					do qq = 1 by 1 until(qq eq b); 

						if qq lt 0 or qq gt 99999 then
							do; 
								putlog  "QQ OUT OF RANGE " qq 12. ; 
							end; 
						
						if ((input(episodes(qq,2),8.) eq episid) and (input(episodes(qq,3),12.2) eq epismaxcopay)) then 	
								do; 
									epismaxcopay = input(episodes(qq,3),12.2); 
								end; 
						else if ((input(episodes(qq,2),8.) eq episid) and (input(episodes(qq,3),12.2) gt epismaxcopay)) then  
								do; 
									epismaxcopay = input(episodes(qq,3),12.2); 
								end; 
						else if ((input(episodes(qq,2),8.) eq episid) and (input(episodes(qq,3),12.2) lt epismaxcopay)) then
							do; 
								epismaxcopay = epismaxcopay; 
							end; 
					end; 
			
					if ((c eq episid) and (revsfrkey eq "")) then 
							do;
								episaccumend = episaccum(c); 
								if missing(epismaxcopay) then epismaxcopay=0.00;
							    if missing(episaccumend) then episaccumend=0;
								if missing(epismaxcopay)=0 or missing(episaccumend)=0 then 
								episcopaydiff = sum(epismaxcopay, -episaccumend); 
								else episcopaydiff=0.00;
							end;
			end; 
		else if ((episid eq 0) and (revsfrkey eq ""))then 
			do; 
				episaccumend = 0;
				epismaxcopay = maxcopay;	
				episcopaydiff = sum(epismaxcopay, -episaccumend);
				qq = 0; 
			end;

	/*logic for benefit days annual accumulators and benefit days episode accumulators*/ 
		if first.membno then 
			do; 		
				d = 1; 
				annualdays(d) = bendays; 
				bendaysannual = annualdays(d);

				z = 1; 
			end; 
		else 
			do; 
				d = sum(d , 1); 
				annualdays(d) = bendays; 
				bendaysannual = sum(bendaysannual,annualdays(d)); 
 			end; 

    /* set the accumulators and static values for each new member*/ 	
	/* for the first record for each member set mdedaccum and moopaccum to 0 */
		if (first.membno) then  
			do q = 1 by 1 while(q le 10); 
				mdedaccum(q) = 0; 
				moopaccum(q) = 0; 
			end; 
			
	 /* for the first record of each member set the static variables and non-array accumulators */ 
			 
			 
		if (first.membno) then 
			do;  
				mdedcap   = 0;
				moopcap   = 1000000;
			
				a = 1;
				deductind = 1;
			 end; 
			 
			 
		if deductind = 1 and dedgrp = 'DED' then do;
			mdedcap   = dedsmx;
			deductind = 0;
		end;
						 
/* reversals logic: 
		the value for w represents the array position that matches the revsfrkey.
		for each reversals, increment variable w by 1 until the end of all reversals is met. 
		w will be assigned to r if the primary key position (reversals(w,1)) equals reversal-from-key for each w increment.
	    for each reversal increment while the primary key position equals reversals-from-key set the:
			- standard amounts for deductible, copayment, and co-insurance to negative of the original value for that reversal; 
		    - accumulators for deductibles and oops; 
		    - episode accumulator to equal the sum of the previous episode accumulator and the copayment amount standard final; 
			- go to to the exit conditions. 

		if the incremement value for w equals the maximum value for the reversals array, 
		then set all the standard amounts for deductible, copay, coins to zero (0) and go to the exit conditions. */

		if revsfrkey ne "" then do; 
					do w = 1 by 1 while(w le 99999);  
						if ((reversals(w,1) eq revsfrkey))   then 
							do; 
								r = w;
						 		dedamtstdfinal   = round(reversals(r,2)*(-1),.01); 
								copayamtstdfinal = round(reversals(r,3)*(-1),.01);
								coinsamtstdfinal = round(reversals(r,4)*(-1),.01); 
								availalw 		 = round(reversals(r,5)*(-1),.01); 

/*								if days_trigger eq "Y" then do; */
								maxcopay      = round(reversals(r,6)*(-1),.01);
								maxcopaysplit = round(reversals(r,7)*(-1),.01);
								maxcopaymcr   = round(reversals(r,8)*(-1),.01);
								maxcopaymcd   = round(reversals(r,9)*(-1),.01);
/*								end; */

 					 			cstshrnetalcoiamt = round(reversals(r,10)*(-1),.01);
	          					cstshrnetalcopamt = round(reversals(r,11)*(-1),.01);
								cstshrnetaldedamt = round(reversals(r,12)*(-1),.01); 
								cstshrtopaycoiamt = round(reversals(r,13)*(-1),.01); 
								cstshrtopaycopamt = round(reversals(r,14)*(-1),.01); 
								cstshrtopaydedamt = round(reversals(r,15)*(-1),.01); 

								mcdtopamt    = round(reversals(r,16)*(-1),.01); 
								mcdnetalwamt = round(reversals(r,17)*(-1),.01);   
								mcrtopamt    = round(reversals(r,18)*(-1),.01);      
								mcrnetalwamt = round(reversals(r,19)*(-1),.01);    
								/*mcralwunt    = round(reversals(r,20)*(-1),.01);   */ 
								/*mcdalwunt    = round(reversals(r,21)*(-1),.01);   */
								mcralwamt    = round(reversals(r,20)*(-1),.01);   /*[B-83127] */
								mcdalwamt    = round(reversals(r,21)*(-1),.01);   /*[B-83127]*/

								mdedaccum(i) = sum(mdedaccum(i),dedamtstdfinal);
								moopaccum(i) = sum(moopaccum(i),dedamtstdfinal, copayamtstdfinal, coinsamtstdfinal); 	

								mdedaccumend = mdedaccum(i); /* for each array variable, set the mdedaccumstart value */ 
								moopaccumend = moopaccum(i); /* for each array variable, set the moopaccumstart value */ 
																
								if episid gt 0 then 
									do; 
										episaccum(c) = sum(episaccum(c), copayamtstdfinal); 
										episaccumend = episaccum(c); 
										episcopaydiff = sum(epismaxcopay, -episaccumend); 
									end;
								 
 								goto exit;
							end; 
						else if w = 99999 then 
							do; 
								dedamtstdfinal = 0; 
								copayamtstdfinal = 0; 
								coinsamtstdfinal = 0;
								goto exit;
							end; 
					end; 
		end; 
					
/* initiate start accumulators. will reflect the prior accumulator record to the ded or oop cost */ 
		mdedaccumstart = mdedaccum(i); /* for each array variable, set the mdedaccumstart value */ 
		moopaccumstart = moopaccum(i); /* for each array variable, set the moopaccumstart value */ 
		/* total amount available to pay the provider (available allowed [availalw]) */ 
/*		availalw = sum(alwamt,-rcvamt,-nonamt,-dscamt); */
			availalw = netalwamt;
			format availalw 12.2;

/* initiate the differences */ 		
		mdeddiff = sum(mdedcap,-mdedaccum(i)); /* for each array variable, update the mdeddiff value */
		moopdiff = sum(moopcap,-moopaccum(i)); /* for each array variable, update the moopdifft value */
		
/* initiate reached deductible and checks edge condition */   

	/* if member dedutible is met/not-met, update reached deductible values */ 
		if (round(mdeddiff,1) eq 0) then 
				reachded = 1;
		else 
				reachded = 0;

	/* if  member oop is met/not-met; or reached deductible is true;  update edge condition values */ 
		if (round(moopdiff,1) eq 0) then  
				edgecond = 9999; 
		else if reachded = 1 then 
				edgecond = moopdiff; 
		else do;
				edgecond = min(moopdiff, mdeddiff); /* edge condition is the value remaining until the next  milestone */ 
		end;
		edgecondpreded = edgecond    ; /* shows edge condition value for that service line record */
		
/* deductible start  */ 
		/* SAS2AWS2: ReplacedFunctionCompress */
		if ((reachded eq 1) or (edgecond eq 9999) or (kcompress(dedgrp) ne "DED")) then
		
				dedamtstdfinal = 0; 	/* dedamtstdfinal = the deductible amount final due to medicaid */
			
		/* SAS2AWS2: ReplacedFunctionCompress */
		else if (kcompress(dedgrp) eq "DED") then
					dedamtstdfinal = min(availalw,dedsmx,edgecond); 
			else 
					dedamtstdfinal = 9999.99; 
			/* amount remaining after deductible is paid, eligible for copay or coinsurance */ 
		remainalwaftdedstd = sum(availalw, -dedamtstdfinal); 
	/* deductible end */ 

	/* update accumulators, differences and edge condition*/ 

		mdedaccum(i)  = sum(mdedaccum(i),dedamtstdfinal);
		mdedaccumend = mdedaccum(i); 
	 	
		moopaccum(i)  = sum(moopaccum(i),dedamtstdfinal);
		moopaccumend = moopaccum(i);  

		moopdiff = sum(moopcap,-moopaccum(i));

		if round(moopdiff, 1) eq 0 then	/* if member oop is met/not-met, update edge condition */ 
					edgecond = 9999; 
			else do;
					edgecond = moopdiff; 
			end;

		edgecondprecopay = edgecond; 
		
/* co-pay start  */ /* copayamtstdfinal = the copay amount final due to medicaid */  
		if ((edgecond eq 9999) or (oopgrp ne "OOP")) then copayamtstdfinal = 0; 
		else 
        if (oopgrp eq "OOP") then copayamtstdfinal = min((min(maxcopay,copamtb)),remainalwaftdedstd,edgecond,episcopaydiff); 
		
		/* amount remaining after deductible and copay is paid, eligible for co-insurance */ 
		remainalwaftcopaystd = sum(remainalwaftdedstd, -copayamtstdfinal); 
	/*  copay end */

	/* update accumulators and edge condition*/ 	
		moopaccum(i) = sum(moopaccum(i), copayamtstdfinal);
		moopaccumend = moopaccum(i);	
					
		moopdiff = sum(moopcap,-moopaccum(i));
		
		if round(moopdiff,1) eq 0 then edgecond = 9999; 			
		else edgecond = moopdiff; 

		edgecondprecoins = edgecond; /* shows edge condition value for that service line record */ 

	/* coins start */

		if ((edgecond eq 9999) or (oopgrp ne "OOP")) then
				coinspctstdperc = 0;  /*coinspctstdperce = co-insurance percent final due to medicaid*/ 
		else if (oopgrp eq "OOP") then	coinspctstdperc = round((coipct *.01),0.001);				
				else coinspctstdperc = 99.99;  
		

		coinsamtstdfinal = round(min((remainalwaftcopaystd*coinspctstdperc),edgecond),.01); /*the co-insurance final due to medicaid */ 
		remainalwaftcoinsstd = sum(remainalwaftcopaystd, -coinsamtstdfinal); /* amount remaining after co-insurance is paid*/ 

	/* coins end */

	/* update accumulators */ 
		moopaccum(i) = sum(moopaccum(i), coinsamtstdfinal);
		moopaccumend = moopaccum(i); 
   

		/* these values shall always equal allowed amount */
		alwvalidation = sum(dedamtstdfinal ,copayamtstdfinal , coinsamtstdfinal , remainalwaftcoinsstd);  

		/* these values shall always resolve to zero */ 
		alwdiff = sum(availalw, -alwvalidation); 
       
		/* update the exit condition for the reversals logic, increment a to move to the next reversal */ 
		exit: 
			if revstokey ne "" then do;
				reversals(a,1) = primarykey; 
				reversals(a,2) = dedamtstdfinal; 
				reversals(a,3) = copayamtstdfinal; 
				reversals(a,4) = coinsamtstdfinal;
			    reversals(a,5) = availalw; 
				reversals(a,5) = availalw; 
				reversals(a,6) = maxcopay; 
				reversals(a,7) = maxcopaysplit; 
				reversals(a,8) = maxcopaymcr; 
				reversals(a,9) = maxcopaymcd; 	
			end;
/*				reversals(a,10) = cstshrnetalcoiamt ; */
/*				reversals(a,11) = cstshrnetalcopamt ; */
/*				reversals(a,12) = cstshrnetaldedamt ; */
/*				reversals(a,13) = cstshrtopaycoiamt ;*/
/*		        reversals(a,14) = cstshrtopaycopamt ; */
/*				reversals(a,15) = cstshrtopaydedamt ; */
/**/
/*				reversals(a,16) = mcdtopamt ; */
/*				reversals(a,17) = mcdnetalwamt ; */
/*				reversals(a,18) = mcrtopamt ; */
/*				reversals(a,19) = mcrnetalwamt ; 	*/
/*				reversals(a,20) = mcralwamt; */
/*				reversals(a,21) = mcdalwamt; 	*/
/**/
/*				a = sum(a, 1); */
/*			end; */
			
        /*update episode accumlators, and incrememt b to move to the next episode */ 
			
		if ((episid gt 0) and (c eq episid) and (revsfrkey eq "")) then 
			do; 
				episaccum(c) = sum(episaccum(c), copayamtstdfinal);
				b = sum(b , 1);
			end;
			
			origcostshare = round(sum(dedamt,copamt,coiamt),0.01); /* medicare component */ 
			memsharestdfinal = round(sum(dedamtstdfinal,copayamtstdfinal,coinsamtstdfinal),0.01); /* medicaid component */ 
			
			year = year(episdate); 

      /* update the csr amount.*/ 
	  /* update allowed amounts, net allowed amounts, and to pay amounts for medicare and medicaid. */ 	
		csramt = sum(memsharestdfinal, -origcostshare); 
		
		if days_trigger eq "N" and revsfrkey eq "" and netalwamt ne 0 /*and topamt ne 0*/ then	do; 
				if (mcr = "Y" and mcd = "N") then do; 
					 mcdalwunt       =  0;
			         mcdtopamt       =  0;
			         mcdnetalwamt    =  0;
			         mcdalwamt       =  0;
			         mcdcovunt       =  0;
			         mcdcovdys       =  0;
			         mcdapcovdys     =  0;
			         mcdutilct       =  0;
			         mcdaputilct     =  0;
			         mcdaptrnamt     =  0;

					 cstshrnetalcoiamt   =  0;
					 cstshrnetalcopamt   =  0;
					 cstshrnetaldedamt   =  0;
					 cstshrtopaycoiamt   =  0;
					 cstshrtopaycopamt   =  0;
					 cstshrtopaydedamt   =  0;

			         mcralwunt       =  alwunt;
			         mcrtopamt       =  topamt;
			         mcrnetalwamt    =  netalwamt;
			         mcralwamt       =  alwamt;
			         mcrcovunt       =  covunt;
			         mcrcovdys       =  covdys;
			         mcrapcovdys     =  apcovdys;
			         mcrutilct       =  utilct;
			         mcraputilct     =  aputilct;
			         mcraptrnamt     =  aptrnamt;
					end; 
				else if (mcr = "N" and mcd = "Y") then do; 
					 mcdalwunt       =  alwunt;
			         mcdtopamt       =  topamt;
			         mcdnetalwamt    =  netalwamt;
			         mcdalwamt       =  alwamt;
			         mcdcovunt       =  covunt;
			         mcdcovdys       =  covdys;
			         mcdapcovdys     =  apcovdys;
			         mcdutilct       =  utilct;
			         mcdaputilct     =  aputilct;
			         mcdaptrnamt     =  aptrnamt;

					 cstshrnetalcoiamt   =  0;
					 cstshrnetalcopamt   =  0;
					 cstshrnetaldedamt   =  0;
					 cstshrtopaycoiamt   =  0;
					 cstshrtopaycopamt   =  0;
					 cstshrtopaydedamt   =  0;

			         mcralwunt       =  0;
			         mcrtopamt       =  0;
			         mcrnetalwamt    =  0;
			         mcralwamt       =  0;
			         mcrcovunt       =  0;
			         mcrcovdys       =  0;
			         mcrapcovdys     =  0;
			         mcrutilct       =  0;
			         mcraputilct     =  0;
			         mcraptrnamt     =  0;
					end; 
		else if (mcr = "Y" and mcd = "Y") or (mcr = "N" and mcd = "N") then do; 
				 mcdalwunt     =  alwunt;
			     mcdcovunt     =  covunt;
			     mcdcovdys     =  covdys;
			     mcdapcovdys   =  apcovdys;
			     mcdutilct     =  utilct;
			     mcdaputilct   =  aputilct;
				 cstshrnetalcoiamt  =  coinsamtstdfinal;
				 cstshrnetalcopamt  =  copayamtstdfinal;
				 cstshrnetaldedamt  =  dedamtstdfinal;
				 if netalwamt ne 0 then do;
					 cstshrtopaycoiamt  =  round((topamt/netalwamt)*cstshrnetalcoiamt,.01);
					 cstshrtopaycopamt  =  round((topamt/netalwamt)*cstshrnetalcopamt,.01);
					 cstshrtopaydedamt  =  round((topamt/netalwamt)*cstshrnetaldedamt,.01);
				 end;
				 else do;
		 			 cstshrtopaycoiamt  =  0;
					 cstshrtopaycopamt  =  0;
					 cstshrtopaydedamt  =  0;
		         end;

				 mcdalwamt   = availalw; /*medicaid available allowed amount*/ 
				 mcdnetalwamt= 0; /*medicaid net allowed amount*/ 
				 mcdtopamt    = 0 ;

				 mcralwamt   = availalw; /*medicare avialable allowed amount*/ 
				 mcrnetalwamt= sum(availalw, -memsharestdfinal); /*medicare net allowed amount*/ 
			 	if missing(availalw)=1 or availalw eq 0.00 or availalw eq 0 then do;
					  mcrtopamt    = 0;
			 	end;
			 	else do;
				  mcrtopamt    = round((topamt/availalw)*mcrnetalwamt,.01); /*medicare to pay amount (topamt/availalw)=with-hold amount*/ 
				 end;
			  end; 
		end;
	else if days_trigger eq "Y" and revsfrkey eq "" and netalwamt ne 0 /*and topamt ne 0*/ then do;
					if (grptotal le grpmax) then do;
							cstshrnetalcoiamt   =  0;
		  					cstshrnetalcopamt   =  maxcopaysplit;
							cstshrnetaldedamt   =  0;
							if netalwamt ne 0 then do;
								cstshrtopaycoiamt   =  round((topamt/netalwamt)*cstshrnetalcoiamt,.01);
								cstshrtopaycopamt   =  round((topamt/netalwamt)*cstshrnetalcopamt,.01);
								cstshrtopaydedamt   =  round((topamt/netalwamt)*cstshrnetaldedamt,.01);
							end;
							else do;
								cstshrtopaycoiamt   =  0;
								cstshrtopaycopamt   =  0;
								cstshrtopaydedamt   =  0;
							end;

							mcdalwamt    = availalw; /*medicaid available allowed amount*/ 
							mcdnetalwamt = 0; /*medicaid net allowed amount*/ 
							mcdtopamt    = 0; 

							mcralwamt    = availalw; /*medicare avialable allowed amount*/ 
							mcrnetalwamt = maxcopaymcr; /*medicare net allowed amount*/ 
							if availalw ne 0 then mcrtopamt    = round((topamt/availalw)*maxcopaymcr,.01); /*medicare to pay amount*/ 
							else mcrtopamt    = 0; 
					end;
					else if (grptotal gt grpmax) then do;
							var1=sum(grptotal,-bendays);
							if var1 le grpmax then do;
                                cstshrnetalcoiamt   =  0;
                                cstshrnetalcopamt   =  maxcopaysplit;
                                cstshrnetaldedamt   =  0;
									if netalwamt ne 0 then do;
		                                cstshrtopaycoiamt   =  round((topamt/netalwamt)*cstshrnetalcoiamt,.01);
		                                cstshrtopaycopamt   =  round((topamt/netalwamt)*cstshrnetalcopamt,.01);
		                                cstshrtopaydedamt   =  round((topamt/netalwamt)*cstshrnetaldedamt,.01);
									end;
									else do;
										cstshrtopaycoiamt   =  0;
										cstshrtopaycopamt   =  0;
										cstshrtopaydedamt   =  0;
									end;
	                          mcdalwamt    = availalw; /*MEDICAID AVAILABLE ALLOWED AMOUNT*/ 
	                          mcdnetalwamt = maxcopaymcd; /*MEDICAID NET ALLOWED AMOUNT*/ 
	                          mcdtopamt    = maxcopaymcd; /*MEDICAID TO PAY AMOUNT*/ 

	                          mcralwamt    = availalw; /*MEDICARE AVIALABLE ALLOWED AMOUNT*/ 
	                          mcrnetalwamt = maxcopaymcr; /*MEDICARE NET ALLOWED AMOUNT*/ 

						 	if missing(availalw)=1 or availalw eq 0.00 or availalw eq 0 then mcrtopamt    = 0;
						 	else mcrtopamt    = round((topamt/availalw)*maxcopaymcr,.01); /*medicare to pay amount (topamt/availalw)=with-hold amount*/ 
						end;
                       	else do; 
                                 cstshrnetalcoiamt   =  0;
                                 cstshrnetalcopamt   =  0;
                                 cstshrnetaldedamt   =  0;
                                 cstshrtopaycoiamt   =  0;
                                 cstshrtopaycopamt   =  0;
                                 cstshrtopaydedamt   =  0;

                                   mcdalwamt    = availalw; /*MEDICAID AVAILABLE ALLOWED AMOUNT*/ 
                                   mcdnetalwamt = availalw; /*MEDICAID NET ALLOWED AMOUNT*/ 
                                   mcralwamt    = availalw; /*MEDICARE AVIALABLE ALLOWED AMOUNT*/ 
                                   mcrnetalwamt = 0; /*MEDICARE NET ALLOWED AMOUNT*/ 


								 	if missing(availalw)=1 or availalw eq 0.00 or availalw eq 0 then do;
										  mcrtopamt    = 0;
										  mcdtopamt    = 0 ; 
								 	end;
								 	else do;
                                          mcdtopamt    = round((topamt/netalwamt)*mcdnetalwamt,.01); /*MEDICAID TO PAY AMOUNT*/ 
                                          mcrtopamt    = round((topamt/availalw)*mcrnetalwamt,.01); /*MEDICARE TO PAY AMOUNT*/
									 end;
                         end; 
                  end;   								
			end;
			if revstokey ne "" then do;
				reversals(a,10) = cstshrnetalcoiamt ; 
				reversals(a,11) = cstshrnetalcopamt ; 
				reversals(a,12) = cstshrnetaldedamt ; 
				reversals(a,13) = cstshrtopaycoiamt ;
		        reversals(a,14) = cstshrtopaycopamt ; 
				reversals(a,15) = cstshrtopaydedamt ; 

				reversals(a,16) = mcdtopamt ; 
				reversals(a,17) = mcdnetalwamt ; 
				reversals(a,18) = mcrtopamt ; 
				reversals(a,19) = mcrnetalwamt ; 	
				reversals(a,20) = mcralwamt; 
				reversals(a,21) = mcdalwamt; 	

				a = sum(a, 1); 
			end; 
		output; 
	end;
run;

/*B-352044 New logic added for the Cost Split for the bencodes UCC,IB1, IK1, ID1 */
proc sort data=finalsnf_&year.;
by clamno lineno;
run;

%let bencodes=%trim(&bencodes1.);
%let cnt=%sysfunc(countw(&bencodes.),%str(  ));

%do i=1 %to &cnt.;
    %let bencod=%scan(&bencodes.,&i,%str(  ));
    %let maxlimit=%scan(&coiamt1.,&i,%str(  ));
    %let effyear=%scan(&effdat1.,&i,%str(  ));
   %if %eval(&year.) eq %eval(&effyear.) %then
   %do;
    data &bencod._costsplit_&year.;
    set finalsnf_&year.;
    limit=gridcoiamt;                        /* gridcoiamt field comes from benefit grid and it is mapped to limit */
    if bencod="&bencod." and gridcoiamt ^=0;
	if (mcr = "Y" and mcd = "Y") or (mcr = "N" and mcd = "N");
	if days_trigger eq "N";
	run;
	
	proc sort data=&bencod._costsplit_&year.;
	by clamno lineno;
	run;

/* Cost Split logic for the bencodes */	
	data &bencod._costsplit_&year.(drop=cum_insurance stop);
	set &bencod._costsplit_&year.;
	by clamno;
	retain cum_insurance 0 stop 0;
    if first.clamno then 
	do;
      cum_insurance=0;
      stop=0;
    end;

    if stop=0 then do;
       cstshrtopaycoiamt=round((netalwamt*coipct)/100,.01);
       mcrtopamt=netalwamt-cstshrtopaycoiamt;
       cstshrnetalcoiamt=round((netalwamt*coipct)/100,.01);
       mcrnetalwamt=netalwamt-cstshrnetalcoiamt;
	if cum_insurance + cstshrtopaycoiamt > limit then
	do;
	   cstshrtopaycoiamt=limit-cum_insurance;
       cstshrnetalcoiamt=limit-cum_insurance;
       mcrtopamt=netalwamt-cstshrtopaycoiamt;
       mcrnetalwamt=netalwamt-cstshrnetalcoiamt;
       stop=1;
    end;
       cum_insurance + cstshrtopaycoiamt;
    end;
    else do;
    if bencod="&bencod." then do;
       cstshrtopaycoiamt=0;
       cstshrnetalcoiamt=0;
       mcrtopamt=netalwamt;
       mcrnetalwamt=netalwamt;
    end;
   end; 
run;
    
	proc sort data=&bencod._costsplit_&year.;
	by clamno lineno;
	run;

/* Reversal Logic for the bencodes */
    data &bencod._costsplit_&year.(drop=reversals1-reversals49995 a w r);
    do until(last.clamno);
    set &bencod._costsplit_&year.;
    by clamno;
    array reversals(9999,5) $ 23.; /*array for reversals */ 
    if revsfrkey ne "" then do;
     do w = 1 by 1 while(w le 9999);  
      if ((reversals(w,1) eq revsfrkey)) then do;
         r = w;
         cstshrtopaycoiamt = round(reversals(r,2)*(-1),.01); 
         cstshrnetalcoiamt = round(reversals(r,3)*(-1),.01); 
         mcrnetalwamt = round(reversals(r,4)*(-1),.01); 
         mcrtopamt = round(reversals(r,5)*(-1),.01); 
      end;
     end;
    end;

    if first.clamno then a = 1;
      if revstokey ne "" then do;
        reversals(a,1) = primarykey;
        reversals(a,2) = cstshrtopaycoiamt;
        reversals(a,3) = cstshrnetalcoiamt;
        reversals(a,4) = mcrnetalwamt;
        reversals(a,5) = mcrtopamt;				
        a = sum(a, 1);					
      end;
    output;
   end;
run;

	proc sort data=&bencod._costsplit_&year.;
	by clamno lineno;
	run;

proc sql;
	create table &bencod._clm_not_met as select clamno,sum(cstshrtopaycoiamt) from &bencod._costsplit_&year. 
	where revind='N' and netalwamt ^=0 group by clamno having sum(cstshrtopaycoiamt) < &maxlimit. ;
	
	create table &bencod._cs_update as select t1.* from &bencod._costsplit_&year. t1 
	inner join &bencod._clm_not_met t2 on t1.clamno=t2.clamno
	where t1.revind='N'
	order by clamno,lineno;
quit;

data &bencod._cs_update(drop=cum_insurance stop limit);
set &bencod._cs_update;
by clamno;
	retain cum_insurance 0 stop 0;
	if first.clamno then
		do;
			cum_insurance=0;
			stop=0;
		end;

	if stop=0 then
		do;
			cstshrtopaycoiamt=round((netalwamt*coipct)/100, .01);
			mcrtopamt=netalwamt-cstshrtopaycoiamt;
			cstshrnetalcoiamt=round((netalwamt*coipct)/100, .01);
			mcrnetalwamt=netalwamt-cstshrnetalcoiamt;
			
			if cum_insurance + cstshrtopaycoiamt > limit then
				do;
					cstshrtopaycoiamt=limit-cum_insurance;
					cstshrnetalcoiamt=limit-cum_insurance;
					mcrtopamt=netalwamt-cstshrtopaycoiamt;
					mcrnetalwamt=netalwamt-cstshrnetalcoiamt;
					stop=1;
				end;
			cum_insurance + cstshrtopaycoiamt;			
		end;
	else do;
			if bencod="&bencod." then
				do;
					cstshrtopaycoiamt=0;
					cstshrnetalcoiamt=0;
					mcrtopamt=netalwamt;
					mcrnetalwamt=netalwamt;
				end;
		end;
run;

proc sort data=&bencod._cs_update;
by clamno lineno;

data &bencod._costsplit_&year.(drop=limit);
update &bencod._costsplit_&year. &bencod._cs_update;
by clamno lineno;
run;
	
/* Updating the main dataset once the reversal calcuations are completed */	
    data finalsnf_&year.;
    update finalsnf_&year. &bencod._costsplit_&year.;
	by clamno lineno;
	run;
%end;
%end;

proc sql;
    alter table finalsnf_&year.
    drop mcr,mcd,topamt,netalwamt,days_trigger,coipct,revsfrkey,revstokey,primarykey,bencod,revind,gridcoiamt;
quit;

/* SAS2AWS2: added proc append & proc sort statement */
proc append base=finalsnf data=finalsnf_&year. ;
run;

%mend spltlogic;                                                                     
/*%spltlogic(2013);*/

%global fida_switch;
%macro switch;/*Macro to read fida.csv file and assign value for fidaswitch */
data inp;
  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
  infile "&drv2/DWAC0901/fida.csv" delimiter=',';
  input fida $ comp;
  com=put(comp,2.);
  /* SAS2AWS2: ReplacedFunctionTrim */
  compn=ktrim(com);
  if missing(comp) or missing(fida) then delete; /* To remove if any missing records in fida.csv*/

 run;
%nobs(inp);
%let recs=&nobs.;
proc sql noprint;
select fida,compn into :fida_1-:fida_&recs., :compn1-:compn&recs. from inp;
quit;
%do i=1 %to &recs.;
%let compn=&&compn&i;
%let fid=&&fida_&i ;
 data _null_;
   %if "&compn."="%ktrim(&compno.)" %then %do;/*To check compno from csv file matches the compno from sasjob */
   %let fida_switch=&fid;
   %end;
run;
%end;
%mend switch;
%switch;
%macro dwaf8901 ;	/*ALM 5682 - BC Modified the Macro name*/

/* SAS2AWS2: CommentedMacroLock */
/*%lock(dataset = &libnx..svclines);                */

data _null_;
set yearcall;
call execute('%spltlogic('||Year||');');
run;

/*SAS2AWS2 - added logic to read svclines table from RS*/
/*B-223040 - modified libn1 as rsstr*/
proc sort data=/*&libn1.*/rsstr.svclines out=svclines;
by clamno lineno;
run;

proc sort data=finalsnf;
by clamno lineno;
run;

options msglevel=N;
/* SAS2AWS2: changed library name from libn1 to work */
    data /*&libn1..svclines_fnl*/ rsstr.svclines;
    /* SAS2AWS2: commented libnx and also changed modify statement as update*/
        update /*&libn1..*/svclines finalsnf; /* SAS2AWS2: changed library name from libn1 to work */
         by clamno lineno;
     run;
     
/* B-278321-Start- Appending svclinesa datasest from DWCC0901 programs */

data rsstr.svclines;
     set rsstr.svclines rsstr.svclinesa;
run;

/* B-278321-End- Appending svclinesa datasest from DWCC0901 programs */

%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries */
	 
/* SAS2AWS2: added sas2rs_dataload_bul macro to load data into RS table */
/* %sas2rs_dataload_bul(loadtech=delete, srclib=work, srctbl=svclines_fnl, trgtlib=&libn1., trgttbl=svclines); */
    %sas2rs_dataload_bul_pp(loadtech=delete,srclib=rsstr,srctbl=svclines,trgtlib=&libn1.,trgttbl=svclines,split_num=&SVLINES_PARTS.);

options MSGLEVEL=I ;

/*FIDA (CC data) MCD, MCR & Cost Sharing fields are assigned as 0 */
/*FIDA (MSO data) MCD, MCR & Cost Sharing fields are assigned as 0 */
/*Macro variable FIDA switch provided by wkly34_CDC and Wkly60_CDC job*/
/* SAS2AWS2: ReplacedFunctionUpcase */
%if %kupcase(&Fida_switch.) = YES %then %do;

/* SAS2AWS2: changed library name from libn1 to work */
	 data /*&libn1..svclines_fnl*/ rsstr.svclines;
	 /*B-223040 - modified libn1 as rsstr*/
         set /*&libn1.*/rsstr.svclines ; /* SAS2AWS2: changed modify statement as set */
		 /* SAS2AWS2: ReplacedFunctionUpcase */
		 if kupcase(lobcod)='FDA' or kupcase(lobcod)='NSF' then do;
         mcralwunt = 0;
         mcrtopamt = 0;
         mcrnetalwamt = 0;
         mcralwamt = 0;
         mcrcovunt = 0;
         mcrcovdys = 0;
         mcrapcovdys = 0;
         mcrutilct = 0;
         mcraputilct = 0;
         mcraptrnamt = 0;
         mcdalwunt = 0;
         mcdtopamt = 0;
         mcdnetalwamt = 0;
         mcdalwamt = 0;
         mcdcovunt = 0;
         mcdcovdys = 0;
         mcdapcovdys = 0;
         mcdutilct = 0;
         mcdaputilct = 0;
         mcdaptrnamt = 0;
         cstshrnetalcoiamt = 0;
         cstshrnetalcopamt = 0;
         cstshrnetaldedamt = 0;
         cstshrtopaycoiamt = 0;
         cstshrtopaycopamt = 0;
         cstshrtopaydedamt = 0;
		 end;
     run;

%include "&drv/AUTOEXEC/reconnect.sas"; /*Reconnecting Redshift Libraries */

/* SAS2AWS2: added sas2rs_dataload_bul macro to load data into RS table */
/* %sas2rs_dataload_bul(loadtech=delete, srclib=work, srctbl=svclines_fnl, trgtlib=&libn1., trgttbl=svclines); */
    %sas2rs_dataload_bul_pp(loadtech=delete,srclib=rsstr,srctbl=svclines,trgtlib=&libn1.,trgttbl=svclines,split_num=&SVLINES_PARTS.);

%end;

/* SAS2AWS2: CommentedMacroUnlock */
/*%unlock(dataset = &libnx..svclines);              */

%mend dwaf8901;	/*ALM 5682 - BC Modified the Macro name*/

%dwaf8901;	/*ALM 5682 - BC Modified the Macro name*/

proc contents data=&libn1..svclines;              
     title "Descriptor for &libn1..SVCLINES ";    
run;


/* B-278321-Start-Delete splitted datasest that are created from DWCC0901 sas programs */

proc datasets library = rsstr;
     delete svclinesa svclinesb;
run;

/* B-278321-End-Delete splitted datasest that are created from DWCC0901 sas programs */

%time(timeparm=e); 
%timestamp;                                                                               

%initvar;
quit;

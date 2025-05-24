%macro define_mcorcat_pv(tablex);					        		
 
data mcormapping;
set &tablex;
run;
 
/* UP - HS# 447254 - File must be hierarchal order */
proc sort data=mcormapping;					
	by hierarchy mcorcat varname;			
run;
 
/* Count the number of variables used in each MCORCAT */
data varcnt(rename=(oldmcorcat=mcorcat oldhierarchy=hierarchy));				/* KS - 171281 - 09212009 */
	set mcormapping end=last;					/* KS - 202643 - 06282010 */
	length oldhierarchy   $5. oldmcorcat   $15. oldvarname $40.;			/* KS - 171281 - 09212009 */
	retain oldmcorcat oldvarname oldhierarchy;						/* KS - 171281 - 09212009 */
	select;												/* KS - 171281 - 09212009 */
	when(_N_ = 1) do;									/* KS - 171281 - 09212009 */
	    varcnt=1;
	    oldhierarchy = hierarchy;
oldmcorcat = mcorcat;							/* KS - 171281 - 09212009 */
		oldvarname = varname;							/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
	when(oldmcorcat not = mcorcat) do;					/* KS - 171281 - 09212009 */
	    output varcnt;									/* KS - 171281 - 09212009 */
		varcnt=1;
		oldhierarchy = hierarchy;
oldmcorcat = mcorcat;							/* KS - 171281 - 09212009 */
		oldvarname = varname;							/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
	when(oldvarname not = varname) do;					/* KS - 171281 - 09212009 */
	    varcnt+1;
	    oldhierarchy = hierarchy;
		oldvarname=varname;								/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
otherwise;											/* KS - 171281 - 09212009 */
end;												/* KS - 171281 - 09212009 */	
	keep oldhierarchy oldmcorcat varcnt;								/* KS - 171281 - 09212009 */
	if last then goto last;								/* KS - 171281 - 09212009 */
	return;												/* KS - 171281 - 09212009 */
last:  oldmcorcat=mcorcat;								/* KS - 171281 - 09212009 */
output varcnt;									/* KS - 171281 - 09212009 */
run;													/* KS - 171281 - 09212009 */
 
/* Append the variable count to original file */
proc sql;												/* KS - 171281 - 09212009 */
	create table newmcor as								/* KS - 171281 - 09212009 */
	select a.*, b.varcnt								/* KS - 171281 - 09212009 */
	from mcormapping as a left join				/* KS - 202643 - 06282010 */
	varcnt as b											/* KS - 171281 - 09212009 */
	on													/* KS - 171281 - 09212009 */
	a.mcorcat = b.mcorcat and a.hierarchy = b.hierarchy;								/* KS - 171281 - 09212009 */
quit;													/* KS - 171281 - 09212009 */
 
/* Count the number of times a variable is used in each MCORCAT */
data funccnt(rename=(oldvarname=varname oldmcorcat=mcorcat oldhierarchy=hierarchy));	/* KS - 171281 - 09212009 */
	set mcormapping end=last;					/* KS - 202643 - 06282010 */
	length oldhierarchy  $5. oldmcorcat  $15. oldvarname $40.;			/* KS - 162774 - 02082010 */ 						
	retain oldmcorcat oldvarname funccnt oldhierarchy;				/* KS - 171281 - 09212009 */
	select;												/* KS - 171281 - 09212009 */
	when(_N_ = 1) do;									/* KS - 171281 - 09212009 */
	    funccnt=0;	
	    oldhierarchy = hierarchy;
oldmcorcat = mcorcat;							/* KS - 171281 - 09212009 */
		oldvarname = varname;							/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
	when(oldmcorcat not = mcorcat) do;					/* KS - 171281 - 09212009 */
	    output funccnt;									/* KS - 171281 - 09212009 */
		funccnt=0;
		oldhierarchy = hierarchy;
oldmcorcat = mcorcat;							/* KS - 171281 - 09212009 */
		oldvarname = varname;							/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
	when(oldvarname not = varname) do;					/* KS - 171281 - 09212009 */
	    output funccnt;									/* KS - 171281 - 09212009 */
		funccnt=0;	
		oldhierarchy = hierarchy;
		oldvarname=varname;								/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
otherwise;											/* KS - 171281 - 09212009 */
end;												/* KS - 171281 - 09212009 */	
	funccnt+1;											/* KS - 171281 - 09212009 */
	keep oldmcorcat oldhierarchy oldvarname funccnt;					/* KS - 171281 - 09212009 */
	if last then goto last;								/* KS - 171281 - 09212009 */
	return;												/* KS - 171281 - 09212009 */
last:  oldmcorcat=mcorcat;								/* KS - 171281 - 09212009 */
oldvarname=varname;								/* KS - 171281 - 09212009 */
output funccnt;									/* KS - 171281 - 09212009 */
run;													/* KS - 171281 - 09212009 */
 
/* Append the function count to original file */
proc sql;												/* KS - 171281 - 09212009 */
	create table newmcor2 as							/* KS - 171281 - 09212009 */
	select a.*, b.funccnt								/* KS - 171281 - 09212009 */
	from newmcor as a left join							/* KS - 171281 - 09212009 */
	funccnt as b										/* KS - 171281 - 09212009 */
	on													/* KS - 171281 - 09212009 */
	a.mcorcat = b.mcorcat and							/* KS - 171281 - 09212009 */
	a.varname = b.varname	and  a.hierarchy = b.hierarchy							/* KS - 171281 - 09212009 */
	order by hierarchy, mcorcat;						/* KS - 162774 - 1104009 */
quit;													/* KS - 171281 - 09212009 */
 
/* File must be hierarchal order */
proc sort data=newmcor2;								/* KS - 171281 - 09212009 */
	by hierarchy mcorcat varname;						/* KS - 171281 - 09212009 */
run;													/* KS - 171281 - 09212009 */
 
data _null_;
	set newmcor2 end=last;
	length newmcorcat $15.;
	if last then do;
	   newmcorcat=mcorcat;
call symput('lstmcorcat', newmcorcat);
	   end;
run;
 
%put &lstmcorcat;

proc sort data=newmcor2 nodup;
by _all_;
run;
 
/* Construct the SAS statements used to define the MCORCATs */
data _null_;											/* KS - 171281 - 09212009 */
	set newmcor2 end=last;								/* KS - 171281 - 09212009 */
	file outfile ;										/* KS - 171281 - 09212009 */
	length oldmcorcat outmcorcat $15. oldhierarchy outhierarchy $5. oldvarname $40. left_paren right_paren $1.	/* KS - 162774 - 1104009 */
andst $5. orst $4. command $32767	/* UP - 424199 - Increased length to maximum to handle ICD10 codes */ /* KS - 171281 - 09212009 */
	 mmst $2. ddst $2. yyst $4. startst endst $12.;              /* KS - 171281 - 09212009 */
	retain oldmcorcat oldhierarchy outhierarchy oldvarname outmcorcat command stmt_cnt command_cnt hold_cnt oldvarcnt;	/* KS - 162774 - 1104009 */
	left_paren='(';										/* KS - 171281 - 09212009 */
	right_paren=')';									/* KS - 171281 - 09212009 */
	andst=' and ';										/* KS - 171281 - 09212009 */
	orst=' orz';										/* KS - 171281 - 09212009 */
	select;												/* KS - 171281 - 09212009 */
	when(_N_ = 1) do;									/* KS - 171281 - 09212009 */
	    put 'select;';									/* KS - 171281 - 09212009 */
		if varcnt > 1 then								/* KS - 171281 - 09212009 */
		put 'when ' left_paren left_paren;				/* KS - 171281 - 09212009 */
		else put 'when ' left_paren;					/* KS - 171281 - 09212009 */
	    oldmcorcat = mcorcat;							/* KS - 171281 - 09212009 */
		outmcorcat=oldmcorcat;
		oldhierarchy = hierarchy;
		outhierarchy = oldhierarchy;
		oldvarname = varname;							/* KS - 171281 - 09212009 */
		oldvarcnt = varcnt;								/* KS - 171281 - 09212009 */
		command=' ';									/* KS - 171281 - 09212009 */
stmt_cnt=1;										/* KS - 171281 - 09212009 */
		hold_cnt=0;										/* KS - 171281 - 09212009 */
		command_cnt=funccnt;							/* KS - 171281 - 09212009 */
		if varcnt > 1 then command = left_paren;		/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
	when(oldmcorcat not = mcorcat) do;					/* KS - 171281 - 09212009 */
	    if oldvarcnt > 1 then							/* KS - 171281 - 09212009 */
	    command=kstrip(command)||right_paren||right_paren; 	
		else command=kstrip(command)||right_paren; 		
		command=ktranslate(command,' ','z'); 				
		put  command  'do; mcorcat = '  "'" outmcorcat"'" '; HIERARCHY = ' "'" outhierarchy"'" '; end;' ;	/* KS - 162774 - 1104009 */
		stmt_cnt+1;										/* KS - 171281 - 09212009 */
		select;											/* KS - 171281 - 09212009 */
		     when(last NE 1)  do;				/* KS - 171281 - 09212009 */
			 if varcnt > 1 then							/* KS - 171281 - 09212009 */
command =  'when ' || left_paren ||left_paren;	/* KS - 171281 - 09212009 */
			 else command =  'when ' || left_paren;		/* KS - 171281 - 09212009 */
		     put command;								/* KS - 171281 - 09212009 */
			 end;										/* KS - 171281 - 09212009 */
			 otherwise;									/* KS - 171281 - 09212009 */
			 end;										/* KS - 171281 - 09212009 */
		
		command_cnt=funccnt;							/* KS - 171281 - 09212009 */
		hold_cnt=0;										/* KS - 171281 - 09212009 */
		oldmcorcat = mcorcat;							/* KS - 171281 - 09212009 */
		outmcorcat=oldmcorcat;
		oldhierarchy = hierarchy;
		outhierarchy = oldhierarchy;
		oldvarname = varname;							/* KS - 171281 - 09212009 */
		oldvarcnt=varcnt;								/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
	when(oldvarname not = varname) do;					/* KS - 171281 - 09212009 */
	    command = kstrip(command)||right_paren||andst||left_paren; 	
		command=ktranslate(command,' ','z'); 				
		put command;									/* KS - 171281 - 09212009 */
		command_cnt=funccnt;							/* KS - 171281 - 09212009 */
		hold_cnt=0;										/* KS - 171281 - 09212009 */
		oldvarname=varname;								/* KS - 171281 - 09212009 */
		end;											/* KS - 171281 - 09212009 */
otherwise;											/* KS - 171281 - 09212009 */
end;												/* KS - 171281 - 09212009 */
hold_cnt+1;											/* KS - 171281 - 09212009 */
select;												/* KS - 171281 - 09212009 */
	  when(varname = ' ')								/* KS - 171281 - 09212009 */
command='otherwise do; mcorcat = '||"'"||outmcorcat||"'"||'; hierarchy = '||"'"||outhierarchy||"'"||'; end;'; /* KS - 171281 - 09212009 */
 
	  when(varname in(&dateflds) 						
and hold_cnt < command_cnt and varname2=' ') do;	/* KS - 159267 - 07092010 */			
	       mmst=scan(start,1,'/');						/* KS - 162774 - 1104009 */
		   ddst=scan(start,2,'/');						/* KS - 162774 - 1104009 */
		   yyst=scan(start,-1,'/');						/* KS - 162774 - 1104009 */
		   if klength(mmst)=1 then mmst='0'||mmst; 		
		   if klength(ddst)=1 then ddst='0'||ddst; 		
		   select;										/* KS - 171281 - 09212009 */
		   	   when(mmst='01') startst='"'||ddst||'JAN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='02') startst='"'||ddst||'FEB'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='03') startst='"'||ddst||'MAR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='04') startst='"'||ddst||'APR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='05') startst='"'||ddst||'MAY'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='06') startst='"'||ddst||'JUN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='07') startst='"'||ddst||'JUL'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='08') startst='"'||ddst||'AUG'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='09') startst='"'||ddst||'SEP'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='10') startst='"'||ddst||'OCT'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='11') startst='"'||ddst||'NOV'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='12') startst='"'||ddst||'DEC'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   otherwise;								/* KS - 171281 - 09212009 */
			   end;										/* KS - 171281 - 09212009 */
		   mmst=scan(end,1,'/');						/* KS - 162774 - 1104009 */
		   ddst=scan(end,2,'/');						/* KS - 162774 - 1104009 */
		   yyst=scan(end,-1,'/');						/* KS - 162774 - 1104009 */
		   if klength(mmst)=1 then mmst='0'||mmst; 		
		   if klength(ddst)=1 then ddst='0'||ddst; 		
		   select;
		   	   when(mmst='01') endst='"'||ddst||'JAN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='02') endst='"'||ddst||'FEB'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='03') endst='"'||ddst||'MAR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='04') endst='"'||ddst||'APR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='05') endst='"'||ddst||'MAY'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='06') endst='"'||ddst||'JUN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='07') endst='"'||ddst||'JUL'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='08') endst='"'||ddst||'AUG'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='09') endst='"'||ddst||'SEP'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='10') endst='"'||ddst||'OCT'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='11') endst='"'||ddst||'NOV'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='12') endst='"'||ddst||'DEC'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   otherwise;									/* KS - 171281 - 09212009 */
			   end;											/* KS - 171281 - 09212009 */
 
	  command=kstrip(startst)||'<='||kstrip(varname)||'<='||kstrip(endst)||orst; 	
 
	       command=ktranslate(command,' ','z'); 			
	       put command;									/* KS - 171281 - 09212009 */
		   end;											/* KS - 171281 - 09212009 */
	  when(varname in(&dateflds) 						
and hold_cnt < command_cnt and varname2 ne ' ') do;	/* KS - 159267 - 07092010 */
		   command=kstrip(varname)||'='||kstrip(varname2)||orst; 	
	       command=ktranslate(command,' ','z'); 			
	       put command;									/* KS - 159267 - 07092010 */
		   end;											/* KS - 159267 - 07092010 */
	  when(varname in (&dateflds) and hold_cnt = command_cnt and varname2=' ')do;	/* KS - 159267 - 07092010 */
	       mmst=scan(start,1,'/');						/* KS - 162774 - 1104009 */
		   ddst=scan(start,2,'/');						/* KS - 162774 - 1104009 */
		   yyst=scan(start,-1,'/');						/* KS - 162774 - 1104009 */
		   if klength(mmst)=1 then mmst='0'||mmst; 		
		   if klength(ddst)=1 then ddst='0'||ddst; 		
		   select;										/* KS - 171281 - 09212009 */
		   	   when(mmst='01') startst='"'||ddst||'JAN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='02') startst='"'||ddst||'FEB'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='03') startst='"'||ddst||'MAR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='04') startst='"'||ddst||'APR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='05') startst='"'||ddst||'MAY'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='06') startst='"'||ddst||'JUN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='07') startst='"'||ddst||'JUL'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='08') startst='"'||ddst||'AUG'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='09') startst='"'||ddst||'SEP'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='10') startst='"'||ddst||'OCT'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='11') startst='"'||ddst||'NOV'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='12') startst='"'||ddst||'DEC'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   otherwise;								/* KS - 171281 - 09212009 */
			   end;										/* KS - 171281 - 09212009 */
		   mmst=scan(end,1,'/');						/* KS - 162774 - 1104009 */
		   ddst=scan(end,2,'/');						/* KS - 162774 - 1104009 */
		   yyst=scan(end,-1,'/');						/* KS - 162774 - 1104009 */
		   if klength(mmst)=1 then mmst='0'||mmst; 		
		   if klength(ddst)=1 then ddst='0'||ddst; 		
		   select;										/* KS - 171281 - 09212009 */
		   	   when(mmst='01') endst='"'||ddst||'JAN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='02') endst='"'||ddst||'FEB'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='03') endst='"'||ddst||'MAR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='04') endst='"'||ddst||'APR'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='05') endst='"'||ddst||'MAY'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='06') endst='"'||ddst||'JUN'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='07') endst='"'||ddst||'JUL'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='08') endst='"'||ddst||'AUG'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='09') endst='"'||ddst||'SEP'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='10') endst='"'||ddst||'OCT'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='11') endst='"'||ddst||'NOV'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   when(mmst='12') endst='"'||ddst||'DEC'||yyst||'"d';	/* KS - 171281 - 09212009 */
			   otherwise;									/* KS - 171281 - 09212009 */
			   end;											/* KS - 171281 - 09212009 */
		
	       command=kstrip(startst)||'<='||kstrip(varname)||'<='||kstrip(endst); 	
		   command=ktranslate(command,' ','z'); 			
	       end;											/* KS - 171281 - 09212009 */
	  when(varname in (&dateflds) and hold_cnt = command_cnt and varname2 ne ' ')do; /* KS - 159267 - 07092010 */
		   command=kstrip(varname)||'='||kstrip(varname2);  	
		   command=ktranslate(command,' ','z');  			
	       end;	                                         /* KS - 159267 - 07092010 */											
	  when(varname in(&numflds) and hold_cnt < command_cnt and varname2 = ' ') do;	/* KS - 159267 - 07092010 */	
	  /* UP - 424199 - Added NULL logic for ICD10 Codes - Macro will create 'IN'Operator
	  condition if the end value is 'NULL'*/
	  if kupcase(end) = 'NULL' then do;
	  command=kstrip(varname)||' in '||kstrip(start)||orst; 	
	  end;
	  else do;
	  command=kstrip(start)||'<='||kstrip(varname)||'<='||kstrip(end)||orst; 	
	  end;
	       command=ktranslate(command,' ','z'); 			
	       put command;									/* KS - 171281 - 09212009 */
		   end;											/* KS - 171281 - 09212009 */
	  when(varname in(&numflds) and hold_cnt < command_cnt and varname2 ne ' ') do;	/* KS - 159267 - 07092010 */	
	  command=kstrip(varname)||'='||kstrip(varname2)||orst;  	
	       command=ktranslate(command,' ','z');  			
	       put command;                                   /* KS - 159267 - 07092010 */									
		   end;                                           /* KS - 159267 - 07092010 */											
	  when(varname in(&numflds) and hold_cnt = command_cnt and varname2 = ' ')do;	/* KS - 159267 - 07092010 */
	  /* UP - 424199 - Added NULL logic for ICD10 Codes - Macro will create 'IN'Operator
	  condition if the end value is 'NULL'*/
	  if kupcase(end) = 'NULL' then do;
	  command=kstrip(varname)||' in '||kstrip(start); 	
	  end;
	  else do;
	  command=kstrip(start)||'<='||kstrip(varname)||'<='||kstrip(end);  	
	  end;
		   command=ktranslate(command,' ','z'); 			
	       end;											/* KS - 171281 - 09212009 */
	  when(varname in(&numflds) and hold_cnt = command_cnt and varname2 ne ' ')do;	/* KS - 159267 - 07092010 */
	       command=kstrip(varname)||'='||kstrip(varname2);  	
		   command=ktranslate(command,' ','z');  			
	       end;	                                         /* KS - 159267 - 07092010 */										
	  when(varname = 'SVCCOD' and hold_cnt < command_cnt and varname2 = ' ') do; /* KS - 159267 - 07092010 */	
	  /* UP - 424199 - Commented out old start and end logic as it will assign numerous Zero's if we change
	  the length of START and END variable and added New logic*/
	/*	  	   start=right(start);					*/						/* KS - 171281 - 09212009 */
/*		   start=translate(start,'0',' ');		*/			/* KS - 171281 - 09212009 */
	/*		   end=right(end);						*/
	/*		   end=translate(end,'0',' ');			*/
	 if klength(start) < 10 then  start = CATT(REPEAT('0',10-klength(start)),start);
	 if klength(end) < 10 then end = CATT(REPEAT('0',10-klength(end)),end);
 
	    command="'"||kstrip(start)||"'"||'<='||kstrip(varname)||'<='||"'"||kstrip(end)||"'"||orst; 	
 
		   command=ktranslate(command,' ','z'); 			
		   put command;									/* KS - 171281 - 09212009 */
		   end;											/* KS - 171281 - 09212009 */
	  when(varname = 'SVCCOD' and hold_cnt < command_cnt and varname2 ne ' ') do; /* KS - 159267 - 07092010 */	
	        command=kstrip(varname)||'='||kstrip(varname2)||orst;  	
		   command=ktranslate(command,' ','z');  			
		   put command;                                         /* KS - 159267 - 07092010 */									
		   end;                                                 /* KS - 159267 - 07092010 */											
	  when(varname = 'SVCCOD' and varname2 = ' ') do;   /* KS - 159267 - 07092010 */						
	  /* UP - 424199 - Commented out old start and end logic as it will assign numerous Zero's if we change
	  the length of START and END variable and added New logic*/
	 /*	  	   start=right(start);					*/		/* KS - 171281 - 09212009 */
/*		   start=translate(start,'0',' ');*/				/* KS - 171281 - 09212009 */
	 /*		   end=right(end);						*/
	 /*		   end=translate(end,'0',' ');				*/
	  if klength(start) < 10 then  start = CATT(REPEAT('0',10-klength(start)),start);
	 if klength(end) < 10 then end = CATT(REPEAT('0',10-klength(end)),end); 		
 
command="'"||kstrip(start)||"'"||'<='||kstrip(varname)||'<='||"'"||kstrip(end)||"'"; 	
		   command=ktranslate(command,' ','z'); 			
		   end;											/* KS - 171281 - 09212009 */
	  when(varname = 'SVCCOD' and varname2 ne ' ') do;  /* KS - 159267 - 07092010 */						
	  	   command=kstrip(varname)||'='||kstrip(varname2);  	
		   command=ktranslate(command,' ','z');  			
		   end;                                          /* KS - 159267 - 07092010 */											
	  when( hold_cnt < command_cnt and varname2 = ' ') do; /* KS - 159267 - 07092010 */					
	  /* UP - 424199 - Added NULL logic for ICD10 Codes - Macro will create 'IN'Operator
	  condition if the end value is 'NULL'*/
	  if kupcase(end) = 'NULL' then do;
	  		command=kstrip(varname)||' in '||kstrip(start)||orst;
	  end;
	  else do;
			command="'"||kstrip(start)||"'"||'<='||kstrip(varname)||'<='||"'"||kstrip(end)||"'"||orst; 	
	  end;
			command=ktranslate(command,' ','z'); 			
	       put command;									/* KS - 171281 - 09212009 */
		   end;											/* KS - 171281 - 09212009 */
	  when( hold_cnt < command_cnt and varname2 ne ' ') do;	   /* KS - 159267 - 07092010 */				
			command=kstrip(varname)||'='||kstrip(varname2)||orst;  	
			command=ktranslate(command,' ','z');  		
	       put command;                                         /* KS - 159267 - 07092010 */									
		   end;                                                 /* KS - 159267 - 07092010 */											
	  /* UP - 424199 - Added NULL logic for ICD10 Codes - Macro will create 'IN'Operator
	  condition if the end value is 'NULL'   */
	  otherwise do;	
if varname2 = ' ' and kupcase(end) = 'NULL' then
	command=kstrip(varname)||' in '||kstrip(start); 	
	else if varname2 = ' ' and kupcase(end) ne 'NULL' then
	command="'"||kstrip(start)||"'"||'<='||kstrip(varname)||'<='||"'"||kstrip(end)||"'"; 	
	  else
	command=kstrip(varname)||'='||kstrip(varname2);
	
	  end;												/* KS - 171281 - 09212009 */
	  end;												/* KS - 171281 - 09212009 */
	
	if last then goto last;								/* KS - 171281 - 09212009 */
	return;												/* KS - 171281 - 09212009 */

last:  oldmcorcat=mcorcat;	oldhierarchy = hierarchy;					/* KS - 171281 - 09212009 */
if command_cnt > 1 then command=kstrip(command)||right_paren; 	
	 command=ktranslate(command,' ','z'); 
	 put 'otherwise do; mcorcat = '  "'" mcorcat"'" '; hierarchy = ' "'" hierarchy"'" '; end;';	/* KS - 171281 - 09212009 */
	 	
run;													/* KS - 171281 - 09212009 */
 
 
%mend define_mcorcat_pv;
 

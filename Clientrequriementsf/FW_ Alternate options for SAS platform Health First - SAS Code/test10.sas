
*rsubmit;

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19104 Program Start execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

Data &RGA191..EditPassClmLine1_&compno._&run_id.; 
		length clamno $20.; /* HS 569650 */
	set &RGA191..EditPassClmLine_&compno._&run_id.;	
	if claimfrq = '1' and icn ne '' and "&N4TON5_icn_add." ne "Y" then do; /* HS 567501 - added logic to change claim frequnecy value */
	claimfrq = '8';
	end;
Run;
/*********************************************************************************************/
/*                Start   - Calling Macro for Diagnosis code reassignment & Pointer Logic    */
/*********************************************************************************************/

*%pointer_diag;/*CR# CHG0031183 - commented*/
/*********************************************************************************************/
/*                End   -  Calling Macro for Diagnosis code reassignment & Pointer Logic     */
/*********************************************************************************************/

*%proc_cod; /*CR# CHG0031183 - commented*/


/* HS 542635 - Added Proc delete statement */
*Proc delete data=&RGA191..EditPassClmLine_&compno._&run_id.;*run;

%macro poaexempt_check;
data &RGA191..EditPassClmLine2_&compno._&run_id. ;
set &RGA191..EditPassClmLine1_&compno._&run_id.;
if typecode in ('I') then do;
	%do i= 1 %to 25 %by 1; 		
	      if newPoaDiag&i ='1' then    newPoaDiag&i='';
  %end;	
end;
run;
%mend poaexempt_check;
%poaexempt_check;

%macro APD_mapping(ds_mod2,Edit_Ind);

proc sort data = &RGA191..&ds_mod2.  out = &RGA191..Encounter3_&Edit_Ind._&compno._&run_id. ;
by clamno lineno ;
run;

  DATA    &RGA191..prof_clm_&Edit_Ind._&compno._&run_id.(keep = &keep_prof_clm_vars.)
          &RGA191..prof_svc_&Edit_Ind._&compno._&run_id.(keep = &keep_prof_svc_vars.)		  
          &RGA191..inst_clm_&Edit_Ind._&compno._&run_id.(keep = &keep_inst_clm_vars.)
          &RGA191..inst_svc_&Edit_Ind._&compno._&run_id.(keep = &keep_inst_svc_vars.)
          &RGA191..Bad_data_&compno._&run_id. ;
          length      
                  PR_2010AA_NM103_BI_PRV_LST                $60.	/* B-146417 $32. */
                  PR_2010AA_NM104_BI_PRV_FST                $35.    /* B-146417 $32. */
                  IN_2300_DTP03_END_DATE                    $20. 
                  IN_2300_DTP03_ADM_DATE                    $20. 
                  IN_2300_HI01_4_OCC_SP_D1_tmp              $20.  
                  IN_2300_HI02_4_OCC_SP_D2_tmp              $20. 
                  IN_2300_HI03_4_OCC_SP_D3_tmp              $20. 
                  IN_2300_HI04_4_OCC_SP_D4_tmp              $20. 
                  IN_2300_HI05_4_OCC_SP_D5_tmp              $20. 
                  IN_2300_HI06_4_OCC_SP_D6_tmp              $20. 
                  IN_2300_HI07_4_OCC_SP_D7_tmp              $20. 
                  IN_2300_HI08_4_OCC_SP_D8_tmp              $20. 
                  IN_2300_HI09_4_OCC_SP_D9_tmp              $20. 
                  IN_2300_HI10_4_OCC_SP_D10_tmp             $20. 
                  IN_2400_DTP03_SL_SER_DT                   $20.
                  PR_2400_NTE02                             $20. 
          				/* VVKR Code Start -- B-75041 */
									IN_2430_CAS01 IN_2430_CAS05 IN_2430_CAS09 IN_2430_CAS13 IN_2430_CAS17 IN_2430_CAS21 $2.
				  				IN_2430_CAS02 IN_2430_CAS06 IN_2430_CAS10 IN_2430_CAS14 IN_2430_CAS18 $8.
				  				IN_2430_CAS11 IN_2430_CAS12 IN_2430_CAS15 IN_2430_CAS16 IN_2430_CAS19 IN_2430_CAS20 8.
				  				PR_2430_CAS01 PR_2430_CAS05 PR_2430_CAS09 PR_2430_CAS13 PR_2430_CAS17 PR_2430_CAS21 $2.
				  				PR_2430_CAS02 PR_2430_CAS06 PR_2430_CAS10 PR_2430_CAS14 PR_2430_CAS18 $8.
				  				PR_2430_CAS11 PR_2430_CAS12 PR_2430_CAS15 PR_2430_CAS16 PR_2430_CAS19  PR_2430_CAS20 8.
				  				/* VVKR Code End -- B-75041 */
				  				
				  				IN_2430_CAS01_MCR IN_2430_CAS05_MCR IN_2430_CAS09_MCR IN_2430_CAS13_MCR IN_2430_CAS17_MCR IN_2430_CAS21_MCR $2.
				  				IN_2430_CAS02_MCR IN_2430_CAS06_MCR IN_2430_CAS10_MCR IN_2430_CAS14_MCR IN_2430_CAS18_MCR $8.
				  				IN_2430_CAS11_MCR IN_2430_CAS12_MCR IN_2430_CAS15_MCR IN_2430_CAS16_MCR IN_2430_CAS19_MCR IN_2430_CAS20_MCR 8.
				  				PR_2430_CAS01_MCR PR_2430_CAS05_MCR PR_2430_CAS09_MCR PR_2430_CAS13_MCR PR_2430_CAS17_MCR PR_2430_CAS21_MCR $2.
				  				PR_2430_CAS02_MCR PR_2430_CAS06_MCR PR_2430_CAS10_MCR PR_2430_CAS14_MCR PR_2430_CAS18_MCR $8.
				  				PR_2430_CAS11_MCR PR_2430_CAS12_MCR PR_2430_CAS15_MCR PR_2430_CAS16_MCR PR_2430_CAS19_MCR  PR_2430_CAS20_MCR 8.
                  ;		

		%if "&x12_ctrl." ne "EIS" %then %do;	/* B-136184 */
			%if (&compno=45 and "&LOB." ne "MPPO")  or (&compno=42 and "&LOB." eq "QHP") %then %do;	/*B-248997*/
		  		length IN_2330B_ICN PR_2330B_ICN $22.
			           IN_2330B_NM109 PR_2330B_NM109_OTPAY_HMO $24.; /*B-274606 Added*/				
			%end;
			%else %do;
				length IN_2330B_ICN_MCR IN_2330B_ICN_MCD PR_2330B_ICN_MCR PR_2330B_ICN_MCD $22.;
			%end;
		%end;
		
		/*B-274606 Start's here */
		%if (&compno.=01 or &compno.=20 or &compno.=42) %then %do; 
			length PR_2430_SVD01_HMO IN_2430_SVD01 $24.;
			%if ((&compno=42 and "&LOB." eq "EP") or &compno.=01 or &compno.=20) %then %do; 
				length PR_2330B_NM109_OTPAY_HMO_MCD IN_2330B_NM109_MCD $24.;
			%end;
		%end;
		/*B-274606 End's here */		

		 FORMAT	PR_2300_CLM05_1_FAC_TYPE z2.;				/* B-89828 keep leading 0 from poscod */

     	 RETAIN            
	            prof_clm_count_&Edit_Ind.                         0				
	            prof_svc_count_&Edit_Ind.                         0				
	            inst_clm_count_&Edit_Ind.                         0
	            inst_svc_count&Edit_Ind.                          0
	            inst_I_clm_count_&Edit_Ind.                       0
	            inst_O_clm_count_&Edit_Ind.   					  0
	            inst_I_svc_count_&Edit_Ind.    					  0
	            inst_O_svc_count_&Edit_Ind.    					  0
	            PR_2300_CLM02_TOTCHG              				  0  
			    PR_2320_AMT02_PPAD_A_MCD                          0 
				PR_2320_AMT02_PPAD_A_MCR                          0
				PR_2320_AMT02_PPAD_A							  0 /* B-121242 QHP, HFIC */ 
	            PR_2400_LX01_LINE_NUM             				  0
	            PR_2300_CLM05_1_FAC_TYPE                     								 
				IN_2300_CLM02_TOTCHG               				 0  
				IN_2320_AMT02_COB_PRIPAY_AMT_MCD                 0 
				IN_2320_AMT02_COB_PRIPAY_AMT_MCR                 0
 				IN_2320_AMT02_COB_PRIPAY_AMT    	  			 0	/* B-121242 QHP, HFIC */ 
	            IN_2400_LX01_LINE_NUM             				 0
	            IN_2300_HI01_2_DRG_CDE   					 '    '   				
    		  ;
      SET &RGA191..Encounter3_&Edit_Ind._&compno._&run_id.    ;        
	  by clamno lineno ;


/*********************************************************************************************/
/*                             START of  PROFESSIONAL SEGMENT                                 */
/*********************************************************************************************/
      if typecode in ('P') then 
      do;

/*********************************************************************************************/
/*                             START of  2010AA     Billing Provider Name                     */
/*********************************************************************************************/
				   PR_2000A_PRV03_PRV_TAX_CD=BIPRVTXY;
				   PR_2010AA_NM102_BI_PRV_ENTC=BI_PRV_ENTC;
				   PR_2010AA_NM103_BI_PRV_LST              =BI_PRV_LST ;           
				   PR_2010AA_NM104_BI_PRV_FST              =BI_PRV_FST ;  
				   PR_2010AA_NM109_BI_PRV_NPIEINSSN        = /*BIPRVNPI */ BIPRVNPI_Updated;  /* CR# CHG0032056 kv */

				   if missing(BI_PRV_ADD1) = 0 then
				     do;
				           PR_2010AA_N301_BI_PRV_ADD1       = BI_PRV_ADD1;      
				           PR_2010AA_N302_BI_PRV_ADD2       = BI_PRV_ADD2;
				     end;
				   else 
				    do;
				          PR_2010AA_N301_BI_PRV_ADD1              =     BI_PRV_ADD2;   
				          PR_2010AA_N302_BI_PRV_ADD2              =     ' ' ;
				    end;

				    PR_2010AA_N401_BI_PRV_CITY              =     BI_PRV_CITY;            
				    PR_2010AA_N402_BI_PRV_STATE             =     BI_PRV_STATE;   
					PR_2010AA_N403_BI_PRV_ZIP=BI_PRV_ZIP;
				    PR_2010AA_REF02_BI_PRV_MPI = BI_PRV_EIN;

					if missing(BIPRVNPI)=1 then PR_2010AA_REF02=/*mdcaid_1*/ '';/* CR# CHG0032056 kv */
  					if missing(BIPRVNPI_Updated) =1 then PR_2010AA_REF02_PRV =mrg_provno; /* CR# CHG0032056 kv */


/*********************************************************************************************/
/*                             End of  2010AA     Billing Provider Name                       */
/*********************************************************************************************/

/*********************************************************************************************/
/*                             Start of  2010BA     SUBSCRIBER NAME                          */
/*********************************************************************************************/

/* B-96285 start QHP, B-121242 HFIC logic */
%IF (&compno=45  and "&LOB." ne "MPPO" ) or (&compno=42 and "&LOB." eq "QHP") %then %do ;	/*B-248997*/

	if Relcod=1 then do;
		PR_2000B_SBR02='18';
	end ;
	else do;
		PR_2000B_SBR02='';
	end;

	if Relcod ne 1 then do;

					  PR_2010BA_NM103_SUB_LST         =   SUB_MLSTNAM;   
					  PR_2010BA_NM104_SUB_FST         =   SUB_MFSTNAM;         
					  PR_2010BA_NM105_SUB_MID         =   SUB_MMIDFULL;         
					  PR_2010BA_NM109_SUB_MDCARID     =   SUB_MDCARID;
 
					  if missing(SUB_MADRLN1) = 0 then do; /* CR# CHG0032372 kv */
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2010BA_N301=kcompress(SUB_MADRLN1,"@:*`~#");
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2010BA_N302=kcompress(SUB_MADRLN2,"@:*`~#");
					  end;	
					  else do;
								    /* SAS2AWS2: ReplacedFunctionCompress */
					  			  PR_2010BA_N301=kcompress(SUB_MADRLN2,"@:*`~#");
					  end;
					  PR_2010BA_N401=SUB_MCITYCD;
					  PR_2010BA_N402=SUB_MSTACOD;              
					  PR_2010BA_N403=SUB_MZIPCOD;
					  PR_2010BA_DMG02_SUB_DOB       =     put(SUB_BTHDAT,yymmddn8.);   

					  if SUB_SEXCOD NOT IN ('F','M') THEN 
					     do;
					        PR_2010BA_DMG03_SUB_SEX  = 'U';
					     end;
					     else
					     do;
					 		PR_2010BA_DMG03_SUB_SEX  = SUB_SEXCOD ; 
					     end;

					  PR_2010BA_REF02=SUB_socsec;	

					PR_2000C_HL ='HL';
					PR_2000C_HL01=' ';
					PR_2000C_HL02=' ';
					PR_2000C_HL03='23 ';
					PR_2000C_HL04='0';
					PR_2000C_PAT='PAT';

					if relcod eq '2' then 
						PR_2000C_PAT01='01';
					else if relcod eq '3' then
						PR_2000C_PAT01='19';
					else 
						PR_2000C_PAT01='G8';

				    PR_2010CA_NM1 ='NM1';
					PR_2010CA_NM101='QC';
					PR_2010CA_NM102='1';
					PR_2010CA_NM103=MEM_MLSTNAM;
					PR_2010CA_NM104=MEM_MFSTNAM;
					PR_2010CA_NM105=MEM_MMIDFULL;
					PR_2010CA_NM106=' ';
					PR_2010CA_NM107=' ';
					PR_2010CA_NM108='MI';
					PR_2010CA_NM109=MEM_MDCARID;
					PR_2010CA_N3='N3';

					if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0032372 kv */
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2010CA_N301=kcompress(MEM_MADRLN1,"@:*`~#");
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2010CA_N302=kcompress(MEM_MADRLN2,"@:*`~#");
					end;	
					else do;
								    /* SAS2AWS2: ReplacedFunctionCompress */
					  			  PR_2010CA_N301=kcompress(MEM_MADRLN2,"@:*`~#");
					end;

					PR_2010CA_N4='N4';
					PR_2010CA_N401=MEM_MCITYCD;
					PR_2010CA_N402=MEM_MSTACOD;
					PR_2010CA_N403=MEM_MZIPCOD;
					PR_2010CA_DMG='DMG';
					PR_2010CA_DMG01='D8';
					PR_2010CA_DMG02= put(MEM_BTHDAT,yymmddn8.);

					if MEM_SEXCOD NOT IN ('F','M') then
					    do;
					        PR_2010CA_DMG03 = 'U';
					    end;
						else
					    do;
					 		PR_2010CA_DMG03 = MEM_SEXCOD ;      
					    end;					 
	End;
	else do;
		   	 PR_2010BA_NM103_SUB_LST         =     MEM_MLSTNAM;   
					  PR_2010BA_NM104_SUB_FST         =     MEM_MFSTNAM;         
					  PR_2010BA_NM105_SUB_MID         =     MEM_MMIDFULL;         
					  PR_2010BA_NM109_SUB_MDCARID      =   MEM_MDCARID; 				 
					 if missing(MEM_MADRLN1) = 0 then do;/* CR# CHG0032372 kv */
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2010BA_N301=kcompress(MEM_MADRLN1,"@:*`~#");
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2010BA_N302=kcompress(MEM_MADRLN2,"@:*`~#");
					 end;	
					 else do;
								    /* SAS2AWS2: ReplacedFunctionCompress */
					  			  PR_2010BA_N301=kcompress(MEM_MADRLN2,"@:*`~#");
					 end;	
					  PR_2010BA_N401=MEM_MCITYCD;
					  PR_2010BA_N402=MEM_MSTACOD;              
					  PR_2010BA_N403=MEM_MZIPCOD;
					  PR_2010BA_DMG02_SUB_DOB       =     put(MEM_BTHDAT,yymmddn8.);   

					  IF MEM_SEXCOD NOT IN ('F','M') THEN 
					     do;
					        PR_2010BA_DMG03_SUB_SEX='U';
					     end;
						ELSE
					     do;
					 		 PR_2010BA_DMG03_SUB_SEX       =     MEM_SEXCOD ;      
					     end;
						 PR_2010BA_REF02=MEM_socsec;	
					PR_2000C_HL =' ';
					PR_2000C_HL01=' ';
					PR_2000C_HL02=' ';
					PR_2000C_HL03=' ';
					PR_2000C_HL04=' ';
					PR_2000C_PAT=' ';
					PR_2000C_PAT01=' ';

				    PR_2010CA_NM1 =' ';
					PR_2010CA_NM101=' ';
					PR_2010CA_NM102=' ';
					PR_2010CA_NM103=' ';
					PR_2010CA_NM104=' ';
					PR_2010CA_NM105=' ';
					PR_2010CA_NM106=' ';
					PR_2010CA_NM107=' ';
					PR_2010CA_NM108=' ';
					PR_2010CA_NM109=' ';
					PR_2010CA_N3=' ';
					PR_2010CA_N301=' ';
					PR_2010CA_N302=' ';
					PR_2010CA_N4=' ';
					PR_2010CA_N401=' ';
					PR_2010CA_N402=' ';
					PR_2010CA_N403=' ';
					PR_2010CA_DMG=' ';
					PR_2010CA_DMG01=' ';
					PR_2010CA_DMG02=' ';
					PR_2010CA_DMG03=' ';	   		    	
	end;
%end;	/* B-96285 end QHP, HFIC logic */
%else %do;
        		      PR_2010BA_NM103_SUB_LST         =     MEM_MLSTNAM;   
					  PR_2010BA_NM104_SUB_FST         =     MEM_MFSTNAM;         
					  PR_2010BA_NM105_SUB_MID         =     MEM_MMIDFULL;
					%if &compno=30 or  (&compno=45 and "&LOB." eq "MPPO") %then %do ;	/* B-145499 MCR */ /*/*B-248997*/
					  PR_2010BA_NM109_SUB_MDCARID     =     MEM_MDCARID; /* B-146417 */
					%end;
					%else %if &compno=34 %then %do;
						PR_2010BA_NM109_SUB_MDCARID     =     MEM_MDCARID;  
					%end;/*B-226231*/
					%else %If &compno.=01 and &LOB. =DSNP  %then %do;
						PR_2010BA_NM109_SUB_MDCARID     =     MEM_MDCARID;  
					%end;				
					%else %do;	
					  PR_2010BA_NM109_SUB_MDCARID     =     MEM_MDCARID;
					%end;
/*					  PR_2010BA_N301=MEM_MADRLN1;*/
/*					  PR_2010BA_N302=MEM_MADRLN2;*/
					 if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0033527 modified */
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2010BA_N301=kcompress(MEM_MADRLN1,"@:*`~#");
						       /* SAS2AWS2: ReplacedFunctionCompress */
						       PR_2010BA_N302=kcompress(MEM_MADRLN2,"@:*`~#");
					 End;
					 else do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2010BA_N301=kcompress(MEM_MADRLN2,"@:*`~#");
					 end;			
					  PR_2010BA_N401=MEM_MCITYCD;
					  PR_2010BA_N402=MEM_MSTACOD;              
					  PR_2010BA_N403=MEM_MZIPCOD;
					  PR_2010BA_DMG02_SUB_DOB       =     put(MEM_BTHDAT,yymmddn8.);   

					  IF MEM_SEXCOD NOT IN ('F','M') THEN 
					     do;
					        PR_2010BA_DMG03_SUB_SEX='U';
					     end;
						ELSE
					     do;
					 		 PR_2010BA_DMG03_SUB_SEX     =     MEM_SEXCOD ;      
					     end;
						 PR_2010BA_REF02=MEM_socsec;

								
%end;

/*********************************************************************************************/
/*                             End of  2010BA     SUBSCRIBER NAME                             */
/*********************************************************************************************/
      
/*********************************************************************************************/
/*                             start of  2300     CLAIM INFORMATION                           */
/*********************************************************************************************/ 
						
					/*	 if N5_flag='Y' or N4_flag='Y' then do;
							
						 	PR_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(CLAMNO,1,13)||'CR'); * HS 569650 - added substr;
						  end;
						  else do;	
				
						  	PR_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(CLAMNO,1,13)); * HS 569650 - added substr;
						  end;													   * B-160659 section moved to PWK_Segment logic */

			%IF &compno=30 or (&compno=45 and "&LOB." eq "MPPO")  %then %do ;	/* B-145499 MCR *//*B-248997*/
						PR_2300_NTE02= '';
			%end;
			%else %do;						 
						/* SAS2AWS2: ReplacedFunctionCompress */
						PR_2300_NTE02=kcompress(cos||prvspcd);
			%end;

if  relcs1='AA' then do;
					 	PR_2300_CLM11_1_REL_CAUSE1= relcs1;

						
						PR_2300_CLM11_4_AUTO_AC_ST=' ';		
						PR_2300_SGMT_HDR1A='DTP';
						PR_2300_DTP01_439='439';
						PR_2300_DTP02_D8='D8';	
						If missing(ACCDAT)= 0 then do;
							PR_2300_DTP03_ACC_DT=put(ACCDAT,yymmddn8.); 
						End;	
						Else if missing(FIRSTDOS)=0 then do;
							PR_2300_DTP03_ACC_DT=put(FIRSTDOS,yymmddn8.); 
						End;
						Else do;
							PR_2300_DTP03_ACC_DT='';
						End;

				  end;
				  else do;
				  		PR_2300_CLM11_1_REL_CAUSE1='';
						PR_2300_CLM11_4_AUTO_AC_ST='';
						PR_2300_SGMT_HDR1A='';
						PR_2300_DTP01_439='';
						PR_2300_DTP02_D8='';
						PR_2300_DTP03_ACC_DT='';	
				  end;		   

	  
					If first.clamno then
                        do;
							 PR_2300_CLM05_1_FAC_TYPE = poscod;	
                        End;

					   if PR_2300_CLM05_1_FAC_TYPE in ("21","51","61") then 
						do;
						
							PR_2300_SGMT_HDR1B="DTP";						
							PR_2300_DTP01="435";
							PR_2300_DTP02="D8";
							if missing(admdat)=0 then
								do;
									PR_2300_DTP03=put(admdat,yymmddn8.);
								end;
								else 
								do;
								    PR_2300_DTP03=put(firstdos,yymmddn8.);
								end;

						end;
						else
						do;
						    PR_2300_SGMT_HDR1B="";
						    PR_2300_DTP01="";
							PR_2300_DTP02="";
							PR_2300_DTP03="";
						end;		

						if PR_2300_CLM05_1_FAC_TYPE='41' then do;							
						    PR_2300_PWK01='AM';
							PR_2300_PWK02='AA';
						end;	
				/* CR# CHG0035759 - added mapping for 2300 loop pwk segment for N4 & N5 claims */
						if PWK_Segment='N' then do; /* B-160659 PWK_Segment added*/
							PR_2300_PWK01='  ';
							PR_2300_PWK02='  ';
							/* SAS2AWS2: ReplacedFunctionSubstr */
							PR_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(CLAMNO,1,13));			/* B-160659 inserted */
						end;						    
						else if N5_flag='Y' or N4_flag='Y' or PWK_Segment='Y' then do; 		/* B-160659 PWK_Segment added*/
							PR_2300_PWK01='09';
							PR_2300_PWK02='AA';
							/* SAS2AWS2: ReplacedFunctionSubstr */
							PR_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(CLAMNO,1,13)||'CR'); 	/* B-160659 inserted */
						end;
						else if PR_2300_CLM05_1_FAC_TYPE='41' then do;							
						    PR_2300_PWK01='AM';
							PR_2300_PWK02='AA';
							/* SAS2AWS2: ReplacedFunctionSubstr */
							PR_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(CLAMNO,1,13));			/* B-160659 inserted */
						end;
						else do ;
							PR_2300_PWK01='';
							PR_2300_PWK02='';
							/* SAS2AWS2: ReplacedFunctionSubstr */
							PR_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(CLAMNO,1,13));			/* B-160659 inserted */
						end;

                        PR_2300_CLM05_3_FREQ_CDE=claimfrq;
						PR_2300_REF02_ORIG_REFNO='';						
					  PR_2300_HI01_1=NewDiag1_Q;
					  PR_2300_HI02_1=NewDiag2_Q;
					  PR_2300_HI03_1=NewDiag3_Q;
					  PR_2300_HI04_1=NewDiag4_Q;
					  PR_2300_HI05_1=NewDiag5_Q;
					  PR_2300_HI06_1=NewDiag6_Q;
					  PR_2300_HI07_1=NewDiag7_Q;
					  PR_2300_HI08_1=NewDiag8_Q;
					  PR_2300_HI09_1=NewDiag9_Q;
					  PR_2300_HI10_1=NewDiag10_Q;
					  PR_2300_HI11_1=NewDiag11_Q;
					  PR_2300_HI12_1=NewDiag12_Q;		  
		               /* SAS2AWS2: ReplacedFunctionCompress */
		               PR_2300_HI01_2_PRIN_DIAG1               =     kcompress(NewDiag1,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI02_2_DIAG_CDE2                =     kcompress(NewDiag2,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI03_2_DIAG_CDE3                =     kcompress(NewDiag3,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI04_2_DIAG_CDE4                =     kcompress(NewDiag4,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI05_2_DIAG_CDE5                =     kcompress(NewDiag5,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI06_2_DIAG_CDE6                =     kcompress(NewDiag6,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI07_2_DIAG_CDE7                =     kcompress(NewDiag7,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI08_2_DIAG_CDE8                =     kcompress(NewDiag8,'.');
	                   /* SAS2AWS2: ReplacedFunctionCompress */
	                   PR_2300_HI09_2_DIAG_CDE9                =     kcompress(NewDiag9,'.');
                       /* SAS2AWS2: ReplacedFunctionCompress */
                       PR_2300_HI10_2_DIAG_CDE10               =     kcompress(NewDiag10,'.');
                       /* SAS2AWS2: ReplacedFunctionCompress */
                       PR_2300_HI11_2_DIAG_CDE11               =     kcompress(NewDiag11,'.');
                       /* SAS2AWS2: ReplacedFunctionCompress */
                       PR_2300_HI12_2_DIAG_CDE12               =     kcompress(NewDiag12,'.');


/*********************************************************************************************/
/*      CR# CHG0031238 kv                        Modified 2310A loop                           */
/*********************************************************************************************/ 

/*					   PR_2310A_NM103=RF_PHY_LST;*/
/*					   PR_2310A_NM104=RF_PHY_FST;*/
/*					   PR_2310A_NM109=RF_PHY_Npiid;			  */
/*					   PR_2310A_PRV03=''; */
/*					   PR_2310A_REF02=RF_PHY_EIN;*/

 					   
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if kcompress(RF_PHY_npiid) ne ' ' and  klength(kcompress(RF_PHY_npiid))= 10 and kcompress(RF_PHY_npiid)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(RF_PHY_npiid), '0123456789') = 0 and ( kcompress(RF_PHY_LST) ne ' ' or kcompress(RF_PHY_FST) ne ' ' )
		/* SAS2AWS2: ReplacedFunctionCompress */
		and (kcompress(RF_PHY_LST_1) ne ' ' or kcompress(RF_PHY_FST_1) ne ' ') and (rpanpi ne pcpnpiid) and (rpanpi ne "")
		/* SAS2AWS2: ReplacedFunctionCompress */
		and (pcpnpiid ne "")  and kcompress(RF_PHY_npiid_1) ne ' '
		then do;
						PR_2310A_RECID ='2310A';
						PR_2310A_HDR1='NM1';
						PR_2310A_NM101='DN';
						PR_2310A_NM102='1';
						PR_2310A_NM103=RF_PHY_LST;
						PR_2310A_NM104=RF_PHY_FST;
						PR_2310A_NM105='';
						PR_2310A_NM106='';
						PR_2310A_NM107='';
						PR_2310A_NM108='XX';
						PR_2310A_NM109=RF_PHY_Npiid;
						PR_2310A_HDR2='PRV';
						PR_2310A_PRV01='RF';
						PR_2310A_PRV02='';
						PR_2310A_PRV03='';
						PR_2310A_HDR3='REF';
						PR_2310A_REF01='G2';
						PR_2310A_REF02=RF_PHY_EIN;
					   
				end;
				else do;
						PR_2310A_RECID ='2310A';
						PR_2310A_HDR1='';
						PR_2310A_NM101='';
						PR_2310A_NM102='';
						PR_2310A_NM103='';
						PR_2310A_NM104='';
						PR_2310A_NM105='';
						PR_2310A_NM106='';
						PR_2310A_NM107='';
						PR_2310A_NM108='';
						PR_2310A_NM109='';
						PR_2310A_HDR2='';
						PR_2310A_PRV01='';
						PR_2310A_PRV02='';
						PR_2310A_PRV03='';
						PR_2310A_HDR3='';
						PR_2310A_REF01='';
						PR_2310A_REF02='';
				end;
/*********************************************************************************************/
/*      CR# CHG0031238 kv                         Modified 2310B loop                        */
/*********************************************************************************************/ 
/*					   PR_2310B_NM103=RN_PHY_LST;*/
/*					   PR_2310B_NM104=RN_PHY_FST;*/
/*					   PR_2310B_NM109=RN_PHY_Npiid;*/
/*					   PR_2310B_PRV03=RN_PHY_PRVTXY;*/
/*					   PR_2310B_REF02=RN_PHY_EIN;    */

				/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
				if kcompress(RN_PHY_Npiid) ne ' ' and  klength(kcompress(RN_PHY_Npiid))= 10 and kcompress(RN_PHY_Npiid)  NE '0000000000' and
				/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
				(kcompress(RN_PHY_LST) ne ' ' or kcompress(RN_PHY_FST) ne ' ') and kverify(kcompress(RN_PHY_Npiid), '0123456789') = 0
		 		then do;
					   	PR_2310B_RECID='2310B';
						PR_2310B_HDR1 ='NM1';
						PR_2310B_NM101='82';
						PR_2310B_NM102='1';
						PR_2310B_NM103=RN_PHY_LST;
						PR_2310B_NM104=RN_PHY_FST;
						PR_2310B_NM105='';
						PR_2310B_NM106='';
						PR_2310B_NM107='';
						PR_2310B_NM108='XX';				   
						PR_2310B_NM109=RN_PHY_Npiid;
						PR_2310B_HDR2='PRV';
						PR_2310B_PRV01='PE';
						PR_2310B_PRV02='PXC';
						PR_2310B_PRV03=RN_PHY_PRVTXY;
						PR_2310B_HDR3='REF';
						PR_2310B_REF01='G2';
						PR_2310B_REF02=RN_PHY_EIN;    
				end; 
				else do;
						PR_2310B_RECID='2310B';
						PR_2310B_HDR1 ='';
						PR_2310B_NM101='';
						PR_2310B_NM102='';
						PR_2310B_NM103='';
						PR_2310B_NM104='';
						PR_2310B_NM105='';
						PR_2310B_NM106='';
						PR_2310B_NM107='';
						PR_2310B_NM108='';				   
						PR_2310B_NM109='';
						PR_2310B_HDR2='';
						PR_2310B_PRV01='';
						PR_2310B_PRV02='';
						PR_2310B_PRV03='';
						PR_2310B_HDR3='';
						PR_2310B_REF01='';
						PR_2310B_REF02='';   

				end; 


/* HS 569650 KV  - commented  ---->   start */

/*					   if missing(PROVFIRPRACADDR)=0 then do;
						   PR_2310C_N301=kcompress(PROVFIRPRACADDR,"@:*`~#");
					       PR_2310C_N302=kcompress(PROVSECPRACADDR,"@:*`~#");
						   PR_2310C_N401=kcompress(PROVPRACCITY,"@:*`~#");
						   PR_2310C_N402=kcompress(PROVPRACSTATE,"@:*`~#");
						   if Length(kcompress(PROVPRACPOSTAL,"@:*`~#- "))=5 then do;
							   PR_2310C_N403=kcompress(kcompress(PROVPRACPOSTAL,"@:*`~#- ")||"9998");
							end;
						   else do;
						   		PR_2310C_N403=kcompress(PROVPRACPOSTAL,"@:*`~#- ");
						   end;
					   end;
					   else do;						
						    if missing(padrln1) = 0 then do;
							   PR_2310C_N301=kcompress(padrln1,"@:*`~#");
						       PR_2310C_N302=kcompress(padrln2,"@:*`~#");
						   End;
						   else do;
							   PR_2310C_N301=kcompress(padrln2,"@:*`~#");
						   end;
						   PR_2310C_N401=kcompress(pcitycd,"@:*`~#");
						   PR_2310C_N402=kcompress(pstacod,"@:*`~#");
						   if Length(kcompress(pzipcod,"@:*`~#- "))=5 then do;
							   PR_2310C_N403=kcompress(kcompress(pzipcod,"@:*`~#- ")||"9998");
							end;
						   else do;
						   		PR_2310C_N403=kcompress(pzipcod,"@:*`~#- ");
						   end;						 	    
					 	  end; 				     
						  if missing(NPIID_1)=0 then do;
						      PR_2310C_NM103=catx(' ',Plstnam_1,pfstnam_1)  ;
							  PR_2310C_NM109=NPIID_1;
						  end;
						  else do;
						     PR_2310C_NM103=catx(' ',RN_PHY_LST,RN_PHY_FST);
						  	 PR_2310C_NM109=RN_PHY_Npiid;							
						  end;
 
*/ /* HS 569650 KV  -  commented  ---->   end */
 
/* HS 569650 KV  -  service provider information mapping logic changes --->   start */
					   
					  /* SAS2AWS2: ReplacedFunctionCatx */
  					PR_2310C_NM103=catx(' ',Plstnam_1,pfstnam_1)  ;
					if missing(padrln1) = 0 then do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2310C_N301=kcompress(padrln1,"@:*`~#");
						       /* SAS2AWS2: ReplacedFunctionCompress */
						       PR_2310C_N302=kcompress(padrln2,"@:*`~#");
					 End;
					 else do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2310C_N301=kcompress(padrln2,"@:*`~#");
					 end;
					  /* SAS2AWS2: ReplacedFunctionCompress */
					  PR_2310C_N401=kcompress(pcitycd,"@:*`~#");
						   /* SAS2AWS2: ReplacedFunctionCompress */
						   PR_2310C_N402=kcompress(pstacod,"@:*`~#");
						   /* SAS2AWS2: ReplacedFunctionCompress */
						   if Length(kcompress(pzipcod,"@:*`~#- "))=5 then do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2310C_N403=kcompress(kcompress(pzipcod,"@:*`~#- ")||"9998");
							end;
						   else do;
								   /* SAS2AWS2: ReplacedFunctionCompress */
						   		PR_2310C_N403=kcompress(pzipcod,"@:*`~#- ");
						   end;		
					 PR_2310C_NM109=NPIID_1;
					 if missing(NPIID_1)=1 then PR_2310C_REF02=mrg_provno; /* CR# CHG0032056 kv */

/* HS 569650 KV  - service provider information mapping logic changes  ---->   end */
/*********************************************************************************************/
/*    CR# CHG0031238 kv                         Modified 2310A loop                           */
/*********************************************************************************************/ 
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if kcompress(RF_PHY_npiid) ne ' ' and  klength(kcompress(RF_PHY_npiid))= 10 and kcompress(RF_PHY_npiid)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(RF_PHY_npiid), '0123456789') = 0 and ( kcompress(RF_PHY_LST) ne ' ' or kcompress(RF_PHY_FST) ne ' ' )
		/* SAS2AWS2: ReplacedFunctionCompress */
		and (kcompress(RF_PHY_LST_1) ne ' ' or kcompress(RF_PHY_FST_1) ne ' ') and (rpanpi ne pcpnpiid) and (rpanpi ne "")
		/* SAS2AWS2: ReplacedFunctionCompress */
		and (pcpnpiid ne "")  and kcompress(RF_PHY_npiid_1) ne ' '
		then do;
					 if rpanpi ne pcpnpiid and rpanpi ne "" and pcpnpiid ne "" then 
					 do;
					 	PR_2310A_HDR1_PCP='NM1';
						PR_2310A_NM101_PCP='P3';
						PR_2310A_NM102_PCP='1';
						PR_2310A_NM103_PCP=RF_PHY_LST_1;
						PR_2310A_NM104_PCP=RF_PHY_FST_1;
						PR_2310A_NM105_PCP="";
						PR_2310A_NM106_PCP="";
						PR_2310A_NM107_PCP="";
						PR_2310A_NM108_PCP='XX';
						PR_2310A_NM109_PCP=RF_PHY_Npiid_1;
					 end;
				end;
				else do;
					 	PR_2310A_HDR1_PCP='';
						PR_2310A_NM101_PCP='';
						PR_2310A_NM102_PCP='';
						PR_2310A_NM103_PCP='';
						PR_2310A_NM104_PCP='';
						PR_2310A_NM105_PCP="";
						PR_2310A_NM106_PCP="";
						PR_2310A_NM107_PCP="";
						PR_2310A_NM108_PCP='';
						PR_2310A_NM109_PCP='';
				end;
					  	  
					 
/*********************************************************************************************/
/*                             End of  2300     CLAIM INFORMATION                             */
/*********************************************************************************************/ 

/*********************************************************************************************/
/*                             Start of  2320                            */
/*********************************************************************************************/ 

/* B-96285 start QHP, B-121242 HFIC logic */
%IF (&compno=45 and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do ;	 /*B-248997*/

   if clmstat in ("PD","PA","AP") then do;
						PR_2320_SGMT_HDR2 = '';
						PR_2320_CAS01= '';
						PR_2320_CAS02 = '';
						PR_2320_CAS03 = '';		
   end;	
  
	PR_2320_SBR01_PAY2RES_C='P';

    if Relcod  eq 1 then
	   PR_2320_SBR02='18';
	Else if relcod eq '2' then 
	   PR_2320_SBR02='01';
	else if relcod eq '3' then
	   PR_2320_SBR02='19';
	else 
	   PR_2320_SBR02='G8';	

	PR_2320_SBR03=MEM_subsno;
	PR_2320_SBR06='6';
	PR_2320_SBR09='CI';

%end;	/* B-96285 end QHP, HFIC logic */

%else %do;
  
	PR_2320_SGMT_HDR2_MCD = '';
	PR_2320_CAS01_MCD= '';
	PR_2320_CAS02_MCD = '';
	PR_2320_CAS03_MCD = '';		  
	PR_2320_SBR01_PAY2RES_C_MCD='P';
  PR_2320_SBR02_MCD=18;
	PR_2320_SBR03_MCD=MEM_MEMBNO;
	PR_2320_SBR06_MCD='6';
	PR_2320_SBR09_MCD='MC';

	%if &compno.=30 or (&compno.=45 and "&LOB." eq "MPPO") %then %do;	/*B-248997*/ /* B-145499 Medicare, assign APD _MCD values to _MCR, blank out _MCD */
		PR_2320_RECID_MCR='2320';
		PR_2320_SGMT_HDR1_MCR='SBR';		
		PR_2320_SBR01_PAY2RES_C_MCR='P';
		PR_2320_SBR01_PAY2RES_C_MCD=' ';
		PR_2320_SBR02_MCR=18;
		PR_2320_SBR02_MCD=' ';
		PR_2320_SBR03_MCR=MEM_MEMBNO; /* B-146417 */
		PR_2320_SBR03_MCD='';
		PR_2320_SBR04_MCR='';
		PR_2320_SBR05_MCR='';
		PR_2320_SBR06_MCR='6';
		PR_2320_SBR06_MCD=' ';
		PR_2320_SBR07_MCR='';
		PR_2320_SBR08_MCR='';
		PR_2320_SBR09_MCR='MA';
		PR_2320_SBR09_MCD='  ';
		PR_2320_SGMT_HDR2_MCR='';
		PR_2320_CAS01_MCR='';
		PR_2320_CAS02_MCR='';
		PR_2320_CAS03_MCR='';
		PR_2320_SGMT_HDR3_MCR='AMT';		
		PR_2320_AMT01_D_MCR='D';		
		PR_2320_SGMT_HDR5_MCR='';
		PR_2320_OI03_ASN_BEN_IN_MCR='';
		PR_2320_OI04_MCR='';
		PR_2320_OI06_MCR='';
	%end;	
	%else %if &compno.=34 or (&compno.=60 and &LOB. =NSF) %then %do;
		PR_2320_RECID_MCR='2320';
		PR_2320_SGMT_HDR1_MCR='SBR';
		PR_2320_SBR01_PAY2RES_C_MCD='S'; /* B-267578 - Update the Hardcoded P to S */
		PR_2320_SBR01_PAY2RES_C_MCR='P';
		PR_2320_SBR02_MCR=18;
		PR_2320_SBR03_MCR=MEM_MEMBNO;
		PR_2320_SBR04_MCR='';
		PR_2320_SBR05_MCR='';
		PR_2320_SBR06_MCR='1';
		PR_2320_SBR07_MCR='';
		PR_2320_SBR08_MCR='';
		PR_2320_SBR09_MCR='16'; /*B-222679 Replaced MA with 16*/
		PR_2320_SGMT_HDR2_MCR='';
		PR_2320_CAS01_MCR='';
		PR_2320_CAS02_MCR='';
		PR_2320_CAS03_MCR='';
		PR_2320_SGMT_HDR3_MCR='AMT';
		PR_2320_AMT01_D_MCR='D';	
		PR_2320_SGMT_HDR5_MCR='';
		PR_2320_OI03_ASN_BEN_IN_MCR='';
		PR_2320_OI04_MCR='';
		PR_2320_OI06_MCR='';
	%end;
	%else %if &compno.=01 and &LOB. =DSNP %then %do;
		PR_2320_SBR03_MCD=dsnp_membno;
		PR_2320_RECID_MCR='2320';
		PR_2320_SGMT_HDR1_MCR='SBR';
		PR_2320_SBR01_PAY2RES_C_MCD='S'; 
		PR_2320_SBR01_PAY2RES_C_MCR='P';
		PR_2320_SBR02_MCR=18;
		PR_2320_SBR03_MCR=dsnp_membno;
		PR_2320_SBR04_MCR='';
		PR_2320_SBR05_MCR='';
		PR_2320_SBR06_MCR='1';
		PR_2320_SBR07_MCR='';
		PR_2320_SBR08_MCR='';
		PR_2320_SBR09_MCR='16';
		PR_2320_SGMT_HDR2_MCR='';
		PR_2320_CAS01_MCR='';
		PR_2320_CAS02_MCR='';
		PR_2320_CAS03_MCR='';
		PR_2320_SGMT_HDR3_MCR='AMT';
		PR_2320_AMT01_D_MCR='D';	
		PR_2320_SGMT_HDR5_MCR='';
		PR_2320_OI03_ASN_BEN_IN_MCR='';
		PR_2320_OI04_MCR='';
		PR_2320_OI06_MCR='';
	%end;
	%else %do ;
		PR_2320_RECID_MCR='2320';
		PR_2320_SGMT_HDR1_MCR='   ';
		PR_2320_SBR01_PAY2RES_C_MCR='';
		PR_2320_SBR02_MCR='';
		PR_2320_SBR03_MCR='';
		PR_2320_SBR04_MCR='';
		PR_2320_SBR05_MCR='';
		PR_2320_SBR06_MCR='';
		PR_2320_SBR07_MCR='';
		PR_2320_SBR08_MCR='';
		PR_2320_SBR09_MCR='';
		PR_2320_SGMT_HDR2_MCR='';
		PR_2320_CAS01_MCR='';
		PR_2320_CAS02_MCR='';
		PR_2320_CAS03_MCR='';
		PR_2320_SGMT_HDR3_MCR='';
		PR_2320_AMT01_D_MCR='';
		PR_2320_AMT02_PPAD_A_MCR=0;
		PR_2320_SGMT_HDR5_MCR='';
		PR_2320_OI03_ASN_BEN_IN_MCR='';
		PR_2320_OI04_MCR='';
		PR_2320_OI06_MCR='';

	%end;

%end;
										
/*********************************************************************************************/
/*                             End of  2320                            */
/*********************************************************************************************/ 


/*********************************************************************************************/
/*                             Start of  2330A     OTHER SUBSCRIBER NAME                       */
/*********************************************************************************************/ 

/* B-96285 start QHP, HFIC logic */
%IF (&compno=45 and "&LOB." ne "MPPO")  or (&compno=42 and "&LOB." eq "QHP") %then %do ;		/* B-121242  */ /*B-248997*/

                     PR_2330A_NM103_SUB_LST          =SUB_MLSTNAM;  /* 477378 */
					 PR_2330A_NM104_SUB_FST          =SUB_MFSTNAM;  /* 477378 */

                     PR_2330A_NM109_SUB_MDCARID      =     SUB_MDCARID;														
					 if missing(SUB_MADRLN1) = 0 then do;/* CR# CHG0032372 kv */
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2330A_N301=kcompress(SUB_MADRLN1,"@:*`~#");
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2330A_N302=kcompress(SUB_MADRLN2,"@:*`~#");
					 end;	
					 else do;
								    /* SAS2AWS2: ReplacedFunctionCompress */
					  			  PR_2330A_N301=kcompress(SUB_MADRLN2,"@:*`~#");
					 end;
                     PR_2330A_NM401                  =     SUB_MCITYCD;
                     PR_2330A_NM402                  =     SUB_MSTACOD;
				     PR_2330A_NM403                  =     SUB_MZIPCOD;  

%end;	/* B-96285 end QHP, HFIC logic */

%else %do;
	
                     PR_2330A_NM103_SUB_LST_MCD          =MEM_MLSTNAM;  
					 PR_2330A_NM104_SUB_FST_MCD          =MEM_MFSTNAM;  
			 		 PR_2330A_NM109_SUB_MDCARID_MCD =     MEM_MDCARID; 												
/*					 PR_2330A_N301_MCD		             =     MEM_MADRLN1;*/
/*					 PR_2330A_N302_MCD                   =     MEM_MADRLN2;*/
					  if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0033527 modified */
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2330A_N301_MCD=kcompress(MEM_MADRLN1,"@:*`~#");
						       /* SAS2AWS2: ReplacedFunctionCompress */
						       PR_2330A_N302_MCD=kcompress(MEM_MADRLN2,"@:*`~#");
					 End;
					 else do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2330A_N301_MCD=kcompress(MEM_MADRLN2,"@:*`~#");
					 end;
                     PR_2330A_NM401_MCD                  =     MEM_MCITYCD;
                     PR_2330A_NM402_MCD                  =     MEM_MSTACOD;
				     PR_2330A_NM403_MCD                  =     MEM_MZIPCOD;  

			 		 %if &compno.=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do;/*B-248997*/ /* B-145499 Medicare, assign APD _MCD values to _MCR, blank out _MCD */
					 	PR_2330A_RECID_MCR='2330A';
						PR_2330A_SGMT_HDR1_MCR='NM1';
						PR_2330A_NM101_MCR='IL';
						PR_2330A_NM102_MCR='1';
						PR_2330A_NM103_SUB_LST_MCR=MEM_MLSTNAM; 
						PR_2330A_NM104_SUB_FST_MCR=MEM_MFSTNAM;  
						PR_2330A_NM108_MI_MCR='MI';
						PR_2330A_NM109_SUB_MDCARID_MCR=MEM_MDCARID; /* B-146417 */	
						PR_2330A_SGMT_HDR1A_MCR='N3';

						if missing(MEM_MADRLN1) = 0 then do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2330A_N301_MCR=kcompress(MEM_MADRLN1,"@:*`~#");
						       /* SAS2AWS2: ReplacedFunctionCompress */
						       PR_2330A_N302_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
						end;
						else do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2330A_N301_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
					 	end;
						PR_2330A_SGMT_HDR2_MCR='N4';
						PR_2330A_NM401_MCR=MEM_MCITYCD;
						PR_2330A_NM402_MCR=MEM_MSTACOD;
						PR_2330A_NM403_MCR=MEM_MZIPCOD;						
						
						PR_2330A_NM103_SUB_LST_MCD='';
						PR_2330A_NM104_SUB_FST_MCD='';						
						PR_2330A_NM109_SUB_MDCARID_MCD='';						
						PR_2330A_N301_MCD='';
						PR_2330A_N302_MCD='';						
						PR_2330A_NM401_MCD='';
						PR_2330A_NM402_MCD='';
						PR_2330A_NM403_MCD='';			 
					 %end;
					 %else %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
					 	PR_2330A_RECID_MCR='2330A';
						PR_2330A_SGMT_HDR1_MCR='NM1';
						PR_2330A_NM101_MCR='IL';
						PR_2330A_NM102_MCR='1';
						PR_2330A_NM103_SUB_LST_MCR=MEM_MLSTNAM; 
						PR_2330A_NM104_SUB_FST_MCR=MEM_MFSTNAM;  
						PR_2330A_NM108_MI_MCR='MI';
						PR_2330A_NM109_SUB_MDCARID_MCR=MEMBNO;	
						PR_2330A_SGMT_HDR1A_MCR='N3';
/*						PR_2330A_N301_MCR=MEM_MADRLN1;*/
/*						PR_2330A_N302_MCR=MEM_MADRLN2;*/
						if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0033527 modified */
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2330A_N301_MCR=kcompress(MEM_MADRLN1,"@:*`~#");
						       /* SAS2AWS2: ReplacedFunctionCompress */
						       PR_2330A_N302_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
						End;
						else do;
							   /* SAS2AWS2: ReplacedFunctionCompress */
							   PR_2330A_N301_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
					 	end;
						PR_2330A_SGMT_HDR2_MCR='N4';
						PR_2330A_NM401_MCR=MEM_MCITYCD;
						PR_2330A_NM402_MCR=MEM_MSTACOD;
						PR_2330A_NM403_MCR=MEM_MZIPCOD;  
					 %end;
					%else %If &compno.=01 and &LOB. =DSNP  %then %do;
					 	PR_2330A_RECID_MCR='2330A';
						PR_2330A_SGMT_HDR1_MCR='NM1';
						PR_2330A_NM101_MCR='IL';
						PR_2330A_NM102_MCR='1';
						PR_2330A_NM103_SUB_LST_MCR=MEM_MLSTNAM_MCR; 
						PR_2330A_NM104_SUB_FST_MCR=MEM_MFSTNAM_MCR;  
						PR_2330A_NM108_MI_MCR='MI';
						PR_2330A_NM109_SUB_MDCARID_MCR=MEM_MEMBNO_MCR;	
						PR_2330A_SGMT_HDR1A_MCR='N3';
						if missing(MEM_MADRLN1_MCR) = 0 then do; 
							   PR_2330A_N301_MCR=kcompress(MEM_MADRLN1_MCR,"@:*`~#");
						       PR_2330A_N302_MCR=kcompress(MEM_MADRLN2_MCR,"@:*`~#");
						End;
						else do;
							   PR_2330A_N301_MCR=kcompress(MEM_MADRLN2_MCR,"@:*`~#");
					 	end;
						PR_2330A_SGMT_HDR2_MCR='N4';
						PR_2330A_NM401_MCR=MEM_MCITYCD_MCR;
						PR_2330A_NM402_MCR=MEM_MSTACOD_MCR;
						PR_2330A_NM403_MCR=MEM_MZIPCOD_MCR; 
					 %end;					 
					 %else %do;
					 	PR_2330A_RECID_MCR='2330A';
						PR_2330A_SGMT_HDR1_MCR='';
						PR_2330A_NM101_MCR='';
						PR_2330A_NM102_MCR='';
						PR_2330A_NM103_SUB_LST_MCR='';
						PR_2330A_NM104_SUB_FST_MCR='';
						PR_2330A_NM108_MI_MCR='';
						PR_2330A_NM109_SUB_MDCARID_MCR='';
						PR_2330A_SGMT_HDR1A_MCR='';
						PR_2330A_N301_MCR='';
						PR_2330A_N302_MCR='';
						PR_2330A_SGMT_HDR2_MCR='';
						PR_2330A_NM401_MCR='';
						PR_2330A_NM402_MCR='';
						PR_2330A_NM403_MCR='';
					 %end;

%end;					

/*********************************************************************************************/
/*                             End of  2330A     OTHER SUBSCRIBER NAME                       */
/*********************************************************************************************/ 

/*********************************************************************************************/
/*                             Start of  2330B    OTHER PAYER NAME                            */
/*********************************************************************************************/ 

/* B-96285 start QHP, HFIC logic */
%IF (&compno=45 and "&LOB." ne "MPPO")  or (&compno=42 and "&LOB." eq "QHP") %then %do ; /*B-248997*/	/* B-121242 */

					 	PR_2330B_ICN  = clamno ; 
						
                        if PR_2300_CLM05_3_FREQ_CDE in ("7","8") OR "&N4TON5_icn_add." eq "Y" then
                        do;
                              PR_2330B_ICN1  = icn; 
                        end;
                		else 
                        do;
                       		  PR_2330B_ICN1  = '                ' ;                        
                         end;
				  
						PR_2330B_DTP03=put(max_pidate,yymmddn8.) ; /*[B-315447] - DS - Updates*/

						%if "&x12_ctrl." ne "EIS" %then %do;		/* B-121242 */
						PR_2330B_NM103_OTPAY_PRV_LST = "&cname.";	/* B-121242 */

							%If &compno.=45  and "&LOB." ne "MPPO" %then %do;	/*B-248997*/
								PR_2330B_NM109_OTPAY_HMO = HIOSID;		/* B-146417 add hiosid from elgibility outbound */
								%if ("&hiosflg." = "Y") %then %do; /* B-274606 */
									PR_2330B_NM109_OTPAY_HMO = hios_plan_id_cd;
								%end;								
							%end;
							%else %do;
								PR_2330B_NM109_OTPAY_HMO = "&HIOSID.";	/* B-121242 */
								%if ("&hiosflg." = "Y") %then %do; /* B-274606 */
									PR_2330B_NM109_OTPAY_HMO = hix_policy_num_cd;
								%end;								
							%end;						
						%end;

		/* Start 477378 */
					 if relcod not in ("1") then do;
						 PR_2330C_SGMT_HDR1 ='NM1';
				         PR_2330C_NM101='QC';
					     PR_2330C_NM102='1';
						 PR_2330C_NM103_SUB_LST          =  MEM_MLSTNAM;  
						 PR_2330C_NM104_SUB_FST			 = MEM_MFSTNAM;
						 PR_2330C_NM108_MI='MI';
	                     PR_2330C_NM109_SUB_MDCARID      =     MEM_MDCARID;														
						 PR_2330C_SGMT_HDR1A='N3';
						 if missing(MEM_MADRLN1) = 0 then do;/* CR# CHG0032372 kv */
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2330C_N301=kcompress(MEM_MADRLN1,"@:*`~#");
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  PR_2330C_N302=kcompress(MEM_MADRLN2,"@:*`~#");
					 	 end;	
					 	 else do;
								    /* SAS2AWS2: ReplacedFunctionCompress */
					  			  PR_2330C_N301=kcompress(MEM_MADRLN2,"@:*`~#");
					 	 end;
						 PR_2330C_SGMT_HDR2='N4';
	                     PR_2330C_NM401                  =     MEM_MCITYCD;
	                     PR_2330C_NM402                  =     MEM_MSTACOD;
					     PR_2330C_NM403                  =     MEM_MZIPCOD;
				    end; 
		/* End 477378 */

%end;	/* B-96285 end QHP, HFIC logic */

%else %do;

						PR_2330B_ICN_MCD  = clamno ; 					
                        if PR_2300_CLM05_3_FREQ_CDE in ("7","8") OR "&N4TON5_icn_add." eq "Y" then
                        do;
                              PR_2330B_ICN1_MCD  = icn; 
                        end;
                		else 
                        do;
                       		  PR_2330B_ICN1_MCD  = '                ' ;                        
                        end;				  
						PR_2330B_DTP03_MCD=put(max_pidate,yymmddn8.) ;
						PR_2330B_NM103_OTPAY_PRV_LST_MCD="&cname.";
						PR_2330B_NM109_OTPAY_HMO_MCD="&HIOSID.";
							%if ("&hiosflg." = "Y" and (&compno.=01 or &compno.=20 or (&compno.=42 and "&LOB." eq "EP"))) %then %do; /* B-274606 */
								PR_2330B_NM109_OTPAY_HMO_MCD = hix_policy_num_cd;
							%end;						

						%if &compno.=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do; /*B-248997*/ /* B-145499 Medicare, assign APD _MCD values to _MCR, blank out _MCD */
							PR_2330B_RECID_MCR='2330B';
							PR_2330B_SGMT_HDR1_MCR='NM1';
							PR_2330B_NM101_MCR='PR';
							PR_2330B_NM102_OTPAY_PRV_ENT_MCR='2';
							PR_2330B_NM103_OTPAY_PRV_LST_MCR="&cname.";
							PR_2330B_NM108_OTPAY_PRV_Q_MCR='PI';
							PR_2330B_NM109_OTPAY_HMO_MCR=HIOSID; /* B-146417 replace "&HIOSID." with elgibility var */
							PR_2330B_SGMT_HDR2_MCR='N3';
							PR_2330B_N301_MCR='';
							PR_2330B_N302_MCR='';
							PR_2330B_SGMT_HDR3_MCR='N4';	
							PR_2330B_N401_MCR='';
							PR_2330B_N402_MCR='';
							PR_2330B_N403_MCR='';
							PR_2330B_SGMT_HDR4_MCR='DTP';
							PR_2330B_DTP01_MCR='573';
							PR_2330B_DTP02_MCR='D8';
							PR_2330B_DTP03_MCR=put(max_pidate,yymmddn8.) ;	/*[B-315447] - DS - Updates*/
							PR_2330B_SGMT_HDR5_MCR='REF'; /*B-222679*/
					    PR_2330B_REF01_OPSI_MCR='';/*B-222679*/
					    PR_2330B_REF02_OPSI_MCR='';/*B-222679*/
							PR_2330B_SGMT_HDR6_MCR='REF';
							PR_2330B_F8_MCR='F8';
							PR_2330B_ICN_MCR= clamno ; 
							PR_2330B_SGMT_HDR7_MCR='REF';
							PR_2330B_BP_MCR='BP';							
							
							PR_2330B_NM103_OTPAY_PRV_LST_MCD=' ';							
							PR_2330B_NM109_OTPAY_HMO_MCD='';							
							PR_2330B_N301_MCD='';
							PR_2330B_N302_MCD='';							
							PR_2330B_N401_MCD='';
							PR_2330B_N402_MCD='';
							PR_2330B_N403_MCD='';							
							PR_2330B_DTP03_MCD='';							
										
							 if PR_2300_CLM05_3_FREQ_CDE in ("7","8") OR "&N4TON5_icn_add." eq "Y" then
                      		  	do;
                    	          PR_2330B_ICN1_MCR  = icn; 
                        		end;
                				else 
                        		do;
                       		 		 PR_2330B_ICN1_MCR  = '                      ' ;                        
                        	 	end;
						%end;
						%else %If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
							PR_2330B_RECID_MCR='2330B';
							PR_2330B_SGMT_HDR1_MCR='NM1';
							PR_2330B_NM101_MCR='PR';
							PR_2330B_NM102_OTPAY_PRV_ENT_MCR='2';
							PR_2330B_NM103_OTPAY_PRV_LST_MCR="Medicare";
							PR_2330B_NM108_OTPAY_PRV_Q_MCR='PI';
							/*PR_2330B_NM109_OTPAY_HMO_MCR="&HIOSID."; */
							PR_2330B_NM109_OTPAY_HMO_MCR='N95284]H3359]034'; /*B-222679*/	
							PR_2330B_SGMT_HDR2_MCR='N3';
							PR_2330B_N301_MCR='';
							PR_2330B_N302_MCR='';
							PR_2330B_SGMT_HDR3_MCR='N4';	
							PR_2330B_N401_MCR='';
							PR_2330B_N402_MCR='';
							PR_2330B_N403_MCR='';
							PR_2330B_SGMT_HDR4_MCR='DTP';
							PR_2330B_DTP01_MCR='573';
							PR_2330B_DTP02_MCR='D8';
							PR_2330B_DTP03_MCR=put(max_pidate,yymmddn8.) ;	/*[B-315447] - DS - Updates*/
							PR_2330B_SGMT_HDR5_MCR='REF'; /*B-222679*/
					    PR_2330B_REF01_OPSI_MCR='2U';/*B-222679*/
					    PR_2330B_REF02_OPSI_MCR='INTDUAL';/*B-222679*/
							PR_2330B_SGMT_HDR6_MCR='REF';
							PR_2330B_F8_MCR='F8';
							PR_2330B_ICN_MCR= clamno ; 
							PR_2330B_SGMT_HDR7_MCR='REF';
							PR_2330B_BP_MCR='BP';
			
							if PR_2300_CLM05_3_FREQ_CDE in ("7","8") OR "&N4TON5_icn_add." eq "Y" then
                      		        PR_2330B_ICN1_MCR  = icn; 
                        	else	PR_2330B_ICN1_MCR  = '                      ' ;                        
						%end;
						%else %If &compno.=01 and &LOB. =DSNP  %then %do;
							PR_2330B_RECID_MCR='2330B';
							PR_2330B_SGMT_HDR1_MCR='NM1';
							PR_2330B_NM101_MCR='PR';
							PR_2330B_NM102_OTPAY_PRV_ENT_MCR='2';
							PR_2330B_NM103_OTPAY_PRV_LST_MCR="Medicare";
							PR_2330B_NM108_OTPAY_PRV_Q_MCR='PI';
							PR_2330B_NM109_OTPAY_HMO_MCR='N95284]H3359]038'; 
							PR_2330B_SGMT_HDR2_MCR='N3';
							PR_2330B_N301_MCR='';
							PR_2330B_N302_MCR='';
							PR_2330B_SGMT_HDR3_MCR='N4';	
							PR_2330B_N401_MCR='';
							PR_2330B_N402_MCR='';
							PR_2330B_N403_MCR='';
							PR_2330B_SGMT_HDR4_MCR='DTP';
							PR_2330B_DTP01_MCR='573';
							PR_2330B_DTP02_MCR='D8';
							PR_2330B_DTP03_MCR=put(max_pidate,yymmddn8.) ;	
							PR_2330B_SGMT_HDR5_MCR='REF';
							PR_2330B_REF01_OPSI_MCR='2U';
							PR_2330B_REF02_OPSI_MCR='INTDUAL';
							PR_2330B_SGMT_HDR6_MCR='REF';
							PR_2330B_F8_MCR='F8';
							PR_2330B_ICN_MCR= clamno_MCR ; 
							PR_2330B_SGMT_HDR7_MCR='REF';
							PR_2330B_BP_MCR='BP';
			
							if PR_2300_CLM05_3_FREQ_CDE in ("7","8") OR "&N4TON5_icn_add." eq "Y" then
                      		        PR_2330B_ICN1_MCR  = icn; 
                        	else	PR_2330B_ICN1_MCR  = '                      ' ;                        
						%end;						
						%else %do;
							PR_2330B_RECID_MCR='2330B';
							PR_2330B_SGMT_HDR1_MCR='';
							PR_2330B_NM101_MCR='';
							PR_2330B_NM102_OTPAY_PRV_ENT_MCR='';
							PR_2330B_NM103_OTPAY_PRV_LST_MCR='';
							PR_2330B_NM108_OTPAY_PRV_Q_MCR='';
							PR_2330B_NM109_OTPAY_HMO_MCR='';
							PR_2330B_SGMT_HDR2_MCR='';
							PR_2330B_N301_MCR='';
							PR_2330B_N302_MCR='';
							PR_2330B_SGMT_HDR3_MCR='';
							PR_2330B_N401_MCR='';
							PR_2330B_N402_MCR='';
							PR_2330B_N403_MCR='';
							PR_2330B_SGMT_HDR4_MCR='';
							PR_2330B_DTP01_MCR='';
							PR_2330B_DTP02_MCR='';
							PR_2330B_DTP03_MCR='';
							PR_2330B_SGMT_HDR5_MCR=''; /*B-222679*/
					    PR_2330B_REF01_OPSI_MCR='';/*B-222679*/
					    PR_2330B_REF02_OPSI_MCR='';/*B-222679*/
							PR_2330B_SGMT_HDR6_MCR='';
							PR_2330B_F8_MCR='';
							PR_2330B_ICN_MCR='';
							PR_2330B_SGMT_HDR7_MCR='';
							PR_2330B_BP_MCR='';
							PR_2330B_ICN1_MCR='';
						%end;

%end;
                 
/*********************************************************************************************/
/*                             End of  2330B    OTHER PAYER NAME                              */
/*********************************************************************************************/

/*********************************************************************************************/
/*                             Start of  2400    SERVICE LINE                                 */
/*********************************************************************************************/ 
                  

                    PR_2400_SV101_3_S_PRC_MOD1    =     Mod_Cod1;      
             		PR_2400_SV101_4_S_PRC_MOD2    =     Mod_Cod2;        
                    PR_2400_SV101_5_S_PRC_MOD3    =     Mod_Cod3;        
                    PR_2400_SV101_6_S_PRC_MOD4    =     Mod_Cod4;  
					PR_2400_SV103_UN=put(svccod,$svccod.);
                    PR_2400_SV107_1_DRG_POINT1 =  ptr1;   
    	            PR_2400_SV107_2_DRG_POINT2 =  ptr2;       
        	        PR_2400_SV107_3_DRG_POINT3  =  ptr3;       
            	    PR_2400_SV107_4_DRG_POINT4 =  ptr4; 

			 if svcdat ne enddat  then 
				do;

					PR_2400_DTP02='RD8';
					 /* SAS2AWS2: ReplacedFunctionCatx */
				 	PR_2400_DTP03=catx("-",put(SVCDAT,yymmddn8.),put(ENDDAT,yymmddn8.)) ;
					
				end;
				else
				do;
				 	PR_2400_DTP02='D8';
				 	PR_2400_DTP03=put(SVCDAT,yymmddn8.);
				end;				
				PR_2400_CN101_05='';
		   
/*********************************************************************************************/
/*                             End of  2400    SERVICE LINE                                   */
/*********************************************************************************************/ 

/*********************************************************************************************/
/*                             Start of  2410    DRUG IDENTIFICATION                          */
/*********************************************************************************************/ 
                          /* SAS2AWS2: ReplacedFunctionLength */
                          If klength(NDCSVC)=11 then
                              do;
                                    PR_2410_LIN02_PRD_Q="N4";
                               end;
                     	 Else 
                               do;
                             	     PR_2410_LIN02_PRD_Q=" ";
                               end;

							     /* SAS2AWS2: ReplacedFunctionLength-ReplacedFunctionUpcase */
							     if kupcase(NDCSVC) not in ('MISSING','N4') and klength(NDCSVC)=11 then do;/*HS# 569650 - added length statement*/
				   					 PR_2410_LIN03_DR_NDC_C=NDCSVC;
				  				  end;

/*								  if upcase(NDCUOM) ne "UN" then do;*/ /* HS 569650 - commented */
								  	PR_2410_CTP05_1=NDCUOM;
/*								  end;*/


/*********************************************************************************************/
/*                             End of  2410    DRUG IDENTIFICATION                            */
/*********************************************************************************************/ 

/*********************************************************************************************/
/*                             Start of  2430    LINE ADJUDICATION INFORMATION                */
/*********************************************************************************************/ 
                      
	                    PR_2430_SVD03_3_SL_CPTMD1     =     Mod_Cod1;        
	                    PR_2430_SVD03_4_SL_CPTMD2     =     Mod_Cod2;            
	                    PR_2430_SVD03_5_SL_CPTMD3     =     Mod_Cod3;       
	                    PR_2430_SVD03_6_SL_CPTMD4     =     Mod_Cod4;  
	                    
	                    PR_2430_SGMT_HDR1_MCR             =  '   ';
	                    PR_2430_SVD03_1_SL_SVCTYP_MCR     =  '  ';
	                    PR_2430_SVD03_2_SL_CPTCOD_MCR     =  '                 ';
	                    PR_2430_SVD03_3_SL_CPTMD1_MCR     =  '        ';        
	                    PR_2430_SVD03_4_SL_CPTMD2_MCR     =  '        ';             
	                    PR_2430_SVD03_5_SL_CPTMD3_MCR     =  '        ';
	                    PR_2430_SVD03_6_SL_CPTMD4_MCR     =  '        ';
	                    %If &compno.=34 %then %do;
	                    PR_2430_SGMT_HDR1_MCR             = 'SVD';
	                    PR_2430_SVD03_1_SL_SVCTYP_MCR     = 'HC';
	                    PR_2430_SVD03_2_SL_CPTCOD_MCR     =  svccod;
	                    PR_2430_SVD03_3_SL_CPTMD1_MCR     =  Mod_Cod1;        
	                    PR_2430_SVD03_4_SL_CPTMD2_MCR     =  Mod_Cod2;            
	                    PR_2430_SVD03_5_SL_CPTMD3_MCR     =  Mod_Cod3;       
	                    PR_2430_SVD03_6_SL_CPTMD4_MCR     =  Mod_Cod4;
	                  %end;
						%If &compno.=01 and &LOB. =DSNP  %then %do;
						PR_2430_SGMT_HDR1_MCR             = 'SVD';
						PR_2430_SVD03_1_SL_SVCTYP_MCR     = 'HC';
						PR_2430_SVD03_2_SL_CPTCOD_MCR     =  svccod_MCR;
						PR_2430_SVD03_3_SL_CPTMD1_MCR     =  Mod_Cod_MCR1;        
						PR_2430_SVD03_4_SL_CPTMD2_MCR     =  Mod_Cod_MCR2;            
						PR_2430_SVD03_5_SL_CPTMD3_MCR     =  Mod_Cod_MCR3;       
						PR_2430_SVD03_6_SL_CPTMD4_MCR     =  Mod_Cod_MCR4;
						%end;					  
	                       
					    PR_2430_SGMT_HDR1A='CAS';
						/*B-120951 - Assign default value*/
						PR_2430_CAS01=' ';
						PR_2430_CAS02=' ';
						PR_2430_CAS03=.;
						PR_2430_CAS04=.;
						PR_2430_CAS05=' ';
						PR_2430_CAS06=' ';
						PR_2430_CAS07=.;
						PR_2430_CAS08=.;
						PR_2430_CAS09=' ';
						PR_2430_CAS10=' ';
						PR_2430_CAS11=.;
						PR_2430_CAS12=.;
						PR_2430_CAS13=' ';
						PR_2430_CAS14=' ';
						PR_2430_CAS15=.;
						PR_2430_CAS16=.;
						PR_2430_CAS17=' ';
						PR_2430_CAS18=' ';
						PR_2430_CAS19=.;
						PR_2430_CAS20=.;
						PR_2430_CAS21=' ';
						
						
						
						PR_2430_SGMT_HDR1A_MCR='CAS';
						PR_2430_CAS01_MCR=' ';
						PR_2430_CAS02_MCR=' ';
						PR_2430_CAS03_MCR=.;
						PR_2430_CAS04_MCR=.;
						PR_2430_CAS05_MCR=' ';
						PR_2430_CAS06_MCR=' ';
						PR_2430_CAS07_MCR=.;
						PR_2430_CAS08_MCR=.;
						PR_2430_CAS09_MCR=' ';
						PR_2430_CAS10_MCR=' ';
						PR_2430_CAS11_MCR=.;
						PR_2430_CAS12_MCR=.;
						PR_2430_CAS13_MCR=' ';
						PR_2430_CAS14_MCR=' ';
						PR_2430_CAS15_MCR=.;
						PR_2430_CAS16_MCR=.;
						PR_2430_CAS17_MCR=' ';
						PR_2430_CAS18_MCR=' ';
						PR_2430_CAS19_MCR=.;
						PR_2430_CAS20_MCR=.;
						PR_2430_CAS21_MCR=' ';

						/* ALM# 5160 - Commented old CAS mapping logic*/ 
						  /*%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
						  	    PR_2430_SGMT_HDR1A='CAS';
								PR_2430_CAS01='OA';               
								PR_2430_CAS02='23';
								PR_2430_CAS04='PR';
								PR_2430_CAS05='2';
								PR_2430_CAS07='PR';
								PR_2430_CAS08='3';
								PR_2430_CAS10='CO';
								PR_2430_CAS11='45';
								PR_2430_CAS13='CO';
								PR_2430_CAS14='96';
								PR_2430_CAS16='OA';
								PR_2430_CAS17='131';
								PR_2430_CAS19='';
								PR_2430_CAS20='';
								PR_2430_CAS21=0;	
						  %end;
						  %else %do;
						  	    PR_2430_SGMT_HDR1A='CAS';
								PR_2430_CAS01='PR';
								PR_2430_CAS02='1';
								PR_2430_CAS04='PR';
								PR_2430_CAS05='2';
								PR_2430_CAS07='PR';
								PR_2430_CAS08='3';
								PR_2430_CAS10='CO';
								PR_2430_CAS11='45';
								PR_2430_CAS13='CO';
								PR_2430_CAS14='96';
								PR_2430_CAS16='OA';
								PR_2430_CAS17='131';
								PR_2430_CAS19='';
								PR_2430_CAS20='';
								PR_2430_CAS21=0;		
						  %end;*/
						/* ALM# 5160 - added new CAS mapping logic*/
						%If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;

/*B-120951 Commented the old CAS logic and Include the new CAS logic */
/*								if capind = 'F' then*/
/*									do;*/
/*										   	PR_2430_CAS01='OA';*/
/*											PR_2430_CAS02='45';	*/
/*								end;*/
/*								else if capind = 'C' then*/
/*									do;*/
/*										  PR_2430_CAS01='CO';*/
/*											PR_2430_CAS02='24';*/	
/*								end;*/
/*										  PR_2430_CAS05='OA';*/
/*											PR_2430_CAS06='45';*/
/*											PR_2430_CAS09=' ';*/
/*											PR_2430_CAS10=' ';*/
/*											PR_2430_CAS11=.;*/
/*											PR_2430_CAS12=.;*/
/*											PR_2430_CAS13=' ';*/
/*											PR_2430_CAS14=' ';*/
/*											PR_2430_CAS15=.;*/
/*											PR_2430_CAS16=.;*/
/*											PR_2430_CAS17=' ';*/
/*											PR_2430_CAS18=' ';*/
/*											PR_2430_CAS19=.;*/
/*											PR_2430_CAS20=.;*/
/*											PR_2430_CAS21=' ';*/
											/* VVKR Code Start -- B-75041 */
/*											if cas_flag=1 then do;*/
/*												PR_2430_CAS09='CO';*/
/*												PR_2430_CAS10=CARC;*/
/*												PR_2430_CAS11=0;*/
/*												PR_2430_CAS12=0;*/
/*											end;*/
											/* VVKR Code End -- B-75041 */

							if capind = 'F'  and (PR_2430_CAS03_Var > 0 or (PR_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										/*PR_2430_CAS01='CO';*/ /* B-121242 */
										/*PR_2430_CAS02='45';*/										
										if cas_flag <> 1 then do; PR_2430_CAS01='CO';PR_2430_CAS02='45'; end;
										if cas_flag=1 then do; PR_2430_CAS01='CO'; PR_2430_CAS02=CARC; end;											
										PR_2430_CAS03=PR_2430_CAS03_Var;
										if PR_2430_CAS04_Var < 0 then PR_2430_CAS04=0; else PR_2430_CAS04=PR_2430_CAS04_Var;
									end;

								else if capind = 'C'  and (PR_2430_CAS03_Var > 0 or (PR_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										PR_2430_CAS01='CO';
										PR_2430_CAS02='24';	
										PR_2430_CAS03=PR_2430_CAS03_Var;
										if PR_2430_CAS04_Var < 0 then PR_2430_CAS04=0; else PR_2430_CAS04=PR_2430_CAS04_Var;
									end;
							/*[B-272655]_[APD Compcare CAS Segment Capitated Encounters Update]  CAS05 TO CAS08 Added*/
							if capind = 'C'  and (PR_2430_CAS07_Var > 0 or (PR_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
										if cas_flag <> 1 then do; PR_2430_CAS05='CO'; PR_2430_CAS06='45'; end;
										if cas_flag=1		 then do; PR_2430_CAS05='CO'; PR_2430_CAS06=CARC; end;
										PR_2430_CAS07=PR_2430_CAS07_Var;
										if PR_2430_CAS08_Var < 0 then PR_2430_CAS08=0;else PR_2430_CAS08=PR_2430_CAS08_Var;
									end;

							/*if capind = 'F'  and (PR_2430_CAS07_Var > 0 or (PR_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
										if cas_flag <> 1 then do; PR_2430_CAS05='CO';*/ /* B-121242 */ /*PR_2430_CAS06='45'; end;
										if cas_flag=1 then do; PR_2430_CAS05='CO'; PR_2430_CAS06=CARC; end;	
										PR_2430_CAS07=PR_2430_CAS07_Var;
										if PR_2430_CAS08_Var < 0 then PR_2430_CAS08=0;else PR_2430_CAS08=PR_2430_CAS08_Var;
									end;

								else if capind = 'C'  and (PR_2430_CAS07_Var > 0 or (PR_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
										PR_2430_CAS05='CO';*/ /* B-121242 */
										/*PR_2430_CAS06='45';	
										PR_2430_CAS07=PR_2430_CAS07_Var;
										if PR_2430_CAS08_Var < 0 then PR_2430_CAS08=0;else PR_2430_CAS08=PR_2430_CAS08_Var;
									end;*/
									
									
									if capind = 'F'  and (PR_2430_CAS03_MCR_Var > 0 or (PR_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										PR_2430_CAS01_MCR='CO';PR_2430_CAS02_MCR='45'; 
										PR_2430_CAS03_MCR=PR_2430_CAS03_MCR_Var;
										if PR_2430_CAS04_MCR_Var < 0 then PR_2430_CAS04_MCR=0; else PR_2430_CAS04_MCR=PR_2430_CAS04_MCR_Var;
									end;

								else if capind = 'C'  and (PR_2430_CAS03_MCR_Var > 0 or (PR_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										PR_2430_CAS01_MCR='CO';PR_2430_CAS02_MCR='24'; 
										PR_2430_CAS03_MCR=PR_2430_CAS03_MCR_Var;
										if PR_2430_CAS04_MCR_Var < 0 then PR_2430_CAS04_MCR=0; else PR_2430_CAS04_MCR=PR_2430_CAS04_MCR_Var;
									end;
									
									/*[B-272655]_[APD Compcare CAS Segment Capitated Encounters Update]  CAS05 TO CAS08 Added*/
									if capind = 'C'  and (PR_2430_CAS07_MCR_Var > 0 or (PR_2430_CAS07_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										if cas_flag <> 1 then do; PR_2430_CAS05_MCR='CO'; PR_2430_CAS06_MCR='45'; end;
										if cas_flag=1		 then do; PR_2430_CAS05_MCR='CO'; PR_2430_CAS06_MCR=CARC; end;
										PR_2430_CAS07_MCR=PR_2430_CAS07_MCR_Var;
										if PR_2430_CAS08_MCR_Var < 0 then PR_2430_CAS08_MCR=0;else PR_2430_CAS08_MCR=PR_2430_CAS08_MCR_Var;
									end;
									
						%end;
						%else %If &compno.=01 and &LOB. =DSNP  %then %do;

							if capind = 'F'  and (PR_2430_CAS03_Var > 0 or (PR_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
							
										if cas_flag <> 1 then do; PR_2430_CAS01='CO';PR_2430_CAS02='45'; end;
										if cas_flag=1 then do; PR_2430_CAS01='CO'; PR_2430_CAS02=CARC; end;											
										PR_2430_CAS03=PR_2430_CAS03_Var;
										if PR_2430_CAS04_Var < 0 then PR_2430_CAS04=0; else PR_2430_CAS04=PR_2430_CAS04_Var;
									end;

							else if capind = 'C'  and (PR_2430_CAS03_Var > 0 or (PR_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										PR_2430_CAS01='CO';
										PR_2430_CAS02='24';	
										PR_2430_CAS03=PR_2430_CAS03_Var;
										if PR_2430_CAS04_Var < 0 then PR_2430_CAS04=0; else PR_2430_CAS04=PR_2430_CAS04_Var;
									end;
							
							if capind = 'C'  and (PR_2430_CAS07_Var > 0 or (PR_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
										if cas_flag <> 1 then do; PR_2430_CAS05='CO'; PR_2430_CAS06='45'; end;
										if cas_flag=1		 then do; PR_2430_CAS05='CO'; PR_2430_CAS06=CARC; end;
										PR_2430_CAS07=PR_2430_CAS07_Var;
										if PR_2430_CAS08_Var < 0 then PR_2430_CAS08=0;else PR_2430_CAS08=PR_2430_CAS08_Var;
									end;

								
							if capind_MCR = 'F'  and (PR_2430_CAS03_MCR_Var > 0 or (PR_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										PR_2430_CAS01_MCR='CO';PR_2430_CAS02_MCR='45'; 
										PR_2430_CAS03_MCR=PR_2430_CAS03_MCR_Var;
										if PR_2430_CAS04_MCR_Var < 0 then PR_2430_CAS04_MCR=0; else PR_2430_CAS04_MCR=PR_2430_CAS04_MCR_Var;
									end;

							else if capind_MCR = 'C'  and (PR_2430_CAS03_MCR_Var > 0 or (PR_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										PR_2430_CAS01_MCR='CO';PR_2430_CAS02_MCR='24'; 
										PR_2430_CAS03_MCR=PR_2430_CAS03_MCR_Var;
										if PR_2430_CAS04_MCR_Var < 0 then PR_2430_CAS04_MCR=0; else PR_2430_CAS04_MCR=PR_2430_CAS04_MCR_Var;
									end;
									
							if capind_MCR = 'C'  and (PR_2430_CAS07_MCR_Var > 0 or (PR_2430_CAS07_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										if cas_flag <> 1 then do; PR_2430_CAS05_MCR='CO'; PR_2430_CAS06_MCR='45'; end;
										if cas_flag=1		 then do; PR_2430_CAS05_MCR='CO'; PR_2430_CAS06_MCR=CARC; end;
										PR_2430_CAS07_MCR=PR_2430_CAS07_MCR_Var;
										if PR_2430_CAS08_MCR_Var < 0 then PR_2430_CAS08_MCR=0;else PR_2430_CAS08_MCR=PR_2430_CAS08_MCR_Var;
									end;
									
						%end;						
						%else %do;
/*								if capind = 'F' then*/
/*									do;*/
/*										  PR_2430_CAS01='OA';*/
/*											PR_2430_CAS02='45';*/	
/*											PR_2430_CAS05=' ';*/
/*											PR_2430_CAS06=' ';*/
/*								end;*/
/*								else if capind = 'C' and typecode in ('P') then*/
/*									do;*/
/*										  PR_2430_CAS01='CO';*/
/*											PR_2430_CAS02='24';*/
/*											PR_2430_CAS05='OA';*/
/*											PR_2430_CAS06='45';*/
/*								end;*/
/*											PR_2430_CAS09=' ';*/
/*											PR_2430_CAS10=' ';*/
/*											PR_2430_CAS11=.;*/		/* B-96285 QHP had =' '  */
/*											PR_2430_CAS12=.;*/		/* B-96285 QHP had =' '  */
/*											PR_2430_CAS13=' ';*/
/*											PR_2430_CAS14=' ';*/
/*											PR_2430_CAS15=.;*/		/* B-96285 QHP had =' '  */
/*											PR_2430_CAS16=.;*/		/* B-96285 QHP had =' '  */
/*											PR_2430_CAS17=' ';*/
/*											PR_2430_CAS18=' ';*/
/*											PR_2430_CAS19=.;*/		/* B-96285 QHP had =' '  */
/*											PR_2430_CAS20=.;*/		/* B-96285 QHP had =' '  */
/*											PR_2430_CAS21=' ';*/
											/* VVKR Code Start -- B-75041 */
/*											if cas_flag=1 then do;*/
/*												PR_2430_CAS09='CO';*/
/*												PR_2430_CAS10=CARC;*/
/*												PR_2430_CAS11=0;*/	/* B-96285 QHP had ='0'  */
/*												PR_2430_CAS12=0;*/	/* B-96285 QHP had ='0'  */
/*											end;*/
											/* VVKR Code End -- B-75041 */

						if capind = 'F'  and (PR_2430_CAS03_Var > 0 or (PR_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
										if cas_flag<>1 then do; PR_2430_CAS01='CO'; /* B-121242 */ PR_2430_CAS02='45'; end;
										if cas_flag=1 then do; PR_2430_CAS01='CO'; PR_2430_CAS02=CARC; end;	
										PR_2430_CAS03=PR_2430_CAS03_Var;
										if PR_2430_CAS04_Var < 0 then PR_2430_CAS04=0; else PR_2430_CAS04=PR_2430_CAS04_Var;
									end;

								else if capind = 'C'  and (PR_2430_CAS03_Var > 0 or (PR_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
										PR_2430_CAS01='CO';
										PR_2430_CAS02='24';	
										PR_2430_CAS03=PR_2430_CAS03_Var;
										if PR_2430_CAS04_Var < 0 then PR_2430_CAS04=0; else PR_2430_CAS04=PR_2430_CAS04_Var;
									end;

								if capind = 'C'  and (PR_2430_CAS07_Var > 0  or (PR_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
										PR_2430_CAS05='CO'; /* B-121242 */
										PR_2430_CAS06='45';	
										PR_2430_CAS07=PR_2430_CAS07_Var;
										if PR_2430_CAS08_Var < 0 then PR_2430_CAS08=0;else PR_2430_CAS08=PR_2430_CAS08_Var;
									end;

							%end;
									
									
                        PR_2430_DTP03_SL_ADJ_DTE      =     put(pidate,yymmddn8.) ; /*[B-315447] - DS - Updates*/
                        
            %If &compno.=34  %then %do;								
							PR_2430_SGMT_HDR2_MCR             =  'DTP';
							PR_2430_DTP01_MCR                 =  '573';
							PR_2430_DTP02_MCR                 =  'D8';
							PR_2430_DTP03_SL_ADJ_DTE_MCR      =  put(pidate,yymmddn8.) ; /*[B-315447] - DS - Updates*/
						%end;
						%else %If &compno.=01 and &LOB. =DSNP  %then %do;								
							PR_2430_SGMT_HDR2_MCR             =  'DTP';
							PR_2430_DTP01_MCR                 =  '573';
							PR_2430_DTP02_MCR                 =  'D8';
							PR_2430_DTP03_SL_ADJ_DTE_MCR      =  put(pidate_MCR,yymmddn8.) ; 
						%end;
						%else %do;
							PR_2430_SGMT_HDR2_MCR             =  '   ';
							PR_2430_DTP01_MCR                 =  '   ';
							PR_2430_DTP02_MCR                 =  '  ';
							PR_2430_DTP03_SL_ADJ_DTE_MCR      =  '        ';
						%end; 

						%If &compno.=30 or &compno.=45 %then %do;	/* B-146417 add hiosid from elgibility outbound */	
							PR_2430_SVD01_HMO=HIOSID;
								%if ("&hiosflg." = "Y" and (&compno.=45 and "&LOB." ne "MPPO")) %then %do; /* B-274606 */
									PR_2430_SVD01_HMO = hios_plan_id_cd;
								%end;							
						%end;
						%else %do;
							PR_2430_SVD01_HMO="&HIOSID.";
								%if (&compno.=01 and &LOB. =DSNP) %then %do; 
									PR_2430_SVD01_HMO="&HIOSID.";
								%end;
								%else %if ("&hiosflg." = "Y" and (&compno.=01 or &compno.=20 or &compno.=42)) %then %do; /* B-274606 */
									PR_2430_SVD01_HMO = hix_policy_num_cd;
								%end;							
						%end; 
						
						%If &compno.=34 %then %do;	
							PR_2430_SVD01_HMO_MCR="N95284]H3359]034";
						%end;
						%else %If &compno.=01 and &LOB. =DSNP  %then %do;	
							PR_2430_SVD01_HMO_MCR="N95284]H3359]038";
						%end;
						%else %do;
							PR_2430_SVD01_HMO_MCR='                ';
						%end; 

/*********************************************************************************************/
/*                             End of  2430    LINE ADJUDICATION INFORMATION                  */
/*********************************************************************************************/                    

/*********************************************************************************************/
/*          Start Recalculating     : Professional                                                                      */
/*                            1. Dollar amounts                                                                               */
/*                            2. Service Line Number                                                                           */
/*********************************************************************************************/
                  if first.clamno then
                      do;
					  		  PR_2400_LX01_LINE_NUM         = 0;
                              PR_2300_CLM02_TOTCHG          = 0; 
							  PR_2320_AMT02_PPAD_A_MCD      = 0;  							  
							  PR_2320_AMT02_PPAD_A_MCR      = 0;
							  PR_2320_AMT02_PPAD_A          = 0;		/* B-96285 QHP, HFIC */							  
                      end;

                 	 PR_2400_LX01_LINE_NUM   + 1 ;
                     PR_2300_CLM02_TOTCHG + PR_2400_SV102_S_CHG_AMT       ;  
					 PR_2320_AMT02_PPAD_A_MCD + PR_2430_SVD02_SL_PAID_AMT ;
					 PR_2320_AMT02_PPAD_A 	  + PR_2430_SVD02_SL_PAID_AMT ;	/* B-96285 QHP, HFIC */

					 %if &compno.=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do;	/*B-248997*/ /* B-145499 */
					  	 PR_2320_AMT02_PPAD_A_MCR + PR_2430_SVD02_SL_PAID_AMT ;
						 PR_2320_AMT02_PPAD_A_MCD = 0;
					 %end;
					 %else %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
						 /*PR_2320_AMT02_PPAD_A_MCR + PR_2430_CAS03;*/
						 PR_2320_AMT02_PPAD_A_MCR + PR_2430_SVD02_SL_PAID_AMT_MCR; /*B-247073*/
						 
					 %end;
					 %else %If &compno.=01 and &LOB. =DSNP  %then %do;
						 PR_2320_AMT02_PPAD_A_MCR + PR_2430_SVD02_SL_PAID_AMT_MCR; 
					 %end;					 
					 %else %do;
					 	PR_2320_AMT02_PPAD_A_MCR=0;
					 %end;


/*********************************************************************************************/
/*          End   Recalculating     : Professional                                                                      */
/*                            1. Dollar amounts                                                                               */
/*                            2. Service Line Number                                                                           */
/*********************************************************************************************/

/*********************************************************************************************/
/*                Start   -  Mapping for Hard coded values                                   */
/*********************************************************************************************/
      %include ctrlout ; 
/*********************************************************************************************/
/*                End   -  Mapping for Hard coded values                                     */
/*********************************************************************************************/

            if last.clamno then 
            do;
			      if typecode = "P" then do;
                     prof_clm_count_&Edit_Ind. + 1 ;
					 output &RGA191..prof_clm_&Edit_Ind._&compno._&run_id. ;
                  end ;			                                 
            end;

				 if typecode = "P" then do;
                      prof_svc_count_&Edit_Ind. + 1 ;
					  output  &RGA191..prof_svc_&Edit_Ind._&compno._&run_id.;
                  end ;	                 
              
      	    end;

             


/**********************************************************************************************/
/*                            END of  PROFESSIONAL SEGMENT                                     */
/**********************************************************************************************/

/**********************************************************************************************/

/**********************************************************************************************/
/*                            START of  INSTITUITIONAL SEGMENT                                 */
/**********************************************************************************************/

Else if typecode in('I','O') then
        do;

/*********************************************************************************************/
/*                             Start of  2010AA     Billing Provider Name                     */
/*********************************************************************************************/
				  IN_2000A_PRV03_PRV_TAX_CD=BIPRVTXY;
                  IN_2010AA_NM103_BI_PRV_LST              =     BI_PRV_LST;  
                  IN_2010AA_NM104_BI_PRV_FST              =     BI_PRV_FST;   

                  IN_2010AA_NM109_BI_PRV_NPIEINSSN        =     /*BIPRVNPI */ BIPRVNPI_Updated;  /* CR# CHG0032056 kv */

                  if missing(BI_PRV_ADD1) = 0 then
                        do;
		                    IN_2010AA_N301_BI_PRV_ADD1              =     BI_PRV_ADD1;      
		                    IN_2010AA_N302_BI_PRV_ADD2              =     BI_PRV_ADD2  ;
                       end;
                  else 
                        do;
                                  IN_2010AA_N301_BI_PRV_ADD1              =     BI_PRV_ADD2;   
                                  IN_2010AA_N302_BI_PRV_ADD2              =           ' ' ;
                        end;

                  IN_2010AA_N401_BI_PRV_CITY              =     BI_PRV_CITY ;      
                  IN_2010AA_N402_BI_PRV_STATE             =     BI_PRV_STATE;   

			   IN_2010AA_N403_BI_PRV_ZIP=BI_PRV_ZIP;
               IN_2010AA_REF02_BI_PRV_MPI=BI_PRV_EIN; 
			   if missing(BIPRVNPI)=1 then IN_2010AA_REF02=/*mdcaid_1*/ '';/* CR# CHG0032056 kv */
			   if missing(BIPRVNPI_Updated) =1 then IN_2010AA_REF02_PRV=mrg_provno;/* CR# CHG0032056 kv */


/*********************************************************************************************/
/*                             End of  2010AA     Billing Provider Name                       */
/*********************************************************************************************/

/*********************************************************************************************/
/*                             Start of  2010BA     SUBSCRIBER NAME                           */
/*********************************************************************************************/


/* B-96285 start QHP, HFIC logic */
%IF (&compno=45 and "&LOB." ne "MPPO")  or (&compno=42 and "&LOB." eq "QHP") %then %do ; /*B-248997*/	/* B-121242 */

if Relcod=1 then do;
	IN_2000B_SBR02='18';
end ;
else do;
	IN_2000B_SBR02='';
end;

if Relcod  ne 1 then do ;

			  IN_2010BA_NM103_SUB_LST                 =     Sub_MLSTNAM;      
              IN_2010BA_NM104_SUB_FST                 =     Sub_MFSTNAM;      
              IN_2010BA_NM105_SUB_MID                 =     Sub_MMIDFULL;      
	          IN_2010BA_NM109_SUB_MEM_MDCARID         =     Sub_MDCARID; 
			 
			  if missing(Sub_MADRLN1) = 0 then do;/* CR# CHG0032372 kv */
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  IN_2010BA_N301_SUB_ADR1=kcompress(Sub_MADRLN1,"@:*`~#");
								  /* SAS2AWS2: ReplacedFunctionCompress */
								  IN_2010BA_N302_SUB_ADR2=kcompress(Sub_MADRLN2,"@:*`~#");
			  end;	
			  else do;
								    /* SAS2AWS2: ReplacedFunctionCompress */
					  			  IN_2010BA_N301_SUB_ADR1=kcompress(Sub_MADRLN2,"@:*`~#");
			  end;
	          IN_2010BA_N401_SUB_CITY                 =     Sub_MCITYCD;
	          IN_2010BA_N402_SUB_STATE                =     Sub_MSTACOD;
			  IN_2010BA_N403_SUB_ZIP 				  =		Sub_MZIPCOD;
          	  IN_2010BA_DMG02_SUB_DOB                 =     put(Sub_BTHDAT,yymmddn8.);   

              If SUB_SEXCOD not in ('M','F') then
			  	 IN_2010BA_DMG03_SUB_SEX     ='U';                    
              Else 
			     IN_2010BA_DMG03_SUB_SEX              =     Sub_SEXCOD ; 

				 IN_2010BA_REF02=SUB_socsec;

            IN_2000C_HL ='HL';
			IN_2000C_HL01 =' ';
			IN_2000C_HL02 =' ';
			IN_2000C_HL03 ='23';
			IN_2000C_HL04 ='0';
			IN_2000C_PAT ='PAT';

			if relcod eq '2' then 
			   IN_2000C_PAT01='01';
			else if relcod eq '3' then
		       IN_2000C_PAT01='19';
			else 
		    	IN_2000C_PAT01='G8';		

			IN_2010CA_NM1 ='NM1';
			IN_2010CA_NM101='QC';
			IN_2010CA_NM102='1';
			IN_2010CA_NM103=MEM_MLSTNAM;
			IN_2010CA_NM104= MEM_MFSTNAM;
			IN_2010CA_NM105=MEM_MMIDFULL;
			IN_2010CA_NM106=' ';
			IN_2010CA_NM107=' ';
			IN_2010CA_NM108='MI';
			IN_2010CA_NM109=MEM_MDCARID;
			IN_2010CA_N3='N3';

			if missing(MEM_MADRLN1) = 0 then do;/* CR# CHG0032372 kv */
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2010CA_N301=kcompress(MEM_MADRLN1,"@:*`~#");
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2010CA_N302=kcompress(MEM_MADRLN2,"@:*`~#");
			end;	
			else do;
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2010CA_N301=kcompress(MEM_MADRLN1,"@:*`~#");
			end;

			IN_2010CA_N4='N4';
			IN_2010CA_N401=MEM_MCITYCD;
			IN_2010CA_N402=MEM_MSTACOD;
			IN_2010CA_N403=MEM_MZIPCOD;
			IN_2010CA_DMG='DMG ';
			IN_2010CA_DMG01='D8';
			IN_2010CA_DMG02=put(MEM_BTHDAT,yymmddn8.);
 
			 IF MEM_SEXCOD NOT IN ('F','M') THEN 
					     do;
					        IN_2010CA_DMG03='U';
					     end;
						ELSE
					     do;
					 		 IN_2010CA_DMG03       =     MEM_SEXCOD ;      
					     end;
     End;
	 else do;

	 		 IN_2010BA_NM103_SUB_LST                 =     MEM_MLSTNAM;      
              IN_2010BA_NM104_SUB_FST                 =     MEM_MFSTNAM;      
              IN_2010BA_NM105_SUB_MID                 =     MEM_MMIDFULL;      
	          IN_2010BA_NM109_SUB_MEM_MDCARID         =     MEM_MDCARID; 
			 
			 if missing(MEM_MADRLN1) = 0 then do;/* CR# CHG0032372 kv */
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2010BA_N301_SUB_ADR1=kcompress(MEM_MADRLN1,"@:*`~#");
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2010BA_N302_SUB_ADR2=kcompress(MEM_MADRLN2,"@:*`~#");
			 end;	
			 else do;
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2010BA_N301_SUB_ADR1=kcompress(MEM_MADRLN1,"@:*`~#");
			 end;
	          IN_2010BA_N401_SUB_CITY                 =     MEM_MCITYCD;
	          IN_2010BA_N402_SUB_STATE                =     MEM_MSTACOD;


			   IN_2010BA_N403_SUB_ZIP =MEM_MZIPCOD;
          	  IN_2010BA_DMG02_SUB_DOB                 =     put(MEM_BTHDAT,yymmddn8.);   

              If MEM_SEXCOD not in ('M','F') then
			  	 IN_2010BA_DMG03_SUB_SEX     ='U';                    
              Else 
			     IN_2010BA_DMG03_SUB_SEX                 =     MEM_SEXCOD ; 
				 IN_2010BA_REF02=MEM_socsec;

            IN_2000C_HL =' ';
			IN_2000C_HL01 =' ';
			IN_2000C_HL02 =' ';
			IN_2000C_HL03 =' ';
			IN_2000C_HL04 =' ';
			IN_2000C_PAT =' ';
			IN_2000C_PAT01=' ';
			IN_2010CA_NM1 =' ';
			IN_2010CA_NM101=' ';
			IN_2010CA_NM102=' ';
			IN_2010CA_NM103=' ';
			IN_2010CA_NM104=' ';
			IN_2010CA_NM105=' ';
			IN_2010CA_NM106=' ';
			IN_2010CA_NM107=' ';
			IN_2010CA_NM108=' ';
			IN_2010CA_NM109=' ';
			IN_2010CA_N3=' ';
			IN_2010CA_N301=' ';
			IN_2010CA_N302=' ';
			IN_2010CA_N4=' ';
			IN_2010CA_N401=' ';
			IN_2010CA_N402=' ';
			IN_2010CA_N403=' ';
			IN_2010CA_DMG=' ';
			IN_2010CA_DMG01=' ';
			IN_2010CA_DMG02=' ';
			IN_2010CA_DMG03=' ';	   	
	 end;
%end;	/* B-96285 end QHP, HFIC logic */       
%else %do;
	 		  IN_2010BA_NM103_SUB_LST                 =     MEM_MLSTNAM;      
              IN_2010BA_NM104_SUB_FST                 =     MEM_MFSTNAM;      
              IN_2010BA_NM105_SUB_MID                 =     MEM_MMIDFULL;
 			%IF &compno=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do ;	/*B-248997*/ /* B-145499 */
			  IN_2010BA_NM109_SUB_MEM_MDCARID         =     MEM_MDCARID; /* B-146417 */
			%end;
			%else %do;
	          IN_2010BA_NM109_SUB_MEM_MDCARID         =     MEM_MDCARID;
			%end; 
/*			  IN_2010BA_N301_SUB_ADR1				  = 	MEM_MADRLN1;*/
/*			  IN_2010BA_N302_SUB_ADR2				  = 	MEM_MADRLN2;*/
			  if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0033527 modified */
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2010BA_N301_SUB_ADR1=kcompress(MEM_MADRLN1,"@:*`~#");
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2010BA_N302_SUB_ADR2=kcompress(MEM_MADRLN2,"@:*`~#");
			  End;
			  else do;
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2010BA_N301_SUB_ADR1=kcompress(MEM_MADRLN2,"@:*`~#");
			  end; 			 
	          IN_2010BA_N401_SUB_CITY                 =     MEM_MCITYCD;
	          IN_2010BA_N402_SUB_STATE                =     MEM_MSTACOD;
			  IN_2010BA_N403_SUB_ZIP 				  =     MEM_MZIPCOD;
          	  IN_2010BA_DMG02_SUB_DOB                 =     put(MEM_BTHDAT,yymmddn8.);   
              If MEM_SEXCOD not in ('M','F') then
			  	 IN_2010BA_DMG03_SUB_SEX     ='U';                    
              Else 
			     IN_2010BA_DMG03_SUB_SEX                 =     MEM_SEXCOD ; 
				 IN_2010BA_REF02=MEM_socsec;
%end;
/*********************************************************************************************/
/*                             End of  2010BA     SUBSCRIBER NAME                             */
/*********************************************************************************************/

/*********************************************************************************************/
/*                             Start of  2300     CLAIM INFORMATION                           */
/*********************************************************************************************/
	              /* SAS2AWS2: ReplacedFunctionSubstr */
              	IN_2300_CLM05_1_FAC_TYPE          = ksubstr(bilcod,1,2) ;
 
			/*	 if N5_flag='Y' or N4_flag='Y' then do;
						 	IN_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(clamno,1,13)||'CR'); * HS 569650 - added substr;
						  end;
						  else do;
						  	IN_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(clamno,1,13)); * HS 569650 - added substr;
						  end;													   * B-160659 section moved to PWK_Segment logic */

			%IF &compno=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do ; /*B-248997*/ /*B-248997*/	/* B-145499 */
						IN_2300_NTE02= '';
			%end;
			%else %do;
				if typecode="I" then do;
				  if missing(/*inp_spccod*/ prvspcd)=0 then do;
						/* SAS2AWS2: ReplacedFunctionCompress */
						IN_2300_NTE02=kcompress(cos||/*inp_spccod*/ prvspcd); /*B-289556*/
				   end;
				   else do;
						   /* SAS2AWS2: ReplacedFunctionCompress */
				   		IN_2300_NTE02=kcompress(cos||799);
				   end;
				end;
				else do;
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2300_NTE02=kcompress(cos||prvspcd);
				end;
			%end;

                IN_2300_CLM05_3_FREQ_CDE_1        = claimfrq ; 
                if typecode='I' then 
                        do;
                              IN_2300_DTP01_DDH='096';
                              /* SAS2AWS2: ReplacedFunctionCompress */
                              IN_2300_DTP03_DISCHG_TM   =     kcompress(DISTIM,':');
                        end;
                 else
                       do;
                                IN_2300_DTP01_DDH='';    
                                IN_2300_DTP03_DISCHG_TM =     ''  ;
                        end;                
                  /* SAS2AWS2: ReplacedFunctionCatx */
                  IN_2300_DTP03_END_DATE            = catx("-",put(SVCDAT1,yymmddn8.),put(ENDDAT1,yymmddn8.));
                  if typecode = 'I' then  IN_2300_DTP02_A02='DT';
                  if typecode = 'O' then IN_2300_DTP02_A02='D8';        
                  if typecode = 'I' then do;
		                  /* SAS2AWS2: ReplacedFunctionSubstr */
                  		hour=ksubstr(admtim,1,2);
		                  /* SAS2AWS2: ReplacedFunctionSubstr */
                 		 min=ksubstr(admtim,4,2);
                 		 If min gt 59 then 
                  			do;
                              min='59';    
                             end;
			                  /* SAS2AWS2: ReplacedFunctionCats */
                 			 IN_2300_DTP03_ADM_DATE  = cats(put(FIRSTDOS,yymmddn8.),kcompress(hour),kcompress(min));
             	  end;
                  else do;
                       IN_2300_DTP03_ADM_DATE  = '';
                  end;                      
                  IN_2300_CL101_ADM_TYPE       = admtype ;


                 If ( admtype='4' and admsrc in ('5','6') ) then 
                    do;
                        IN_2300_CL102_ADM_SOURCE=admsrc;
                    end;
                 Else if N5_flag  ='Y' and admtype ne '4' and admsrc = '7' then /* CHG0031624 */
				 	do;
					 	IN_2300_CL102_ADM_SOURCE='9'; 
				 	end;
                 else If ( admtype ne '4' and admsrc in ('1','2','3','4','5','6','7','8','9') ) then
                  	do;
                       	IN_2300_CL102_ADM_SOURCE=admsrc;
                   	end;
                 Else
                   	do;   
                        IN_2300_CL102_ADM_SOURCE='9'; 
                   	end;

				/* B-156657 OSDS Newborn source logic */
				%if "&x12_ctrl." ne "EIS" %then %do;
					if admtype ='4' then 
					do;
						if admsrc in('5','6') then 
                		do;
                        	IN_2300_CL102_ADM_SOURCE=admsrc;
                    	end;
						/* SAS2AWS2: ReplacedFunctionSubstr */
						else if (admsrc not in('5','6') and ksubstr(bilcod,1,2) = '11') then
						do;
							IN_2300_CL102_ADM_SOURCE = '5';
						end;
						else  
						do;
                        	IN_2300_CL102_ADM_SOURCE = '6';
                    	end;
					end;
				%end;

				/* CR# CHG0035759 - added mapping for 2300 loop pwk segment for N4 & N5 claims */
					if PWK_Segment='N' then do; /* B-160659 PWK_Segment added*/
							IN_2300_PWK01='  ';
							IN_2300_PWK02='  ';
							/* SAS2AWS2: ReplacedFunctionSubstr */
							IN_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(clamno,1,13));	/* B-160659 PWK_Segment added*/
					end;
				   	else if N5_flag='Y' or N4_flag='Y' or PWK_Segment='Y' then do; /* B-160659 PWK_Segment added*/
							IN_2300_PWK01='09';
							IN_2300_PWK02='AA';
							/* SAS2AWS2: ReplacedFunctionSubstr */
							IN_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(clamno,1,13)||'CR');	/* B-160659 inserted */
					end;
					else do ;
							IN_2300_PWK01='';
							IN_2300_PWK02='';
							/* SAS2AWS2: ReplacedFunctionSubstr */
							IN_2300_CLM01_PAT_CLMNO=kcompress(ksubstr(clamno,1,13));	/* B-160659 PWK_Segment added*/
					end;
IN_2300_CN101_05='';

				IN_2300_REF02_ORIG_REFNO = ' ';
				
				   if missing(MEDNUM) =0 then do;/* CR# CHG0035401 KV - Modified compress function */
						 IN_2300_REF02_MED_RECNO    =   /* compress(MEDNUM,"|@:*`~#") */ compress(MEDNUM,,"kad")  ; /* CR# CHG0031183 KV - Added compress function, B-166920 */   
				   end;
				   else do;
						 IN_2300_REF02_MED_RECNO    =   /* compress(patnum,"|@:*`~#") */ compress(patnum,,"kad")  ; /* CR# CHG0031183 KV - Added compress function, B-166920 */
				   end;

                 
                  if typecode ='I' then 
		          do;
		                IN_2300_SGMT_HDR10='HI';
		                IN_2300_HI01_AD='HI01';
		                IN_2300_HI01_2_AD=admdiag_Q; 
		                /* SAS2AWS2: ReplacedFunctionCompress */
		                IN_2300_HI02_AD=kcompress(ADMTDIAG,'.') ;

		          end;
          	     else
				 do;
                       IN_2300_SGMT_HDR10='';
                       IN_2300_HI01_AD='';
                       IN_2300_HI01_2_AD='';
                       IN_2300_HI02_AD='' ; 
                  end;  
                 if first.clamno then IN_2300_HI01_2_DRG_CDE = '      ';
                 if missing(IN_2300_HI01_2_DRG_CDE) then
                    do;
                          if typecode = "O" then
                                      do;
                                                  if SVCTYP = 'DG' and svcstat = 'PA' then                                                                                         
                                                              /* SAS2AWS2: ReplacedFunctionSubstr */
                                                              IN_2300_HI01_2_DRG_CDE = ksubstr(svccod,2,4) ;
                                                  else
                                                              IN_2300_HI01_2_DRG_CDE = ' ';
                                      end;
                          else 
                          do;
                    
                                                  if Inp_dg_line_flag = "Y" then  do;   /* HS# 565133 KV - added zero prefix to svccod */
													/*%If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;*/ /*B-110462 Commented the If/Else block*/
														 /* SAS2AWS2: ReplacedFunctionLength */
 														if klength(svccod_dg_line)=5 then do;
														   /* SAS2AWS2: ReplacedFunctionSubstr */
														   IN_2300_HI01_2_DRG_CDE = ksubstr(svccod_dg_line,2,4) ;
														end;
														else do;
	                                                       /*IN_2300_HI01_2_DRG_CDE = compress('0'||substr(svccod_dg_line,2,4)) ; */
														   /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
														   IN_2300_HI01_2_DRG_CDE = kcompress(ksubstr(svccod_dg_line,2,4)||'0') ; /*B-110462 Added*/
														end;
													/*%end;
													%else %do;
														IN_2300_HI01_2_DRG_CDE = ksubstr(svccod_dg_line,2,4) ;
													%end;*/
													end;
                                                  else                                                                                                                                                        
                                                        IN_2300_HI01_2_DRG_CDE = ' ';                                                                         
                    
                          end;
                    end;

				     IN_2300_HI01_1_PD=NewDiag1_Q ;
					IN_2300_HI01_1_OTH_DIAGQ1=NewDiag2_Q ;
					IN_2300_HI02_1_OTH_DIAGQ2=NewDiag3_Q ;
					IN_2300_HI03_1_OTH_DIAGQ3=NewDiag4_Q ;
					IN_2300_HI04_1_OTH_DIAGQ4=NewDiag5_Q ;
					IN_2300_HI05_1_OTH_DIAGQ5=NewDiag6_Q ;
					IN_2300_HI06_1_OTH_DIAGQ6=NewDiag7_Q ;
					IN_2300_HI07_1_OTH_DIAGQ7=NewDiag8_Q ;
					IN_2300_HI08_1_OTH_DIAGQ8=NewDiag9_Q ;
					IN_2300_HI09_1_OTH_DIAGQ9=NewDiag10_Q ;
					IN_2300_HI10_1_OTH_DIAGQ10=NewDiag11_Q ;
					IN_2300_HI11_1_OTH_DIAGQ11=NewDiag12_Q ;
					IN_2300_HI12_1_OTH_DIAGQ12=NewDiag13_Q ;
					IN_2300_HI13_1_OTH_DIAGQ13=NewDiag14_Q ;
					IN_2300_HI14_1_OTH_DIAGQ14=NewDiag15_Q ;
					IN_2300_HI15_1_OTH_DIAGQ15=NewDiag16_Q ;
					IN_2300_HI16_1_OTH_DIAGQ16=NewDiag17_Q ;
					IN_2300_HI17_1_OTH_DIAGQ17=NewDiag18_Q ;
					IN_2300_HI18_1_OTH_DIAGQ18=NewDiag19_Q ;
					IN_2300_HI19_1_OTH_DIAGQ19=NewDiag20_Q ;
					IN_2300_HI20_1_OTH_DIAGQ20=NewDiag21_Q ;
					IN_2300_HI21_1_OTH_DIAGQ21=NewDiag22_Q ;
					IN_2300_HI22_1_OTH_DIAGQ22=NewDiag23_Q ;
					IN_2300_HI23_1_OTH_DIAGQ23=NewDiag24_Q ;
					IN_2300_HI24_1_OTH_DIAGQ24=NewDiag25_Q ;



				IN_2300_HI01_2_PRIN_DIAG_C              =kcompress(NewDiag1,'.');                    
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI01_2_OTH_DIAGC1               =      kcompress(NewDiag2,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI02_2_OTH_DIAGC2               =      kcompress(NewDiag3,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI03_2_OTH_DIAGC3               =      kcompress(NewDiag4,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI04_2_OTH_DIAGC4               =      kcompress(NewDiag5,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI05_2_OTH_DIAGC5               =     kcompress(NewDiag6,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI06_2_OTH_DIAGC6               =      kcompress(NewDiag7,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI07_2_OTH_DIAGC7               =      kcompress(NewDiag8,'.') ;
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI08_2_OTH_DIAGC8               =      kcompress(NewDiag9,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI09_2_OTH_DIAGC9               =      kcompress(NewDiag10,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI10_2_OTH_DIAGC10              =      kcompress(NewDiag11,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI11_2_OTH_DIAGC11              =      kcompress(NewDiag12,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI12_2_OTH_DIAGC12              =     kcompress(NewDiag13,'.') ;
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI13_2_OTH_DIAGC13               =      kcompress(NewDiag14,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI14_2_OTH_DIAGC14               =      kcompress(NewDiag15,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI15_2_OTH_DIAGC15               =      kcompress(NewDiag16,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI16_2_OTH_DIAGC16               =      kcompress(NewDiag17,'.');
	            /* SAS2AWS2: ReplacedFunctionCompress */
	            IN_2300_HI17_2_OTH_DIAGC17               =     kcompress(NewDiag18,'.') ;

	
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI18_2_OTH_DIAGC18 =     kcompress(NewDiag19,'.') ;
				 /* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI19_2_OTH_DIAGC19 =     kcompress(NewDiag20,'.') ; 
				 /* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI20_2_OTH_DIAGC20 =     kcompress(NewDiag21,'.') ; 
				 /* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI21_2_OTH_DIAGC21 =     kcompress(NewDiag22,'.') ; 
				 /* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI22_2_OTH_DIAGC22 =     kcompress(NewDiag23,'.') ; 
				 /* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI23_2_OTH_DIAGC23 =     kcompress(NewDiag24,'.') ; 
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI24_2_OTH_DIAGC24 =     kcompress(NewDiag25,'.') ;


				IN_2300_HI01_9_PRIN_PD=kcompress(newPoaDiag1); 
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI01_9_PD=kcompress(newPoaDiag2,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI02_9_PD=kcompress(newPoaDiag3,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI03_9_PD=kcompress(newPoaDiag4,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI04_9_PD=kcompress(newPoaDiag5,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI05_9_PD=kcompress(newPoaDiag6,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI06_9_PD=kcompress(newPoaDiag7,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI07_9_PD=kcompress(newPoaDiag8,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI08_9_PD=kcompress(newPoaDiag9,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI09_9_PD=kcompress(newPoaDiag10,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI10_9_PD=kcompress(newPoaDiag11,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI11_9_PD=kcompress(newPoaDiag12,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI12_9_PD=kcompress(newPoaDiag13,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI13_9_PD=kcompress(newPoaDiag14,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI14_9_PD=kcompress(newPoaDiag15,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI15_9_PD=kcompress(newPoaDiag16,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI16_9_PD=kcompress(newPoaDiag17,'.');
                /* SAS2AWS2: ReplacedFunctionCompress */
                IN_2300_HI17_9_PD=kcompress(newPoaDiag18,'.');

		
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI18_9_PD=kcompress(newPoaDiag19,'.');
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI19_9_PD=kcompress(newPoaDiag20,'.');
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI20_9_PD=kcompress(newPoaDiag21,'.');
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI21_9_PD=kcompress(newPoaDiag22,'.');
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI22_9_PD=kcompress(newPoaDiag23,'.');
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI23_9_PD=kcompress(newPoaDiag24,'.');
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2300_HI24_9_PD=kcompress(newPoaDiag25,'.');

      		    IN_2300_HI01_1_PRIN_PRC_Q=Proc_Cod1_Q;
				IN_2300_HI01_1_OTH_PRC1_Q_BN=Proc_Cod2_Q;
				IN_2300_HI02_1_OTH_PRC2_Q_BN=Proc_Cod3_Q;
				IN_2300_HI03_1_OTH_PRC3_Q_BN=Proc_Cod4_Q;
				IN_2300_HI04_1_OTH_PRC4_Q=Proc_Cod5_Q;
				IN_2300_HI05_1_OTH_PRC5_Q=Proc_Cod6_Q;
/* B-105917 Start */
				IN_2300_HI06_1_OTH_PRC6_Q=Proc_Cod7_Q;
				IN_2300_HI07_1_OTH_PRC7_Q=Proc_Cod8_Q;
				IN_2300_HI08_1_OTH_PRC8_Q=Proc_Cod9_Q;
				IN_2300_HI09_1_OTH_PRC9_Q=Proc_Cod10_Q;
				IN_2300_HI10_1_OTH_PRC10_Q=Proc_Cod11_Q;
				IN_2300_HI11_1_OTH_PRC11_Q=Proc_Cod12_Q;
				IN_2300_HI12_1_OTH_PRC12_Q=Proc_Cod13_Q;
				IN_2300_HI13_1_OTH_PRC13_Q=Proc_Cod14_Q;
				IN_2300_HI14_1_OTH_PRC14_Q=Proc_Cod15_Q;
				IN_2300_HI15_1_OTH_PRC15_Q=Proc_Cod16_Q;
				IN_2300_HI16_1_OTH_PRC16_Q=Proc_Cod17_Q;
				IN_2300_HI17_1_OTH_PRC17_Q=Proc_Cod18_Q;
				IN_2300_HI18_1_OTH_PRC18_Q=Proc_Cod19_Q;
				IN_2300_HI19_1_OTH_PRC19_Q=Proc_Cod20_Q;
				IN_2300_HI20_1_OTH_PRC20_Q=Proc_Cod21_Q;
				IN_2300_HI21_1_OTH_PRC21_Q=Proc_Cod22_Q;
				IN_2300_HI22_1_OTH_PRC22_Q=Proc_Cod23_Q;
				IN_2300_HI23_1_OTH_PRC23_Q=Proc_Cod24_Q;
				IN_2300_HI24_1_OTH_PRC24_Q=Proc_Cod25_Q;
/*  B-105917 End */

               /* SAS2AWS2: ReplacedFunctionCompress */
               IN_2300_HI01_2_PRIN_PRC_C=kcompress(proc_cod1,'.');
               /* SAS2AWS2: ReplacedFunctionCompress */
               IN_2300_HI01_2_OTH_PRC1_C =kcompress(proc_cod2,'.');
               /* SAS2AWS2: ReplacedFunctionCompress */
               IN_2300_HI02_2_OTH_PRC2_C =kcompress(proc_cod3,'.');
               /* SAS2AWS2: ReplacedFunctionCompress */
               IN_2300_HI03_2_OTH_PRC3_C =kcompress(proc_cod4,'.');
               /* SAS2AWS2: ReplacedFunctionCompress */
               IN_2300_HI04_2_OTH_PRC4_C =kcompress(proc_cod5,'.');
               /* SAS2AWS2: ReplacedFunctionCompress */
               IN_2300_HI05_2_OTH_PRC5_C =kcompress(proc_cod6,'.');
/* B-105917 Start */
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI06_2_OTH_PRC6_C =kcompress(proc_cod7,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI07_2_OTH_PRC7_C =kcompress(proc_cod8,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI08_2_OTH_PRC8_C =kcompress(proc_cod9,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI09_2_OTH_PRC9_C =kcompress(proc_cod10,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI10_2_OTH_PRC10_C =kcompress(proc_cod11,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI11_2_OTH_PRC11_C =kcompress(proc_cod12,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI12_2_OTH_PRC12_C =kcompress(proc_cod13,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI13_2_OTH_PRC13_C =kcompress(proc_cod14,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI14_2_OTH_PRC14_C =kcompress(proc_cod15,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI15_2_OTH_PRC15_C =kcompress(proc_cod16,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI16_2_OTH_PRC16_C =kcompress(proc_cod17,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI17_2_OTH_PRC17_C =kcompress(proc_cod18,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI18_2_OTH_PRC18_C =kcompress(proc_cod19,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI19_2_OTH_PRC19_C =kcompress(proc_cod20,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI20_2_OTH_PRC20_C =kcompress(proc_cod21,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI21_2_OTH_PRC21_C =kcompress(proc_cod22,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI22_2_OTH_PRC22_C =kcompress(proc_cod23,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI23_2_OTH_PRC23_C =kcompress(proc_cod24,'.');
			   /* SAS2AWS2: ReplacedFunctionCompress */
			   IN_2300_HI24_2_OTH_PRC24_C =kcompress(proc_cod25,'.');
/* B-105917 End */

               IN_2300_HI01_4_PRIN_PRC_D  =Proc_dt1;
               IN_2300_HI01_4_OTH_PRC1_D   =Proc_dt2;
               IN_2300_HI02_4_OTH_PRC2_D  =Proc_dt3;
               IN_2300_HI03_4_OTH_PRC3_D   =Proc_dt4;
               IN_2300_HI04_4_OTH_PRC4_D  =Proc_dt5;
               IN_2300_HI05_4_OTH_PRC5_D   =Proc_dt6;
/* B-105917 Start */
			   IN_2300_HI06_4_OTH_PRC6_D   =Proc_dt7; 
			   IN_2300_HI07_4_OTH_PRC7_D   =Proc_dt8; 
			   IN_2300_HI08_4_OTH_PRC8_D   =Proc_dt9; 
			   IN_2300_HI09_4_OTH_PRC9_D   =Proc_dt10; 
			   IN_2300_HI10_4_OTH_PRC10_D   =Proc_dt11; 
			   IN_2300_HI11_4_OTH_PRC11_D   =Proc_dt12; 
			   IN_2300_HI12_4_OTH_PRC12_D   =Proc_dt13; 
			   IN_2300_HI13_4_OTH_PRC13_D   =Proc_dt14; 
			   IN_2300_HI14_4_OTH_PRC14_D   =Proc_dt15; 
			   IN_2300_HI15_4_OTH_PRC15_D   =Proc_dt16; 
			   IN_2300_HI16_4_OTH_PRC16_D   =Proc_dt17; 
			   IN_2300_HI17_4_OTH_PRC17_D   =Proc_dt18; 
			   IN_2300_HI18_4_OTH_PRC18_D   =Proc_dt19; 
			   IN_2300_HI19_4_OTH_PRC19_D   =Proc_dt20; 
			   IN_2300_HI20_4_OTH_PRC20_D   =Proc_dt21; 
			   IN_2300_HI21_4_OTH_PRC21_D   =Proc_dt22; 
			   IN_2300_HI22_4_OTH_PRC22_D   =Proc_dt23; 
			   IN_2300_HI23_4_OTH_PRC23_D   =Proc_dt24; 
			   IN_2300_HI24_4_OTH_PRC24_D   =Proc_dt25; 			   
/* B-105917 End */
 
%occ_dt;
%val_cd;
               IN_2300_HI01_2_OCC_SP_C1                =     SPNCOD1 ;      
               IN_2300_HI02_2_OCC_SP_C2                =     SPNCOD2 ;      
               IN_2300_HI03_2_OCC_SP_C3                =     SPNCOD3 ;      
               IN_2300_HI04_2_OCC_SP_C4                =     SPNCOD4 ;      
               IN_2300_HI05_2_OCC_SP_C5                =     SPNCOD5 ;      
               IN_2300_HI06_2_OCC_SP_C6                =     SPNCOD6 ;      
               IN_2300_HI07_2_OCC_SP_C7                =     SPNCOD7 ;      
               IN_2300_HI08_2_OCC_SP_C8                =     SPNCOD8 ;      
               IN_2300_HI09_2_OCC_SP_C9                =     SPNCOD9 ;  
               IN_2300_HI10_2_OCC_SP_C10                =    SPNCOD10 ;  



       			 if missing(SPNBEG1) = 0 and missing(SPNEND1) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI01_4_OCC_SP_D1_tmp = catx("-",put(SPNBEG1,yymmddn8.),put(SPNEND1,yymmddn8.)) ;
                  else
                        IN_2300_HI01_4_OCC_SP_D1_tmp = '  ' ;

                  if missing(SPNBEG2) = 0 and missing(SPNEND2) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI02_4_OCC_SP_D2_tmp = catx("-",put(SPNBEG2,yymmddn8.),put(SPNEND2,yymmddn8.)) ;
                  else IN_2300_HI02_4_OCC_SP_D2_tmp = '  ' ;

                  if missing(SPNBEG3) = 0 and missing(SPNEND3) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI03_4_OCC_SP_D3_tmp = catx("-",put(SPNBEG3,yymmddn8.),put(SPNEND3,yymmddn8.)) ;
                  else IN_2300_HI03_4_OCC_SP_D3_tmp = '  ' ;

                  if missing(SPNBEG4) = 0 and missing(SPNEND4) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI04_4_OCC_SP_D4_tmp = catx("-",put(SPNBEG4,yymmddn8.),put(SPNEND4,yymmddn8.)) ;
                  else IN_2300_HI04_4_OCC_SP_D4_tmp = '  ' ;

                  if missing(SPNBEG5) = 0 and missing(SPNEND5) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI05_4_OCC_SP_D5_tmp = catx("-",put(SPNBEG5,yymmddn8.),put(SPNEND5,yymmddn8.)) ;
                  else IN_2300_HI05_4_OCC_SP_D5_tmp = '  ' ;

                  if missing(SPNBEG6) = 0 and missing(SPNEND6) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI06_4_OCC_SP_D6_tmp = catx("-",put(SPNBEG6,yymmddn8.),put(SPNEND6,yymmddn8.)) ;
                  else IN_2300_HI06_4_OCC_SP_D6_tmp = '  ' ;

                  if missing(SPNBEG7) = 0 and missing(SPNEND7) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI07_4_OCC_SP_D7_tmp = catx("-",put(SPNBEG7,yymmddn8.),put(SPNEND7,yymmddn8.)) ;
                  else IN_2300_HI07_4_OCC_SP_D7_tmp = '  ' ;

                  if missing(SPNBEG8) = 0 and missing(SPNEND8) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI08_4_OCC_SP_D8_tmp = catx("-",put(SPNBEG8,yymmddn8.),put(SPNEND8,yymmddn8.)) ;
                  else IN_2300_HI08_4_OCC_SP_D8_tmp = '  ' ;

                  if missing(SPNBEG9) = 0 and missing(SPNEND9) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI09_4_OCC_SP_D9_tmp = catx("-",put(SPNBEG9,yymmddn8.),put(SPNEND9,yymmddn8.)) ;
                  else IN_2300_HI09_4_OCC_SP_D9_tmp = '  ' ;

                  if missing(SPNBEG10) = 0 and missing(SPNEND10) = 0 then 
                        /* SAS2AWS2: ReplacedFunctionCatx */
                        IN_2300_HI10_4_OCC_SP_D10_tmp = catx("-",put(SPNBEG10,yymmddn8.),put(SPNEND10,yymmddn8.)) ;
                  else IN_2300_HI10_4_OCC_SP_D10_tmp = '  ' ;

                  %occ_span_dt;

                  IN_2300_HI01_4_OCC_SP_D1            = occ_span_dt1 ; 
                  IN_2300_HI02_4_OCC_SP_D2            = occ_span_dt2 ; 
                  IN_2300_HI03_4_OCC_SP_D3            = occ_span_dt3 ; 
                  IN_2300_HI04_4_OCC_SP_D4            = occ_span_dt4 ; 
                  IN_2300_HI05_4_OCC_SP_D5            = occ_span_dt5 ; 
                  IN_2300_HI06_4_OCC_SP_D6            = occ_span_dt6 ; 
                  IN_2300_HI07_4_OCC_SP_D7            = occ_span_dt7 ; 
                  IN_2300_HI08_4_OCC_SP_D8            = occ_span_dt8 ; 
                  IN_2300_HI09_4_OCC_SP_D9            = occ_span_dt9 ; 
                  IN_2300_HI10_4_OCC_SP_D10           = occ_span_dt10 ; 



				IN_2300_HI01_2_OCC_C1                   =     occ_cd1        ;      
                IN_2300_HI02_2_OCC_C2                   =     occ_cd2        ;      
                IN_2300_HI03_2_OCC_C3                   =     occ_cd3        ;      
                IN_2300_HI04_2_OCC_C4                   =     occ_cd4        ;      
                IN_2300_HI05_2_OCC_C5                   =     occ_cd5        ;      
                IN_2300_HI06_2_OCC_C6                   =     occ_cd6        ;      
                IN_2300_HI07_2_OCC_C7                   =     occ_cd7        ;      
                IN_2300_HI08_2_OCC_C8                   =     occ_cd8       ;      
                IN_2300_HI09_2_OCC_C9                   =     occ_cd9       ;      
                IN_2300_HI10_2_OCC_C10                  =     occ_cd10      ;   
      
             	  IN_2300_HI01_4_OCC_D1             = put(occ_dt1,yymmddn8.);  
              	  IN_2300_HI02_4_OCC_D2             = put(occ_dt2,yymmddn8.);    
              	  IN_2300_HI03_4_OCC_D3             = put(occ_dt3,yymmddn8.);  
                  IN_2300_HI04_4_OCC_D4             = put(occ_dt4,yymmddn8.);  
                  IN_2300_HI05_4_OCC_D5             = put(occ_dt5,yymmddn8.);                                                                                                                                                                     
                  IN_2300_HI06_4_OCC_D6             = put(occ_dt6,yymmddn8.);                                                                                                                                                                   
                  IN_2300_HI07_4_OCC_D7             =put(occ_dt7,yymmddn8.);                                                                                                                                                                   
                  IN_2300_HI08_4_OCC_D8             =put(occ_dt8,yymmddn8.);                                                                                                                                                                   
                  IN_2300_HI09_4_OCC_D9             =put(occ_dt9,yymmddn8.);                                                                                                                                                                   
                  IN_2300_HI10_4_OCC_D10            =put(occ_dt10,yymmddn8.);  

                  IN_2300_HI01_2_VALUE_C1                 =     val_cd1       ;  

		
				IN_2300_HI02_2_VALUE_C2                 =     val_cd2       ;   
                IN_2300_HI03_2_VALUE_C3                 =     val_cd3       ;            
                IN_2300_HI04_2_VALUE_C4                 =     val_cd4       ;      
                IN_2300_HI05_2_VALUE_C5                 =     val_cd5       ;      
                IN_2300_HI06_2_VALUE_C6                 =     val_cd6       ;      
                IN_2300_HI07_2_VALUE_C7                 =     val_cd7       ;      
                IN_2300_HI08_2_VALUE_C8                 =     val_cd8       ;      
                IN_2300_HI09_2_VALUE_C9                 =     val_cd9       ;      
                IN_2300_HI10_2_VALUE_C10                =     val_cd10       ;      
                IN_2300_HI11_2_VALUE_C11                =     val_cd11       ;      
                IN_2300_HI12_2_VALUE_C12                =     val_cd12      ;      
                       
/* CR# CHG0034118 - modified Value amount mapping logic ---> start    */               
/*             
if typecode not in ("I") then do;

				   if val_cd1='A0' and klength(kcompress(VALA01))=5 then
					 IN_2300_HI01_5_VALUE_A1   = kcompress(VALA01 ||'999.9');
				  else if val_cd1='A0' and klength(kcompress(VALA01))=4 then
				  	 IN_2300_HI01_5_VALUE_A1   = kcompress('0'||VALA01 ||'999.9'); 
				  else IN_2300_HI01_5_VALUE_A1   = VALA01; 

                 IN_2300_HI02_5_VALUE_A2                 =     VALA02        ; 
				 IN_2300_HI03_5_VALUE_A3                 =     VALA03        ; 
				 IN_2300_HI04_5_VALUE_A4                 =     VALA04        ;  
				 IN_2300_HI05_5_VALUE_A5                 =     VALA05        ;
				 IN_2300_HI06_5_VALUE_A6                 =     VALA06        ;  
				 IN_2300_HI07_5_VALUE_A7                 =     VALA07        ; 
				 IN_2300_HI08_5_VALUE_A8                 =     VALA08        ; 
				 IN_2300_HI09_5_VALUE_A9                 =     VALA09        ;  
				 IN_2300_HI10_5_VALUE_A10                =     VALA10        ;
				 IN_2300_HI11_5_VALUE_A11                =     VALA11        ; 
				 IN_2300_HI12_5_VALUE_A12                =     VALA12       ;
end;
else if typecode in ("I") then do;
*//* CR# CHG0035401 - Modified the rate code logic when value amount equal to 'A0' */
/*				    if val_cd1='A0' and length(Compress(val_amt1))=5 then
						 IN_2300_HI01_5_VALUE_A1   = Compress(val_amt1 ||'999.9');
				  else if val_cd1='A0' and klength(kcompress(val_amt1))=4 then
				  	 	IN_2300_HI01_5_VALUE_A1   = kcompress('0'||val_amt1 ||'999.9'); */
				  if val_cd1='A0' then  IN_2300_HI01_5_VALUE_A1   = kcompress(val_amt1) ; /* CR# CHG0035401 - added */
				  /* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
				  else if val_cd1='24' then IN_2300_HI01_5_VALUE_A1   = ksubstr(kcompress((kcompress(val_amt1,".@:*`~#"))),1,4);
				  else IN_2300_HI01_5_VALUE_A1   = kcompress(val_amt1);

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd2='24' then IN_2300_HI02_5_VALUE_A2   = ksubstr(kcompress((kcompress(val_amt2,".@:*`~#"))),1,4);
				 	else IN_2300_HI02_5_VALUE_A2                 =     kcompress(val_amt2)  ; 

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd3='24' then IN_2300_HI03_5_VALUE_A3   = ksubstr(kcompress((kcompress(val_amt3,".@:*`~#"))),1,4);
				 	else IN_2300_HI03_5_VALUE_A3                 =     kcompress(val_amt3)  ; 

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd4='24' then IN_2300_HI04_5_VALUE_A4   = ksubstr(kcompress((kcompress(val_amt4,".@:*`~#"))),1,4);
				 	else IN_2300_HI04_5_VALUE_A4                 =     kcompress(val_amt4)  ; 

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd5='24' then IN_2300_HI05_5_VALUE_A5   = ksubstr(kcompress((kcompress(val_amt5,".@:*`~#"))),1,4);
				 	else IN_2300_HI05_5_VALUE_A5                 =     kcompress(val_amt5)  ;

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd6='24' then IN_2300_HI06_5_VALUE_A6   = ksubstr(kcompress((kcompress(val_amt6,".@:*`~#"))),1,4);
				 	else IN_2300_HI06_5_VALUE_A6                 =     kcompress(val_amt6)  ; 

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd7='24' then IN_2300_HI07_5_VALUE_A7   = ksubstr(kcompress((kcompress(val_amt7,".@:*`~#"))),1,4);
				 	else IN_2300_HI07_5_VALUE_A7                 =     kcompress(val_amt7)  ; 

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd8='24' then IN_2300_HI08_5_VALUE_A8   = ksubstr(kcompress((kcompress(val_amt8,".@:*`~#"))),1,4);
				 	else IN_2300_HI08_5_VALUE_A8                 =     kcompress(val_amt8)  ; 

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd9='24' then IN_2300_HI09_5_VALUE_A9   = ksubstr(kcompress((kcompress(val_amt9,".@:*`~#"))),1,4);
				 	else IN_2300_HI09_5_VALUE_A9                 =     kcompress(val_amt9)  ; 
 
					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd10='24' then IN_2300_HI10_5_VALUE_A10   = ksubstr(kcompress((kcompress(val_amt10,".@:*`~#"))),1,4);
				 	else IN_2300_HI10_5_VALUE_A10                =     kcompress(val_amt10) ;

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd11='24' then IN_2300_HI11_5_VALUE_A11   = ksubstr(kcompress((kcompress(val_amt11,".@:*`~#"))),1,4);
				 	else IN_2300_HI11_5_VALUE_A11                =     kcompress(val_amt11) ; 

					/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionSubstr */
					if val_cd12='24' then IN_2300_HI12_5_VALUE_A12   = ksubstr(kcompress((kcompress(val_amt12,".@:*`~#"))),1,4);
				 	else IN_2300_HI12_5_VALUE_A12                =     kcompress(val_amt12) ;
/*end;*/

/* CR# CHG0034118 - modified Value amount mapping logic ---> end    */               
          

%cond_cd;

                  IN_2300_HI01_2_COND_C1                    =     cond_cd1       ;      
                  IN_2300_HI02_2_COND_C2                  =     cond_cd2       ;      
                  IN_2300_HI03_2_COND_C3                  =     cond_cd3       ;      
                  IN_2300_HI04_2_COND_C4                  =     cond_cd4       ;      
                  IN_2300_HI05_2_COND_C5                  =     cond_cd5       ;  
             
                  IN_2300_HI06_2_COND_C6                  =     cond_cd6       ;      
                  IN_2300_HI07_2_COND_C7                  =     cond_cd7       ;      
                  IN_2300_HI08_2_COND_C8                  =     cond_cd8       ;      
                  IN_2300_HI09_2_COND_C9                  =     cond_cd9       ;      
                  IN_2300_HI10_2_COND_C10                 =     cond_cd10      ;      
                  IN_2300_HI11_2_COND_C11                 =     cond_cd11      ;      
                  IN_2300_HI12_2_COND_C12                 =     cond_cd12      ;     
	
/*********************************************************************************************/
/*                             End of  2300     CLAIM INFORMATION                             */
/*********************************************************************************************/

/*********************************************************************************************/
/*                             Start of  2310A     ATTENDING PROVIDER NAME                    */
/*********************************************************************************************/

/*********************************************************************************************/
/*          CR# CHG0031238 kv                    Modified 2310A loop                           */
/*********************************************************************************************/ 
	
/*		IN_2310A_NM103_ATT_PHY_LST= AT_PHY_LST;*/
/*		IN_2310A_NM104_ATT_PHY_FST=AT_PHY_FST;*/
/*		IN_2310A_NM109_ATT_PHY_NPIEINSSN=ATPRVNPI;*/
/*		IN_2310A_PRV03=AT_PHY_PRVTXY ;*/
/*		IN_2310A_REF02=AT_PHY_EIN;*/

/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
if kcompress(ATPRVNPI) ne ' ' and klength(kcompress(ATPRVNPI))= 10 and kcompress(ATPRVNPI)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(ATPRVNPI), '0123456789') = 0 and (kcompress(AT_PHY_LST) ne ' ' or kcompress(AT_PHY_FST) ne ' ')
		 then do;
				IN_2310A_REC_ID='2310A';
				IN_2310A_SGMT_HDR1='NM1';
				IN_2310A_NM101='71';
				IN_2310A_NM102_ATT_PHY_ENTC='1';
				IN_2310A_NM103_ATT_PHY_LST=AT_PHY_LST;
				IN_2310A_NM104_ATT_PHY_FST=AT_PHY_FST;
				IN_2310A_NM105_ATT_PHY_MID='';
				IN_2310A_NM106='';
				IN_2310A_NM107='';
				IN_2310A_NM108_ATT_PHY_Q='XX';
				IN_2310A_NM109_ATT_PHY_NPIEINSSN=ATPRVNPI;
				IN_2310A_SGMT_HDR2='PRV';
				IN_2310A_PRV01='AT';
				IN_2310A_PRV02='PXC';
				IN_2310A_PRV03=AT_PHY_PRVTXY;
				IN_2310A_SGMT_HDR3='REF';
				IN_2310A_REF01='G2';
				IN_2310A_REF02=AT_PHY_EIN;


			end;
			else do;

				IN_2310A_REC_ID='2310A';
				IN_2310A_SGMT_HDR1='';
				IN_2310A_NM101='';
				IN_2310A_NM102_ATT_PHY_ENTC='';
				IN_2310A_NM103_ATT_PHY_LST='';
				IN_2310A_NM104_ATT_PHY_FST='';
				IN_2310A_NM105_ATT_PHY_MID='';
				IN_2310A_NM106='';
				IN_2310A_NM107='';
				IN_2310A_NM108_ATT_PHY_Q='';
				IN_2310A_NM109_ATT_PHY_NPIEINSSN='';
				IN_2310A_SGMT_HDR2='';
				IN_2310A_PRV01='';
				IN_2310A_PRV02='';
				IN_2310A_PRV03='';
				IN_2310A_SGMT_HDR3='';
				IN_2310A_REF01='';
				IN_2310A_REF02='';

			end;
/*********************************************************************************************/
/*                             End of  2310A     ATTENDING PROVIDER NAME                      */
/*********************************************************************************************/

/*********************************************************************************************/
/*                             Start of  2310B     OPERATING PHYSICIAN NAME                   */
/*********************************************************************************************/
		
/*********************************************************************************************/
/*          CR# CHG0031238 kv                    Modified 2310B loop                           */
/*********************************************************************************************/
/*IN_2310B_NM103_OP_PHY_LST= OP_PHY_LST;*/
/*IN_2310B_NM104_OP_PHY_FST=OP_PHY_FST;*/
/*IN_2310B_NM109_OP_PHY_NPIEINSSN=OPPRVNPI;*/
/*IN_2310B_REF02=OP_PHY_EIN;*/

		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
		if  kcompress(OPPRVNPI) ne ' ' and  klength(kcompress(OPPRVNPI))= 10 and kcompress(OPPRVNPI)  NE '0000000000' and
		/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionVerify */
		kverify(kcompress(OPPRVNPI), '0123456789') = 0 and ( kcompress(OP_PHY_LST) ne ' ' or kcompress(OP_PHY_FST) ne ' ' )
		 then do;
				IN_2310B_REC_ID='2310B';
				IN_2310B_SGMT_HDR1='NM1';
				IN_2310B_NM101='72';
				IN_2310B_NM102='1';
				IN_2310B_NM103_OP_PHY_LST=OP_PHY_LST;
				IN_2310B_NM104_OP_PHY_FST=OP_PHY_FST;
				IN_2310B_NM105_OP_PHY_MID='';
				IN_2310B_NM106='';
				IN_2310B_NM107='';
				IN_2310B_NM108_OP_PHY_Q='XX';
				IN_2310B_NM109_OP_PHY_NPIEINSSN=OPPRVNPI;
				IN_2310B_SGMT_HDR3='REF';
				IN_2310B_REF01='G2';
				IN_2310B_REF02=OP_PHY_EIN;

			end;
			else do;
				IN_2310B_REC_ID='2310B';
				IN_2310B_SGMT_HDR1='';
				IN_2310B_NM101='';
				IN_2310B_NM102='';
				IN_2310B_NM103_OP_PHY_LST='';
				IN_2310B_NM104_OP_PHY_FST='';
				IN_2310B_NM105_OP_PHY_MID='';
				IN_2310B_NM106='';
				IN_2310B_NM107='';
				IN_2310B_NM108_OP_PHY_Q='';
				IN_2310B_NM109_OP_PHY_NPIEINSSN='';
				IN_2310B_SGMT_HDR3='';
				IN_2310B_REF01='';
				IN_2310B_REF02='';
			end;

IN_2310E_NM109=NPIID_1;

/* SAS2AWS2: ReplacedFunctionCatx */
IN_2310E_NM103=catx(' ',Plstnam_1,pfstnam_1)  ;
if padrln1 = "" then do;
	IN_2310E_NM301=/*padrln2_1*/kcompress(padrln2,"@:*`~#");	/* CR# CHG0032056 kv */	
end;
else do;
	IN_2310E_NM301=/*padrln1_1*/kcompress(padrln1,"@:*`~#");/* CR# CHG0032056 kv */	
	IN_2310E_NM302=/*padrln2_1*/kcompress(padrln2,"@:*`~#");/* CR# CHG0032056 kv */	
end;
IN_2310E_NM401=/*pcitycd_1*/ kcompress(pcitycd,"@:*`~#");/* CR# CHG0032056 kv */	
IN_2310E_NM402=/*pstacod_1*/ kcompress(pstacod,"@:*`~#");/* CR# CHG0032056 kv */	
					  
/*IN_2310E_NM403=pzipcod_1;*/
/* SAS2AWS2: ReplacedFunctionCompress */
if Length(kcompress(pzipcod,"@:*`~#- "))=5 then do;
	 /* SAS2AWS2: ReplacedFunctionCompress */
	 IN_2310E_NM403=kcompress(kcompress(pzipcod,"@:*`~#- ")||"9998");
end;
else do;
	/* SAS2AWS2: ReplacedFunctionCompress */
	IN_2310E_NM403=kcompress(pzipcod,"@:*`~#- ");
end;	
if missing(NPIID_1)=1 then IN_2310E_REF02=mrg_provno; /* CR# CHG0032056 kv */


 /*********************************************************************************************/
/*                             End of  2310B     OPERATING PHYSICIAN NAME                     */
/*********************************************************************************************/
	
/*********************************************************************************************/
/*                             Start of  2320                            */
/*********************************************************************************************/ 

/* B-96285 start QHP, HFIC logic */
%IF (&compno=45 and "&LOB." ne "MPPO")  or (&compno=42 and "&LOB." eq "QHP") %then %do ;	/* B-121242 */

   if clmstat in ("PD","PA","AP") then do;
						IN_2320_SGMT_HDR2 = '';
						IN_2320_CAS01= '';
						IN_2320_CAS02 = '';
						IN_2320_CAS03 = '';		
   end;	
  
	IN_2320_SBR01_RES_C='P';

    if  relcod eq '1' then 
	  IN_2320_SBR02='18';
	else if relcod eq '2' then 
	   IN_2320_SBR02='01';
	else if relcod eq '3' then
	   IN_2320_SBR02='19';
	else 
	   IN_2320_SBR02='G8';	

	IN_2320_SBR03_GRPNUM=MEM_subsno;
	IN_2320_SBR06='6';
	IN_2320_SBR09_CLM_FIL_IND='CI';

%end;	/* B-96285 end QHP, HFIC logic */

%else %do;
	
   IN_2320_SGMT_HDR2_MCD = '';
   IN_2320_CAS01_MCD= '';
   IN_2320_CAS02_MCD = '';
   IN_2320_CAS03_MCD = '';		
   IN_2320_SBR01_RES_C_MCD='P';
   IN_2320_SBR02_MCD='18';
   IN_2320_SBR03_GRPNUM_MCD=MEM_MEMBNO;
   IN_2320_SBR06_MCD='6';
   IN_2320_SBR09_CLM_FIL_IND_MCD='MC';     

    %if &compno.=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do;/*B-248997*/	/* B-145499 Medicare, assign APD _MCD values to _MCR, blank out _MCD */
   		IN_2320_REC_ID_MCR='2320';
		IN_2320_SGMT_HDR1_MCR='SBR';
		IN_2320_SBR01_RES_C_MCR='P';
		IN_2320_SBR01_RES_C_MCD=' ';
		IN_2320_SBR02_MCR='18';
		IN_2320_SBR02_MCD='  ';
		IN_2320_SBR03_GRPNUM_MCR=MEM_MEMBNO; /* B-146417 */
		IN_2320_SBR03_GRPNUM_MCD='';
		IN_2320_SBR04_INSR_NAME_MCR='';
		IN_2320_SBR05_MCR='';
		IN_2320_SBR06_MCR='6';
		IN_2320_SBR06_MCD=' ';
		IN_2320_SBR07_MCR='';
		IN_2320_SBR08_MCR='';
		IN_2320_SBR09_CLM_FIL_IND_MCR='MA';
		IN_2320_SBR09_CLM_FIL_IND_MCD='  '; 
		IN_2320_SGMT_HDR2_MCR='';
		IN_2320_CAS01_MCR='';
		IN_2320_CAS02_MCR='';
		IN_2320_CAS03_MCR='';
		IN_2320_SGMT_HDR3_MCR='AMT';
		IN_2320_AMT01_MCR='D';	
		IN_2320_SGMT_HDR4_MCR='';
		IN_2320_OI03_ASN_BEN_MCR='';
		IN_2320_OI06_REL_INF_CD_MCR='';
   %end;
   %else
   %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
   		IN_2320_REC_ID_MCR='2320';
		IN_2320_SGMT_HDR1_MCR='SBR';
		IN_2320_SBR01_RES_C_MCD='S';  /* B-267578 - Update the Hardcoded P to S */
		IN_2320_SBR01_RES_C_MCR='P';
		IN_2320_SBR02_MCR='18';
		IN_2320_SBR03_GRPNUM_MCR=MEM_MEMBNO;
		IN_2320_SBR04_INSR_NAME_MCR='';
		IN_2320_SBR05_MCR='';
		IN_2320_SBR06_MCR='1';
		IN_2320_SBR07_MCR='';
		IN_2320_SBR08_MCR='';
		IN_2320_SBR09_CLM_FIL_IND_MCR='16'; /*B-222679 Replaced MA with 16*/
		IN_2320_SGMT_HDR2_MCR='';
		IN_2320_CAS01_MCR='';
		IN_2320_CAS02_MCR='';
		IN_2320_CAS03_MCR='';
		IN_2320_SGMT_HDR3_MCR='AMT';
		IN_2320_AMT01_MCR='D';		
		IN_2320_SGMT_HDR4_MCR='';
		IN_2320_OI03_ASN_BEN_MCR='';
		IN_2320_OI06_REL_INF_CD_MCR='';
   %end;
   %else
   %if &compno.=01 and &LOB. =DSNP  %then %do;
		IN_2320_SBR03_GRPNUM_MCD=dsnp_membno; 
   		IN_2320_REC_ID_MCR='2320';
		IN_2320_SGMT_HDR1_MCR='SBR';
		IN_2320_SBR01_RES_C_MCD='S';  
		IN_2320_SBR01_RES_C_MCR='P';
		IN_2320_SBR02_MCR='18';
		IN_2320_SBR03_GRPNUM_MCR=dsnp_membno;
		IN_2320_SBR04_INSR_NAME_MCR='';
		IN_2320_SBR05_MCR='';
		IN_2320_SBR06_MCR='1';
		IN_2320_SBR07_MCR='';
		IN_2320_SBR08_MCR='';
		IN_2320_SBR09_CLM_FIL_IND_MCR='16'; 
		IN_2320_SGMT_HDR2_MCR='';
		IN_2320_CAS01_MCR='';
		IN_2320_CAS02_MCR='';
		IN_2320_CAS03_MCR='';
		IN_2320_SGMT_HDR3_MCR='AMT';
		IN_2320_AMT01_MCR='D';		
		IN_2320_SGMT_HDR4_MCR='';
		IN_2320_OI03_ASN_BEN_MCR='';
		IN_2320_OI06_REL_INF_CD_MCR='';
   %end;   
   %else %do;
   		IN_2320_REC_ID_MCR='2320';
		IN_2320_SGMT_HDR1_MCR='';
		IN_2320_SBR01_RES_C_MCR='';
		IN_2320_SBR02_MCR='';
		IN_2320_SBR03_GRPNUM_MCR='';
		IN_2320_SBR04_INSR_NAME_MCR='';
		IN_2320_SBR05_MCR='';
		IN_2320_SBR06_MCR='';
		IN_2320_SBR07_MCR='';
		IN_2320_SBR08_MCR='';
		IN_2320_SBR09_CLM_FIL_IND_MCR='';
		IN_2320_SGMT_HDR2_MCR='';
		IN_2320_CAS01_MCR='';
		IN_2320_CAS02_MCR='';
		IN_2320_CAS03_MCR='';
		IN_2320_SGMT_HDR3_MCR='';
		IN_2320_AMT01_MCR='';		
		IN_2320_SGMT_HDR4_MCR='';
		IN_2320_OI03_ASN_BEN_MCR='';
		IN_2320_OI06_REL_INF_CD_MCR='';
   %end;

%end;

   
/*********************************************************************************************/
/*                             Start of  2330A     OTHER SUBSCRIBER NAME                     */
/*********************************************************************************************/

/* B-96285 start QHP, HFIC logic */
%IF (&compno=45 and "&LOB." ne "MPPO")  or (&compno=42 and "&LOB." eq "QHP") %then %do ;/*B-248997*/	/* B-121242 */

 	/* 477378 */
              IN_2330A_NM103_SUB_LST                 = SUB_MLSTNAM;      
              IN_2330A_NM104_SUB_FST                 = SUB_MFSTNAM;      
              IN_2330A_NM105_SUB_MID                 = SUB_MMIDFULL;      
              IN_2330A_NM109_SUB_MEM_MDCARID         = SUB_MDCARID; 	
			 if missing(SUB_MADRLN1) = 0 then do;/* CR# CHG0032372 kv */
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2330A_N301_OTH_SUB_ADR1=kcompress(SUB_MADRLN1,"@:*`~#");
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2330A_N302_OTH_SUB_ADR2=kcompress(SUB_MADRLN2,"@:*`~#");
			 end;	
			 else do;
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2330A_N301_OTH_SUB_ADR1=kcompress(SUB_MADRLN1,"@:*`~#");
			 end;
	          IN_2330A_N401_OTH_SUB_CITY             = SUB_MCITYCD;
	          IN_2330A_N402_OTH_SUB_STATE            = SUB_MSTACOD;
			  IN_2330A_N403_OTH_SUB_ZIP				 = SUB_MZIPCOD;		

%end;	/* B-96285 end QHP, HFIC logic */

%else %do;

              IN_2330A_NM103_SUB_LST_MCD                 = MEM_MLSTNAM;      
              IN_2330A_NM104_SUB_FST_MCD                 = MEM_MFSTNAM;      
              IN_2330A_NM105_SUB_MID_MCD                 = MEM_MMIDFULL;      
              IN_2330A_NM109_SUB_MEM_MCD                  = MEM_MDCARID; 	
/*			  IN_2330A_N301_OTH_SUB_ADR1_MCD			 = MEM_MADRLN1;	*/
/*			  IN_2330A_N302_OTH_SUB_ADR2_MCD			 = MEM_MADRLN2;*/
			  if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0033527 modified */
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N301_OTH_SUB_ADR1_MCD=kcompress(MEM_MADRLN1,"@:*`~#");
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N302_OTH_SUB_ADR2_MCD=kcompress(MEM_MADRLN2,"@:*`~#");
			  End;
			  else do;
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N301_OTH_SUB_ADR1_MCD=kcompress(MEM_MADRLN2,"@:*`~#");
			  end; 	
	          IN_2330A_N401_OTH_SUB_CITY_MCD             = MEM_MCITYCD;
	          IN_2330A_N402_OTH_SUB_STATE_MCD            = MEM_MSTACOD;
			  IN_2330A_N403_OTH_SUB_ZIP_MCD				 = MEM_MZIPCOD;
	
			  %if &compno.=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do;	/*B-248997*/ /* B-145499 Medicare, assign APD _MCD values to _MCR, blank out _MCD */
			  	IN_2330A_REC_ID_MCR ='2330A';
				IN_2330A_SGMT_HDR1_MCR='NM1';
				IN_2330A_NM101_IO_MCR='IL';
				IN_2330A_NM102_1_MCR='1';
				IN_2330A_NM103_SUB_LST_MCR= MEM_MLSTNAM; 
				IN_2330A_NM104_SUB_FST_MCR= MEM_MFSTNAM;   
				IN_2330A_NM105_SUB_MID_MCR= MEM_MMIDFULL;  
				IN_2330A_NM108_MI_MCR='MI';
				IN_2330A_NM109_SUB_MEM_MCR = MEM_MDCARID; /* B-146417 */ 
				IN_2330A_SGMT_HDR2_MCR='N3';

 				if missing(MEM_MADRLN1) = 0 then do; 
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N301_OTH_SUB_ADR1_MCR=kcompress(MEM_MADRLN1,"@:*`~#");
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N302_OTH_SUB_ADR2_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
			  	End;
			  	else do;
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N301_OTH_SUB_ADR1_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
			  	end; 		
				IN_2330A_SGMT_HDR3_MCR ='N4';
				IN_2330A_N401_OTH_SUB_CITY_MCR = MEM_MCITYCD;
				IN_2330A_N402_OTH_SUB_STATE_MCR = MEM_MSTACOD;
				IN_2330A_N403_OTH_SUB_ZIP_MCR = MEM_MZIPCOD;

				IN_2330A_NM103_SUB_LST_MCD='';
				IN_2330A_NM104_SUB_FST_MCD='';
				IN_2330A_NM105_SUB_MID_MCD='';
				IN_2330A_NM109_SUB_MEM_MCD='';				
				IN_2330A_N301_OTH_SUB_ADR1_MCD='';
				IN_2330A_N302_OTH_SUB_ADR2_MCD='';
				IN_2330A_N401_OTH_SUB_CITY_MCD='';
				IN_2330A_N402_OTH_SUB_STATE_MCD='';
				IN_2330A_N403_OTH_SUB_ZIP_MCD='';
			  %end;
			  %else %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
			  	IN_2330A_REC_ID_MCR ='2330A';
				IN_2330A_SGMT_HDR1_MCR='NM1';
				IN_2330A_NM101_IO_MCR='IL';
				IN_2330A_NM102_1_MCR='1';
				IN_2330A_NM103_SUB_LST_MCR= MEM_MLSTNAM; 
				IN_2330A_NM104_SUB_FST_MCR= MEM_MFSTNAM;   
				IN_2330A_NM105_SUB_MID_MCR= MEM_MMIDFULL;  
				IN_2330A_NM108_MI_MCR='MI';
				IN_2330A_NM109_SUB_MEM_MCR = MEMBNO; 
				IN_2330A_SGMT_HDR2_MCR='N3';
/*				IN_2330A_N301_OTH_SUB_ADR1_MCR= MEM_MADRLN1;	*/
/*				IN_2330A_N302_OTH_SUB_ADR2_MCR= MEM_MADRLN2;*/
 				if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0033527 modified */
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N301_OTH_SUB_ADR1_MCR=kcompress(MEM_MADRLN1,"@:*`~#");
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N302_OTH_SUB_ADR2_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
			  	End;
			  	else do;
					/* SAS2AWS2: ReplacedFunctionCompress */
					IN_2330A_N301_OTH_SUB_ADR1_MCR=kcompress(MEM_MADRLN2,"@:*`~#");
			  	end; 		
				IN_2330A_SGMT_HDR3_MCR ='N4';
				IN_2330A_N401_OTH_SUB_CITY_MCR = MEM_MCITYCD;
				IN_2330A_N402_OTH_SUB_STATE_MCR = MEM_MSTACOD;
				IN_2330A_N403_OTH_SUB_ZIP_MCR = MEM_MZIPCOD;
			  %end;
			  %else %if &compno.=01 and &LOB. =DSNP  %then %do;
			  	IN_2330A_REC_ID_MCR ='2330A';
				IN_2330A_SGMT_HDR1_MCR='NM1';
				IN_2330A_NM101_IO_MCR='IL';
				IN_2330A_NM102_1_MCR='1';
 				IN_2330A_NM108_MI_MCR='MI';
				IN_2330A_NM109_SUB_MEM_MCR = MEMBNO_MCR; 
				IN_2330A_SGMT_HDR2_MCR='N3';
				IN_2330A_NM103_SUB_LST_MCR= MEM_MLSTNAM_MCR; 
				IN_2330A_NM104_SUB_FST_MCR= MEM_MFSTNAM_MCR;   
				IN_2330A_NM105_SUB_MID_MCR= MEM_MMIDFULL_MCR; 
 				if missing(MEM_MADRLN1) = 0 then do; 
					IN_2330A_N301_OTH_SUB_ADR1_MCR=kcompress(MEM_MADRLN1_MCR,"@:*`~#");
					IN_2330A_N302_OTH_SUB_ADR2_MCR=kcompress(MEM_MADRLN2_MCR,"@:*`~#");
			  	End;
			  	else do;
					IN_2330A_N301_OTH_SUB_ADR1_MCR=kcompress(MEM_MADRLN2_MCR,"@:*`~#");
			  	end; 		
				IN_2330A_SGMT_HDR3_MCR ='N4';
				IN_2330A_N401_OTH_SUB_CITY_MCR = MEM_MCITYCD_MCR;
				IN_2330A_N402_OTH_SUB_STATE_MCR = MEM_MSTACOD_MCR;
				IN_2330A_N403_OTH_SUB_ZIP_MCR = MEM_MZIPCOD_MCR;
			  %end;			  
			  %else %do;
			  	IN_2330A_REC_ID_MCR ='2330A';
				IN_2330A_SGMT_HDR1_MCR='';
				IN_2330A_NM101_IO_MCR='';
				IN_2330A_NM102_1_MCR='';
				IN_2330A_NM103_SUB_LST_MCR='';
				IN_2330A_NM104_SUB_FST_MCR='';
				IN_2330A_NM105_SUB_MID_MCR='';
				IN_2330A_NM108_MI_MCR='';
				IN_2330A_NM109_SUB_MEM_MCR='';
				IN_2330A_SGMT_HDR2_MCR='';
				IN_2330A_N301_OTH_SUB_ADR1_MCR='';
				IN_2330A_N302_OTH_SUB_ADR2_MCR='';
				IN_2330A_SGMT_HDR3_MCR='';
				IN_2330A_N401_OTH_SUB_CITY_MCR='';
				IN_2330A_N402_OTH_SUB_STATE_MCR='';
				IN_2330A_N403_OTH_SUB_ZIP_MCR='';
			  %end;

%end;

/*********************************************************************************************/
/*                             End of  2330A     OTHER SUBSCRIBER NAME                       */
/*********************************************************************************************/

/*********************************************************************************************/
/*                             Start of  2330B    OTHER PAYER NAME                            */
/*********************************************************************************************/ 

/* B-96285 start QHP, HFIC logic */
%IF (&compno=45  and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do ;	/*B-248997*/ /* B-121242 */

			 IN_2330B_ICN = clamno;
             
		  If IN_2300_CLM05_3_FREQ_CDE_1 in ("7","8") OR "&N4TON5_icn_add." eq "Y" then 
			 IN_2330B_ICN1 =icn ;  
          else 
			 IN_2330B_ICN1 = '                      ';   
			 IN_2330B_DTP03_ADJU_DAT= put(max_pidate,yymmddn8.)  ;	/*[B-315447] - DS - Updates*/

			 %if "&x12_ctrl." ne "EIS" %then %do;	/* B-121242 */
			 IN_2330B_NM103="&cname.";				/* B-121242 */
			 	%If (&compno=45 and "&LOB." ne "MPPO") %then %do;	/*B-248997*/ /* B-146417 add hiosid from elgibility outbound */
					IN_2330B_NM109=HIOSID;
						%if ("&hiosflg." = "Y") %then %do; /* B-274606 */
							IN_2330B_NM109 = hios_plan_id_cd;
						%end;					
				%end;
				%else %do;
					IN_2330B_NM109="&HIOSID.";		/* B-121242 */
						%if ("&hiosflg." = "Y") %then %do; /* B-274606 */
							IN_2330B_NM109 = hix_policy_num_cd;
						%end;					
				%end;
			 %end;

		/* 477378 */
		IF relcod not in ("1") then do;
			  IN_2330C_SGMT_HDR1='NM1';
			  IN_2330C_NM101_IO='QC';
			  IN_2330C_NM102_1='1';
			  IN_2330C_NM103_SUB_LST                 = MEM_MLSTNAM;      
              IN_2330C_NM104_SUB_FST                 = MEM_MFSTNAM;      
              IN_2330C_NM105_SUB_MID                 = MEM_MMIDFULL;      
			  IN_2330C_NM108_MI='MI';
              IN_2330C_NM109_SUB_MEM_MDCARID         = MEM_MDCARID; 
			  IN_2330C_SGMT_HDR2='N3';		
			  if missing(MEM_MADRLN1) = 0 then do; /* CR# CHG0032372 kv */
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2330C_N301_OTH_SUB_ADR1=kcompress(MEM_MADRLN1,"@:*`~#");
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2330C_N302_OTH_SUB_ADR2=kcompress(MEM_MADRLN2,"@:*`~#");
			 end;	
			 else do;
				/* SAS2AWS2: ReplacedFunctionCompress */
				IN_2330C_N301_OTH_SUB_ADR1=kcompress(MEM_MADRLN1,"@:*`~#");
			 end;
			  IN_2330C_SGMT_HDR3='N4';
	          IN_2330C_N401_OTH_SUB_CITY             = MEM_MCITYCD;
	          IN_2330C_N402_OTH_SUB_STATE            = MEM_MSTACOD;
			  IN_2330C_N403_OTH_SUB_ZIP				 = MEM_MZIPCOD;	
		End;

%end;	/* B-96285 end QHP, HFIC logic */

%else %do;

			IN_2330B_ICN_MCD = clamno;             
			  If IN_2300_CLM05_3_FREQ_CDE_1 in ("7","8") OR "&N4TON5_icn_add." eq "Y" then 
				 IN_2330B_ICN1_MCD =icn ;  
               else 
				 IN_2330B_ICN1_MCD = '                      ';   

				 IN_2330B_DTP03_ADJU_DAT_MCD = put(max_pidate,yymmddn8.)  ;	/*[B-315447] - DS - Updates*/
				 IN_2330B_NM103_MCD="&cname.";				 
				 IN_2330B_NM109_MCD="&HIOSID.";
				 	%if ("&hiosflg." = "Y" and (&compno.=01 or &compno.=20 or (&compno.=42 and "&LOB." eq "EP"))) %then %do; /* B-274606 */
						IN_2330B_NM109_MCD = hix_policy_num_cd;
					%end;

				  %if &compno.=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do;	/* B-145499 Medicare, assign APD _MCD values to _MCR, blank out _MCD */
				   	IN_2330B_REC_ID_MCR='2330B';
					IN_2330B_SGMT_HDR1_MCR='NM1';
					IN_2330B_NM101_MCR ='PR';
					IN_2330B_NM102_MCR ='2';
					IN_2330B_NM103_MCR="&cname.";
					IN_2330B_NM108_MCR='PI';
					IN_2330B_NM109_MCR=HIOSID;	/* B-146417 replace "&HIOSID." with elgibility var */
					IN_2330B_SGMT_HDR2_MCR ='N3';
					IN_2330B_N301_MCR= '';
					IN_2330B_N302_MCR='';
					IN_2330B_SGMT_HDR3_MCR='N4';
					IN_2330B_N401_MCR='';
					IN_2330B_N402_MCR='';
					IN_2330B_N403_MCR='';
					IN_2330B_SGMT_HDR4_MCR='DTP';
					IN_2330B_DTP01_MCR='573';
					IN_2330B_DTP02_MCR='D8';
					IN_2330B_DTP03_ADJU_DAT_MCR= put(max_pidate,yymmddn8.)  ;	/*[B-315447] - DS - Updates*/
					IN_2330B_SGMT_HDR5_MCR='REF';
					IN_2330B_REF01_OPSI_MCR='';
					IN_2330B_REF02_OPSI_MCR='';
					IN_2330B_SGMT_HDR6_MCR='REF';
					IN_2330B_REF01_OPPAN_MCR='';
					IN_2330B_REF02_OPPAN_MCR='';
					IN_2330B_SGMT_HDR8_MCR='REF';
					IN_2330B_F8_MCR='F8';
					IN_2330B_ICN_MCR=clamno;   
					IN_2330B_SGMT_HDR9_MCR='REF';
					IN_2330B_BP_MCR='BP';

					IN_2330B_NM103_MCD='';
					IN_2330B_NM109_MCD='';
					IN_2330B_N301_MCD='';
					IN_2330B_N302_MCD='';
					IN_2330B_N401_MCD='';
					IN_2330B_N402_MCD='';
					IN_2330B_N403_MCD='';
					IN_2330B_DTP03_ADJU_DAT_MCD='';
					IN_2330B_REF01_OPSI_MCD='';
					IN_2330B_REF02_OPSI_MCD='';
					IN_2330B_REF01_OPPAN_MCD='';
					IN_2330B_REF02_OPPAN_MCD='';
					
					 If IN_2300_CLM05_3_FREQ_CDE_1 in ("7","8") OR "&N4TON5_icn_add." eq "Y" then 
							IN_2330B_ICN1_MCR =icn ;  
              		 else 	IN_2330B_ICN1_MCR = '                      ';
				  %end;
				  %else %if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
				  	IN_2330B_REC_ID_MCR='2330B';
					IN_2330B_SGMT_HDR1_MCR='NM1';
					IN_2330B_NM101_MCR ='PR';
					IN_2330B_NM102_MCR ='2';
					IN_2330B_NM103_MCR="Medicare"; 
					IN_2330B_NM108_MCR='PI';
					/*IN_2330B_NM109_MCR="&HIOSID.";*/
					IN_2330B_NM109_MCR='N95284]H3359]034'; /*B-222679*/	
					IN_2330B_SGMT_HDR2_MCR ='N3';
					IN_2330B_N301_MCR= '';
					IN_2330B_N302_MCR='';
					IN_2330B_SGMT_HDR3_MCR='N4';
					IN_2330B_N401_MCR='';
					IN_2330B_N402_MCR='';
					IN_2330B_N403_MCR='';
					IN_2330B_SGMT_HDR4_MCR='DTP';
					IN_2330B_DTP01_MCR='573';
					IN_2330B_DTP02_MCR='D8';
					IN_2330B_DTP03_ADJU_DAT_MCR= put(max_pidate,yymmddn8.)  ;	/*[B-315447] - DS - Updates*/
					IN_2330B_SGMT_HDR5_MCR='REF';
					IN_2330B_REF01_OPSI_MCR='2U';
					IN_2330B_REF02_OPSI_MCR='INTDUAL';
					IN_2330B_SGMT_HDR6_MCR='REF';
					IN_2330B_REF01_OPPAN_MCR='';
					IN_2330B_REF02_OPPAN_MCR='';
					IN_2330B_SGMT_HDR8_MCR='REF';
					IN_2330B_F8_MCR='F8';
					IN_2330B_ICN_MCR=clamno;   
					IN_2330B_SGMT_HDR9_MCR='REF';
					IN_2330B_BP_MCR='BP';
					 If IN_2300_CLM05_3_FREQ_CDE_1 in ("7","8") OR "&N4TON5_icn_add." eq "Y" then
							 IN_2330B_ICN1_MCR =icn ;  
              		 else 
				 			IN_2330B_ICN1_MCR = '                      ';  
				  %end;
				  %else %if &compno.=01 and &LOB. =DSNP %then %do;
				  	IN_2330B_REC_ID_MCR='2330B';
					IN_2330B_SGMT_HDR1_MCR='NM1';
					IN_2330B_NM101_MCR ='PR';
					IN_2330B_NM102_MCR ='2';
					IN_2330B_NM103_MCR="Medicare"; 
					IN_2330B_NM108_MCR='PI';
					IN_2330B_NM109_MCR='N95284]H3359]038'; 
					IN_2330B_SGMT_HDR2_MCR ='N3';
					IN_2330B_N301_MCR= '';
					IN_2330B_N302_MCR='';
					IN_2330B_SGMT_HDR3_MCR='N4';
					IN_2330B_N401_MCR='';
					IN_2330B_N402_MCR='';
					IN_2330B_N403_MCR='';
					IN_2330B_SGMT_HDR4_MCR='DTP';
					IN_2330B_DTP01_MCR='573';
					IN_2330B_DTP02_MCR='D8';
					IN_2330B_DTP03_ADJU_DAT_MCR= put(max_pidate_MCR,yymmddn8.)  ;	
					IN_2330B_SGMT_HDR5_MCR='REF';
					IN_2330B_REF01_OPSI_MCR='2U';
					IN_2330B_REF02_OPSI_MCR='INTDUAL';
					IN_2330B_SGMT_HDR6_MCR='REF';
					IN_2330B_REF01_OPPAN_MCR='';
					IN_2330B_REF02_OPPAN_MCR='';
					IN_2330B_SGMT_HDR8_MCR='REF';
					IN_2330B_F8_MCR='F8';
					IN_2330B_ICN_MCR=clamno_mcr;   
					IN_2330B_SGMT_HDR9_MCR='REF';
					IN_2330B_BP_MCR='BP';
					 If IN_2300_CLM05_3_FREQ_CDE_1 in ("7","8") OR "&N4TON5_icn_add." eq "Y" then
							 IN_2330B_ICN1_MCR =icn ;  
              		 else 
				 			IN_2330B_ICN1_MCR = '                      ';  
				  %end;				  
				  %else %do;
					IN_2330B_REC_ID_MCR='2330B';
					IN_2330B_SGMT_HDR1_MCR='';
					IN_2330B_NM101_MCR='';
					IN_2330B_NM102_MCR='';
					IN_2330B_NM103_MCR='';
					IN_2330B_NM108_MCR='';
					IN_2330B_NM109_MCR='';
					IN_2330B_SGMT_HDR2_MCR='';
					IN_2330B_N301_MCR='';
					IN_2330B_N302_MCR='';
					IN_2330B_SGMT_HDR3_MCR='';
					IN_2330B_N401_MCR='';
					IN_2330B_N402_MCR='';
					IN_2330B_N403_MCR='';
					IN_2330B_SGMT_HDR4_MCR='';
					IN_2330B_DTP01_MCR='';
					IN_2330B_DTP02_MCR='';
					IN_2330B_DTP03_ADJU_DAT_MCR='';
					IN_2330B_SGMT_HDR5_MCR='';
					IN_2330B_REF01_OPSI_MCR='';
					IN_2330B_REF02_OPSI_MCR='';
					IN_2330B_SGMT_HDR6_MCR='';
					IN_2330B_REF01_OPPAN_MCR='';
					IN_2330B_REF02_OPPAN_MCR='';
					IN_2330B_SGMT_HDR8_MCR='';
					IN_2330B_F8_MCR='';
					IN_2330B_ICN_MCR='';
					IN_2330B_SGMT_HDR9_MCR='';
					IN_2330B_BP_MCR='';
					IN_2330B_ICN1_MCR='';
                  %end;

%end;
			
/*********************************************************************************************/
/*                             Start of  2400    SERVICE LINE                             */
/*********************************************************************************************/

			    if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1 )  then do; /* 499550 */
					IN_2400_SV202_1_HC="  ";
				end;
				/* SAS2AWS2: ReplacedFunctionSubstr */
				else if  Compress(ksubstr(bilcod,1,2)) in ("21","18","32") and
						 /* SAS2AWS2: ReplacedFunctionCompress */
						 (kcompress(REVCOD) in ("022","023","024","22","23","24") or
						  /* SAS2AWS2: ReplacedFunctionCompress */
						  kcompress(svccod)  in ("022","023","024","22","23","24")) then do;
				    IN_2400_SV202_1_HC="HP";
                end;
				else do;							
					IN_2400_SV202_1_HC="HC";
				end;
                   
				if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1) then do;
				end;
				else if typecode="O" then do;
							if missing(cptcod)=0 then IN_2400_SV202_2_SL_HCPCS  = cptcod ;
							else IN_2400_SV202_2_SL_HCPCS  = svccod;
				End;


          IN_2400_SV201_SL_REV_C = put(input(revenue_cd,4.),z4.) ;

            if typecode = "O" then 
				do;


								if SVCTYP = 'RV' then    
							 	do; 
/*			                       IN_2400_SV202_2_SL_HCPCS = cptcod ;*/
								end;
			                     else 
							     do;
/*			                        IN_2400_SV202_2_SL_HCPCS = svccod ;*/
								 end;

						if BIPRVNPI in (&binpiid.) and IN_2400_SV202_2_SL_HCPCS in (&NDC_Svccod.) then do;
						/* IN_2400_SV202_3_SL_MOD1="UD"; 	 									B-103975 moved to RGA19201   */
						   IN_2400_SV202_4_SL_MOD2                 =     CPT_Cod2         ;  /* B-96285 QHP had = Mod_Cod2  */ 
		                   IN_2400_SV202_5_SL_MOD3                 =     CPT_Cod3         ;  /* B-96285 QHP had = Mod_Cod3  */   
		                   IN_2400_SV202_6_SL_MOD4                 =     CPT_Cod4         ;  /* B-96285 QHP had = Mod_Cod4  */	
						end;
						else do;						  
                            IN_2400_SV202_3_SL_MOD1                 =     CPT_Cod1         ; /* B-96285 QHP had = Mod_Cod1  */   
		           		 	IN_2400_SV202_4_SL_MOD2                 =     CPT_Cod2         ; /* B-96285 QHP had = Mod_Cod2  */  
		                 	IN_2400_SV202_5_SL_MOD3                 =     CPT_Cod3         ; /* B-96285 QHP had = Mod_Cod3  */    
		                 	IN_2400_SV202_6_SL_MOD4                 =     CPT_Cod4         ; /* B-96285 QHP had = Mod_Cod4  */ 	
						end;						
                    End;             
            else if typecode = "I" then		                   
					do;
					      IN_2400_SV202_3_SL_MOD1  = " " ;										
	           		      IN_2400_SV202_4_SL_MOD2  = " " ;    
	                      IN_2400_SV202_5_SL_MOD3  = " " ;      
	                      IN_2400_SV202_6_SL_MOD4  = " " ;    	
	            	end;				
	
        /* SAS2AWS2: ReplacedFunctionCatx */
        IN_2400_DTP03_SL_SER_DT            = catx("-",put(SVCDAT,yymmddn8.),put(ENDDAT,yymmddn8.)) ;
		  			

/*********************************************************************************************/
/*                             End of  2400    SERVICE LINE                                  */
/*********************************************************************************************/      

/*********************************************************************************************/
/*                             Start of  2410     DRUG IDENTIFICATION                         */
/*********************************************************************************************/
             /* SAS2AWS2: ReplacedFunctionLength */
             If klength(NDCSVC)=11 then
                  do;
                        IN_2410_LIN02_PRD_Q="N4";
                  end;
        	Else 
                  do;
                        IN_2410_LIN02_PRD_Q=" ";
                  end; 
            /* SAS2AWS2: ReplacedFunctionLength */
            If klength(NDCSVC)=11 then
                  do;
                        IN_2410_Sgmt_Hdr2="N4";
                  end;
            Else 
                  do;
                        IN_2410_Sgmt_Hdr2="";
                  end;
				  /* SAS2AWS2: ReplacedFunctionLength-ReplacedFunctionUpcase */
				  if kupcase(NDCSVC) ne 'MISSING' and  klength(NDCSVC)=11 then do;/*HS 569650 kv - added length statement */
				     IN_2410_LIN03_DR_NDC_C=NDCSVC;
				  end;
				 
/*				    if upcase(NDCUOM) ne "UN" then do;*//* HS 569650 - commented */
					 	IN_2410_CTP05_1=NDCUOM;
/*					End;*/

      
/*********************************************************************************************/
/*                             End of  2410     DRUG IDENTIFICATION                            */
/*********************************************************************************************/      
      
/*********************************************************************************************/
/*                             Start of  2430    LINE ADJUDICATION INFORMATION                */
/*********************************************************************************************/      
                   

             if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1 )  then do; /* 499550 */
					IN_2430_SVD03_1_PROC_ID_Q="  ";
				end;
				/* SAS2AWS2: ReplacedFunctionSubstr */
				else if  kcompress(ksubstr(bilcod,1,2)) in ("21","18","32") and
						 /* SAS2AWS2: ReplacedFunctionCompress */
						 (kcompress(REVCOD) in ("022","023","024","22","23","24") or
						  /* SAS2AWS2: ReplacedFunctionCompress */
						  kcompress(svccod)  in ("022","023","024","22","23","24")) then do;
				    IN_2430_SVD03_1_PROC_ID_Q="HP";
                end;
				else do;							
					IN_2430_SVD03_1_PROC_ID_Q="HC";
				end;
				
				
				IN_2430_SVD03_1_PROC_ID_Q_MCR="  ";
				
				%if &compno.=34  %then %do;
				if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1 )  then do;
					IN_2430_SVD03_1_PROC_ID_Q_MCR="  ";
				end;
				else if  kcompress(ksubstr(bilcod,1,2)) in ("21","18","32") and
						 (kcompress(REVCOD) in ("022","023","024","22","23","24") or
						  kcompress(svccod)  in ("022","023","024","22","23","24")) then do;
				    IN_2430_SVD03_1_PROC_ID_Q_MCR="HP";
                end;
				else do;							
					IN_2430_SVD03_1_PROC_ID_Q_MCR="HC";
				end;
			%end;
			
			%If &compno.=01 and &LOB. =DSNP  %then %do;
				if (typecode = "I") or (typecode ="O" and svctyp_MCR="RV" and missing(CPTCOD_MCR)=1 )  then do;
					IN_2430_SVD03_1_PROC_ID_Q_MCR="  ";
				end;
				else if  kcompress(ksubstr(bilcod,1,2)) in ("21","18","32") and
						 (kcompress(REVCOD_MCR) in ("022","023","024","22","23","24") or
						  kcompress(svccod_MCR)  in ("022","023","024","22","23","24")) then do;
				    IN_2430_SVD03_1_PROC_ID_Q_MCR="HP";
                end;
				else do;							
					IN_2430_SVD03_1_PROC_ID_Q_MCR="HC";
				end;
			%end;
                   
				if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1) then do;
				end;
				else if typecode="O" then do;
							if missing(cptcod)=0 then IN_2430_SVD03_2_PROC_CODE  = cptcod ;
							else IN_2430_SVD03_2_PROC_CODE  = svccod;
				end;

				IN_2430_SVD03_2_PROC_CODE_MCR='                 ';

				%if &compno.=34  %then %do;
				if (typecode = "I") or (typecode ="O" and svctyp="RV" and missing(CPTCOD)=1) then do;
				end;
				else if typecode="O" then do;
							if missing(cptcod)=0 then IN_2430_SVD03_2_PROC_CODE_MCR  = cptcod ;
							else IN_2430_SVD03_2_PROC_CODE_MCR  = svccod;
				End;
			%end;

			%else %If &compno.=01 and &LOB. =DSNP  %then %do;
				if (typecode = "I") or (typecode ="O" and svctyp_MCR="RV" and missing(CPTCOD_MCR)=1) then do;
				end;
				else if typecode="O" then do;
							if missing(cptcod_MCR)=0 then IN_2430_SVD03_2_PROC_CODE_MCR  = cptcod_MCR ;
							else IN_2430_SVD03_2_PROC_CODE_MCR  = svccod_MCR;
				End;
			%end;


             if typecode = "O" then 
            	do; 

					   if BIPRVNPI in (&binpiid.) and IN_2400_SV202_2_SL_HCPCS in (&NDC_Svccod.) then do;
						        IN_2430_SVD03_3_PROC1_MOD               ="UD";
						        IN_2430_SVD03_4_PROC2_MOD               =     CPT_Cod2        ; /* B-96285 QHP had = Mod_Cod2  */
	                  IN_2430_SVD03_5_PROC3_MOD               =     CPT_Cod3        ; /* B-96285 QHP had = Mod_Cod3  */     
	                  IN_2430_SVD03_6_PROC4_MOD               =     CPT_Cod4        ; /* B-96285 QHP had = Mod_Cod4  */  	
						end;
						else do;						  
						 	      IN_2430_SVD03_3_PROC1_MOD               =     CPT_Cod1        ; /* B-96285 QHP had = Mod_Cod1  */ 
		           			IN_2430_SVD03_4_PROC2_MOD               =     CPT_Cod2        ; /* B-96285 QHP had = Mod_Cod2  */     
	                  IN_2430_SVD03_5_PROC3_MOD               =     CPT_Cod3        ; /* B-96285 QHP had = Mod_Cod3  */     
	                  IN_2430_SVD03_6_PROC4_MOD               =     CPT_Cod4        ; /* B-96285 QHP had = Mod_Cod4  */  	
						end;			
	       			
                end;
            else if  typecode = "I" then 
			    do;

					         IN_2430_SVD03_3_PROC1_MOD               =     ""         ;      
	     			       IN_2430_SVD03_4_PROC2_MOD               =     ""         ;      
	                 IN_2430_SVD03_5_PROC3_MOD               =     ""         ;      
	                 IN_2430_SVD03_6_PROC4_MOD               =     ""        ;     
				end;

                   IN_2430_SVD03_3_PROC1_MOD_MCR               =     "  "         ;      
	     			 			 IN_2430_SVD03_4_PROC2_MOD_MCR               =     "        "         ;      
	                 IN_2430_SVD03_5_PROC3_MOD_MCR               =     "        "         ;      
	                 IN_2430_SVD03_6_PROC4_MOD_MCR               =     "        "        ; 

%if &compno.=34  %then %do;
		if typecode = "O" then 
            	do; 
				   			if BIPRVNPI in (&binpiid.) and IN_2400_SV202_2_SL_HCPCS in (&NDC_Svccod.) then do;
						    			IN_2430_SVD03_3_PROC1_MOD_MCR               =     "UD";
						    			IN_2430_SVD03_4_PROC2_MOD_MCR               =     CPT_Cod2        ; 
	                    IN_2430_SVD03_5_PROC3_MOD_MCR               =     CPT_Cod3        ; 
	                    IN_2430_SVD03_6_PROC4_MOD_MCR               =     CPT_Cod4        ; 
									end;
						else do;						  
						 				IN_2430_SVD03_3_PROC1_MOD_MCR               =     CPT_Cod1        ; 
		           			IN_2430_SVD03_4_PROC2_MOD_MCR               =     CPT_Cod2        ; 
	                  IN_2430_SVD03_5_PROC3_MOD_MCR               =     CPT_Cod3        ; 
	                  IN_2430_SVD03_6_PROC4_MOD_MCR               =     CPT_Cod4        ; 
								end;			
	       			
end;
            else if  typecode = "I" then 
			    do;
					 IN_2430_SVD03_3_PROC1_MOD_MCR               =     ""         ;      
	     		 IN_2430_SVD03_4_PROC2_MOD_MCR               =     ""         ;      
	         IN_2430_SVD03_5_PROC3_MOD_MCR               =     ""         ;      
	         IN_2430_SVD03_6_PROC4_MOD_MCR               =     ""        ;     
				end;
%end;

%If &compno.=01 and &LOB. =DSNP  %then %do;;
		if typecode = "O" then 
            	do; 
				   		if BIPRVNPI in (&binpiid.) and IN_2400_SV202_2_SL_HCPCS in (&NDC_Svccod.) then do;
						    			IN_2430_SVD03_3_PROC1_MOD_MCR               =     "UD";
						    			IN_2430_SVD03_4_PROC2_MOD_MCR               =     CPT_Cod_MCR2        ; 
	                    IN_2430_SVD03_5_PROC3_MOD_MCR               =     CPT_Cod_MCR3        ; 
	                    IN_2430_SVD03_6_PROC4_MOD_MCR               =     CPT_Cod_MCR4        ; 
						end;
						else do;						  
						 				IN_2430_SVD03_3_PROC1_MOD_MCR               =     CPT_Cod_MCR1        ; 
		           			IN_2430_SVD03_4_PROC2_MOD_MCR               =     CPT_Cod_MCR2        ; 
	                  IN_2430_SVD03_5_PROC3_MOD_MCR               =     CPT_Cod_MCR3        ; 
	                  IN_2430_SVD03_6_PROC4_MOD_MCR               =     CPT_Cod_MCR4       ; 
						end;			
	       			
		end;
            else if  typecode = "I" then 
			    do;
					 IN_2430_SVD03_3_PROC1_MOD_MCR               =     ""         ;      
	     		 IN_2430_SVD03_4_PROC2_MOD_MCR               =     ""         ;      
	         IN_2430_SVD03_5_PROC3_MOD_MCR               =     ""         ;      
	         IN_2430_SVD03_6_PROC4_MOD_MCR               =     ""        ;     
			end;
%end;



            IN_2430_SVD04_CMP_ID = put(input(revenue_cd,4.),z4.) ;
            
            IN_2430_SVD04_CMP_ID_MCR = '    ' ;
            %if &compno.=34  %then %do;
            IN_2430_SVD04_CMP_ID_MCR = put(input(revenue_cd,4.),z4.) ;
            %end;
            %If &compno.=01 and &LOB. =DSNP  %then %do;
            IN_2430_SVD04_CMP_ID_MCR = put(input(revenue_cd_mcr,4.),z4.) ;
            %end;
					
						/* ALM# 5160 - Commented old CAS mapping logic*/
                  			  /*%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
							  	IN_2430_SGMT_HDR1A='CAS';
								IN_2430_CAS01='OA';                
								IN_2430_CAS02='23';
								IN_2430_CAS04='PR';
								IN_2430_CAS05='2';
								IN_2430_CAS07='PR';
								IN_2430_CAS08='3';
								IN_2430_CAS10='CO';
								IN_2430_CAS11='45';
								IN_2430_CAS13='CO';
								IN_2430_CAS14='96';
								IN_2430_CAS16='OA';
								IN_2430_CAS17='131';
								IN_2430_CAS19='';
								IN_2430_CAS20='';
								IN_2430_CAS21=0;
							  %end;
							  %else %do;	
								IN_2430_SGMT_HDR1A='CAS';
								IN_2430_CAS01='PR';
								IN_2430_CAS02='1';
								IN_2430_CAS04='PR';
								IN_2430_CAS05='2';
								IN_2430_CAS07='PR';
								IN_2430_CAS08='3';
								IN_2430_CAS10='CO';
								IN_2430_CAS11='45';
								IN_2430_CAS13='CO';
								IN_2430_CAS14='96';
								IN_2430_CAS16='OA';
								IN_2430_CAS17='131';
								IN_2430_CAS19='';
								IN_2430_CAS20='';
								IN_2430_CAS21=0;
							 %end;*/
							/* ALM# 5160 - Added new CAS mapping logic*/
							IN_2430_SGMT_HDR1A='CAS';

							/*B-120951 - Assign default value*/
							IN_2430_CAS01=' ';
							IN_2430_CAS02=' ';
							IN_2430_CAS03=.;
							IN_2430_CAS04=.;
							IN_2430_CAS05=' ';
							IN_2430_CAS06=' ';
							IN_2430_CAS07=.;
							IN_2430_CAS08=.;
							IN_2430_CAS09=' ';
							IN_2430_CAS10=' ';
							IN_2430_CAS11=.;
							IN_2430_CAS12=.;
							IN_2430_CAS13=' ';
							IN_2430_CAS14=' ';
							IN_2430_CAS15=.;
							IN_2430_CAS16=.;
							IN_2430_CAS17=' ';
							IN_2430_CAS18=' ';
							IN_2430_CAS19=.;
							IN_2430_CAS20=.;
							IN_2430_CAS21=' ';
							
							
							IN_2430_SGMT_HDR1A_MCR='CAS';
							IN_2430_CAS01_MCR=' ';
							IN_2430_CAS02_MCR=' ';
							IN_2430_CAS03_MCR=.;
							IN_2430_CAS04_MCR=.;
							IN_2430_CAS05_MCR=' ';
							IN_2430_CAS06_MCR=' ';
							IN_2430_CAS07_MCR=.;
							IN_2430_CAS08_MCR=.;
							IN_2430_CAS09_MCR=' ';
							IN_2430_CAS10_MCR=' ';
							IN_2430_CAS11_MCR=.;
							IN_2430_CAS12_MCR=.;
							IN_2430_CAS13_MCR=' ';
							IN_2430_CAS14_MCR=' ';
							IN_2430_CAS15_MCR=.;
							IN_2430_CAS16_MCR=.;
							IN_2430_CAS17_MCR=' ';
							IN_2430_CAS18_MCR=' ';
							IN_2430_CAS19_MCR=.;
							IN_2430_CAS20_MCR=.;
							IN_2430_CAS21_MCR=' ';

/*B-120951 Commented the old CAS logic and Include the new CAS logic */
							%If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
/*								if capind = 'F' then*/
/*									do;*/
/*											IN_2430_CAS01='OA';*/
/*											IN_2430_CAS02='45';*/
/*								end;*/
/*								else if capind = 'C' then*/
/*									do;*/
/*											IN_2430_CAS01='CO';*/
/*											IN_2430_CAS02='24';*/	
/*								end;*/				
/*										  IN_2430_CAS05='OA';*/
/*											IN_2430_CAS06='45';*/
/*											IN_2430_CAS09=' ';*/
/*											IN_2430_CAS10=' ';*/
/*											IN_2430_CAS11=.;*/
/*											IN_2430_CAS12=.;*/
/*											IN_2430_CAS13=' ';*/
/*											IN_2430_CAS14=' ';*/
/*											IN_2430_CAS15=.;*/
/*											IN_2430_CAS16=.;*/
/*											IN_2430_CAS17=' ';*/
/*											IN_2430_CAS18=' ';*/
/*											IN_2430_CAS19=.;*/
/*											IN_2430_CAS20=.;*/
/*											IN_2430_CAS21=' ';*/
											/* VVKR Code Start -- B-75041 */
/*											if cas_flag=1 then do;*/
/*												IN_2430_CAS09='CO';*/
/*												IN_2430_CAS10=CARC;*/
/*												IN_2430_CAS11=0;*/
/*												IN_2430_CAS12=0;*/
/*											end;*/
											/* VVKR Code End -- B-75041 */	

								if capind = 'F' and (IN_2430_CAS03_Var > 0 or (IN_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											/*IN_2430_CAS01='CO'; *//* B-121242 */
											/*IN_2430_CAS02='45';*/											
											if cas_flag<>1 then do; IN_2430_CAS01='CO'; IN_2430_CAS02='45'; end;
											if cas_flag=1 then do; IN_2430_CAS01='CO'; IN_2430_CAS02=CARC; end;											
											IN_2430_CAS03=IN_2430_CAS03_Var;
											if IN_2430_CAS04_Var < 0 then IN_2430_CAS04=0;else IN_2430_CAS04=IN_2430_CAS04_Var;
									end;

								else if capind = 'C' and (IN_2430_CAS03_Var > 0 or (IN_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											IN_2430_CAS01='CO';
											IN_2430_CAS02='24';	
											IN_2430_CAS03=IN_2430_CAS03_Var;
											if IN_2430_CAS04_Var < 0 then IN_2430_CAS04=0;else IN_2430_CAS04=IN_2430_CAS04_Var;
									end;

								/*[B-272655]_[APD Compcare CAS Segment Capitated Encounters Update]  CAS05 TO CAS08 Added*/	
								if capind = 'C' and (IN_2430_CAS07_Var > 0 or (IN_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											if cas_flag<>1 then do; IN_2430_CAS05='CO';IN_2430_CAS06='45'; END;
											if cas_flag=1 then do; IN_2430_CAS05='CO'; IN_2430_CAS06=CARC; end;	
											IN_2430_CAS07=IN_2430_CAS07_Var;
											if IN_2430_CAS08_Var < 0 then IN_2430_CAS08=0; else IN_2430_CAS08=IN_2430_CAS08_Var;
									end;


								/*if capind = 'F' and (IN_2430_CAS07_Var > 0 or (IN_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											if cas_flag<>1 then do; IN_2430_CAS05='CO';*/ /* B-121242 *//* IN_2430_CAS06='45'; end;
											if cas_flag=1 then do; IN_2430_CAS05='CO'; IN_2430_CAS06=CARC; end;		
											IN_2430_CAS07=IN_2430_CAS07_Var;
											if IN_2430_CAS08_Var < 0 then IN_2430_CAS08=0; else IN_2430_CAS08=IN_2430_CAS08_Var;
									end;

								else if capind = 'C' and (IN_2430_CAS07_Var > 0 or (IN_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											IN_2430_CAS05='CO'; *//* B-121242 */ 
											/*IN_2430_CAS06='45'; 	
											IN_2430_CAS07=IN_2430_CAS07_Var;
											if IN_2430_CAS08_Var < 0 then IN_2430_CAS08=0; else IN_2430_CAS08=IN_2430_CAS08_Var;
									end;*/


								if capind = 'F' and (IN_2430_CAS03_MCR_Var > 0 or (IN_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											if cas_flag<>1 then do; IN_2430_CAS01_MCR='CO'; IN_2430_CAS02_MCR='45'; end;
											if cas_flag=1 then do; IN_2430_CAS01_MCR='CO'; IN_2430_CAS02_MCR=CARC; end;	
											IN_2430_CAS03_MCR=IN_2430_CAS03_MCR_Var;
											if IN_2430_CAS04_MCR_Var < 0 then IN_2430_CAS04_MCR=0;else IN_2430_CAS04_MCR=IN_2430_CAS04_MCR_Var;
									end;

								else if capind = 'C' and (IN_2430_CAS03_MCR_Var > 0 or (IN_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											IN_2430_CAS01_MCR='CO';
											IN_2430_CAS02_MCR='24';	
											IN_2430_CAS03_MCR=IN_2430_CAS03_MCR_Var;
											if IN_2430_CAS04_MCR_Var < 0 then IN_2430_CAS04_MCR=0;else IN_2430_CAS04_MCR=IN_2430_CAS04_MCR_Var;
									end;
									
									/*[B-272655]_[APD Compcare CAS Segment Capitated Encounters Update]  CAS05 TO CAS08 Added*/
									if capind = 'C' and (IN_2430_CAS07_MCR_Var > 0 or (IN_2430_CAS07_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											if cas_flag<>1 then do; IN_2430_CAS05_MCR='CO'; IN_2430_CAS06_MCR='45'; end;
											if cas_flag=1 then do; IN_2430_CAS05_MCR='CO'; IN_2430_CAS06_MCR=CARC; end;	
											IN_2430_CAS07_MCR=IN_2430_CAS07_MCR_Var;
											if IN_2430_CAS08_MCR_Var < 0 then IN_2430_CAS08_MCR=0;else IN_2430_CAS08_MCR=IN_2430_CAS08_MCR_Var;
									end;

							%end;
							%else %If &compno.=01 and &LOB. =DSNP  %then %do;
								if capind = 'F' and (IN_2430_CAS03_Var > 0 or (IN_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;											
											if cas_flag<>1 then do; IN_2430_CAS01='CO'; IN_2430_CAS02='45'; end;
											if cas_flag=1 then do; IN_2430_CAS01='CO'; IN_2430_CAS02=CARC; end;											
											IN_2430_CAS03=IN_2430_CAS03_Var;
											if IN_2430_CAS04_Var < 0 then IN_2430_CAS04=0;else IN_2430_CAS04=IN_2430_CAS04_Var;
									end;

								else if capind = 'C' and (IN_2430_CAS03_Var > 0 or (IN_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											IN_2430_CAS01='CO';
											IN_2430_CAS02='24';	
											IN_2430_CAS03=IN_2430_CAS03_Var;
											if IN_2430_CAS04_Var < 0 then IN_2430_CAS04=0;else IN_2430_CAS04=IN_2430_CAS04_Var;
									end;

								if capind = 'C' and (IN_2430_CAS07_Var > 0 or (IN_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											if cas_flag<>1 then do; IN_2430_CAS05='CO';IN_2430_CAS06='45'; END;
											if cas_flag=1 then do; IN_2430_CAS05='CO'; IN_2430_CAS06=CARC; end;	
											IN_2430_CAS07=IN_2430_CAS07_Var;
											if IN_2430_CAS08_Var < 0 then IN_2430_CAS08=0; else IN_2430_CAS08=IN_2430_CAS08_Var;
									end;

								if capind_MCR = 'F' and (IN_2430_CAS03_MCR_Var > 0 or (IN_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											if cas_flag<>1 then do; IN_2430_CAS01_MCR='CO'; IN_2430_CAS02_MCR='45'; end;
											if cas_flag=1 then do; IN_2430_CAS01_MCR='CO'; IN_2430_CAS02_MCR=CARC; end;	
											IN_2430_CAS03_MCR=IN_2430_CAS03_MCR_Var;
											if IN_2430_CAS04_MCR_Var < 0 then IN_2430_CAS04_MCR=0;else IN_2430_CAS04_MCR=IN_2430_CAS04_MCR_Var;
									end;

								else if capind_MCR = 'C' and (IN_2430_CAS03_MCR_Var > 0 or (IN_2430_CAS03_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											IN_2430_CAS01_MCR='CO';
											IN_2430_CAS02_MCR='24';	
											IN_2430_CAS03_MCR=IN_2430_CAS03_MCR_Var;
											if IN_2430_CAS04_MCR_Var < 0 then IN_2430_CAS04_MCR=0;else IN_2430_CAS04_MCR=IN_2430_CAS04_MCR_Var;
									end;
									
									if capind_MCR = 'C' and (IN_2430_CAS07_MCR_Var > 0 or (IN_2430_CAS07_MCR_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;
											if cas_flag<>1 then do; IN_2430_CAS05_MCR='CO'; IN_2430_CAS06_MCR='45'; end;
											if cas_flag=1 then do; IN_2430_CAS05_MCR='CO'; IN_2430_CAS06_MCR=CARC; end;	
											IN_2430_CAS07_MCR=IN_2430_CAS07_MCR_Var;
											if IN_2430_CAS08_MCR_Var < 0 then IN_2430_CAS08_MCR=0;else IN_2430_CAS08_MCR=IN_2430_CAS08_MCR_Var;
									end;

							%end;							
							%else %do;
/*								if capind = 'F' then*/
/*									do;*/				
/*											IN_2430_CAS01='OA';*/
/*											IN_2430_CAS02='45';*/
/*										   	IN_2430_CAS05=' ';*/
/*											IN_2430_CAS06=' ';*/										
/*								end;*/
/*								else if capind = 'C' and typecode in ('O') then*/
/*									do;*/
/*											IN_2430_CAS01='CO';*/
/*											IN_2430_CAS02='24';*/
/*											IN_2430_CAS05='OA';*/
/*											IN_2430_CAS06='45';*/
/*								end;*/

/*								else if capind = 'C' and typecode in ('I') then*/
/*								do;*/

/*											IN_2430_CAS01='OA';*/
/*											IN_2430_CAS02='45';*/	
/*											IN_2430_CAS05=' ';*/
/*											IN_2430_CAS06=' ';*/
/*								end;*/		
/*											IN_2430_CAS09=' ';*/
/*											IN_2430_CAS10=' ';*/
/*											IN_2430_CAS11=.;*/		/* B-96285 QHP had =' '  */
/*											IN_2430_CAS12=.;*/		/* B-96285 QHP had =' '  */
/*											IN_2430_CAS13=' ';*/
/*											IN_2430_CAS14=' ';*/
/*											IN_2430_CAS15=.;*/		/* B-96285 QHP had =' '  */
/*											IN_2430_CAS16=.;*/		/* B-96285 QHP had =' '  */
/*											IN_2430_CAS17=' ';*/
/*											IN_2430_CAS18=' ';*/
/*											IN_2430_CAS19=.;*/		/* B-96285 QHP had =' '  */
/*											IN_2430_CAS20=.;*/		/* B-96285 QHP had =' '  */
/*											IN_2430_CAS21=' ';*/
											/* VVKR Code Start -- B-75041 */
/*											if cas_flag=1 then do;*/
/*												IN_2430_CAS09='CO';*/
/*												IN_2430_CAS10=CARC;*/
/*												IN_2430_CAS11=0;*/	/* B-96285 QHP had ='0'  */
/*												IN_2430_CAS12=0;*/	/* B-96285 QHP had ='0'  */
/*											end;*/
											/* VVKR Code End -- B-75041 */

								if capind = 'F' and (IN_2430_CAS03_Var > 0 or (IN_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y")) then
									do;				
											if cas_flag<>1 then do; IN_2430_CAS01='CO'; /* B-121242 */ IN_2430_CAS02='45'; end;
											if cas_flag=1 then do; IN_2430_CAS01='CO'; IN_2430_CAS02=CARC; end;	
										   	IN_2430_CAS03=IN_2430_CAS03_Var;
											if IN_2430_CAS04_Var < 0 then IN_2430_CAS04=0;else IN_2430_CAS04=IN_2430_CAS04_Var;											
									end;

								else if capind = 'C' and (IN_2430_CAS03_Var > 0 or (IN_2430_CAS03_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
											if typecode = 'O' then do; IN_2430_CAS01='CO';	IN_2430_CAS02='24';	end;
											if typecode = 'I' then do; IN_2430_CAS01='CO'; /* B-121242 */ IN_2430_CAS02='45';	end;
											IN_2430_CAS03=IN_2430_CAS03_Var;
											if IN_2430_CAS04_Var < 0 then IN_2430_CAS04=0;else IN_2430_CAS04=IN_2430_CAS04_Var;		
									end;	

								if capind = 'C' and typecode = 'O' and (IN_2430_CAS07_Var > 0 or (IN_2430_CAS07_Var < 0 and "&CAS_Negative_chk."="Y"))  then
									do;
											IN_2430_CAS05='CO'; /* B-121242 */
											IN_2430_CAS06='45';	
											IN_2430_CAS07=IN_2430_CAS07_Var;
											if IN_2430_CAS08_Var < 0 then IN_2430_CAS08=0; else IN_2430_CAS08=IN_2430_CAS08_Var;											
									end;
							%end;

						

            IN_2430_DTP03_SLADJ_DTE                 =     put(pidate,yymmddn8.)  ;	/*[B-315447] - DS - Updates*/
            

			%If &compno.=30 or &compno.=45 %then %do;	/* B-146417 added hiosid from elgibility outbound */
				IN_2430_SVD01 =HIOSID;
					%if ("&hiosflg." = "Y" and (&compno.=45 and "&LOB." ne "MPPO")) %then %do; /* B-274606 */
						IN_2430_SVD01 = hios_plan_id_cd;
					%end;				
			%end;
			%else %do;
				IN_2430_SVD01 ="&HIOSID.";
					%if (&compno.=01 and &LOB. =DSNP) %then %do; 
						IN_2430_SVD01 ="&HIOSID.";
					%end;
					%else %if ("&hiosflg." = "Y" and (&compno.=01 or &compno.=20 or &compno.=42)) %then %do; /* B-274606 */
						IN_2430_SVD01 = hix_policy_num_cd;
					%end;
			%end;  
			
			%If &compno.=34 %then %do;
				IN_2430_SGMT_HDR1_MCR='SVD';
				IN_2430_SVD01_MCR ="N95284]H3359]034";
				IN_2430_SGMT_HDR2_MCR='DTP';
				IN_2430_DTP01_MCR='573';
				IN_2430_DTP02_MCR='D8';
				IN_2430_DTP03_SLADJ_DTE_MCR=put(pidate,yymmddn8.)  ;	/*[B-315447] - DS - Updates*/
			%end;
			%else
			%If &compno.=01 and &LOB. =DSNP  %then %do;
				IN_2430_SGMT_HDR1_MCR='SVD';
				IN_2430_SVD01_MCR ="N95284]H3359]038";
				IN_2430_SGMT_HDR2_MCR='DTP';
				IN_2430_DTP01_MCR='573';
				IN_2430_DTP02_MCR='D8';
				IN_2430_DTP03_SLADJ_DTE_MCR=put(pidate_MCR,yymmddn8.)  ;	/*[B-315447] - DS - Updates*/
			%end;				
			%else %do;
				IN_2430_SGMT_HDR1_MCR='   ';
				IN_2430_SVD01_MCR ='                 ';
				IN_2430_SGMT_HDR2_MCR='   ';
				IN_2430_DTP01_MCR='   ';
				IN_2430_DTP02_MCR='   ';
				IN_2430_DTP03_SLADJ_DTE_MCR='         ';
			%end; 
			

/*********************************************************************************************/
/*                             End of  2430    LINE ADJUDICATION INFORMATION                 */
/*********************************************************************************************/      

/*********************************************************************************************/
/*          Start Recalculating     : Institutional                                                                     */
/*                            1. Dollar amounts                                                                               */
/*                            2. Service Line Number                                                                           */
/*********************************************************************************************/
         if first.clamno then
                do;
                  IN_2300_CLM02_TOTCHG                = 0; 
				  IN_2320_AMT02_COB_PRIPAY_AMT_MCD    = 0;
				  IN_2320_AMT02_COB_PRIPAY_AMT_MCR    = 0;
				  IN_2320_AMT02_COB_PRIPAY_AMT    	  = 0;	/* B-96285 QHP, HFIC */
                  IN_2400_LX01_LINE_NUM               = 0;
                end;

                  IN_2300_CLM02_TOTCHG      +     IN_2400_SV203_SL_CHG_AMT      ; 
                  IN_2320_AMT02_COB_PRIPAY_AMT_MCD +  IN_2430_SVD02_SL_PAID_AM;				  
				  IN_2320_AMT02_COB_PRIPAY_AMT 	   +  IN_2430_SVD02_SL_PAID_AM; /* B-96285 QHP, HFIC */

				  %If &compno.=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do;	/* B-145499 */
				  	IN_2320_AMT02_COB_PRIPAY_AMT_MCR + IN_2430_SVD02_SL_PAID_AM;
					IN_2320_AMT02_COB_PRIPAY_AMT_MCD = 0;
				  %end;
				  %else
				  %If &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
				  	/*IN_2320_AMT02_COB_PRIPAY_AMT_MCR + IN_2430_CAS03;*/
					IN_2320_AMT02_COB_PRIPAY_AMT_MCR + IN_2430_SVD02_SL_PAID_AM_MCR;/*B-247073*/
				  %end;
				  %else
				  %If &compno.=01 and &LOB. =DSNP  %then %do;
				  	IN_2320_AMT02_COB_PRIPAY_AMT_MCR + IN_2430_SVD02_SL_PAID_AM_MCR;
				  %end;				  
				  %else %do;
				  	IN_2320_AMT02_COB_PRIPAY_AMT_MCR=0;
				  %end;

                  IN_2400_LX01_LINE_NUM  + 1 ; 


 /*********************************************************************************************/
/*          End   Recalculating     : Institutional                                                                     */
/*                            1. Dollar amounts                                                                               */
/*                            2. Service Line Number                                                                           */
/*********************************************************************************************/
/*********************************************************************************************/
/*                Start   -  Mapping for Hard coded values                                    */
/*********************************************************************************************/ 
      %include ctrlout ; 
/*********************************************************************************************/
/*                End   -  Mapping for Hard coded values                                      */
/*********************************************************************************************/

            if last.clamno then 
                  do;
                  inst_clm_count_&Edit_Ind.     + 1 ;
            
                  if typecode = "I" then do;
                     inst_I_clm_count_&Edit_Ind.      + 1;
                  end ;

                  if typecode = "O" then do;
                     inst_O_clm_count_&Edit_Ind.      + 1;
                  end ;

                  output &RGA191..inst_clm_&Edit_Ind._&compno._&run_id. ;
                  end;

                  inst_svc_count_&Edit_Ind. + 1 ;
            
                  if typecode = "I" then do;
                     inst_I_svc_count_&Edit_Ind.      + 1;
                  end ;

                  if typecode = "O" then do;
                     inst_O_svc_count_&Edit_Ind. + 1 ;
                  end ;

                  output &RGA191..inst_svc_&Edit_Ind._&compno._&run_id.; 


        end;

      else 
      output &RGA191..Bad_data_&compno._&run_id.;

            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("prof_clm_count_&Edit_Ind.",kcompress(prof_clm_count_&Edit_Ind.));
            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("prof_svc_count_&Edit_Ind.",kcompress(prof_svc_count_&Edit_Ind.));
			
            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("inst_clm_count_&Edit_Ind.",kcompress(inst_clm_count_&Edit_Ind.));
            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("inst_svc_count_&Edit_Ind.",kcompress(inst_svc_count_&Edit_Ind.));

            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("inst_I_clm_count_&Edit_Ind.",kcompress(inst_I_clm_count_&Edit_Ind.));
            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("inst_O_clm_count_&Edit_Ind.",kcompress(inst_O_clm_count_&Edit_Ind.));
            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("inst_I_svc_count_&Edit_Ind.",kcompress(inst_I_svc_count_&Edit_Ind.));
            /* SAS2AWS2: ReplacedFunctionCompress */
            call symput("inst_O_svc_count_&Edit_Ind.",kcompress(inst_O_svc_count_&Edit_Ind.));

Run;

/**********************************************************************************************/
/*                                                    END of  INSTITUITIONAL SEGMENT                                          */
/**********************************************************************************************/
%mend APD_mapping;


%macro Diag_proc(dsn);

	Data DV_&dsn. DIn_&dsn.(drop=diagvind rename=(procvind=procvind_temp)); 
		set &RGA191..&dsn.;
		if diagvind in ('C') then output DIn_&dsn. ;
		else output DV_&dsn.;
	Run;
Data DV_&dsn.;
	Set DV_&dsn.;
	NewDiag1_version=diagvind; 		NewDiag2_version=diagvind; 		NewDiag3_version=diagvind;
	NewDiag4_version=diagvind; 		NewDiag5_version=diagvind; 		NewDiag6_version=diagvind;
	NewDiag7_version=diagvind; 		NewDiag8_version=diagvind; 		NewDiag9_version=diagvind;
	NewDiag10_version=diagvind; 	NewDiag11_version=diagvind;		NewDiag12_version=diagvind;
	NewDiag13_version=diagvind;     NewDiag14_version=diagvind;		NewDiag15_version=diagvind;
	NewDiag16_version=diagvind; 	NewDiag17_version=diagvind;     NewDiag18_version=diagvind;
	NewDiag19_version=diagvind;		NewDiag20_version=diagvind; 	NewDiag21_version=diagvind;
	NewDiag22_version=diagvind;		NewDiag23_version=diagvind;		NewDiag24_version=diagvind;
	NewDiag25_version=diagvind;     ADMTDIAG_version=diagvind; 
run;
Proc sql noprint;
            select count(*) into :count from DIn_&dsn.;
      quit;
%if &count. ne 0 %then %do;
	%macro Diag_version(value);
			%checkicd(dsn=DIn_&dsn.,DiagAllFields=&value.)
			Data DIn_&dsn.;
			set DIn_&dsn.;
			rename diagvind=&value._version;
			run;
	%mend;
	%diag_version(NewDiag1);%diag_version(NewDiag2);%diag_version(NewDiag3);%diag_version(NewDiag4);
	%diag_version(NewDiag5);%diag_version(NewDiag6);%diag_version(NewDiag7);%diag_version(NewDiag8);
	%diag_version(NewDiag9);%diag_version(NewDiag10);%diag_version(NewDiag11);%diag_version(NewDiag12);
	%diag_version(NewDiag13);%diag_version(NewDiag14);%diag_version(NewDiag15);%diag_version(NewDiag16);
	%diag_version(NewDiag17);%diag_version(NewDiag18);%diag_version(NewDiag19);%diag_version(NewDiag20);
	%diag_version(NewDiag21);%diag_version(NewDiag22);%diag_version(NewDiag23);%diag_version(NewDiag24);
	%diag_version(NewDiag25); %diag_version(ADMTDIAG); 
	Data &RGA191..&dsn.;
	set DV_&dsn. DIn_&dsn.(rename=(procvind_temp=procvind));
	run;
%end;
%else %do;
	Data &RGA191..&dsn.;
	set DV_&dsn. ;
	run;
%end;

Data PV_&dsn. PIN_&dsn.(drop=procvind);
set &RGA191..&dsn. ;
	if procvind in ('C') then output PIN_&dsn. ;
	else output PV_&dsn.;
run;
Data PV_&dsn.;
set PV_&dsn.;
Proc_Cod1_version=procvind;
Proc_Cod2_version=procvind;
Proc_Cod3_version=procvind;
Proc_Cod4_version=procvind;
Proc_Cod5_version=procvind;
Proc_Cod6_version=procvind;
/* B-105917 Start */
Proc_Cod7_version=procvind;  Proc_Cod8_version=procvind;  Proc_Cod9_version=procvind;  Proc_Cod10_version=procvind;
Proc_Cod11_version=procvind; Proc_Cod12_version=procvind; Proc_Cod13_version=procvind; Proc_Cod14_version=procvind;
Proc_Cod15_version=procvind; Proc_Cod16_version=procvind; Proc_Cod17_version=procvind; Proc_Cod18_version=procvind;
Proc_Cod19_version=procvind; Proc_Cod20_version=procvind; Proc_Cod21_version=procvind; Proc_Cod22_version=procvind;
Proc_Cod23_version=procvind; Proc_Cod24_version=procvind; Proc_Cod25_version=procvind; 
/* B-105917 End */
run;

Proc sql noprint;
            select count(*) into :count from PIN_&dsn.;
      quit;
%if &count. ne 0 %then %do;
%macro Proc_version(value);
%checkicd(dsn=PIN_&dsn.,ProcAllFields=&value.);
Data PIN_&dsn.;
	set PIN_&dsn.;
	rename procvind=&value._version;
run;
%mend;
%Proc_version(Proc_Cod1);%Proc_version(Proc_Cod2);%Proc_version(Proc_Cod3);
%Proc_version(Proc_Cod4);%Proc_version(Proc_Cod5);%Proc_version(Proc_Cod6);
/* B-105917 Start */
%Proc_version(Proc_Cod7);%Proc_version(Proc_Cod8);%Proc_version(Proc_Cod9);
%Proc_version(Proc_Cod10);%Proc_version(Proc_Cod11);%Proc_version(Proc_Cod12);
%Proc_version(Proc_Cod13);%Proc_version(Proc_Cod14);%Proc_version(Proc_Cod15);
%Proc_version(Proc_Cod16);%Proc_version(Proc_Cod17);%Proc_version(Proc_Cod18);
%Proc_version(Proc_Cod19);%Proc_version(Proc_Cod20);%Proc_version(Proc_Cod21);
%Proc_version(Proc_Cod22);%Proc_version(Proc_Cod23);%Proc_version(Proc_Cod24);%Proc_version(Proc_Cod25);
/* B-105917 End */
Data &RGA191..&dsn.;
set PV_&dsn. PIN_&dsn.;
Run;
%end;
%else %do;
Data &RGA191..&dsn.;
set PV_&dsn.;
Run;
%end;
%mend;
%Diag_proc(EditPassClmLine2_&compno._&run_id.);

data _null_;
cmd=" aws s3 cp &drv72./Encounters_Reporting/APD/Control_Table/ICD10_Beg_Date.csv &drv2./Encounters_Reporting/APD/Control_Table/  --sse --acl bucket-owner-full-control";
call system(cmd);
run; 

/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Filename Control "&drv2./Encounters_Reporting/APD/Control_Table/ICD10_Beg_Date.csv";
data Ctrl;
	infile Control recfm=v dlm=',' Firstobs = 2 missover; 
	length STARTDT $8;			
		input startdt;
		/* SAS2AWS2: ReplacedFunctionTrim */
		startdt1 = input(ktrim(startdt),YYMMDD10.);
		call symput('ICD10_startdat', startdt1);
		

		/* SAS2AWS2: AddedKcvtFunction */
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;

run;

%macro indicator(dsn);
Data &RGA191..&dsn.;
set &RGA191..&dsn.;
%do i=1 %to 25 ;
	%if &i.=1 %then %do;	
		  if missing(NewDiag&i.)=0 and NewDiag&i._version in ('0') then 			
		     NewDiag&i._Q="ABK";
		  else if missing(NewDiag&i.)=0 and NewDiag&i._version in ('9') then	
		   	 NewDiag&i._Q="BK";		 
	%end;
	%if &i. ne 1 %then %do;	
		if missing(NewDiag&i.)=0 and NewDiag&i._version in ('0') then 			
		     NewDiag&i._Q="ABF";
		  else if missing(NewDiag&i.)=0 and NewDiag&i._version in ('9') then	
		  	NewDiag&i._Q="BF";		   
	%end;

%end;

%do j=1 %to 25 ;	/* B-105917 was 6 */
	%if &j.=1 %then %do;	
	if missing(Proc_Cod1)=0 and Proc_Cod1_version in ('0') then 			
	     Proc_Cod1_Q="BBR";
	  else if missing(Proc_Cod1)=0 and Proc_Cod1_version in ('9') then	
	  	Proc_Cod1_Q="BR";
	    
	%end;
	%if &j. ne 1 %then %do;	
			if missing(Proc_Cod&j.)=0 and Proc_Cod&j._version in ('0') then 			
	            Proc_Cod&j._Q="BBQ";
	        else if missing(Proc_Cod&j.)=0 and Proc_Cod&j._version in ('9') then	
	        	Proc_Cod&j._Q="BQ";	    	  
	%end;

%end;

		if missing(ADMTDIAG)=0 and ADMTDIAG_version in ('0') then 			
		     admdiag_Q="ABJ";
		  else if missing(ADMTDIAG)=0 and ADMTDIAG_version in ('9') then	
		   	 admdiag_Q="BJ";		 
run;
%mend;
%indicator(EditPassClmLine2_&compno._&run_id.);

/*CR#CHG0039138 - Added logic to capture procedure code qualifier missing claims into internal rejection if procedure code is not missing --> start*/
Data &RGA191..EditPassClmLine2_&compno._&run_id.
proc_qual_reject (keep = clamno lineno edit_chk_type encapdfld edit_code Recsubind typecode)
diag_qual_reject (keep = clamno lineno edit_chk_type encapdfld edit_code Recsubind typecode); *B-154145 added diag_qual_reject;
length edit_chk_type $100. edit_code $5. encapdfld $50.;
set &RGA191..EditPassClmLine2_&compno._&run_id.;
if ((proc_cod1 ne '' and Proc_Cod1_Q eq '') or (proc_cod2 ne '' and Proc_Cod2_Q eq '') or
(proc_cod3 ne '' and Proc_Cod3_Q eq '') or (proc_cod4 ne '' and Proc_Cod4_Q eq '') or
(proc_cod5 ne '' and Proc_Cod5_Q eq '') or (proc_cod6 ne '' and Proc_Cod6_Q eq '')
/* B-105917  Start */ 
or (proc_cod7 ne '' and Proc_Cod7_Q eq '') or (proc_cod8 ne '' and Proc_Cod8_Q eq '') 
or (proc_cod9 ne '' and Proc_Cod9_Q eq '') or (proc_cod10 ne '' and Proc_Cod10_Q eq '')
or (proc_cod11 ne '' and Proc_Cod11_Q eq '') or (proc_cod12 ne '' and Proc_Cod12_Q eq '')
or (proc_cod13 ne '' and Proc_Cod13_Q eq '') or (proc_cod14 ne '' and Proc_Cod14_Q eq '')
or (proc_cod15 ne '' and Proc_Cod15_Q eq '') or (proc_cod16 ne '' and Proc_Cod16_Q eq '')
or (proc_cod17 ne '' and Proc_Cod17_Q eq '') or (proc_cod18 ne '' and Proc_Cod18_Q eq '')
or (proc_cod19 ne '' and Proc_Cod19_Q eq '') or (proc_cod20 ne '' and Proc_Cod20_Q eq '')
or (proc_cod21 ne '' and Proc_Cod21_Q eq '') or (proc_cod22 ne '' and Proc_Cod22_Q eq '')
or (proc_cod23 ne '' and Proc_Cod23_Q eq '') or (proc_cod24 ne '' and Proc_Cod24_Q eq '')
or (proc_cod25 ne '' and Proc_Cod25_Q eq '')) &proc_qual_Reject_final.	/* B-168796 */
/* B-105917  End */ 
then do; 
		edit_chk_type='Procedure code qualifier Reject';
		encapdfld='proc_qual_Reject';
		Recsubind='N';
		edit_code = '99951';
		output proc_qual_reject;
		delete;
end;	

if ((NewDiag1 ne '' and NewDiag1_Q eq '') or (NewDiag2 ne '' and NewDiag2_Q eq '') or
(NewDiag3 ne '' and NewDiag3_Q eq '') or (NewDiag4 ne '' and NewDiag4_Q eq '') or 
(NewDiag5 ne '' and NewDiag5_Q eq '') or (NewDiag6 ne '' and NewDiag6_Q eq '') or 
(NewDiag7 ne '' and NewDiag7_Q eq '') or (NewDiag8 ne '' and NewDiag8_Q eq '') or 
(NewDiag9 ne '' and NewDiag9_Q eq '') or (NewDiag10 ne '' and NewDiag10_Q eq '') or 
(NewDiag11 ne '' and NewDiag11_Q eq '') or (NewDiag12 ne '' and NewDiag12_Q eq '') or
(NewDiag13 ne '' and NewDiag13_Q eq '') or (NewDiag14 ne '' and NewDiag14_Q eq '') or 
(NewDiag15 ne '' and NewDiag15_Q eq '') or (NewDiag16 ne '' and NewDiag16_Q eq '') or 
(NewDiag17 ne '' and NewDiag17_Q eq '') or (NewDiag18 ne '' and NewDiag18_Q eq '') or 
(NewDiag19 ne '' and NewDiag19_Q eq '') or (NewDiag20 ne '' and NewDiag20_Q eq '') or 
(NewDiag21 ne '' and NewDiag21_Q eq '') or (NewDiag22 ne '' and NewDiag22_Q eq '') or
(NewDiag23 ne '' and NewDiag23_Q eq '') or (NewDiag24 ne '' and NewDiag24_Q eq '') or 
(NewDiag25 ne '' and NewDiag25_Q eq '') or (ADMTDIAG ne '' and admdiag_Q eq '')) &diag_qual_Reject_final. /* B-168796 */
then do; 
		edit_chk_type='diagnosis code qualifier Reject';
		encapdfld='diag_qual_Reject';
		Recsubind='N';
		edit_code = '99941';		*B-154145 added reject;
		output diag_qual_reject;
		delete;
end;
else output  &RGA191..EditPassClmLine2_&compno._&run_id.;
run;	

data proc_qual_reject(rename=(clamno1=clamno)); 
length clamno1 $20. ;
set proc_qual_reject;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;

data diag_qual_reject(rename=(clamno1=clamno)); *B-154145 added reject;
length clamno1 $20. ;
set diag_qual_reject;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;

data &RGA191..EditFailClmLine_&Compno._&run_id.;
set &RGA191..EditFailClmLine_&Compno._&run_id. proc_qual_reject diag_qual_reject; *B-154145 added reject;
run;
proc sql;
		select  count(*) into :total_obs_edit  from &RGA191..EditFailClmLine_&Compno._&run_id. ;
    Quit;
	proc sql;
		select  count(*) into :total_obs_fin  from &RGA191..EditPassClmLine2_&compno._&run_id. ;
    Quit;
%report;
Proc delete data=&RGA191..EditPassClmLine_&compno._&run_id.;run;
/*CR#CHG0039138 - Added logic to capture procedure code qualifier missing claims into internal rejection if procedure code is not missing --> end */
/**********************************************************************************************/
/*                                  Start -Medicare Mapping - Edit Matrix Passed Records           */
/**********************************************************************************************/
%APD_mapping(EditPassClmLine2_&compno._&run_id.,P);
/**********************************************************************************************/
/*                                  End -Medicare Mapping - Edit Matrix Passed Records             */
/**********************************************************************************************/
      
/*********************************************************************************************/
/*             Final File                                                                    */
/********************************************************************************************/
/********************************************************************************************/
/*                                                                                          */
/*               Write out Professional   and DME                                           */
/*                                                                                          */
/********************************************************************************************/
*Rsubmit;
Data temp_job; 
		set &R42173..medical_job_mstr
(drop=output_dataset_name output_file_name sequence_number typecode ) ;
		where run_id = "&run_id.";
run;
%Global p_n;
Proc sort data=&RGA191..encounters_N_IO_P_&compno._&run_id. 
out=&RGA191..holdcode_reject nodupkey
;by clamno lineno ;
run;
Data HOLDCODE_REJECT;
Length edit_chk_type  $100.; 
set HOLDCODE_REJECT;
run;
Data &RGA191..prof_holdcode_reject1;
set &RGA191..holdcode_reject;
hold_code=1;
where typecode='P';
run;
Proc sort data=&RGA191..prof_clm_P_&compno._&run_id.;by clamno;
 run; 
Proc sort data=&RGA191..prof_svc_P_&compno._&run_id.;by clamno;
 run; 

%macro icn_pr;	/* B-96285 added for QHP ICN1 */
Data &RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id.  ;
      merge       &RGA191..prof_clm_P_&compno._&run_id. (drop = lineno typecode ) 
                  &RGA191..prof_svc_P_&compno._&run_id. ;
	  by clamno  ;
					claimfrq=PR_2300_CLM05_3_FREQ_CDE;

				%if &compno=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do ;	/* B-145499 */ 
				   	icn= PR_2330B_ICN1_MCR;		
				%end;
				%else %if (&compno=45 and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do ;	/* B-121242 */ 
				   	icn= PR_2330B_ICN1;		
				%end;
				%else %do;
					icn=PR_2330B_ICN1_MCD;
				%end;
run;
%mend icn_pr;
%icn_pr;

Data &RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id. (rename=(clamno1=clamno)) ; /* HS - 569650 */
		length clamno1 $20.;
      set  &RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id. ;
		/* SAS2AWS2: ReplacedFunctionSubstr */
		clamno1 = ksubstr(clamno,1,13);
		drop clamno;
run;
Data &RGA191..Prof_Fail_&compno._&run_id. (rename=(clamno1=clamno));/* HS - 569650 */
		length clamno1 $20.;
 	set &RGA191..EditFailClmLine_&Compno._&run_id.(keep=clamno lineno typecode icn claimfrq);		
    Recsubind='N'; 
	/* SAS2AWS2: ReplacedFunctionSubstr */
	clamno1 = ksubstr(clamno,1,13);
		drop clamno;
	where typecode in ('P');
 Run;
  Proc sort data=&RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id. out=test1(keep=clamno) nodupkey;by clamno;run;
 proc sort data=&RGA191..Prof_Fail_&compno._&run_id. nodupkey;by clamno lineno;run;
 Data &RGA191..PROF837_Cla_rej &RGA191..PROF837_svc_rej;
 merge test1(in=_a) &RGA191..Prof_Fail_&compno._&run_id.(in=_b);
 if _a and _b then do ; output  &RGA191..PROF837_svc_rej; end;
 if  _b and not _a then do; output  &RGA191..PROF837_Cla_rej; end;
 by clamno;
 run;
 *Rsubmit;
 Data &RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id.;
 set &RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id.   &RGA191..prof837_svc_rej;
 run;
Proc sort data=&RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id. out=test2(keep=clamno) nodupkey;
by clamno ;
run;
data &RGA191..PROF837_hold_svc_rej &RGA191..PROF837_hold_Cla_rej;
merge test2(in=_a) &RGA191..prof_holdcode_reject1(in=_b);
if _a and _b then do ; output  &RGA191..PROF837_hold_svc_rej; end;
if  _b and not _a then do; output  &RGA191..PROF837_hold_Cla_rej; end;
by clamno;
run;
*rsubmit;
Data &RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id.;
set &RGA191..P837_1_Pass_&compno._&fi_ds_dt._&run_id. &RGA191..PROF837_hold_svc_rej; 
run;

data &RGA191..Reject_DG_I_&Compno._&run_id.(rename=(clamno1=clamno)); /* HS - 569650 */
length clamno1 $20. ;
set &RGA191..Reject_DG_I_&Compno._&run_id.;
/* SAS2AWS2: ReplacedFunctionSubstr */
clamno1 = ksubstr(clamno,1,13);
drop clamno;
run;
*Rsubmit;
Data &RGA191..inst837_holdcode_reject1;
set &RGA191..holdcode_reject &RGA191..Reject_DG_I_&Compno._&run_id.;
hold_code=1;
where typecode in ('I','O');
run;
proc sort data=&RGA191..inst837_holdcode_reject1;by clamno;run;
Proc sort data=&RGA191..Inst_clm_P_&compno._&run_id.;by clamno;
run; 
Proc sort data=&RGA191..Inst_svc_P_&compno._&run_id.;by clamno;
run; 

%macro icn_in;	/* B-96285 added for QHP ICN1 */
Data &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.  ;
      merge       &RGA191..Inst_clm_P_&compno._&run_id. (drop = lineno  ) 
                  &RGA191..Inst_svc_P_&compno._&run_id. ;
	  by clamno  ;
				  	claimfrq=IN_2300_CLM05_3_FREQ_CDE_1;
				
				%if &compno=30 or (&compno=45 and "&LOB." eq "MPPO") %then %do ;	/* B-145499 */ 
				   	icn=IN_2330B_ICN1_MCR;		
				%end;
				%else %if (&compno=45 and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do ;	/* B-121242 */ 
				   	icn=IN_2330B_ICN1;		
				%end;
				%else %do;
					icn=IN_2330B_ICN1_MCD;
				%end;
run;
%mend icn_in;
%icn_in;

Data &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id. (rename=(clamno1=clamno)); /*HS - 569650 */
	length clamno1 $20.;
      set  &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id. ;
		/* SAS2AWS2: ReplacedFunctionSubstr */
		clamno1 = ksubstr(clamno,1,13);
		drop clamno;
run;
*Rsubmit;
Proc sort data=&RGA191..Reject_Bilcod_&compno._&run_id. out=&RGA191..bilcode_reject nodupkey;
by clamno lineno;
run;
Data &RGA191..Inst_Fail_&compno._&run_id. (rename=(clamno1=clamno));/*HS - 569650 */
	length clamno1 $20.;
 	set &RGA191..EditFailClmLine_&Compno._&run_id.(keep=clamno lineno typecode);    
    Recsubind='N'; 
	/* SAS2AWS2: ReplacedFunctionSubstr */
	clamno1 = ksubstr(clamno,1,13);
		drop clamno;
	where typecode in ('I','O');	
 Run;
 data &RGA191..Inst_Fail_&compno._&run_id. ;
 set &RGA191..Inst_Fail_&compno._&run_id. &RGA191..bilcode_reject ;
 run;
 Proc sort data=&RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id. out=test1(keep=clamno) nodupkey;by clamno;run;
 proc sort data=&RGA191..Inst_Fail_&compno._&run_id. nodupkey;by clamno lineno;run;
 Data &RGA191..Inst837_Cla_rej &RGA191..Inst837_svc_rej;
 merge test1(in=_a) &RGA191..Inst_Fail_&compno._&run_id.(in=_b);
 if _a and _b then do ; output  &RGA191..Inst837_svc_rej; end;
 if  _b and not _a then do; output  &RGA191..Inst837_Cla_rej; end;
 by clamno;
 run;
 Data &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;
 set &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.   &RGA191..Inst837_svc_rej;
 run;
*Rsubmit;
Proc sort data=&RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id. out=test2(keep=clamno) nodupkey;
by clamno ;
run;
data &RGA191..Inst837_hold_svc_rej &RGA191..Inst837_hold_Cla_rej;
merge test2(in=_a) &RGA191..Inst837_holdcode_reject1(in=_b);
if _a and _b then do ; output  &RGA191..Inst837_hold_svc_rej; end;
if  _b and not _a then do; output  &RGA191..Inst837_hold_Cla_rej; end;
by clamno;
run;

*Rsubmit;
data &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;
length edit_chk_type $100.; 
set &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;
run;


Data &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;
set &RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id. &RGA191..Inst837_hold_svc_rej; 
run;


*Rsubmit;
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
			%pr_f8_icn; /* B-133711 */
	    run;	
		Data Map_seq_temp;
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
				retain &prof_var_list.; /* VVKR Added for columns order -- B-75041*/
				length clamno $20.; /* HS# 569650*/ 
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
				retain &prof_var_list.; /* VVKR Added for columns order -- B-75041*/
				length clamno $20.; /* HS# 569650*/ 
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
	         /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
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
		  %in_f8_icn; /* B-133711 */
	    run;
		Data Map_seq_temp;
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
				retain &inst_var_list.; /* VVKR Added for columns order -- B-75041*/
				length clamno $20.; /* HS# 569650*/ 
				set &RGA191..Tempdatset;
		run;
		%end;
		%else %do;
			Data &RGA191_..inst837_Pass_&compno._&fi_ds_dt._&run_id.(compress = yes keep=&inst_var_list.);/* HS 542635 - added compress option */
				retain &inst_var_list.; /* VVKR Added for columns order -- B-75041*/
				length clamno $20.; /* HS# 569650*/ 
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
	         /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
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
	/* HS 542635 - added logic to pass dynamic record count */
/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
Proc import datafile="&drv2./Encounters_Reporting/APD/Control_Table/record_count.csv"
	out=record_count dbms=csv
	replace;
Run;

proc sql;
/*Select lastobs into :lastobs_i_nomas  from record_count where claimtype = 'Inst' ;	B-97137 added submission_type */
/*Select lastobs into :lastobs_p_nomas  from record_count where claimtype = 'Prof' ; 	B-97137 added submission_type */
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
	/* HS 542635 - added logic to pass dynamic record count */

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
	    call symput('claimfrq_sub', claimfrq);	/* B-103975 submission type break macro assignment */
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
				%if &claimfrq_sub. = 8 %then %do;			/* B-97137 */
				n=ceil(&ds_count/&lastobs_p8_nomas.);		
				%end; %else
				%if &claimfrq_sub. = 7 %then %do;			/* B-97137 */
				n=ceil(&ds_count/&lastobs_p7_nomas.);		
				%end; %else %do;
				n=ceil(&ds_count/&lastobs_p_nomas.);/* HS 542635 - KV*/
				%end;
			%end;
			%else %do;
				%if &claimfrq_sub. = 8 %then %do;			/* B-97137 */
				n=ceil(&ds_count/&lastobs_i8_nomas.);		
				%end; %else
				%if &claimfrq_sub. = 7 %then %do;			/* B-97137 */
				n=ceil(&ds_count/&lastobs_i7_nomas.);		
				%end; %else %do;
				n=ceil(&ds_count/&lastobs_i_nomas.);/* HS 542635 - KV*/	
				%end;
			%end;
		%end;
		if n < 1 then  n=1;
		/* SAS2AWS2: ReplacedFunctionLeft-ReplacedFunctionTrim */
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
						%if &claimfrq_sub. = 8 %then %do;		/* B-97137 */
						%let lastobs=&lastobs_p8_nomas.; 		
						%end; %else
						%if &claimfrq_sub. = 7 %then %do;		/* B-97137 */
						%let lastobs=&lastobs_p7_nomas.; 		
						%end; %else %do;
						%let lastobs=&lastobs_p_nomas.; /* HS 542635 - KV*/
						%end;
					%end;
					%else %do;
						%if &claimfrq_sub. = 8 %then %do;		/* B-97137 */
						%let lastobs=&lastobs_i8_nomas.;		
						%end; %else
						%if &claimfrq_sub. = 7 %then %do;		/* B-97137 */
						%let lastobs=&lastobs_i7_nomas.;		
						%end; %else %do;
						%let lastobs=&lastobs_i_nomas.;/* HS 542635 - KV*/
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
						%if &claimfrq_sub. = 8 %then %do;					/* B-97137 */
						%let lastobs = %eval(&lastobs.+&lastobs_p8_nomas.);	
						%end; %else
						%if &claimfrq_sub. = 7 %then %do;					/* B-97137 */
						%let lastobs = %eval(&lastobs.+&lastobs_p7_nomas.);	
						%end; %else %do;
						%let lastobs = %eval(&lastobs.+&lastobs_p_nomas.);/* HS 542635 - KV*/
						%end;
					%end;
					%else %do;
						%if &claimfrq_sub. = 8 %then %do;					/* B-97137 */
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

*Rsubmit;
Data  &RGA191..Prof_temp ; 
length orig_clamno $20.;/* HS - 569650 added orig_clamno */
 	set  &RGA191..prof_clm_P_&compno._&run_id. 
     	 &RGA191..prof_svc_P_&compno._&run_id.  ; 
	by clamno;	
/* SAS2AWS2: ReplacedFunctionSubstr */
orig_clamno = ksubstr(clamno,1,13);/* HS - 569650 added orig_clamno */
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
%pr_f8_icn; /* B-133711 */
run; 
 
Data  &RGA191..inst_temp; 
length orig_clamno $20.; /* HS - 569650 added orig_clamno */
 	set  &RGA191..inst_clm_P_&compno._&run_id.  
         &RGA191..inst_svc_P_&compno._&run_id.   ; 
	by clamno;	
/* SAS2AWS2: ReplacedFunctionSubstr */
orig_clamno = ksubstr(clamno,1,13); /* HS - 569650 added orig_clamno */
Run; 
Proc sort data=&RGA191_..inst837_Pass_&compno._&fi_ds_dt._&run_id. (keep=clamno sequence_number) 
out=inst_seq  nodupkey;by clamno ;
Run; 
Proc sort data=&RGA191..inst_temp ; by orig_clamno;
Run;
Data &RGA191..inst_temp1(drop = orig_clamno);/* HS - 569650 */
merge &RGA191..inst_temp(in=_a) inst_seq (in=_b rename = (clamno = orig_clamno));
if _a;
by orig_clamno ;
%in_f8_icn; /* B-133711 */
run; 

*Rsubmit;
%Macro Layout(ds,ind)/minoperator; /* B-121242 added for IN operator */

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
	
	/* [DS-B-278698] - encoding = wlatin1 added */

	%if &ind.=P %then %do;
	%if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do; /* ALM 7174 - added lob in name, B-96285 added QHP */ 
			  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		  	filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %if &compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA") %then %do; /* B-146417 */ 
			  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
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
	%else %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do; /* B-248997 */ 
		  	filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %do;
		  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
	  	filename proout 	"&drv2./Encounters_Reporting/APD/Current/Prof_837_&Compno._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
		Data _null_;
	     	 file proout dlm = "|" lrecl = 2000 dsd; 
	      	set Temp_&sequence_number.;     	
		  	by clamno ;
	      	%include Prof;
	    Run;
		/* B-287906*/
		
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
			/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
			if klength(t1)=5 then t1=kcompress("0"||t1);
			call symput('t1',t1);
	    Run;
		Data temp_job_&sequence_number.;
			length output_file_name $40. Temp $24.; /* ALM 7174, B-121242 OSDS Temp - added length statement, B-145499 MCR expand Temp from $23 */
			set temp_job;
			%if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do;/* ALM 7174 - added lob in name, B-96285 added QHP */ 
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
			%else %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do;/* B-248997 */ 
				output_file_name = "Prof_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else %do;
				output_file_name = "Prof_837_&Compno._&fi_ds_dt._&sequence_number." ;
			%end;
			output_dataset_name ="Prof837_pass_&Compno._&fi_ds_dt._&run_id." ;

 			%if &compno. eq 42 and "&LOB." eq "EP" %then %do;
			Temp="TR."||"&TPAID."||".837PE.W.";
			%end;
			%else%if &compno. eq 42 and "&LOB." eq "QHP" %then %do; /* B-96285 added QHP */
			Temp="TR."||"&TPAID."||".837PQ.W.";
			%end;
			%else %if &compno. eq 45 and "&LOB." ne "MPPO" and "&x12_ctrl." ne "EIS" %then %do; /* B-121242 added HFIC */
			Temp="TR."||"&TPAID."||".837PC.W.";
			%end;
			%else %if &compno. eq 20 and "&x12_ctrl." ne "EIS" %then %do; /* B-121242 added CHP */
			Temp="TR."||"&TPAID."||".837PK.W.";
			%end;
			%else %if &compno. eq 30 or (&compno. eq 45 and "&LOB." eq "MPPO") %then %do; /* B-145499 MCR */
				Temp="TR."||"&TPAID."||".837PA.W.";
			%end;
			%else %do;
			Temp="TR."||"&TPAID."||".837PM.W.";
			%end;
			
			/* B-121242 OSDS updates TPAID */
			%if "&x12_ctrl." ne "EIS" %then %do;
				%if &compno. in(01 02 20 34 42 45) %then %do;
				/* SAS2AWS2: ReplacedFunctionTranwrd */
				temp=tranwrd(temp, "NYE", "Z10090");
				%end;
				%if &compno. in(02) %then %do;
				/* SAS2AWS2: ReplacedFunctionTranwrd */
				temp=tranwrd(temp, "T09004", "Z10090");
				%end;
			%end;

			/* Translated_file_name=Temp||Compress(put(today(),yymmdd6.)||"&t1.")||".001.DAT"; */
		  /*Translated_file_name=Temp||Compress(put(today(),yymmdd6.)||"&t1.")||".001";  B-121242 comment */
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
   	
   	/* [DS-B-278698] - encoding = wlatin added */
	 
   %if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do;/* ALM 7174 - added lob in name, B-96285 added QHP */ 
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;  /* ALM 7174 - added lob in the name */
	%end;
	%else %if &compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA") %then %do;/* B-146417 */ 
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %If &compno.=01 and &LOB. =DSNP  %then %do;
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;	
	%else %If &compno.=01 and "&LOB." ne "DSNP"  %then %do;
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;		
	%else %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do; /*B-248997*/
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
	%else %do;
		/* SAS2AWS2: ReplacedSlash-ChangedFolderName */
		filename insout 	"&drv2./Encounters_Reporting/APD/Current/Inst_837_&Compno._&fi_ds_dt._&sequence_number..txt" encoding ="wlatin1"	;
	%end;
		Data _null_;
    	  	file insout dlm = "|" lrecl = 2000 dsd; 
     	 	 set Temp_&sequence_number.;  
		 	 by clamno ;
    	 	 %include inst;
    	Run;
		/* B-287906*/
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
			/* SAS2AWS2: ReplacedFunctionCompress-ReplacedFunctionLength */
			if klength(t1)=5 then t1=kcompress("0"||t1);
			call symput('t1',t1);
	     Run;
			Data temp_job_&sequence_number.;
			length output_file_name $40. Temp $24.; /* ALM 7174, B-121242 OSDS Temp - added length statement, B-145499 MCR expand Temp from $23 */
			set temp_job;
		   %if &compno. eq 42 and ("&LOB." eq "EP" or "&LOB." eq "QHP") %then %do;/* ALM 7174 - added lob in name, B-96285 added QHP */ 
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else  %if &compno. eq 34 and ("&LOB." eq "CCC" or "&LOB." eq "FDA") %then %do;/* B-146417 */ 
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else  %if &compno.=01 and &LOB. =DSNP %then %do;
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;	
			%else  %if &compno.=01 and "&LOB." ne "DSNP" %then %do;
				output_file_name = "Inst_837_&Compno._&fi_ds_dt._&sequence_number." ;
			%end;
			%else  %if &compno. eq 45 and "&LOB." eq "MPPO" %then %do;/* B-248997*/ 
				output_file_name = "Inst_837_&Compno._&lob._&fi_ds_dt._&sequence_number." ; 
			%end;
			%else %do;
				output_file_name = "Inst_837_&Compno._&fi_ds_dt._&sequence_number." ;
			%end;
			output_dataset_name ="Inst837_pass_&Compno._&fi_ds_dt._&run_id." ;

			%if &compno. eq 42 and "&LOB." eq "EP" %then %do;
				Temp="TR."||"&TPAID."||".837IE.W.";
			%end;
			%else %if &compno. eq 42 and "&LOB." eq "QHP" %then %do; /* B-96285 added QHP */
				Temp="TR."||"&TPAID."||".837IQ.W.";
			%end;
			%else %if &compno. eq 45  and "&LOB." ne "MPPO" and "&x12_ctrl." ne "EIS" %then %do; /*B-248997*/ /* B-121242 added HFIC */
				Temp="TR."||"&TPAID."||".837IC.W.";
			%end;
			%else %if &compno. eq 20 and "&x12_ctrl." ne "EIS" %then %do; /* B-121242 added CHP */
				Temp="TR."||"&TPAID."||".837IK.W.";
			%end;
			%else %if &compno. eq 30 or (&compno. eq 45 and "&LOB." eq "MPPO") %then %do; /*B-248997*/ /* B-145499 MCR */
				Temp="TR."||"&TPAID."||".837IA.W.";
			%end;
			%else %do;
				Temp="TR."||"&TPAID."||".837IM.W.";
			%end;

			/* B-121242 OSDS updates TPAID */
			%if "&x12_ctrl." ne "EIS" %then %do;
				%if &compno. in(01 02 20 34 42 45) %then %do; 
				/* SAS2AWS2: ReplacedFunctionTranwrd */
				temp=tranwrd(temp, "NYE", "Z10090");
				%end;
				%if &compno. in(02) %then %do;
				/* SAS2AWS2: ReplacedFunctionTranwrd */
				temp=tranwrd(temp, "T09004", "Z10090");
				%end;
			%end;
			Translated_file_name=kcompress(Temp||put(today(),yymmdd6.)||"&t1.")||".001";
		  /*Translated_file_name=Temp||Compress(put(today(),yymmdd6.)||"&t1.")||".001";  B-121242 comment */
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
*rsubmit;
Data &RGA191..Pass(keep=clamno lineno );
length clamno $20. edit_chk_type $100. encapdfld $50.  edit_code $5.  icn $22.;  /* 541126 - ICN length increased from 15 to 20, B-136184 from 20 to 22 */
set &RGA191..prof837_ds1  &RGA191..inst837_ds1;  
run;
*rsubmit;
Proc sort data=&RGA191..PASS out=&RGA191..PASS1(keep=clamno lineno) nodupkey;by clamno lineno;run;
Data &RGA191..Reject(keep=clamno lineno claimfrq icn);
		merge &RGA191..source(in=_a) &RGA191..PASS1(in=_b keep=clamno lineno);
		if _a and not _b;
		by clamno lineno;
run;	
*rsubmit;

/* B-96285 changed APD to lib macro */
Data &RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.(keep=clamno lineno editchktyp editcd encapdfld Recsubind typecode rundt runid LOB lobcod);/* ALM 7174 - added lobcod */
length edit_code $5. clamno $20. edit_chk_type $100. encapdfld $50.; /* HS 567501 *//* HS# 569650 added clamno*/

set 
	  &RGA191..holdcode_reject
      &RGA191..Reject_Bilcod_&compno._&run_id.
      &RGA191..EncounterEditFail_&compno._&run_id.	  
	  &RGA191..Reject_DG_I_&Compno._&run_id. 
	  &RGA191..Reject_svccod1
	  Rollup_claims
	  Rollup_claims_HG/* HS 565133 */
	  line_reject /* CR# CHG0035759 */	
	  proc_qual_reject /* CR# CHG0039138 */
	  diag_qual_reject /* B-154145  */
	  Reject_DGSVCCOD /* CR# CHG0039334 */
	  &RGA191..Inovalon_delete_reject /* CR# CHG0040374 */
	  &RGA191..Reject_svcholds /* B-108297 */
	  APD_Reject_cntrl_file /* CR# CHG0040549 */
	  APD_Reject_Claim_cntrl_file /* B-166920 */
	  &RGA191..procedure_code_date_reject	/* B-114226 */	
	  &RGA191..NDCSVC_Procbill_Code_rej /*B-261072*/
	  &RGA191..NDCSVC_Proc_Code_rej	   /*B-261072*/
	  &RGA191..NPI_City_Rej /*B-263565*/
	  &RGA191..admtime_Rej /*B-263560*/
    &RGA191..distim_Rej /*263560*/
    &RGA191..MCORCAT_REJECTION /* B-278970 - DS - ADDED */
	  %elig_checkds(&RGA191..eligibility_reject) /* B-131960 */
	  &RGA191..hiosid_reject /*B-274606*/
	  &RGA191..provider_date_reject /* [B-335998] */
	  ;
	   rename
	  	     edit_code=editcd
			 edit_chk_type=editchktyp;
     		 rundt = "&run_dt." ;
     		 runid = "&run_id." ;
	  		 LOB="&Compno.";
			 LOBCOD="&LOB.";
		     label edit_chk_type='Edit Check Type'
		 	   edit_code='Edit Code'
			   encapdfld='Encounter Field'
			   Recsubind='Record Submission Indicator'
			   typecode='Type Code'
			   rundt='Run Date'
			   runid='Run Id';	
Run;
/* HS# 569650 - added */ 
Data Map_seq;
length clamno $20.;
set Map_seq(rename=(Clamno=clamno1));
/* SAS2AWS2: ReplacedFunctionSubstr */
Clamno=kcompress(ksubstr(clamno1,1,13));
run;
Proc sort data=&RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.;by clamno;run;	/* B-96285 changed APD to lib macro */
Proc sort data=Map_seq(keep=clamno sequence_number) out=Map_seq1 nodupkey;by clamno;run;

/* B-96285 changed APD to lib macro */
Data &RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.(compress = yes keep=clamno lineno editchktyp editcd encapdfld Recsubind typecode rundt runid LOB sequence_number lobcod);/* HS 542635 - added compress option */
/* ALM 7174 - added lobcod */
retain  encapdfld editchktyp clamno lineno typecode Recsubind editcd rundt runid LOB sequence_number;
length clamno $20.; /* HS# 569650*/ 
Merge &RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.(in=_a) Map_seq1(in=_b);
if _a;
by clamno;
run; 
Proc sql;
Create table Mcr_sq as select distinct(sequence_number) as sequence_number,
"&run_id." as run_id
from &RGA191_..&lib._Rej_837_&compno._&fi_ds_dt._&run_id.;	/* B-96285 changed APD to lib macro */
quit;


/* B-160659 Diag_xwalk.xlsx and claims difference are loaded into Base table - Start Here*/
%macro Append_Diagcd_Audit;
	%nobs(calims_Diagcd);
    %if &nobs gt 0 %then %do;
				proc sort data=Diag_Poa_mismatch;by clamno;run;
				Data Diag_Poa_mismatch;
				length lobcod $6. rundate $15. compno $3. sequence_number $14. ;
				Merge Diag_Poa_mismatch(in=_a) Map_seq1(in=_b);
				if _a and _b;
				by clamno;
				rundate=put(today(),mmddyy10.);
				compno="&compno.";
				lobcod="&lob.";
				run;
				
				data Diag_Poa_mismatch;
				retain clamno Diagcd_Varible_Name Diagcd_Value Diagcd_Flag comments sequence_number compno lobcod rundate ;
				set Diag_Poa_mismatch;
				run;
	
				proc append base=&RGA193..Diagcd_Audit data=Diag_Poa_mismatch; run;
	%end;
%mend;
%Append_Diagcd_Audit;
/* B-160659 Diag_xwalk.xlsx and claims difference are loaded into Base table - End Here*/


Data temp_job_1;
			set temp_job;			
			output_dataset_name ="&lib._Rej_837_&compno._&fi_ds_dt._&run_id." ;	/* B-96285 changed APD to lib macro */
			typecode='P/I';	
		run;
Data temp_job_2;
merge temp_job_1(in=_a) Mcr_sq(in=_b) ;
if _b;
by run_id;
run;


Data &R42173..medical_job_mstr;
		set &R42173..medical_job_mstr  temp_job_2;
	run;
Data &R42173..medical_job_mstr;
	set &R42173..medical_job_mstr;
	if run_id="&run_id." and sequence_number="" then delete;
Run;

%macro record_cnt(dsn= , recind= , typ= ); /*B-272515 Add count in Medical job Master - Macro Created*/

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

/* HS 542635 - Added Proc delete statement */
Proc delete data=tempdatset;run;
Proc delete data=pv_editpassclmline2_&compno._&run_id.;run;
Proc delete data=editpassclmline1_&compno._&run_id.;run;
Proc delete data=dv_editpassclmline2_&compno._&run_id.;run;
Proc delete data=editpassclmline2_&compno._&run_id.;run;
Proc delete data=inst_temp;run;
Proc delete data=editfailclmline_&compno._&run_id.;run;
Proc delete data=encounter3_p_&compno._&run_id.;
run;
Proc delete data=inst_clm_p_&compno._&run_id.;run;
Proc delete data=inst_svc_p_&compno._&run_id.;run;
Proc delete data=&RGA191..I837_1_Pass_&compno._&fi_ds_dt._&run_id.;run;


%macro RGA193_tmp_ds_del_1  ;
      %if &temp_ds_del_flag. = Y %then 
      %do; 
                   proc datasets lib= &RGA191. nolist  ;
                        delete      editfailclmline_&compno._&run_id.
										editpassclmline_&compno._&run_id.
										encountereditfail_&compno._&run_id.
										encountereditpass_&compno._&run_id.
										bad_data_&compno._&run_id.
										encounter3_p_&compno._&run_id.
										inst_clm_p_&compno._&run_id.
										inst_svc_p_&compno._&run_id.
										prof_clm_p_&compno._&run_id.
										prof_svc_p_&compno._&run_id.
										dme_clm_p_&compno._&run_id.
										dme_svc_p_&compno._&run_id.
										i837_1_pass_&compno._&fi_ds_dt._&run_id.
										inst_fail_&compno._&run_id.
										p837_1_pass_&compno._&fi_ds_dt._&run_id.
										prof_fail_&compno._&run_id.
										DME_fail_&compno._&run_id.
										encountersy2a_&compno._&run_id.
										encountersy2b_&compno._&run_id.
										REJECT_BILCOD_&compno._&run_id.
										EditPassClmLine1_&compno._&run_id.
										EditPassClmLine2_&compno._&run_id.
										exempt_code_&compno.
										prof_temp prof837_ds
										mcr_rej_837 inst_temp
										inst837_ds dme_temp dme837_ds
										dme837_1_pass_&compno._&fi_ds_dt._&run_id.
										tempdatset prof_temp1 dme_temp1 inst_temp1

                  ;
                    quit;
                    
                    proc datasets  lib= &RGA191. memtype=view nolist  ;  
                        delete provider_&compno. encountersy_&Compno._&run_id. encounters_&compno._&run_id.
							   providernpi_&compno.
							   exempt_code_&compno.	; 
                        quit;
run;

			  proc datasets lib= &RGA191. nolist  ;
			      delete 	 finclaims_&Compno. reject prof_qi pass1 pass inst_qi
				  reject_temp reject1 source
				  bilcode_reject dme837_cla_rej dme837_cla_rej1 dme837_ds1 dme837_hold_cla_rej
				  dme837_hold_cla_rej1 dme837_hold_svc_rej  dme837_holdcode_reject1 dme837_qi
				  dme837_svc_rej holdcode_reject inst837_cla_rej inst837_cla_rej1 inst837_ds1
					inst837_hold_cla_rej  inst837_hold_cla_rej1 inst837_hold_svc_rej
				inst837_holdcode_reject1  inst837_qi inst837_svc_rej prof837_cla_rej
				prof837_ds1  prof837_hold_cla_rej prof837_hold_cla_rej1 prof837_hold_svc_rej
				prof837_qi prof837_svc_rej prof_holdcode_reject1 tt prof837_cla_rej1
				encounters_n_io_p_&Compno._&run_id. reject_dg_i_&Compno._&run_id.
				%elig_checkds(eligibility_reject) /* B-131960 */	
					;	
			  title3 ;
			  title4 ;
			  title5 ;
			
			  quit;

  %end;
  
  		%if &compno.=01 and &LOB. = DSNP %then %do;
		       proc datasets  lib= &RGA191. nolist  ;  
                   delete 
					members_30 provider_30 provider2_30 provider3_30 svclines_30 svcholds_30 claims_30 claims_30_mcr1 claims_30_mcr svclines_30
					svclines_30_mcr1 svclines_30_mcr svcholds_30 svcholds_30_mcr1 svcholds_30_mcr	
				   ; 
               quit;		
		%end;
		
%mend RGA193_tmp_ds_del_1  ;
%RGA193_tmp_ds_del_1 ;
/***********************************************************************************************/
proc sql;
update &R42173..medical_job_mstr
set Cmsmap_flag = '1'
where run_id = "&run_id."
;
quit;

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19104 Program End execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

/***********************************************************************************************/  
/*                                                                                             */
/*                   End of Program                                                            */
/*                                                                                             */
/***********************************************************************************************/    

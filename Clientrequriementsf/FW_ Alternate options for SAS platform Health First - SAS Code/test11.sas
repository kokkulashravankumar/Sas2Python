
/***       
$Header: $ 
***/ 

/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19105 Program Start execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */

/********** Initializing macro variables for reporting - Start *********/
*Rsubmit;

%global keepv_members ctrl_tab;		/* B-96285 */

%let total_obs =0;
%let total_obs_post_revind =0;
%let total_obs_hold =0;
%let total_obs_edit =0;
%let total_obs_fin =0;
%let ttt=0; 
%let Bil_cod_Rej_count1=0;
%let QI_duplicate=0;
%let Bil_cod_Rej_count=0;
%let edit_pass_1=0;
%let edit_fail_1=0;
%let ClmTot_Fail_1=0;
%let rec_encY2_1=0;
%let Rej_Enc_N_IO_P_1=0;
%let Hold_Code_Pass_1=0;
%let ClmY_SvcY_Count1=0;
%let II_Count2_1 =0;
%let IO_Count2_1=0;
%let P_Count2_1=0;
%let ClmN_SvcY_Count1=0;
%let inst_I_svc_count_p=0;
%let inst_O_svc_count_p=0;
%let inst_I_clm_count_p=0;
%let inst_O_clm_count_p=0;
%let temp1=0;
%let QI_count             = 0;
%let QI_and_SV_count      = 0;
%let QI_not_SV_count      = 0;
%let QI_count_clm=0;
%let QI_and_clm_count=0;
%let  AT_LCHIT  		  = 0 ;             
%let  AT_MDCHIT           = 0 ;   
%let  AT_NOHIT            = 0 ;   
%let  AT_NPIHIT           = 0 ;   
%let  AT_RATE             = 0 ;   
%let  AT_TOTAL            = 0 ;   
%let  AT_TOT_HIT          = 0 ;   
%let  AT_UPIHIT           = 0 ;   
%let  BI_HIT              = 0 ;   
%let  BI_NOHIT            = 0 ;   
%let  BI_RATE             = 0 ;   
%let  BI_TOTAL            = 0 ;   
%let  ClmN_SvcY_Count     = 0 ;   
%let  ClmY_SvcN_Count     = 0 ;   
%let  ClmY_SvcY_Count     = 0 ;   
%let  Hold_Code_Pass      = 0 ;   
%let  II_Count            = 0 ;   
%let  IO_Count            = 0 ;   
%let  MEM_HIT             = 0 ;   
%let  MEM_NOHIT           = 0 ;   
%let  MEM_RATE            = 0 ;   
%let  MEM_TOTAL           = 0 ;   
%let  OP_HIT              = 0 ;   
%let  OP_NOHIT            = 0 ;   
%let  OP_RATE             = 0 ;   
%let  OP_TOTAL            = 0 ;   
%let  OT_HIT              = 0 ;   
%let  OT_NOHIT            = 0 ;   
%let  OT_PCPHIT           = 0 ;   
%let  OT_RATE             = 0 ;   
%let  OT_TOTAL            = 0 ;   
%let  OT_TOT_HIT          = 0 ;   
%let  P_Count             = 0 ;   
%let  RF_HIT              = 0 ;   
%let  RF_NOHIT            = 0 ;   
%let  RF_PCPHIT           = 0 ;   
%let  RF_RATE             = 0 ;   
%let  RF_TOTAL            = 0 ;   
%let  RF_TOT_HIT          = 0 ;   
%let  RN_HIT              = 0 ;   
%let  RN_NOHIT            = 0 ;   
%let  RN_RATE             = 0 ;   
%let  RN_TOTAL            = 0 ;   
%let  Rej_Enc_N_II        = 0 ;   
%let  Rej_Enc_N_IO_P      = 0 ;   
%let  SVC_HIT             = 0 ;   
%let  SVC_NOHIT           = 0 ;   
%let  SVC_RATE            = 0 ;   
%let  SVC_TOTAL           = 0 ;   
%let  claim_count         = 0 ;   
%let  dup_svc_count       = 0 ;   
%let  edit_fail           = 0 ;   
%let  edit_pass           = 0 ;   
%let  enctyp_n_ii_count   = 0 ;   
%let  inst_clm_count_p    = 0 ;   
%let  inst_svc_count_p    = 0 ;   
%let  miss_mcorcat        = 0 ;   
%let  prof_clm_count_p    = 0 ;   
%let  prof_svc_count_p    = 0 ;  
%let  dme_clm_count_p    = 0 ;   
%let  dme_svc_count_p    = 0 ; 
%let  rec_encY2           = 0 ;   
%let  svc_count           = 0 ;   
%let  uniq_svc_count      = 0 ;   
%let  unknown_typcd_count = 0 ;   
%let  II_Count2			  = 0 ;	
%let  IO_Count2			  = 0 ;
%let  P_Count2            = 0 ;
%let  ClmHoldCd_Pass      = 0 ;
%let  Clmline_Fail        = 0 ;
%let  ClmHoldCd_Fail      = 0 ;
%let  ClmTot_Fail         = 0 ;
%let  Inp_dg_line_cnt     = 0 ;  
%let  ndc_excp_cnt 		  = 0 ; 
%let BI_providernpi_HIT=0;
%let BI_providernpi_NOHIT=0; 
%let AT_providernpi_HIT=0;
%let AT_providernpi_NoHIT=0;
%let BI_PROVIDERNPI_TOTAL=0;
%let  BI_PROVIDERNPI_RATE=0;
%let AT_PROVIDERNPI_TOTAL=0;
%let AT_PROVIDERNPI_RATE=0;


/********** Initializing macro variables for reporting - End *********/


%let keepv_providernpi= ENTITYTYP NPI PROVBUSCITY PROVBUSPOSTAL PROVBUSSTATE NPIDEACTDATE PROVENUMDATE
					    PROVFIRBUSADDR PROVFSTNAM PROVLSTNAM  PROVMIDNAM PROVSECBUSADDR EIN
						AUTHOFFFSTNAM AUTHOFFLSTNAM AUTHOFFMIDNAM HLTHPROVTAXOCODE1;

%let keepv_members=     membno MLSTNAM MFSTNAM MMIDFULL	BTHDAT SEXCOD 
				        MADRLN1 MADRLN2 MCITYCD MSTACOD MZIPCOD relcod 
						subsno socsec mdcarid;	/* B-96285 added macro */

%let keepv_members_mcr=  membno_mcr MLSTNAM_mcr MFSTNAM_mcr MMIDFULL_mcr BTHDAT_mcr SEXCOD_mcr 
				        MADRLN1_mcr MADRLN2_mcr MCITYCD_mcr MSTACOD_mcr MZIPCOD_mcr relcod_mcr 
						subsno_mcr socsec_mcr mdcarid_mcr;							

%let keepv_stat=        BI_HIT BI_NOHIT OP_HIT OP_NOHIT 
					    MEM_HIT MEM_NOHIT 	BI_providernpi_HIT BI_providernpi_NOHIT 
					    AT_providernpi_HIT AT_providernpi_NoHIT	;
*Rsubmit;
%let Keepv_svc=         pstdat  revind   CLAMNO COVUNT CPTCOD ENDDAT LINENO MCORCAT MODCD2 /*[B-315447] - DS - Updates*/
						MODCD3 MODCD4 MODCOD NDCQTY NDCSVC NDCUOM NETALWAMT POSCOD REVCOD SVCCOD SVCDAT
						SVCSTAT SVCTYP TOPAMT UNITCT  APTRNDAT PIDATE CLAAMT RCVDAT bencod ALWAMT dedamt coiamt COPAMT
						nonamt dscamt rcvamt  mcrnetalwamt mcdnetalwamt cptmd1 cptmd2 cptmd3 cptmd4 membno
						cstshrnetaldedamt cstshrnetalcoiamt cstshrnetalcopamt capind primdiag patage benpkg lobcod
						mcdtopamt  mcrtopamt cstshrnetalcoiamt cstshrnetalcoiamt cstshrnetaldedamt cstshrtopaycoiamt
						cstshrtopaycopamt cstshrtopaydedamt grpnum apsvcstat alwunt mcrcovunt formcd mcdcovunt PREAMT; /* B-108297 formcd */ 
						/* CR# CHG0038705  - added apsvcstat *//*ALM 5160 - added alwunt mcrcovunt*/ /*B-329832 - PREAMT*/
						
%let Keepv_svc_mcr=     pstdat_mcr  revind_mcr APTRNDAT_mcr  CLAMNO_mcr COVUNT_mcr CPTCOD_mcr ENDDAT_mcr LINENO_mcr MCORCAT_mcr MODCD2_mcr
						MODCD3_mcr MODCD4_mcr MODCOD_mcr NDCQTY_mcr NDCSVC_mcr NDCUOM_mcr NETALWAMT_mcr POSCOD_mcr REVCOD_mcr SVCCOD_mcr SVCDAT_mcr
						SVCSTAT_mcr SVCTYP_mcr TOPAMT_mcr UNITCT_mcr PIDATE_mcr CLAAMT_mcr RCVDAT_mcr bencod_mcr ALWAMT_mcr dedamt_mcr coiamt_mcr COPAMT_mcr
						nonamt_mcr dscamt_mcr rcvamt_mcr  mcrnetalwamt_mcr mcdnetalwamt_mcr cptmd1_mcr cptmd2_mcr cptmd3_mcr cptmd4_mcr membno_mcr
						cstshrnetaldedamt_mcr cstshrnetalcoiamt_mcr cstshrnetalcopamt_mcr capind_mcr primdiag_mcr patage_mcr benpkg_mcr lobcod_mcr
						mcdtopamt_mcr  mcrtopamt_mcr cstshrnetalcoiamt_mcr cstshrnetalcoiamt_mcr cstshrnetaldedamt_mcr cstshrtopaycoiamt_mcr 
						cstshrtopaycopamt_mcr cstshrtopaydedamt_mcr grpnum_mcr apsvcstat_mcr alwunt_mcr mcrcovunt_mcr formcd_mcr mcdcovunt_mcr PREAMT_MCR; /*B-329832 - PREAMT_MCR*/					

%let Keepv_Claim=       clamno membno formcd bilcod admtyp ADMSRC provno attphy REFPNUM OTHPNUM OPRPNUM
					    PRDIAGCD OCCCD1 admdat admtdiag admtim clmstat concod1 concod10 concod11 concod12
      				    concod2 concod3 concod4 concod5 concod6 concod7 concod8 concod9 diag10 diag11
						diag12 diag13 diag14 diag15 diag17 diag18 diagd2 diagd3 diagd4 diagd5 diagd6	
					    diagd7 diagd8 diagd9 diagn16 disdat distim firstdos lastadj lastdos occcd1
						occcd10 occcd2 occcd3 occcd4 occcd5 occcd6 occcd7 occcd8 occcd9 occdt1 occdt10
					    occdt2  occdt3 occdt4 occdt5 occdt6 occdt7 occdt8 occdt9 oprpfst oprplst oprpmid
						othpfst othplst othpmid paiddate parstat poadiag10 poadiag11 poadiag12 poadiag13
						poadiag14 poadiag15 poadiag16 poadiag17 poadiag18 poadiag2 poadiag3 poadiag4
						poadiag5 poadiag6 poadiag7 poadiag8 poadiag9 poaprdiag postdate procdr2 procdr3
					    procdr4 procdr5	 procdr6 procdt2 procdt3 procdt4 procdt5 procdt6 prprocdr prprocdt
					    spnbeg1 spnbeg10 spnbeg2 spnbeg3 spnbeg4 spnbeg5 spnbeg6 spnbeg7 spnbeg8 spnbeg9
					    spncod1 spncod10 spncod2 spncod3 spncod4 spncod5 spncod6 spncod7 spncod8 spncod9		
					    spnend1 spnend10 spnend2 spnend3 spnend4 spnend5 spnend6 spnend7 spnend8 spnend9
					    toptot  valA01 valA02 valA03 valA04 valA05  valA06 valA07 valA08 valA09 valA10
				        valA11  valA12 valcd01 valcd02 valcd03 valcd04 valcd05 valcd06 valcd07 valcd08
						valcd09 valcd10 valcd11 valcd12 DISTAT ATTPH2 ATPFST ATPMID ATPNPI ATPTXY PRVTXY
						MEDNUM patnum relcs1 relcs2 ACCDAT extclm  blpnpi atpnpi oprnpi 
						diag19 diag20 diag21 diag22 diag23 diag24 diag25
						poadiag19 poadiag20 poadiag21 poadiag22  poadiag23 poadiag24 poadiag25
						Diagvind Procvind  rndnpi rpanpi PCPCOD clatyp clspclcd origclamno /* B-166917 added origclamno */;

*Rsubmit;
%let Keepv_ClmDiag=		prdiagcd diagd2-diagd9 diag10-diag15 diagn16 diag17-diag25; 	/* B-92443 */

%let Keepv_ClmDiag_Comma = prdiagcd,diagd2,diagd3,diagd4,diagd5,diagd6,diagd7,diagd8,diagd9,diag10,diag11,diag12,diag13,diag14,diag15,diagn16,diag17,diag18,diag19,diag20,diag21,diag22,diag23,diag24,diag25; 	/* B-160659 added */

%let keepv_imp=CMSpatstat	ClmN_SvcY_Count	ClmY_SvcY_Count	ClmY_SvcY_Count1	ENDDAT1	II_Count2	II_Count2_1	IO_Count2	
IO_Count2_1	P_Count2	P_Count2_1	SVCDAT1	accdat	admdat	admsrc	admtdiag	admtim	admtyp	admtype	alwamt		
atpfst	atpmid	atpnpi	atptxy	attph2	attphy	bencod	benpkg	bilcod	blpnpi	capind	claamt	claimfrq	clamno	clatyp 	
clm2_provno	clm_distat	clmstat	clspclcd	coiamt	concod1	concod2	concod3	concod4	concod5	concod6	concod7	concod8	concod9	
concod10	concod11	concod12	copamt	covunt	cptcod	cptmd1	cptmd2	cptmd3	cptmd4	cstshrnetalcoiamt origclamno /* B-166917 added origclamno */	
cstshrnetalcopamt	cstshrnetaldedamt	cstshrtopaycoiamt	cstshrtopaycopamt	cstshrtopaydedamt	dedamt	diag10	
diag11	diag12	diag13	diag14	diag15	diag17	diag18	diag19	diag20	diag21	diag22	diag23	diag24	diag25	diagd2	
diagd3	diagd4	diagd5	diagd6	diagd7	diagd8	diagd9	diagn16	diagvind	disdat	distat	distim	dscamt	enddat	extclm	
firstdos	formcd	icn	lastadj	lastdos	lineno	lobcod	APTRNDAT  max_pidate	mcdnetalwamt	mcdnetalwamt_old	mcdtopamt	
mcorcat	mcrnetalwamt	mcrnetalwamt_old	mcrtopamt	mednum	membno	modcd2	modcd3	modcd4	modcod	ndcqty	ndcsvc	
ndcuom	netalwamt	nonamt	occcd1	occcd2	occcd3	occcd4	occcd5	occcd6	occcd7	occcd8	occcd9	occcd10	occdt1	occdt2
occdt3	occdt4	occdt5	occdt6	occdt7	occdt8	occdt9	occdt10	oprnpi	oprpfst	oprplst	oprpmid	oprpnum	othpfst	othplst	
othpmid	othpnum	paiddate	parstat	patage	patnum	pcpcod	pcpnpiid	pidate	poadiag2	poadiag3	poadiag4	poadiag5
poadiag6	poadiag7	poadiag8	poadiag9	poadiag10	poadiag11	poadiag12	poadiag13	poadiag14	poadiag15
poadiag16	poadiag17	poadiag18	poadiag19	poadiag20	poadiag21	poadiag22	poadiag23	poadiag24	poadiag25	
poaprdiag	poscod	postdate	prdiagcd	primdiag	procdr2	procdr3	procdr4	procdr5	procdr6	procdt2	procdt3	procdt4	
procdt5	procdt6	procvind	provno	prprocdr	prprocdt	prvtxy	pstdat	rcvamt	rcvdat	refpnum	relcs1	relcs2	revcod	
revenue_cd	revind	rndnpi	rpanpi	spnbeg1	spnbeg2	spnbeg3	spnbeg4	spnbeg5	spnbeg6	spnbeg7	spnbeg8	spnbeg9	spnbeg10
spncod1	spncod2	spncod3	spncod4	spncod5	spncod6	spncod7	spncod8	spncod9	spncod10	spnend1	spnend2	spnend3	spnend4	spnend5
spnend6	spnend7	spnend8	spnend9	spnend10	svccod	svcdat	svcstat	svctyp	temp	topamt	toptot	typecode	
unitct	valA01	valA02	valA03	valA04	valA05	valA06	valA07	valA08	valA09	valA10	valA11	valA12	valcd01	valcd02	valcd03
valcd04	valcd05	valcd06	valcd07	valcd08	valcd09	valcd10	valcd11	valcd12 mdcarid mdcarid_mcr revenue_cd_mcr
MEM_HIT MEM_MLSTNAM MEM_MFSTNAM MEM_MMIDFULL MEM_MEMBNO MEM_MDCARID MEM_subsno MEM_BTHDAT MEM_SEXCOD MEM_MEDCTY MEM_MADRLN1 MEM_MADRLN2 MEM_MCITYCD MEM_MSTACOD MEM_MZIPCOD MEM_socsec BI_PROVNO BIPRVNPI_Prov Plstnam_1 pfstnam_1 NPIID_1
padrln1_1 padrln2_1 pcitycd_1 pstacod_1 pzipcod_1 FEDNUM_1 mdcaid_1 BI_providernpi_HIT BIPRVNPI BI_PRV_ENTC BI_PRV_LST BI_PRV_FST BI_PRV_ADD1 BI_PRV_ADD2 BI_PRV_ADD2
BI_PRV_STATE BI_PRV_ZIP BIPRVTXY BI_PRV_EIN RF_HIT RF_PHY_LST RF_PHY_FST RF_PHY_npiid RF_PHY_EIN RF_PHY_PRVTXY RF_PHY_LST
RF_PHY_FST RF_PHY_npiid RF_PHY_EIN RF_PHY_PRVTXY RF_HIT_1 RN_HIT RN_PHY_LST RN_PHY_FST RN_PHY_npiid RN_PHY_EIN RN_PHY_PRVTXY
OP_HIT OP_PHY_LST OP_PHY_FST OPPRVNPI OP_PHY_EIN OP_PHY_PRVTXY OP_NOHIT AT_providernpi_HIT AT_PHY_LST AT_PHY_FST AT_PHY_PRVTXY
ATPRVNPI AT_PHY_EIN AT_providernpi_NoHIT AT_PHY_LST AT_PHY_FST AT_PHY_PRVTXY ATPRVNPI AT_PHY_EIN mrg_provno RF_PHY_LST_1 RF_PHY_FST_1 RF_PHY_npiid_1
 RF_PHY_EIN_1 RF_PHY_PRVTXY_1 Disdat  admdat  BI_PRV_CITY MEM_Temp RN_PHY_ADD1 RN_PHY_ADD2 RN_PHY_CITY RN_PHY_STATE RN_PHY_ZIP
AT_PHY_ADD1 AT_PHY_ADD2 AT_PHY_CITY AT_PHY_STATE AT_PHY_ZIP val_rej NETALWAMT_old alwunt mcrcovunt mcdcovunt mcrnetalwamt_old mcdnetalwamt_old mstacod  /*ALM 5160 - added alwunt NETALWAMT_old mcrcovunt, B-108297 mstacod */ 
NPIDEACTDATE PROVENUMDATE
mcd_lin_missfl mcr_lin_missfl dsnp_membno dsnp_membno_mcr
MEM_MLSTNAM_MCR MEM_MFSTNAM_MCR MEM_MMIDFULL_MCR MEM_MEMBNO_MCR  MEM_MDCARID_MCR MEM_subsno_MCR 
MEM_BTHDAT_MCR  MEM_SEXCOD_MCR  MEM_MEDCTY_MCR MEM_MADRLN1_MCR MEM_MADRLN2_MCR MEM_MCITYCD_MCR MEM_MSTACOD_MCR
MEM_MZIPCOD_MCR MEM_socsec_MCR  MEM_Temp_MCR max_pidate_mcr
membno_mcr MLSTNAM_mcr MFSTNAM_mcr MMIDFULL_mcr BTHDAT_mcr SEXCOD_mcr MADRLN1_mcr MADRLN2_mcr MCITYCD_mcr MSTACOD_mcr MZIPCOD_mcr relcod_mcr subsno_mcr
pstdat_mcr  revind_mcr APTRNDAT_mcr  CLAMNO_mcr COVUNT_mcr CPTCOD_mcr ENDDAT_mcr LINENO_mcr MCORCAT_mcr MODCD2_mcr
MODCD3_mcr MODCD4_mcr MODCOD_mcr NDCQTY_mcr NDCSVC_mcr NDCUOM_mcr NETALWAMT_mcr POSCOD_mcr REVCOD_mcr SVCCOD_mcr SVCDAT_mcr
SVCSTAT_mcr SVCTYP_mcr TOPAMT_mcr UNITCT_mcr PIDATE_mcr CLAAMT_mcr RCVDAT_mcr bencod_mcr ALWAMT_mcr dedamt_mcr coiamt_mcr COPAMT_mcr
nonamt_mcr dscamt_mcr rcvamt_mcr  mcrnetalwamt_mcr mcdnetalwamt_mcr cptmd1_mcr cptmd2_mcr cptmd3_mcr cptmd4_mcr
cstshrnetaldedamt_mcr cstshrnetalcoiamt_mcr cstshrnetalcopamt_mcr capind_mcr primdiag_mcr patage_mcr benpkg_mcr lobcod_mcr
mcdtopamt_mcr  mcrtopamt_mcr cstshrnetalcoiamt_mcr cstshrnetalcoiamt_mcr cstshrnetaldedamt_mcr cstshrtopaycoiamt_mcr
cstshrtopaycopamt_mcr cstshrtopaydedamt_mcr grpnum_mcr apsvcstat_mcr NETALWAMT_old_mcr alwunt_mcr mcrcovunt_mcr formcd_mcr mcdcovunt_mcr PREAMT PREAMT_MCR /*B-329832 - PREAMT PREAMT_MCR*/
BI_PROVNUMDT BI_NPIDEACTDT BI_NPIREACTDT RF_PROVNUMDT RF_NPIDEACTDT RF_NPIREACTDT RN_PROVNUMDT RN_NPIDEACTDT RN_NPIREACTDT
OP_PROVNUMDT OP_NPIDEACTDT OP_NPIREACTDT AT_PROVNUMDT AT_NPIDEACTDT AT_NPIREACTDT;  /* [B-335998] */




/* HS 569650 - Added Rendering and attending provider demographic variables *//*CR# CHG0034810 - Added val_rej*/


/* B-166920 */
%let edit_type_Inovalon_delete_rej        ='' ; 
%let enctyp_code_Inovalon_delete_rej      ='' ; 
%let edit_type_eligibility                ='' ; 
%let enctyp_code_eligibility              ='' ;
%let edit_type_DG_SVCCOD                  ='' ; 
%let enctyp_code_DG_SVCCOD                ='' ;
%let edit_type_Rollup_Rej_svctyp_HG       ='' ; 
%let enctyp_code_Rollup_Rej_svctyp_HG     ='' ;
%let edit_type_SVCCOD_Reject_HG           ='' ; 
%let enctyp_code_SVCCOD_Reject_HG         ='' ;
%let edit_type_Inpatient_Paid_Rej		  ='' ; 
%let enctyp_code_Inpatient_Paid_Rej		  ='' ;

/* B-168796 */
%let edit_type_bilcod_rej        		  ='' ; 
%let enctyp_code_bilcod_rej      		  ='' ; 
%let edit_type_holdcode_rej               ='' ; 
%let enctyp_code_holdcode_rej             ='' ;
%let edit_type_proc_code_date_rej         ='' ; 
%let enctyp_code_proc_code_date_rej       ='' ;
%let edit_type_EIPDG_rej        		  ='' ; 
%let enctyp_code_EIPDG_rej      		  ='' ;
%let edit_type_Rollup_Reject		      ='' ; 
%let enctyp_code_Rollup_Reject	          ='' ;

%let edit_type_MEM_socsec_rej   ='';
%let enctyp_code_MEM_socsec_rej  ='';

/* B-261072 Adding the new varibales to list */

%let edit_type_NDCSVC_Procbill_rej ='';
%let enctyp_code_NDCSVC_Procbill_rej = '';

%let edit_type_NDCSVC_Proc_rej ='';
%let enctyp_code_NDCSVC_Proc_rej ='';

/*B-274606*/
%let edit_type_hiosid_reject              ='' ;
%let enctyp_code_hiosid_reject            ='' ;
/*B-274606*/

/* [B-335998] */
%let edit_type_RFPRVNPI              ='' ;
%let enctyp_code_RFPRVNPI            ='' ;

%let edit_type_RNPRVNPI              ='' ;
%let enctyp_code_RNPRVNPI            ='' ;

%let edit_type_ATPRVNPI              ='' ;
%let enctyp_code_ATPRVNPI            ='' ;

%let edit_type_OPPRVNPI              ='' ;
%let enctyp_code_OPPRVNPI            ='' ;

%let edit_type_BIPRVNPI              ='' ;
%let enctyp_code_BIPRVNPI            ='' ;



/*************************************************/
/* B-96285 QHP/APD toggle for _ctrl_tab filename */
/*************************************************/

%macro ctrl;
%if (&compno=45 and "&LOB." ne "MPPO") or (&compno=42 and "&LOB." eq "QHP") %then %do;	/*B-248997*/	/* B-121242 added HFIC and x12_ctrl format */
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

/*%if &compno. eq 42 and "&LOB." eq "QHP" %then %do;
	%let ctrl_tab = R42171_ctrl_tab;
%end;
%else %do;
	%let ctrl_tab = RGA191_ctrl_tab;
%end;*/
%put &x12_ctrl. &ctrl_tab.;
%mend;
%ctrl;
	
/***************************************************************************************************/      
/****                                  Hardcoding logic starts here                                                                                                                                            *******/
/***************************************************************************************************/      
proc sql noprint;
     select count(*) 
     into :cnt_3_hardcd
     from &RGA193..&ctrl_tab. 
     where Flag <> ' ' and Active='Y';

     %let  cnt_3_hardcd = &cnt_3_hardcd;

     select Variable_Name,var_value
     into :var_name1-:var_name&cnt_3_hardcd.,:var_value1-:var_value&cnt_3_hardcd.
     from &RGA193..&ctrl_tab. 
     where Flag <> ' ' and Active='Y' ;
  quit;
      
    
%macro hardcode ;
data _null_ ;
file ctrlout dropover  ;
%let i = 1 ;
%do %while(&i. <= &cnt_3_hardcd.);
put "&&var_name&i. = &&var_value&i.;" ;
%let i = %eval(&i. + 1);
%end;
run;
%mend hardcode ;
%hardcode;

/***************************************************************************************************/      
/****             Hardcoding logic ends here                                                 *******/
/***************************************************************************************************/ 
Data RGA191_Prof_ctrl_output_layout;
    SET &RGA193..&ctrl_tab. ;
	/* SAS2AWS2: ReplacedFunctionUpcase */
	where kupcase(file_type)='PROF' and kupcase(active)='Y';
run;
Data RGA191_Inst_ctrl_output_layout;
    SET &RGA193..&ctrl_tab. ;
	/* SAS2AWS2: ReplacedFunctionUpcase */
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

/**********************************************************************************************/
/***  Create user defined format using ADMXWALK control table to map NJ Adm Type						***/
/**********************************************************************************************/
data &RGA191..ADMXWALK1_&Compno.(keep=fmtname start label); 
		set &RGA193..admxwalk  end=eof; 
		length label $7.
			   start $15.;
		fmtname = "$Medicareadmtype" ;
		start = MhsAdmtyp ;
		label = Medicareadmtype;
		output;
		if eof then
		do;
			fmtname = "$Medicareadmtype" ;
		  	start = 'Other';
		  	label = '9';
		  	output;
		end;
	run;
	
	proc format cntlin=&RGA191..ADMXWALK1_&Compno.;
	run;
	
/**********************************************************************************************/
/***  Create user defined format using medsdisstatusxwalk to map Patient Status or Disposition Code	***/
/**********************************************************************************************/

data disstatus(keep=fmtname start label);
		set &libn2..medsdisstatusxwalk end=eof; 
		length label $7.
			   start $15.;
		fmtname = "$disstat" ;
		start = mhsdistat ;
		label = medsdistat;
		output;
		if eof then
		do;
			fmtname = "$disstat" ;
		  	start = 'Other';
		  	label = '  ';
		  	output;
		end;
	run;

	proc format cntlin=disstatus;
	run;


   /***************************************************************************************************/	
   /****			User defined format for condition code - Starts 							  *******/
   /***************************************************************************************************/	

	
	data cond_look_up(keep=fmtname start label) ;
		set &RGA193..condcdxwalk end=eof;
		length label $7.
			   start $15.;
		fmtname = "$condcd" ;
		start = valid_condcd;
		label = 'valid';
		output;
		if eof then
		do;
			fmtname = "$condcd" ;
		  	start = 'Other';
		  	label = 'invalid';
		  	output;
		end;
	run;

	proc format cntlin = cond_look_up ;
	run;
    

/**********************************************************************************************/
/*                Macro for Diagnosis code reassignment & Pointer Logic Starts Here            */
/***********************************************************************************************/

%macro pointer_diag;
array diag{25}    $     Prdiagcd DIAGD2  DIAGD3  DIAGD4 diagd5 diagd6 diagd7 diagd8 diagd9 diag10 diag11 diag12
                        diag13      diag14 diag15 diagn16 diag17 diag18 
						diag19 diag20 diag21 diag22 diag23 diag24 diag25; 
array poadiag{25}       $ poaprdiag poaDIAG2 poaDIAG3 poaDIAG4 poadiag5 poadiag6 poadiag7 poadiag8 poadiag9 poadiag10
						  poadiag11  poadiag12 poadiag13 poadiag14 poadiag15 poadiag16 poadiag17 poadiag18
						  poadiag19 poadiag20 poadiag21 poadiag22 poadiag23 poadiag24 poadiag25; 
array diag_flag{25}      diag_flag1-diag_flag25;
array diag_seq{25}       diag_seq1-diag_seq25; 
array diag_ptr{25}       ptr1-ptr25 ; 
array diag_NewDiag{25} $  NewDiag1-NewDiag25 ; 
array poadiag_NewDiag{25} $1.   newPoaDiag1-newPoaDiag25 ;
k=1;
do j=1 to 25;
 if diag{j} in (&icd10cd1.) then diag{j}=''; 
end;
do j=1 to 25; 
if poadiag{j}='1' then poadiag{j}='';
end;
i = 1;
do i = 1 to 25;
    if missing(diag{i}) = 1 then 
      do;
            diag_flag{i}      =  0 ;
            diag_seq{i}       = i ;
      end;
    else if missing(diag{i}) = 0 then 
      do;
            diag_flag{i}      =  1 ;
            diag_seq{i}       = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 25;
    if diag_flag{i} = 1 then 
      do;
	        diag_ptr{j} =j;
            diag_NewDiag{j}   =     diag{i} ;
            poadiag_NewDiag{j}      =     poadiag{i} ;
            j = j + 1 ;
      end;
    else if  diag_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j diag_flag1-diag_flag25 diag_seq1-diag_seq25 ;
%mend pointer_diag;


/**********************************************************************************************/
/*   Macro for Diagnosis code reassignment & Pointer Logic Ends Here                           */
/***********************************************************************************************/
/**********************************************************************************************/
/*                Macro for Svcline Modifier code reassignment Logic Starts Here                  */
/***********************************************************************************************/
%macro mod_cod;
array Mod_Cod{4} $           modcod  modcd2  modcd3  modcd4;
array mod_flag{4}            mod_flag1-mod_flag4;
array mod_seq{4}             mod_seq1-mod_seq4;
array arr_Mod_Cod{4} $       Mod_Cod1-Mod_Cod4 ;

i = 1 ;
do i = 1 to 4;
    if missing(Mod_Cod{i}) = 1 then 
      do;
            mod_flag{i} =  0 ;
            mod_seq{i} = i ;
      end;
    else if missing(Mod_Cod{i}) = 0 then 
      do;
            mod_flag{i} =  1 ;
            mod_seq{i} = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 4;
    if mod_flag{i} = 1 then 
      do;
            arr_Mod_Cod{j}    =     Mod_Cod{i} ;
            j = j + 1 ;
      end;
    else if  mod_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j mod_flag1-mod_flag4 mod_seq1-mod_seq4 ;
%mend mod_cod;

%macro mod_cod_mcr;
array Mod_Cod_mcr{4} $           modcod_mcr  modcd2_mcr  modcd3_mcr  modcd4_mcr;
array mod_flag_mcr{4}            mod_flag_mcr1-mod_flag_mcr4;
array mod_seq_mcr{4}             mod_seq_mcr1-mod_seq_mcr4;
array arr_Mod_Cod_mcr{4} $       Mod_Cod_mcr1-Mod_Cod_mcr4;

i = 1 ;
do i = 1 to 4;
    if missing(Mod_Cod_mcr{i}) = 1 then 
      do;
            mod_flag_mcr{i} =  0 ;
            mod_seq_mcr{i} = i ;
      end;
    else if missing(Mod_Cod_mcr{i}) = 0 then 
      do;
            mod_flag_mcr{i} =  1 ;
            mod_seq_mcr{i} = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 4;
    if mod_flag_mcr{i} = 1 then 
      do;
            arr_Mod_Cod_mcr{j}    =     Mod_Cod_mcr{i} ;
            j = j + 1 ;
      end;
    else if  mod_flag_mcr{i} = 0 then 
      do;
      end;
  end;
drop  i j mod_flag_mcr1-mod_flag_mcr4 mod_seq_mcr1-mod_seq_mcr4;
%mend mod_cod_mcr;

%macro CPT_cod;
array CPT_cod{4} $           CPTMD1 CPTMD2 CPTMD3 CPTMD4;
array CPT_flag{4}            CPT_flag1-CPT_flag4;
array CPT_seq{4}             CPT_seq1-CPT_seq4;
array arr_CPT_Cod{4} $       CPT_Cod1-CPT_Cod4 ;

i = 1 ;
do i = 1 to 4;
    /* SAS2AWS2: ReplacedFunctionLength */
    if missing(CPT_Cod{i}) = 1 or klength(CPT_Cod{i}) ne 2 then /* CR# CHG0032501 */
      do;
            CPT_flag{i} =  0 ;
            CPT_seq{i} = i ;
      end;
    else if missing(CPT_Cod{i}) = 0 then 
      do;
            CPT_flag{i} =  1 ;
            CPT_seq{i} = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 4;
    if CPT_flag{i} = 1 then 
      do;
            arr_CPT_Cod{j}    =     CPT_Cod{i} ;
            j = j + 1 ;
      end;
    else if  CPT_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j CPT_flag1-CPT_flag4 CPT_seq1-CPT_seq4 ;
%mend CPT_cod;

%macro CPT_cod_mcr;
array CPT_cod_mcr{4} $           CPTMD1_mcr CPTMD2_mcr CPTMD3_mcr CPTMD4_mcr;
array CPT_flag_mcr{4}            CPT_flag_mcr1-CPT_flag_mcr4;
array CPT_seq_mcr{4}             CPT_seq_mcr1-CPT_seq_mcr4;
array arr_CPT_Cod_mcr{4} $       CPT_Cod_mcr1-CPT_Cod_mcr4 ;

i = 1 ;
do i = 1 to 4;
    /* SAS2AWS2: ReplacedFunctionLength */
    if missing(CPT_Cod_mcr{i}) = 1 or klength(CPT_Cod_mcr{i}) ne 2 then /* CR# CHG0032501 */
      do;
            CPT_flag_mcr{i} =  0 ;
            CPT_seq_mcr{i} = i ;
      end;
    else if missing(CPT_Cod_mcr{i}) = 0 then 
      do;
            CPT_flag_mcr{i} =  1 ;
            CPT_seq_mcr{i} = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 4;
    if CPT_flag_mcr{i} = 1 then 
      do;
            arr_CPT_Cod_mcr{j}    =     CPT_Cod_mcr{i} ;
            j = j + 1 ;
      end;
    else if  CPT_flag_mcr{i} = 0 then 
      do;
      end;
  end;
drop  i j CPT_flag_mcr1-CPT_flag_mcr4 CPT_seq_mcr1-CPT_seq_mcr4 ;
%mend CPT_cod_mcr;
/**********************************************************************************************/
/*   Macro for Svcline Modifier code reassignment Logic Ends Here                           */
/***********************************************************************************************/
/* B-105917 changed all 6's within proc cod macro to 25 */
%macro proc_cod;		
/*array Proc_Cod{6}    $    PRPROCDR  PROCDR2-PROCDR6;   	B-105917 */
  array Proc_Cod{25}   $    Proc_Cod1-Proc_Cod25;
array Proc_flag{25}         Proc_flag1-Proc_flag25;
array Proc_seq{25}          Proc_seq1-Proc_seq25;
array arr_Proc_Cod{25} $    Proc_Cod1-Proc_Cod25 ;
/*array  Proc_dt{6}         PRPROCDT PROCDT2-PROCDT6;	  	B-105917 */
  array  Proc_dt{25}        Proc_dt1-Proc_dt25;
array arr_Proc_dt{25} $     Proc_dt1-Proc_dt25 ;
i = 1 ;
do i = 1 to 25;
if (proc_Cod{i}) in (&icd10cd2.) then proc_Cod{i} = "   " ;
if missing(proc_Cod{i}) = 1 then 
      do;
            proc_flag{i}      =  0 ;
            proc_seq{i}       = i ;
      end;
    else if missing(proc_Cod{i}) = 0 then 
      do;
            proc_flag{i}      =  1 ;
            proc_seq{i}       = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 25;
    if proc_flag{i} = 1 then 
      do;
            arr_proc_Cod{j}   =     proc_Cod{i} ;
            arr_Proc_dt{j}    =     Proc_dt{i}  ; 	/* put(Proc_dt{i},yymmddn8.) B-105917 */
            j = j + 1 ;
      end;
    else if  proc_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j proc_flag1-proc_flag25 proc_seq1-proc_seq25 ;
%mend proc_cod;

/**********************************************************************************************/
/*                Macro for  Procedure code reassignment Logic Ends Here                  */

/***********************************************************************************************/
/**********************************************************************************************/
/*                Macro for  OCCURRENCE SPAN Date reassignment Logic Starts Here               */
/***********************************************************************************************/
%macro occ_span_dt;
array occ_span_date{10}       $     IN_2300_HI01_4_OCC_SP_D1_tmp       IN_2300_HI02_4_OCC_SP_D2_tmp 
                                    IN_2300_HI03_4_OCC_SP_D3_tmp       IN_2300_HI04_4_OCC_SP_D4_tmp 
                                    IN_2300_HI05_4_OCC_SP_D5_tmp       IN_2300_HI06_4_OCC_SP_D6_tmp
                                    IN_2300_HI07_4_OCC_SP_D7_tmp       IN_2300_HI08_4_OCC_SP_D8_tmp 
                                    IN_2300_HI09_4_OCC_SP_D9_tmp       IN_2300_HI10_4_OCC_SP_D10_tmp 
                                    ;
array occ_span_flag{10}       occ_span_flag1-occ_span_flag10;
array occ_span_seq{10}        occ_span_seq1-occ_span_seq10;
array arr_occ_span{10} $20.   occ_span_dt1-occ_span_dt10 ;
i = 1 ;
do i = 1 to 10;
    if missing(occ_span_date{i}) = 1 then 
      do;
            occ_span_flag{i}  =  0 ;
            occ_span_seq{i}   = i ;
      end;
    else if missing(occ_span_date{i}) = 0 then 
      do;
            occ_span_flag{i}  =  1 ;
            occ_span_seq{i}   = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 10;
    if occ_span_flag{i} = 1 then 
      do;
            arr_occ_span{j}   =     occ_span_date{i} ;
            j = j + 1 ;
      end;
    else if  occ_span_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j occ_span_flag1-occ_span_flag10 occ_span_seq1-occ_span_seq10 ;
%mend occ_span_dt;
/**********************************************************************************************/
/*   Macro for  OCCURRENCE SPAN Date reassignment Logic Ends Here                           */
/***********************************************************************************************/
/**********************************************************************************************/
/*                Macro for  OCCURRENCE  Date reassignment Logic Starts Here                  */
/***********************************************************************************************/
*Rsubmit;
%macro occ_dt;
array occ_code{10}    $     occcd1	occcd2	occcd3	occcd4	occcd5	
							occcd6	occcd7	occcd8	occcd9	occcd10;
array occ_dt{10}    $     occdt1 occdt2 occdt3 occdt4 occdt5 
                            occdt6 occdt7 occdt8 occdt9 occdt10;
array occ_flag{10}          occ_flag1-occ_flag10;
array arr_occ_cd{10} $        occ_cd1-occ_cd10 ;
array arr_occ_dt{10}        occ_dt1-occ_dt10 ;  

i = 1 ;
do i = 1 to 10;
    if missing(occ_code{i}) = 1 then 
      do;
            occ_flag{i}     =  0 ;           
      end;
    else if missing(occ_code{i}) = 0 then 
      do;
            occ_flag{i}     =  1 ;           
      end;
  end;
j = 1 ;
  do i = 1 to 10;
    if occ_flag{i} = 1 then 
      do;
            arr_occ_cd{j}     =     occ_code{i} ;
			arr_occ_dt{j} =  occ_dt{i}; 
            j = j + 1 ;
      end;
    else if  occ_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j occ_flag1-occ_flag10  ;

%mend occ_dt;

/**********************************************************************************************/
/*   Macro for  OCCURRENCE  Date reassignment Logic Ends Here                           */
/***********************************************************************************************/

/**********************************************************************************************/
/*                Macro for  Value Code reassignment Logic Starts Here                  */
/***********************************************************************************************/
%macro val_cd;

array value_code{12}    $     valcd01 valcd02 valcd03 valcd04 valcd05 valcd06
                              valcd07 valcd08 valcd09 valcd10 valcd11 valcd12;
array value_amt{12}    $      valA01 valA02 valA03 valA04 valA05  valA06 valA07 
							  valA08 valA09 valA10 valA11  valA12; 
array value_flag{12}          value_flag1-value_flag12;
array value_seq{12}           value_seq1-value_seq12;
array arr_val_cd{12} $        val_cd1-val_cd12 ;
array arr_val_amt{12} $20.   val_amt1-val_amt12 ; 
if typecode="I" then do;
	do M=1 to 12;
		if value_code{M} in (&vcode.) then value_code{M}=''; 
	end;
end;
i = 1 ;
do i = 1 to 12;
    if missing(value_code{i}) = 1 then 
      do;
            value_flag{i}     =  0 ;
            value_seq{i}      = i ;
      end;
    else if missing(value_code{i}) = 0 then 
      do;
            value_flag{i}     =  1 ;
            value_seq{i}      = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 12;
    if value_flag{i} = 1 then 
      do;
            arr_val_cd{j}     =     value_code{i} ;
			arr_val_amt{j} =  value_amt{i}; 
            j = j + 1 ;
      end;
    else if  value_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j value_flag1-value_flag12 value_seq1-value_seq12 ;
%mend val_cd;

/**********************************************************************************************/
/*   Macro for   Value Code reassignment Logic Ends Here                           */
/***********************************************************************************************/
/**********************************************************************************************/
/*                Macro for Condition Code reassignment Logic Starts Here                  */
/***********************************************************************************************/
%macro cond_cd;
array condition_code{12}      $     concod1 concod2 concod3 concod4  concod5   concod6 
                                                concod7 concod8 concod9 concod10 concod11  concod12   ;
array condition_flag{12}            condition_flag1-condition_flag12;
array condition_seq{12}             condition_seq1-condition_seq12;
array arr_cond_cd{12}   $           cond_cd1-cond_cd12 ;

i = 1 ;
do i = 1 to 12;
cond_chk   = put(condition_code{i} ,$condcd. );
      if cond_chk = 'invalid' then
      do;
            condition_code{i} = '  ' ;
      end;
end;
i = 1 ;
do i = 1 to 12;
    if missing(condition_code{i}) = 1 then 
      do;
            condition_flag{i} =  0 ;
            condition_seq{i} = i ;
      end;
    else if missing(condition_code{i}) = 0 then 
      do;
            condition_flag{i} =  1 ;
            condition_seq{i} = i ;
      end;
  end;
j = 1 ;
  do i = 1 to 12;
    if condition_flag{i} = 1 then 
      do;
            arr_cond_cd{j}    =     condition_code{i} ;
            j = j + 1 ;
      end;
    else if  condition_flag{i} = 0 then 
      do;
      end;
  end;
drop  i j condition_flag1-condition_flag12 condition_seq1-condition_seq12 ;
%mend cond_cd;
/**********************************************************************************************/
/*   Macro for   Condition Code reassignment Logic Ends Here                           */
/***********************************************************************************************/

proc format;
      value $svccod
      '00100'-'01999' = 'MJ'
      '99100'-'99140' = 'MJ'      
      other = 'UN'
      ;
run;

/**********************************************************************************************/
/*                Macro for suppressing duplicate Dx codes Starts Here                        */
/**********************************************************************************************/
%macro dup_Dx_chk;
proc sort data=claim_diag; by clamno;                                                                                                                                 
run;
 
proc transpose data=claim_diag out=clm_diag1;                                                                                                    
 by clamno;                                                                                                                                 
 var &Keepv_ClmDiag.;                                                                                                                       
run;                                                                                                                                    
                                                                                                                                  
data clm_diag2 (drop=rc);
  length col1 $12.;
  declare hash found_keys();                                                                                                            
    found_keys.definekey('col1');                                                                                                       
    found_keys.definedone();                                                                                                            
                                                                                                                                        
  do while (not done);                                                                                                                  
    set clm_diag1 end=done;
	 by clamno; if first.clamno then rc=found_keys.clear();                                                                                     
     rc=found_keys.check();                                                                                                              
     if rc^=0 or missing(col1)=1 then do;                                                                                                                   
       rc=found_keys.add();                                                                                                              
       output;                                                                                                                           
     end;     
	 else
	 if rc=0 then do;                                                                                                                   
       col1=' '; 
       output;                                                                                                                           
     end;      
  end;                                                                                                                                  
  stop;                                                                                                                                 
run; 

proc transpose data=clm_diag2 out=claim_diag(drop=_name_);
 by clamno;                                                                                                                                 
 var col1;                                                                                                                              
run; 
%mend dup_Dx_chk;
/**********************************************************************************************/
/*                Macro for suppressing duplicate Dx codes End                                */
/**********************************************************************************************/

/**********************************************************************************************/
/*      Macro for OSDS F8 seg ICN value should be Clamno + Current run ISAnumber B-133711     */
/**********************************************************************************************/
%macro in_f8_icn;
%if "&x12_ctrl." ne "EIS" %then %do;	/* B-136184 */
	%if (&compno.=45 and "&LOB." ne "MPPO")  or (&compno.=42 and "&LOB." eq "QHP") %then %do; /*B-248997*/
		/* SAS2AWS2: ReplacedFunctionCompress */
		IN_2330B_ICN     = kcompress(clamno||sequence_number) ;
	%end;
	%else 
	%if &compno.=30 or (&compno.=45  and "&LOB." eq "MPPO") %then %do;		/*B-248997*/	/* B-145499 */
		/* SAS2AWS2: ReplacedFunctionCompress */
		IN_2330B_ICN_MCR = kcompress(clamno||sequence_number) ;

		IN_2330B_ICN_MCD = '                      ';
		IN_2330B_ICN1_MCD= '                      ';
		IN_2320_SGMT_HDR1_MCD='   ';
		IN_2320_SGMT_HDR3_MCD='   ';
		IN_2320_AMT01_MCD=' ';
		IN_2330A_SGMT_HDR1_MCD='   ';
		IN_2330A_NM101_IO_MCD='  ';
		IN_2330A_NM102_1_MCD=' ';
		IN_2330A_NM108_MI_MCD='  ';
		IN_2330A_SGMT_HDR2_MCD='  ';
		IN_2330A_SGMT_HDR3_MCD='  ';
		IN_2330B_SGMT_HDR1_MCD='   ';
		IN_2330B_NM101_MCD='  ';
		IN_2330B_NM102_MCD=' ';
		IN_2330B_NM108_MCD='  ';
		IN_2330B_SGMT_HDR2_MCD='  ';
		IN_2330B_SGMT_HDR3_MCD='  ';
		IN_2330B_SGMT_HDR4_MCD='   ';
 		IN_2330B_DTP01_MCD='  ';
		IN_2330B_DTP02_MCD='  ';
		IN_2330B_SGMT_HDR5_MCD='   ';
		IN_2330B_SGMT_HDR6_MCD='   ';
		IN_2330B_SGMT_HDR8_MCD='   ';
		IN_2330B_F8_MCD='  ';
		IN_2330B_SGMT_HDR9_MCD='   ';
		IN_2330B_BP_MCD='  ';
	%end;
	%else 
	%if &compno.=34 or ( &compno.=60 and &LOB. =NSF ) %then %do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		IN_2330B_ICN_MCR = kcompress(clamno||sequence_number) ;
		/* SAS2AWS2: ReplacedFunctionCompress */
		IN_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
	%else %if (&compno.=01 and &LOB. =DSNP) %then %do;
		IN_2330B_ICN_MCR = kcompress(IN_2330B_ICN_MCR||sequence_number) ;
		IN_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;	
	%else %do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		IN_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
%end;
%mend in_f8_icn;

%macro pr_f8_icn;
%if "&x12_ctrl." ne "EIS" %then %do;	/* B-136184 */
	%if (&compno.=45 and "&LOB." ne "MPPO") or (&compno.=42 and "&LOB." eq "QHP") %then %do; /*B-248997*/
		/* SAS2AWS2: ReplacedFunctionCompress */
		PR_2330B_ICN     = kcompress(clamno||sequence_number) ;
	%end;
	%else 
	%if &compno.=30 or (&compno.=45  and "&LOB." eq "MPPO")  %then %do;	/* B-145499 */ /*B-248997*/
		/* SAS2AWS2: ReplacedFunctionCompress */
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
		/* SAS2AWS2: ReplacedFunctionCompress */
		PR_2330B_ICN_MCR = kcompress(clamno||sequence_number) ;
		/* SAS2AWS2: ReplacedFunctionCompress */
		PR_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
	%else %if (&compno.=01 and &LOB. =DSNP) %then %do;
		PR_2330B_ICN_MCR = kcompress(PR_2330B_ICN_MCR||sequence_number) ;
		PR_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
	%else %do;
		/* SAS2AWS2: ReplacedFunctionCompress */
		PR_2330B_ICN_MCD = kcompress(clamno||sequence_number) ;
	%end;
%end;
%mend pr_f8_icn;

/* B-168796 added macro */
%macro dynamic_reject(VAL1= , DS=);
proc sql noprint;
select count(*)
      into :total
      from APD_enceditmatrix
      where encapdfld="&VAL1" and edittyp='H' ;
quit;

%let  total = &total ;
%if &total. > 0 %then %do;

proc sql noprint ;
    /* SAS2AWS2: ReplacedFunctionCompress */
    select kcompress(COStype),encTypCode,edittyp
                                                            
    into  :COStype1-:COStype&total.,
          :encTypCode1-:encTypCode&total.,
          :edittyp1-:edittyp&total.
    from APD_enceditmatrix 
    where encapdfld="&VAL1." and edittyp='H' ;
quit;  

data &DS;
length a $10000. x $50.;
	%let i = 1 ;           
     a = " ";
     X="&VAL1.";
  %if &compno. ne 45 and &compno. ne 30 and "&LOB." ne "QHP" %then %do;
	%do %while(&i.  <= &total );
		/* SAS2AWS2: ReplacedFunctionCatx */
		if &i.=1 then do; a= catx('' ,a," AND ((COStype eq '&&COStype&i.' and  typecode eq '&&encTypCode&i.')" );  end;
		/* SAS2AWS2: ReplacedFunctionCatx */
		if &i.=&total then do;  a= catx(' OR ' ,a," (COStype eq '&&COStype&i.' and  typecode eq '&&encTypCode&i.'))" );  end;
		/* SAS2AWS2: ReplacedFunctionCatx */
		else do; a= catx(' OR ' ,a,"(COStype eq '&&COStype&i.' and  typecode eq '&&encTypCode&i.')" );  end;
	%let i = %eval(&i. + 1);
	%end;
  %end;
  %else
  %do;
	%do %while(&i.  <= &total );
		/* SAS2AWS2: ReplacedFunctionCatx */
		if &i.=1 then do; a= catx('' ,a," AND ((typecode eq '&&encTypCode&i.')" );  end;
		/* SAS2AWS2: ReplacedFunctionCatx */
		if &i.=&total then do;  a= catx(' OR ' ,a," (typecode eq '&&encTypCode&i.'))" );  end;
		/* SAS2AWS2: ReplacedFunctionCatx */
		else do; a= catx(' OR ' ,a,"(typecode eq '&&encTypCode&i.')" );  end;
	%let i = %eval(&i. + 1);
	%end;
  %end; 
run;
%end;
%else
%do;
	data &DS;
    length a $10000. x $50.;
    a = "AND 'H' eq 'S'";
    X="&VAL1.";
	run;
%end;
%mend dynamic_reject;

%dynamic_reject(val1=proc_qual_Reject ,DS= DS1);
%dynamic_reject(val1=diag_qual_Reject ,DS= DS2);
%dynamic_reject(val1=line_reject,DS= DS3);

proc sql;
/* SAS2AWS2: ReplacedFunctionTrim */
select ktrim(a) into  :proc_qual_Reject_final from DS1;
/* SAS2AWS2: ReplacedFunctionTrim */
select ktrim(a) into  :diag_qual_Reject_final from DS2;
/* SAS2AWS2: ReplacedFunctionTrim */
select ktrim(a) into  :line_reject_final from DS3;
quit;

/**********************************************************************************************/
/*                Macro for  Dynamic Reject - Control file Approach -B-278970                */
/*********************************************************************************************/

%macro Internal_Reject_Dynamic(encap,input_dsn);
	
%let edit_type_&encap._rej = '';
%let enctyp_code_&encap._rej = '';

proc sql;
Select distinct kcompress(edittyp)  , kcompress("'"||enctypcode||"'") 
into :edit_type_&encap._rej, :enctyp_code_&encap._rej  SEPARATED by ','
from APD_enceditmatrix where upcase(encapdfld) = %upcase("&encap.") and edittyp='H';

data rej_ctrl_inp;	 
 infile "&drv2./Encounters_Reporting/APD/Control_Table/Internal_Reject_Ctrl.csv" recfm=v dsd dlm=',' lrecl=32767  firstobs = 2 ;
 length   encapdfld Output_dsn condn_flag condn1_var condn2_var condn3_var
         condn1_var_opr condn2_var_opr condn3_var_opr condn1_val condn2_val condn3_val
         condn1_var_rule condn2_var_rule edit_code edit_chk_type editcd_flag Recsubind $100. ;         
 input      encapdfld $   
           Output_dsn $
           condn_flag $
           condn1_var $
           condn1_var_opr $
           condn1_val $
           condn1_var_rule $           
           condn2_var $
           condn2_var_opr $
           condn2_val $
           condn2_var_rule $           
           condn3_var $           
           condn3_var_opr $       
           condn3_val $           
           edit_code $
           edit_chk_type $
           editcd_flag $
           Recsubind $  ;            
		   condn1_var_opr = condn1_var_opr;
		   condn1_val = "("||"'"||tranwrd(trim(compress(condn1_val)), ",", "','")||"'"||")";
		   condn2_var_opr = condn2_var_opr;
		   condn2_val = "("||"'"||tranwrd(trim(compress(condn2_val)), ",", "','")||"'"||")";
		   condn3_var_opr = condn3_var_opr;
		   condn3_val = "("||"'"||tranwrd(trim(compress(condn3_val)), ",", "','")||"'"||")";
	       edit_code = "'"||(compress(edit_code))||"'";
		   edit_chk_type = "'"||(compress(edit_chk_type))||"'";
		   editcd_flag = "'"||compress((editcd_flag))||"'";
		   Recsubind = "'"||compress((Recsubind))||"'";				   
		   array charvars(*) _character_;
		   do i=1 to dim(charvars);
		   charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		   end;
		   drop i;
		   row_cnt = _n_;
run;
proc sql;
	select condn_flag into : var_cnt from rej_ctrl_inp 
	where  upcase(encapdfld) = %upcase("&encap.");

	select 
       compress(encapdfld),
       compress(edit_code),
       compress(edit_chk_type),
       compress(editcd_flag),
       compress(Recsubind),
       compress(Output_dsn),
      %do i=1 %to &var_cnt.; 
       compress(condn&&i._var), 
       compress(condn&&i._var_opr),
       compress(condn&&i._val),
      %end;
      %do i=1 %to &var_cnt.-1; 
       compress(condn&&i._var_rule),     
      %end;
       row_cnt
       
 into :encapdfld,
      :edit_code,
      :edit_chk_type,
      :editcd_flag,
      :Recsubind,
      :Output_dsn,
    %do i=1 %to &var_cnt.; 
      :condn&&i._var,
      :condn&&i._var_opr,
      :condn&&i._val,
    %end;      
    %do i=1 %to &var_cnt.-1;
      :condn&&i._var_rule,
    %end;      
      :row_cnt
 from rej_ctrl_inp 
 where upcase(encapdfld) = %upcase("&encap."); /* macro calling parameter */
 quit;
 %if %SYMEXIST(condn1_var) > 0 %then %do; 
 
 data &input_dsn.
	    &RGA191..&Output_dsn.(keep = clamno lineno edit_chk_type edit_code encapdfld Recsubind typecode) ; 
    set  &input_dsn. ;
     if  
        %do i=1 %to &var_cnt.;
           %upcase(&&condn&i._var.) &&condn&i._var_opr. %upcase(&&condn&i._val.) 
             %if &i. lt &var_cnt. %then %do; 
               &&condn&i._var_rule.
             %end;
        %end;     
         then do;      	
	       edit_code     = &edit_code.;
	       encapdfld     = "&encapdfld."; 
	       edit_chk_type = &edit_chk_type.;
	       Recsubind     = &Recsubind.;
	       output &RGA191..&Output_dsn;	
	       delete;				
         end;
     else  output &input_dsn.;
run;
%end;
%else %do;
%put "Please check the error on your Rejection control file";
%end;
%mend;

%macro prvspcd_loop;

%do i=1 %to &NObs;

     %if &&tmp_compno&i. ne NA  %then %do;
           if 1=1  
     %end;    
     
     %if &&tmp_mcor&i. ne NA and (%sysfunc(find(&&tmp_mcor&i..,/)) > 0 ) %then %do;
             and  prxmatch(&&tmp_mcor&i..,(kupcase(MCORCAT))) > 0
     %end;   

     %if &&tmp_mcor&i. ne NA and  (%sysfunc(find(&&tmp_mcor&i..,/)) = 0 )    %then %do;
              and  kcompress(kupcase(MCORCAT)) in ( &&tmp_mcor&i..) 
     %end;
     
     %if &&tmp_formcd&i. ne NA %then %do;
              and  formcd in ( &&tmp_formcd&i..)  
     %end;      
     
         then do; 
 prvspcd_xwalk = "&&tmp_provspec&i."; 
 hierarchy = &&tmp_hierarchy&i.;
  end;

%end;
%mend;

%macro cos_loop;

%do i=1 %to &NObs;  

     %if &&cos_compno&i. ne NA  %then %do;
           if 1=1  
     %end;    
     
     %if &&cos_mmcorcat&i. ne NA and (%sysfunc(find(&&cos_mmcorcat&i..,/)) > 0 ) %then %do;
             and  prxmatch(&&cos_mmcorcat&i..,(kupcase(MCORCAT))) > 0
     %end;   

     %if &&cos_mmcorcat&i. ne NA and  (%sysfunc(find(&&cos_mmcorcat&i..,/)) = 0 )    %then %do;
              and  kcompress(kupcase(MCORCAT)) in ( &&cos_mmcorcat&i..) 
     %end;
     
     %if &&cos_formcd&i. ne NA %then %do;
              and  formcd in ( &&cos_formcd&i..)  
     %end;     
     
      %if &&cos_bilcod&i. ne NA %then %do;
              and  bilcod in ( &&cos_bilcod&i..)  
     %end;  
     
     %if &&cos_poscod&i. ne NA %then %do;
              and  poscod in ( &&cos_poscod&i..)  
     %end;  
     
     %if &&cos_prvspcd&i. ne NA %then %do;
              and  prvspcd in ( &&cos_prvspcd&i..)  
     %end; 
      
         then do; 
 cos1 = "&&cos_cos&i."; 
 hierarchy= &&cos_hierarchy&i.;
  end;

%end;
%mend;

/**********************************************************************************************/
/*                B-308002 - SD - providerspec - SQL file generation for taxonomy logic      */
/*********************************************************************************************/
%macro providerspec(fname,ind);
x "rm -f &drv2./Encounters_Reporting/APD/Control_Table/temp_&ind._all.txt";
filename xls&ind. "&drv2./Encounters_Reporting/APD/Control_Table/&fname..csv" ;
filename flt&ind. "&drv2./Encounters_Reporting/APD/Control_Table/&fname._out.csv" ;

data _null_;
	length record $32767.;
	infile xls&ind. delimiter=' ' lrecl = 32767 dsd truncover;
	input  record $;
	record=ktranslate(record,'|',',');
	record=ktranslate(record,'','"');
		if kindex(record,'(') > 0 then do;
			start = kindex(record,'(') + 1;
			i = start;
				do until (substr(record,i,1)=')');
	   				if substr(record,i,1) = '|' then do;
	   					substr(record,i,1) = ',';
       				end;
	   				i+1;
 				end;
		end; 
		
		file flt&ind. lrecl = 32767; 
		put record;
		array charvars(*) _character_;
		do i=1 to dim(charvars);
		charvars(i) = kcvt(charvars(i),"wlatin1","utf8");
		end;
		drop i;
run;

data mcorall_provspec;															
	length     hierarchy            $6.									
           provsp              	$15.								
		   subprovsp           	$6.									
           varname              $60.					
           start                $32767.									
           end                  $45.					
           varname2             $60.  
           ;
           
	infile flt&ind. delimiter='|' lrecl = 32767 dsd truncover;							
	input      hierarchy             $											
           provsp                $											
		   subprovsp	         $											
           varname               $											
           start                 $											
           end                   $						
           varname2              $					
           ;																					

	if _N_ = 1 then delete;	

	if klength(hierarchy) = 2 then hierarchy = '0'||hierarchy;
	if klength(hierarchy) = 3 then hierarchy = '0'||hierarchy; 
	keep  hierarchy provsp subprovsp varname start end varname2 ;
run;

proc sql noprint;
	select max(hierarchy) into :highhier from mcorall_provspec;
quit;

data mcorall_provspec;
	length table $25. compno $3.;
	table = 'MMCORPROVSPECMAP';
	compno = '01';
	set mcorall_provspec;
	length header_flag $15.;
	header_flag = '';
run;

%macro blank(fld,fldlabel,type);						

  	if &fld = &type then do;							
    	error=": PROVSP = "||provsp|| " &fldlabel (&fld) is missing.";	
		output blank_check;								
  		end;											 
 
%mend blank;

data blank_check;
	length error $120.;
	set mcorall_provspec;

	if hierarchy="&highhier" then
		return;
		
	%blank(hierarchy, Hierarchy Line Number, '');
	%blank(provsp, Provider Speciality Code, '');
	%blank(varname, Field Name, '');

	if varname2=' ' then
		do;
			%blank(start, Starting Field Value, '');
			%blank(end, Ending Field Value, '');
	end;
run;

data  errorlog_provspec;											   
			set blank_check;                             
run;
		
%nobs(errorlog_provspec);										
%let toterrs = &nobs;	

%if &toterrs > 0 %then %do;	 
	data errorlog_all;
		set errorlog_provspec;	 
	run;
%end;

%if &toterrs = 0 %then %do;	

data mcormapping_ds;	
	set mcorall_provspec;
	start=compress(start,"ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890`~!@#$%^&*()-_=+\|[]{};:',.<>?/ " , "kis");
	end=compress(end,"ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890`~!@#$%^&*()-_=+\|[]{};:',.<>?/ " , "kis");
	rename provsp=mcorcat subprovsp=submcorcat; 
run;

filename outfile "&drv2./Encounters_Reporting/APD/Control_Table/temp_&ind._all.txt";

%define_variables_enc(mcormapping_ds);
%define_mcorcat_pv_enc(mcormapping_ds);	

X "sed -i 's/mcorcat/prvspcd_txnxwlk/g' &drv2./Encounters_Reporting/APD/Control_Table/temp_&ind._all.txt";

%end;

%mend providerspec;
%providerspec(Taxonomy_Logic,enc);   


/* VVKR Code Start -- B-75041 */
data _null_;
put " ~~~~~~~~~~~~~~~ RGA19105 Program End execution ~~~~~~~~~~~~~~~";
run;
/* VVKR Code End -- B-75041 */



* PROGRAM: COST_SHARING_MEDICARE_DENTAL.SAS;
* DESCRIPTION: PERFORM COST SHARING ALGORITHM FOR MEDICARE DENTAL CLAIMS;


%MACRO DENTAL_MED_COST_SHARE(SERV_CAT);

  DATA DENTAL_BASEID;
    RETAIN JOIN_ID '1';
    SET &INC(WHERE=(CATEGORY IN('16a')));
  RUN;

  DATA DENTAL_FILE;
    SET PBP_FILE (KEEP =
                   CONTRACT_ID PLAN_ID SEGMENT_ID HPLAN
                   PBP_B16A_MAXENR_MC_AMT 
                   PBP_B16A_MAXENR_MC_PER
                   PBP_B16A_COPAY_MC_AMT
                   PBP_B16A_COPAY_MC_AMT_MIN
                   PBP_B16A_COINS_MC_PCT
                   PBP_B16A_COINS_MC_PCT_MIN
                   PBP_B16A_DED_MC_AMT
                   PBP_A_PLAN_TYPE
                   PBP_D_DIFF_DEDUCT_16A_N_AMT
                   );
    JOIN_ID = '1';
    
    ARRAY MX PBP_B16A_MAXENR_MC_AMT;
    ARRAY PX PBP_B16A_MAXENR_MC_PER;
  
    DO I = 1 TO 1;
      IF MX(I) NE . THEN DO;
        IF PX(I)  = '1' THEN MX(I) = MX(I)  / 3; * EVERY THREE YEARS;
        ELSE IF PX(I)  = '2' THEN MX(I) = MX(I)  / 2; * EVERY TWO YEARS;
        ELSE IF PX(I)  = '3' THEN MX(I) = MX(I) / 1; * EVERY YEAR;
        ELSE IF PX(I)  = '4' THEN MX(I) = MX(I)  * 2; * EVERY SIX MONTHS;
        ELSE IF PX(I)  = '5' THEN MX(I) = MX(I)  * 4; * EVERY THREE MONTHS;
        ELSE IF PX(I)  = '6' THEN MX(I) = MX(I) ; * OTHER;
        ELSE IF PX(I)  = '7' THEN MX(I) = MX(I)  * 12; * EVERY MONTH;        
      END;
    END;
  RUN;

  PROC SQL;
    CREATE TABLE CAT_JOIN AS
      (SELECT B.CATEGORY, B.BASEID, B.YEAR, B.VISITS, B.AMTTOT, P.*
      FROM DENTAL_BASEID B, DENTAL_FILE P
      WHERE B.JOIN_ID = P.JOIN_ID)
      ORDER BY HPLAN, BASEID, YEAR, CATEGORY;
    QUIT;
  RUN;
  
  DATA CAT_JOIN;
    MERGE CAT_JOIN (IN = A) 
          PBPCATS_MCCOST_DED (IN = B WHERE=(CATEGORY IN ('16a')));
    BY HPLAN BASEID YEAR CATEGORY;
    IF A;
    LENGTH PLAN_DED_CAT $1;
    IF B THEN PLAN_DED_CAT='1';
    ELSE PLAN_DED_CAT='0';
  RUN;
  
  DATA &SERV_CAT; 
    SET CAT_JOIN (DROP = JOIN_ID);

    COST_SHARE_COST = 0;
    OTHER_COST = 0;
    COST = 0;
    
    IF PLAN_DED_CAT='1' THEN DO;
      IF PLAN_DEDUCT_PTB>0 THEN CAT_DEDUCTIBLE=PLAN_DED_AMTTOT_MC_PTB;
      ELSE CAT_DEDUCTIBLE=PLAN_DED_AMTTOT_MC;
    END;
    ELSE IF PBP_A_PLAN_TYPE IN('04','31','44','47') THEN 
      CAT_DEDUCTIBLE=PBP_D_DIFF_DEDUCT_16A_N_AMT;
    ELSE CAT_DEDUCTIBLE=PBP_B16A_DED_MC_AMT;
    
    IF PBP_B16A_MAXENR_MC_AMT NE . THEN 
      BENE_MAXIMUM = PBP_B16A_MAXENR_MC_AMT;

    IF PBP_B16A_COPAY_MC_AMT > 0 THEN COPAY_MC_AMT = PBP_B16A_COPAY_MC_AMT;
    ELSE IF PBP_B16A_COPAY_MC_AMT_MIN > 0 THEN COPAY_MC_AMT = PBP_B16A_COPAY_MC_AMT_MIN;
    ELSE COPAY_MC_AMT = 0;
      
    IF PBP_B16A_COINS_MC_PCT > 0 THEN COINS_MC_PCT = PBP_B16A_COINS_MC_PCT;
    ELSE IF PBP_B16A_COINS_MC_PCT_MIN > 0 THEN COINS_MC_PCT = PBP_B16A_COINS_MC_PCT_MIN;
    ELSE COINS_MC_PCT = 0;

    IF CAT_DEDUCTIBLE = . THEN DO;
      COST = SUM(COST,(VISITS*COPAY_MC_AMT));
      COST = SUM(COST,AMTTOT*COINS_MC_PCT);
    END;
    ELSE DO;
      IF AMTTOT <= CAT_DEDUCTIBLE THEN DO;
        COST = AMTTOT;
      END;
      ELSE DO;
        COST = CAT_DEDUCTIBLE;
        DEDUCT_SHARE = CAT_DEDUCTIBLE / AMTTOT;
        AMTTOT = SUM(AMTTOT,-CAT_DEDUCTIBLE);
        VISITS = VISITS * (1 - DEDUCT_SHARE);
        COST = SUM(COST,(VISITS*COPAY_MC_AMT));
        COST = SUM(COST,AMTTOT*COINS_MC_PCT);
      END;
    END;

    IF BENE_MAXIMUM >= 0 AND COST >= 0 THEN
      COST = MIN(COST,BENE_MAXIMUM);

    COST_SHARE_COST = COST;

    COST_SHARE_COST_MC  = COST;
    COST_SHARE_COST_AMO = 0;
        
    CATEGORY = "16a";

  RUN;
  
  PROC SORT DATA = DENTAL;
    BY HPLAN;
  RUN;

  PROC APPEND DATA=DENTAL(KEEP=HPLAN YEAR BASEID COST_SHARE_COST
    COST_SHARE_COST_MC COST_SHARE_COST_AMO
    OTHER_COST COST CATEGORY AMTTOT)  BASE= COSTDATA FORCE;
  RUN;

  PROC DATASETS LIBRARY=WORK NODETAILS NOLIST;
    DELETE DENTAL DENTAL_BASEID DENTAL_FILE CAT_JOIN;
    QUIT;
  RUN;

%MEND;




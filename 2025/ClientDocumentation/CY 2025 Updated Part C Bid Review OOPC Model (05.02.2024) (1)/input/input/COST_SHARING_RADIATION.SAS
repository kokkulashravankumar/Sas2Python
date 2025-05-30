
* PROGRAM: COST_SHARING_RADIATION.SAS;
* DESCRIPTION: PERFORM COST SHARING ALGORITHM FOR RADIATION CLAIMS;

%MACRO RADIATION_COST_SHARE(SERV_CAT);

  PROC SUMMARY DATA=&INC NWAY MISSING;
    WHERE CATEGORY IN('8b1','8b2','8b3');
    CLASS BASEID YEAR;
    VAR AMTTOT;
    OUTPUT OUT=CAT_SUMMARY (DROP=_TYPE_ _FREQ_) SUM=;
  RUN;

  %obsnvars(CAT_SUMMARY);
  
  DATA FMTDATA;
    SET CAT_SUMMARY END=LAST;
    LENGTH HLO $1 START $12;
    FMTNAME='$CATFMT';
    START=BASEID || YEAR;
    LABEL=AMTTOT;
    HLO=' ';
    OUTPUT;
    IF LAST THEN DO;
      START='OTHER';
      LABEL=0;
      HLO='O';
      OUTPUT;
    END;
    KEEP FMTNAME START LABEL HLO;
  RUN;

  PROC FORMAT CNTLIN=FMTDATA;
  RUN;
  PROC SUMMARY DATA=&INC NWAY MISSING;
      WHERE CATEGORY IN('8b2');
      CLASS BASEID YEAR;
      VAR AMTTOT;
      OUTPUT OUT=CAT_SUB_SUMMARY (DROP=_TYPE_ _FREQ_) SUM=;
  RUN;
    
  %obsnvars(CAT_SUB_SUMMARY);
    
  DATA FMTDATAS;
    SET CAT_SUB_SUMMARY END=LAST;
    LENGTH HLO $1 START $12;
    FMTNAME='$CATSFMT';
    START=BASEID || YEAR;
    LABEL=AMTTOT;
    HLO=' ';
    OUTPUT;
    IF LAST THEN DO;
      START='OTHER';
      LABEL=0;
      HLO='O';
      OUTPUT;
    END;
    KEEP FMTNAME START LABEL HLO;
    RUN;
      
  PROC FORMAT CNTLIN=FMTDATAS;
  RUN;

  DATA RADIATION_BASEID;
    RETAIN JOIN_ID '1';
    SET &INC(WHERE=(CATEGORY='8b2'));
  RUN;

  DATA RADIATION_FILE;
    SET PBP_FILE (KEEP =
                  CONTRACT_ID PLAN_ID SEGMENT_ID HPLAN
                  PBP_B8B_MAXENR_AMT 
                  PBP_B8B_MAXENR_PER
                  PBP_B8B_COPAY_AMT_TMC 
                  PBP_B8B_COPAY_AMT_TMC_MAX
                  PBP_B8B_COINS_PCT_TMC 
                  PBP_B8B_COINS_PCT_TMC_MAX
                  PBP_B8A_DED_AMT
                  PBP_B8B_COINS_EHC
                  PBP_B8B_COPAY_EHC
                  PBP_B8B_DED_AMT
                  PBP_A_PLAN_TYPE
                  PBP_D_DIFF_DEDUCT_8B2_AMT
                  PBP_B8B_COPAY_MAX_YN);
    JOIN_ID = '1';
    %ANNUALIZATION(PBP_B8B_MAXENR_PER,PBP_B8B_MAXENR_AMT);
  RUN;

  PROC SQL;
    CREATE TABLE CAT_JOIN AS
      (SELECT B.CATEGORY, B.BASEID, B.VISITS, B.AMTTOT, B.YEAR, B.FROM_DT, P.*
      FROM RADIATION_BASEID B, RADIATION_FILE P
      WHERE B.JOIN_ID = P.JOIN_ID)
      ORDER BY HPLAN, BASEID, YEAR, FROM_DT, CATEGORY;
    QUIT;
  RUN;

  DATA CAT_JOIN;
    MERGE CAT_JOIN (IN = A) 
          PBPCATS_MCCOST_DED (IN = B WHERE=(CATEGORY='8b2'));
    BY HPLAN BASEID YEAR CATEGORY;
    IF A;
    LENGTH PLAN_DED_CAT $1;
    IF B THEN PLAN_DED_CAT='1';
    ELSE PLAN_DED_CAT='0';
  RUN;

  PROC SORT DATA=CAT_JOIN;
    BY HPLAN BASEID YEAR FROM_DT;
  RUN;

  DATA RADIATION_COST;
    SET CAT_JOIN (DROP = JOIN_ID);

    BY HPLAN BASEID YEAR FROM_DT;
    BASEIDYR=BASEID || YEAR;
    %IF "&NOBS." > "0" %THEN %DO;
      PBPCAT_AMTTOT=INPUT(PUT(BASEIDYR,$CATFMT.),8.);
      PBPSUBCAT_AMTTOT=INPUT(PUT(BASEIDYR,$CATSFMT.),8.);
    %END;
    %ELSE %DO;
      PBPCAT_AMTTOT=0;
      PBPSUBCAT_AMTTOT=0;
    %END;

    RETAIN CAT_DEDUCTIBLE;
    
    IF FIRST.YEAR THEN DO;
      COST = 0;
      CAT_DEDUCTIBLE=0;

      IF PLAN_DED_CAT='1' THEN DO;
        IF PLAN_DED_AMTTOT_MC_PTB>0 THEN CAT_DEDUCTIBLE=PLAN_DED_AMTTOT_MC_PTB;
        ELSE CAT_DEDUCTIBLE=PLAN_DED_AMTTOT_MC;
      END;
      ELSE IF PBP_A_PLAN_TYPE IN('04','31','44','47') THEN 
        PBP_CAT_DEDUCTIBLE=PBP_D_DIFF_DEDUCT_8B2_AMT;
      ELSE PBP_CAT_DEDUCTIBLE=PBP_B8B_DED_AMT;
                    
      IF PLAN_DED_CAT^='1' & PBPCAT_AMTTOT>0 THEN
        CAT_DEDUCTIBLE=PBP_CAT_DEDUCTIBLE*(PBPSUBCAT_AMTTOT/PBPCAT_AMTTOT);
    END;


    IF CAT_DEDUCTIBLE = . THEN DO;
      IF SUBSTR(PBP_B8B_COPAY_EHC,3,1)='1' THEN DO;
        IF PBP_B8B_COPAY_MAX_YN='1' THEN 
          COST = SUM(COST, MIN((VISITS*PBP_B8B_COPAY_AMT_TMC),PBP_B8B_COPAY_AMT_TMC_MAX));
        ELSE COST = SUM(COST,(VISITS*PBP_B8B_COPAY_AMT_TMC));
      END;
      IF SUBSTR(PBP_B8B_COINS_EHC,3,1)='1' THEN 
        COST = SUM(COST,(AMTTOT*PBP_B8B_COINS_PCT_TMC));
    END;
    ELSE DO;
      IF AMTTOT <= CAT_DEDUCTIBLE THEN DO;
        CAT_DEDUCTIBLE=SUM(CAT_DEDUCTIBLE,-AMTTOT);
        COST = AMTTOT;
      END;
      ELSE DO;
        COST = CAT_DEDUCTIBLE;
        DEDUCT_SHARE = CAT_DEDUCTIBLE / AMTTOT;
        AMTTOT = SUM(AMTTOT,-CAT_DEDUCTIBLE);
        VISITS = VISITS * (1 - DEDUCT_SHARE);
        IF SUBSTR(PBP_B8B_COPAY_EHC,3,1)='1' THEN DO;
          IF PBP_B8B_COPAY_MAX_YN='1' THEN 
            COST = SUM(COST, MIN((VISITS*PBP_B8B_COPAY_AMT_TMC),PBP_B8B_COPAY_AMT_TMC_MAX));
          ELSE COST = SUM(COST,(VISITS*PBP_B8B_COPAY_AMT_TMC));
        END;
        IF SUBSTR(PBP_B8B_COINS_EHC,3,1)='1' THEN 
          COST = SUM(COST,(AMTTOT*PBP_B8B_COINS_PCT_TMC));
        CAT_DEDUCTIBLE=0;
      END;
    END;

    CATEGORY = "8b2";
  RUN;

  PROC SUMMARY DATA=RADIATION_COST NWAY MISSING;
    CLASS HPLAN BASEID YEAR CATEGORY;
    ID PBP_B8B_MAXENR_AMT;
    VAR COST AMTTOT;
    OUTPUT OUT=RADIATION_SUM (DROP=_TYPE_ _FREQ_) SUM=;
  RUN;

  DATA &SERV_CAT;
    SET RADIATION_SUM;
    OTHER_COST = 0;
    COST_SHARE_COST = 0;

    BASEIDYR=BASEID || YEAR;
    %IF "&NOBS." > "0" %THEN %DO;
      PBPCAT_AMTTOT=INPUT(PUT(BASEIDYR,$CATFMT.),8.);
    %END;
    %ELSE %DO;
      PBPCAT_AMTTOT=0;
    %END;

    IF PBP_B8B_MAXENR_AMT NE . THEN 
      BENE_MAXIMUM = PBP_B8B_MAXENR_AMT;
       
    IF PBPCAT_AMTTOT>0 THEN
      BENE_MAXIMUM=BENE_MAXIMUM*(AMTTOT/PBPCAT_AMTTOT);
    
    IF BENE_MAXIMUM >= 0 AND COST >= 0 THEN 
      COST = MIN(COST,BENE_MAXIMUM);

    COST_SHARE_COST = COST;
    COST_SHARE_COST_MC = COST;
    COST_SHARE_COST_AMO = 0;
  RUN;

  PROC APPEND DATA=RADIATION(KEEP=HPLAN YEAR BASEID COST_SHARE_COST 
    COST_SHARE_COST_MC COST_SHARE_COST_AMO
    OTHER_COST COST CATEGORY AMTTOT)  BASE= COSTDATA FORCE;
  RUN;

  PROC DATASETS LIBRARY=WORK NODETAILS NOLIST;
    DELETE RADIATION RADIATION_BASEID RADIATION_FILE CAT_JOIN RADIATION_COST RADIATION_SUM;
    QUIT;
  RUN;

%MEND;

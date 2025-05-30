* PROGRAM NAME: EXCEPT_TA.SAS;

LIBNAME LIBRARY "&INPUTDIR";

%MACRO EXCEPT_TA;
  
  DATA PBP_PLANS_COV (DROP=BASE_RXCUI RENAME = TIER_LEVEL_VALUE = TIER_LEVEL) 
       PBP_PLANS_EXCEPTIONS (DROP=BASE_RXCUI) 
       PBP_PLANS_THERASUB (DROP=BASE_RXCUI);
    SET PBP_PLANS;
    IF TIER_LEVEL_VALUE = '9' THEN DO;
      LENGTH BASE_RXCUI $17;
      BASE_RXCUI = LEFT(PUT(BASEID,8.)) || "_" || LEFT(PUT(ORXCUI,8.));
      IF PUT(BASE_RXCUI,$NFFMT.) = '2' THEN OUTPUT PBP_PLANS_THERASUB;
      ELSE IF PUT(BASE_RXCUI,$NFFMT.) = '3' THEN OUTPUT PBP_PLANS_EXCEPTIONS;
      ELSE OUTPUT PBP_PLANS_COV;
    END;
    ELSE OUTPUT PBP_PLANS_COV;
  RUN;

  PROC DATASETS;
    DELETE PBP_PLANS;
  RUN;  
  
  DATA PBP_PLANS_EXCEPTIONS;
    SET PBP_PLANS_EXCEPTIONS;
    LENGTH EXTENDED_TIER_ID $1;
    %IF "&BENTYPE."="DS" %THEN %DO;
       EXTENDED_TIER_ID = '1';
    %END;
    %ELSE %IF "&BENTYPE."="DSFORM" %THEN %DO;
       EXTENDED_TIER_ID = '1';
    %END;    
    %ELSE %DO;
       IF MRX_TIER_FORM_EX_2_YN = '1' AND DRUG_TYPE = 'G' THEN EXTENDED_TIER_ID = MRX_TIER_FORM_EX_2;
       ELSE EXTENDED_TIER_ID = MRX_TIER_FORM_EX;
    %END;
  RUN;
  
  PROC SORT DATA = PBP_PLANS_EXCEPTIONS;
    BY HPLAN EXTENDED_TIER_ID;
  RUN;
  
  %IF "&BENTYPE."="DS" %THEN %DO;

    DATA PBP_PLANS_EX;
       MERGE PBP_PLANS_EXCEPTIONS (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_IRA_COPAY
             RENAME = EXTENDED_TIER_ID = TIER_LEVEL) 
             PBP_EXTRACT (IN = B DROP = TIER_LEVEL);
       BY HPLAN;
       IF A AND B; 
       KEEP FORMULARY_ID TIER_LEVEL AVG_PRICE DRUG_TYPE
          BASEID RX_SCRIPT_ORDER RXCUI ORXCUI SCRIPT_CNT30
          HPLAN PLAN_DEDUCTIBLE PLAN_ICP 
          PLAN_IRA_COPAY;
    RUN;
    
  %END;
  %ELSE %IF "&BENTYPE."="AE" %THEN %DO;
    
     DATA PBP_PLANS_EX;
       MERGE PBP_PLANS_EXCEPTIONS (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_COPAY PLAN_COINS PLAN_IRA_COPAY 
             MRX_TIER_CSTSHR_STRUCT_TYPE
             MRX_TIER_FORM_EX MRX_TIER_FORM_EX_2_YN MRX_TIER_FORM_EX_2
             RENAME = EXTENDED_TIER_ID = TIER_LEVEL) 
             PBP_EXTRACT (IN = B);
       BY HPLAN TIER_LEVEL;
       IF A AND B; 
       KEEP TIER_LEVEL AVG_PRICE DRUG_TYPE BASEID RX_SCRIPT_ORDER
           RXCUI ORXCUI SCRIPT_CNT30
           FORMULARY_ID HPLAN PLAN_DEDUCTIBLE PLAN_ICP
           PLAN_COPAY PLAN_COINS PLAN_IRA_COPAY 
           MRX_TIER_CSTSHR_STRUCT_TYPE;         
     RUN;

  %END;

  %ELSE %IF "&BENTYPE."="BA" %THEN %DO;
    
     DATA PBP_PLANS_EX;
       MERGE PBP_PLANS_EXCEPTIONS (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_COPAY PLAN_COINS PLAN_COINS_PRE PLAN_COPAY_PRE
             MRX_FIRST_FILL MRX_ALT_DED_TYPE_YN MRX_ALT_NO_DED_TIER 
             MRX_ALT_GEN_CSTSHR_STRUCT MRX_ALT_PRE_ICL_COST_SHARE
             MRX_TIER_CSTSHR_STRUCT_TYPE MRX_TIER_FORM_EX MRX_TIER_FORM_EX_2_YN MRX_TIER_FORM_EX_2
             PLAN_IRA_COPAY
             RENAME = EXTENDED_TIER_ID = TIER_LEVEL) 
             PBP_EXTRACT (IN = B);
       BY HPLAN TIER_LEVEL;
       IF A AND B; 
       KEEP TIER_LEVEL AVG_PRICE DRUG_TYPE BASEID RX_SCRIPT_ORDER
           RXCUI ORXCUI SCRIPT_CNT30
           FORMULARY_ID HPLAN PLAN_DEDUCTIBLE PLAN_ICP PLAN_COPAY PLAN_COINS
           PLAN_COINS_PRE PLAN_COPAY_PRE PLAN_IRA_COPAY
           MRX_FIRST_FILL MRX_ALT_DED_TYPE_YN
           MRX_ALT_NO_DED_TIER MRX_ALT_GEN_CSTSHR_STRUCT 
           MRX_ALT_PRE_ICL_COST_SHARE MRX_TIER_CSTSHR_STRUCT_TYPE;
     RUN;
          
  %END;

  %ELSE %IF "&BENTYPE."="EA" %THEN %DO;

     DATA PBP_PLANS_EX;
       MERGE PBP_PLANS_EXCEPTIONS (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_COPAY PLAN_COINS
             PLAN_COINS_PRE PLAN_COPAY_PRE MRX_FIRST_FILL MRX_ALT_DED_TYPE_YN
             MRX_ALT_NO_DED_TIER MRX_ALT_PRE_ICL_COST_SHARE
             MRX_TIER_CSTSHR_STRUCT_TYPE MRX_ALT_GEN_CSTSHR_STRUCT MRX_TIER_FORM_EX
             MRX_TIER_FORM_EX_2_YN MRX_TIER_FORM_EX_2 PLAN_IRA_COPAY
             RENAME = EXTENDED_TIER_ID = TIER_LEVEL) 
             PBP_EXTRACT (IN = B);
       BY HPLAN TIER_LEVEL;
       IF A AND B; 
       KEEP TIER_LEVEL AVG_PRICE DRUG_TYPE BASEID RX_SCRIPT_ORDER
            RXCUI ORXCUI SCRIPT_CNT30
            FORMULARY_ID HPLAN PLAN_DEDUCTIBLE PLAN_ICP PLAN_COPAY PLAN_COINS
            PLAN_COINS_PRE PLAN_COPAY_PRE MRX_FIRST_FILL PLAN_IRA_COPAY
            MRX_ALT_DED_TYPE_YN MRX_ALT_NO_DED_TIER
            MRX_ALT_PRE_ICL_COST_SHARE MRX_TIER_CSTSHR_STRUCT_TYPE
            MRX_ALT_GEN_CSTSHR_STRUCT;
     RUN;

  %END;
  
  %ELSE %IF "&BENTYPE."="DSFORM" %THEN %DO;

    DATA PBP_PLANS_EX;
       MERGE PBP_PLANS_EXCEPTIONS (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_IRA_COPAY
             RENAME = EXTENDED_TIER_ID = TIER_LEVEL) 
             PBP_EXTRACT (IN = B DROP = TIER_LEVEL);
       BY HPLAN;
       IF A AND B; 
       KEEP FORMULARY_ID TIER_LEVEL AVG_PRICE DRUG_TYPE
          BASEID RX_SCRIPT_ORDER RXCUI ORXCUI SCRIPT_CNT30
          HPLAN PLAN_DEDUCTIBLE PLAN_ICP 
          PLAN_IRA_COPAY;
    RUN;
    
  %END;  

  PROC SORT DATA = PBP_PLANS_EX;
    BY HPLAN BASEID RX_SCRIPT_ORDER RXCUI TIER_LEVEL AVG_PRICE ORXCUI;
  RUN;  

  DATA PBP_PLANS_THERASUB;
    SET PBP_PLANS_THERASUB;
    LENGTH TAG_ID $3 FIDTAG $11 TIER_LEVEL $1;
    TAG_ID = PUT(RXCUI,TAGFMT.);
    FIDTAG = FORMULARY_ID || TAG_ID;
    TIER_LEVEL = PUT(FIDTAG,$FGTFMT.);
    TAG_AVG_PRICE = INPUT(PUT(FIDTAG,$FTAGFMT.),8.);
    IF INPUT(PUT(FIDTAG,$FRXCFMT.),8.) ^=. THEN 
      RXCUI = INPUT(PUT(FIDTAG,$FRXCFMT.),8.);
  RUN;

  PROC SORT DATA = PBP_PLANS_THERASUB;
    BY HPLAN TIER_LEVEL;    
  RUN;      
  
  %IF "&BENTYPE."="DS" %THEN %DO;

    DATA PBP_PLANS_TS;
       MERGE PBP_PLANS_THERASUB (IN = A DROP = PLAN_DEDUCTIBLE PLAN_ICP 
             PLAN_IRA_COPAY) 
             PBP_EXTRACT (IN = B DROP = TIER_LEVEL FORMULARY_ID);
       BY HPLAN;
       IF A AND B; 
       KEEP FORMULARY_ID TIER_LEVEL AVG_PRICE DRUG_TYPE
          BASEID RX_SCRIPT_ORDER RXCUI ORXCUI SCRIPT_CNT30
          HPLAN PLAN_DEDUCTIBLE PLAN_ICP 
          TAG_AVG_PRICE PLAN_IRA_COPAY;
    RUN;
    
  %END;
  %ELSE %IF "&BENTYPE."="AE" %THEN %DO;
    
     DATA PBP_PLANS_TS;
       MERGE PBP_PLANS_THERASUB (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_COPAY PLAN_COINS PLAN_IRA_COPAY
             MRX_TIER_CSTSHR_STRUCT_TYPE
             MRX_TIER_FORM_EX MRX_TIER_FORM_EX_2_YN MRX_TIER_FORM_EX_2) 
             PBP_EXTRACT (IN = B);
       BY HPLAN TIER_LEVEL;
       IF A AND B; 
       KEEP TIER_LEVEL AVG_PRICE DRUG_TYPE BASEID RX_SCRIPT_ORDER
           RXCUI ORXCUI SCRIPT_CNT30
           FORMULARY_ID HPLAN PLAN_DEDUCTIBLE PLAN_ICP
           PLAN_COPAY PLAN_COINS PLAN_IRA_COPAY
           MRX_TIER_CSTSHR_STRUCT_TYPE TAG_AVG_PRICE;         
     RUN;

  %END;

  %ELSE %IF "&BENTYPE."="BA" %THEN %DO;
    
     DATA PBP_PLANS_TS;
       MERGE PBP_PLANS_THERASUB (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_COPAY PLAN_COINS PLAN_COINS_PRE PLAN_COPAY_PRE PLAN_IRA_COPAY
             MRX_FIRST_FILL MRX_ALT_DED_TYPE_YN MRX_ALT_NO_DED_TIER
             MRX_ALT_GEN_CSTSHR_STRUCT MRX_ALT_PRE_ICL_COST_SHARE
             MRX_TIER_CSTSHR_STRUCT_TYPE MRX_TIER_FORM_EX MRX_TIER_FORM_EX_2_YN MRX_TIER_FORM_EX_2) 
             PBP_EXTRACT (IN = B);
       BY HPLAN TIER_LEVEL;
       IF A AND B; 
       KEEP TIER_LEVEL AVG_PRICE DRUG_TYPE BASEID RX_SCRIPT_ORDER
           RXCUI ORXCUI SCRIPT_CNT30
           FORMULARY_ID HPLAN PLAN_DEDUCTIBLE PLAN_ICP PLAN_COPAY PLAN_COINS
           PLAN_COINS_PRE PLAN_COPAY_PRE PLAN_IRA_COPAY
           MRX_FIRST_FILL MRX_ALT_DED_TYPE_YN
           MRX_ALT_NO_DED_TIER MRX_ALT_GEN_CSTSHR_STRUCT 
           MRX_ALT_PRE_ICL_COST_SHARE MRX_TIER_CSTSHR_STRUCT_TYPE TAG_AVG_PRICE;
     RUN;
          
  %END;

  %ELSE %IF "&BENTYPE."="EA" %THEN %DO;

     DATA PBP_PLANS_TS;
       MERGE PBP_PLANS_THERASUB (IN = A DROP = FORMULARY_ID PLAN_DEDUCTIBLE PLAN_ICP
             PLAN_COPAY PLAN_COINS PLAN_IRA_COPAY
             PLAN_COINS_PRE PLAN_COPAY_PRE MRX_FIRST_FILL MRX_ALT_DED_TYPE_YN
             MRX_ALT_NO_DED_TIER MRX_ALT_PRE_ICL_COST_SHARE
             MRX_TIER_CSTSHR_STRUCT_TYPE MRX_ALT_GEN_CSTSHR_STRUCT MRX_TIER_FORM_EX
             MRX_TIER_FORM_EX_2_YN MRX_TIER_FORM_EX_2) 
             PBP_EXTRACT (IN = B);
       BY HPLAN TIER_LEVEL;
       IF A AND B; 
       KEEP TIER_LEVEL AVG_PRICE DRUG_TYPE BASEID RX_SCRIPT_ORDER
            RXCUI ORXCUI SCRIPT_CNT30
            FORMULARY_ID HPLAN PLAN_DEDUCTIBLE PLAN_ICP PLAN_COPAY PLAN_COINS
            PLAN_IRA_COPAY
            PLAN_COINS_PRE PLAN_COPAY_PRE MRX_FIRST_FILL
            MRX_ALT_DED_TYPE_YN MRX_ALT_NO_DED_TIER
            MRX_ALT_PRE_ICL_COST_SHARE MRX_TIER_CSTSHR_STRUCT_TYPE
            MRX_ALT_GEN_CSTSHR_STRUCT TAG_AVG_PRICE;
     RUN;

  %END;

  %ELSE %IF "&BENTYPE."="DSFORM" %THEN %DO;

    DATA PBP_PLANS_TS;
       MERGE PBP_PLANS_THERASUB (IN = A DROP = PLAN_DEDUCTIBLE PLAN_ICP 
             PLAN_IRA_COPAY) 
             PBP_EXTRACT (IN = B DROP = TIER_LEVEL FORMULARY_ID);
       BY HPLAN;
       IF A AND B; 
       KEEP FORMULARY_ID TIER_LEVEL AVG_PRICE DRUG_TYPE
          BASEID RX_SCRIPT_ORDER RXCUI ORXCUI SCRIPT_CNT30
          HPLAN PLAN_DEDUCTIBLE PLAN_ICP 
          TAG_AVG_PRICE PLAN_IRA_COPAY;
    RUN;
    
  %END;

  PROC SORT DATA = PBP_PLANS_TS;
    BY HPLAN BASEID RX_SCRIPT_ORDER RXCUI TIER_LEVEL AVG_PRICE ORXCUI; 
  RUN;
      
  DATA PBP_PLANS (RENAME = TIER_LEVEL = TIER_LEVEL_VALUE);
    SET PBP_PLANS_COV (IN = A) 
        PBP_PLANS_EX (IN = B) 
        PBP_PLANS_TS (IN = C);
    BY HPLAN BASEID RX_SCRIPT_ORDER RXCUI TIER_LEVEL AVG_PRICE ORXCUI; 
    IF C THEN DO;
      IF TIER_LEVEL ^= '9' THEN AVG_PRICE = TAG_AVG_PRICE;
    END;
  RUN;
  
  PROC DATASETS;
    DELETE PBP_PLANS_COV PBP_PLANS_EX PBP_PLANS_TS PBP_PLANS_THERASUB PBP_PLANS_EXCEPTIONS;
  RUN;

%MEND;

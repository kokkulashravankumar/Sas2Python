* PROGRAM NAME: JSON_IMPORT.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

LIBNAME JSONLIB "&INPUTJSONDIR.";

PROC SQL;
  CREATE TABLE PBP_JSON AS
  SELECT PBP.CONTRACTID AS PBP_A_CONTRACT_NUMBER, PBP.PLANID AS PBP_A_PLAN_IDENTIFIER, PBP.SEGMENTID AS PBP_A_SEGMENT_ID,
    PBP_PLANCHARACT.HPMSPLANTYPE as PBP_A_PLAN_TYPE,
    PBP_PLANCHARACT.PLANNAME as PBP_A_PLAN_NAME,
    PUT(CAT(PBP.CONTRACTID,PBP.PLANID,PUT(PBP.SEGMENTID,Z3.)),$11.) AS QID,
    (CASE
      WHEN UPCASE(PBP_PLANCHARACT.ISOFFERRX) = 'YES' THEN '1'
      ELSE '2'
    END) AS MRX_DRUG_BEN_YN,
    (CASE
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'LOCAL CCP' THEN '01'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'MSA' THEN '02'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'RFB' THEN '03'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'PFFS' THEN '04'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'DEMO' THEN '05'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = '1876 COST' THEN '06'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'HCPP - 1833 COST' THEN '07'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'NATIONAL PACE' THEN '08'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'PDP' THEN '10'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'REGIONAL CCP' THEN '11'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'FALLBACK' THEN '12'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'EMPLOYER/UNION ONLY DIRECT CONTRACT PDP' THEN '13'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'EMPLOYER/UNION ONLY DIRECT CONTRACT PFFS' THEN '14'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'RFB LOCAL CCP' THEN '15'
      WHEN UPCASE(PBP_PLANCHARACT.ORGANIZATIONTYPE) = 'EMPLOYER/UNION ONLY DIRECT CONTRACT LOCAL CCP' THEN '17'
      ELSE ' '
    END) AS PBP_A_ORG_TYPE     
  FROM JSONLIB.PBP AS PBP,
    JSONLIB.PBP_PLANCHARACT AS PBP_PLANCHARACT
  WHERE (PBP.ORDINAL_PBP = PBP_PLANCHARACT.ORDINAL_PBP)
  AND UPCASE(PBP_PLANCHARACT.ISEMPLOYERONLYPLAN) ^= 'YES'
;
QUIT;
RUN;

PROC SQL;
  CREATE TABLE PBPMRX_SQL AS
    SELECT PBP.CONTRACTID AS PBP_A_CONTRACT_NUMBER, PBP.PLANID AS PBP_A_PLAN_IDENTIFIER, PBP.SEGMENTID AS PBP_A_SEGMENT_ID,
      (CASE
        WHEN RXSETUP_RXSETUPDETAILS.DRUGBENEFITTYPE = 'DS' THEN '1'
        WHEN RXSETUP_RXSETUPDETAILS.DRUGBENEFITTYPE = 'AE' THEN '2'
        WHEN RXSETUP_RXSETUPDETAILS.DRUGBENEFITTYPE = 'BA' THEN '3'
        WHEN RXSETUP_RXSETUPDETAILS.DRUGBENEFITTYPE = 'EA' THEN '4'
        ELSE ' '
      END) AS MRX_BENEFIT_TYPE,    
      (CASE
        WHEN RXSETUP_RXSETUPDETAILS.FORMULARYEXCEPTIONSTIER = ' ' THEN ' '
        ELSE RXSETUP_RXSETUPDETAILS.FORMULARYEXCEPTIONSTIER
      END) AS MRX_TIER_FORM_EX,
      (CASE
        WHEN UPCASE(PBP_PLANCHARACT.ISOFFERRX) = 'YES' THEN '1'
        ELSE '2'
      END) AS MRX_DRUG_BEN_YN,     
      (CASE
        WHEN RXSETUP_RXSETUPDETAILS.DRUGBENEFITTYPE = 'AE' AND 
             RXCOSTSHARE_RXCOSSHADET.COSTSHARPREICLCOSTSHARMETH='2' THEN '1'
        WHEN RXSETUP_RXSETUPDETAILS.DRUGBENEFITTYPE = 'AE' AND 
             RXCOSTSHARE_RXCOSSHADET.COSTSHARPREICLCOSTSHARMETH='3' THEN '2'
        ELSE ' '
      END) AS MRX_AE_CSTSHR_APPLY,
      RXSETUP_RXSETUPDETAILS.HASSECONDLESSEXPENSIVETIER AS MRX_TIER_FORM_EX_2_YN,
      RXSETUP_RXSETUPDETAILS.SECONDLESSEXPENSIVETIER AS MRX_TIER_FORM_EX_2,
      RXSETUP_RXSETUPDETAILS.FIRSTFREEFILL AS MRX_FIRST_FILL,

      RXCOSTSHARE_RXCOSSHADET.COSTSHAREDEDUCTTIEREXEMPT,
      RXCOSTSHARE_RXCOSSHADET.COSTSHAREDEDUCTIBLETYPE AS MRX_ALT_DED_CHARGE,
      RXCOSTSHARE_RXCOSSHADET.COSTSHAREDEDUCTIBLEAMOUNT AS MRX_ALT_DED_AMOUNT,
      RXCOSTSHARE_RXCOSSHADET.COSTSHARPREICLCOSTSHARMETH AS MRX_ALT_PRE_ICL_COST_SHARE,
      RXCOSTSHARE_RXCOSSHADET.COSTSHARDEDUAPPLTOALLTIER AS MRX_ALT_DED_TYPE_YN,
      RXCOSTSHARE_RXCOSSHADET.COSTSHARTIERDEDUMATCPREICL AS MRX_ALT_DED_TIER_CSTSHR_YN,
      RXCOSTSHARE_RXCOSSHADET.COSTSHARETIERDEDUCTCOINSU AS MRX_ALT_GEN_COINS_PCT,
      RXCOSTSHARE_RXCOSSHADET.COSTSHARETIERDEDUCTIBLECOPAY AS MRX_ALT_GEN_COPAY_AMT,
      RXCOSTSHARE_RXCOSSHADET.COSTSHARETIERDEDUCTSTRUCT AS MRX_ALT_GEN_CSTSHR_STRUCT,
      PUT(CAT(PBP.CONTRACTID,PBP.PLANID,PUT(PBP.SEGMENTID,Z3.)),$11.) AS QID,
      INPUT(RXSETUP_RXSETUPDETAILS.TIERCOUNT,3.) AS TIERCOUNT_NUM,
      DSINSCOSSHA_DSINSCOSSHADET.INSULDSRETAIONEMONTHCOPAY AS MRX_GEN_IRA_RSTD_COPAY_1M
    FROM
      JSONLIB.ROOT AS ROOT
        LEFT JOIN JSONLIB.PBP AS PBP ON
          ROOT.ORDINAL_ROOT = PBP.ORDINAL_ROOT
        LEFT JOIN JSONLIB.PBP_PLANCHARACT AS PBP_PLANCHARACT ON
          PBP.ORDINAL_PBP = PBP_PLANCHARACT.ORDINAL_PBP
        LEFT JOIN JSONLIB.PBP_RX AS PBP_RX ON
          PBP.ORDINAL_PBP = PBP_RX.ORDINAL_PBP
        LEFT JOIN JSONLIB.RXDETAILS_RXSETUP AS RXDETAILS_RXSETUP ON
          PBP_RX.ORDINAL_RX = RXDETAILS_RXSETUP.ORDINAL_RXDETAILS
        LEFT JOIN JSONLIB.RXSETUP_RXSETUPDETAILS AS RXSETUP_RXSETUPDETAILS ON
          RXDETAILS_RXSETUP.ORDINAL_RXSETUP = RXSETUP_RXSETUPDETAILS.ORDINAL_RXSETUP
        LEFT JOIN JSONLIB.RXSETUP_RXCOSTSHARE AS RXSETUP_RXCOSTSHARE ON
          RXDETAILS_RXSETUP.ORDINAL_RXSETUP = RXSETUP_RXCOSTSHARE.ORDINAL_RXSETUP
        LEFT JOIN JSONLIB.RXCOSTSHARE_RXCOSSHADET AS RXCOSTSHARE_RXCOSSHADET ON
          RXSETUP_RXCOSTSHARE.ORDINAL_RXCOSTSHARE = RXCOSTSHARE_RXCOSSHADET.ORDINAL_RXCOSTSHARE
        LEFT JOIN JSONLIB.RXSETUP_DSINSCOSSHA AS RXSETUP_DSINSCOSSHA ON
          RXDETAILS_RXSETUP.ORDINAL_RXSETUP = RXSETUP_DSINSCOSSHA.ORDINAL_RXSETUP
        LEFT JOIN JSONLIB.DSINSCOSSHA_DSINSCOSSHADET AS DSINSCOSSHA_DSINSCOSSHADET ON
          RXSETUP_DSINSCOSSHA.ORDINAL_DSINSCOSSHA = DSINSCOSSHA_DSINSCOSSHADET.ORDINAL_DSINSCOSSHA
    WHERE UPCASE(PBP_PLANCHARACT.ISEMPLOYERONLYPLAN) ^= 'YES'
;
QUIT;
RUN;

DATA PBPMRX_JSON(KEEP=QID
  PBP_A_CONTRACT_NUMBER
  PBP_A_PLAN_IDENTIFIER
  PBP_A_SEGMENT_ID
  MRX_DRUG_BEN_YN
  MRX_BENEFIT_TYPE
  MRX_ALT_DED_CHARGE
  MRX_ALT_DED_AMOUNT
  MRX_AE_CSTSHR_APPLY
  MRX_ALT_PRE_ICL_COST_SHARE
  MRX_ALT_DED_TYPE_YN
  MRX_ALT_NO_DED_TIER
  MRX_ALT_DED_TIER_CSTSHR_YN
  MRX_ALT_GEN_COINS_PCT
  MRX_ALT_GEN_COPAY_AMT
  MRX_ALT_GEN_CSTSHR_STRUCT
  MRX_FIRST_FILL
  MRX_TIER_FORM_EX
  MRX_TIER_FORM_EX_2_YN
  MRX_TIER_FORM_EX_2
  TIERCOUNT
  MRX_GEN_IRA_RSTD_COPAY_1M
  );
  SET PBPMRX_SQL (RENAME=(
    TIERCOUNT_NUM = TIERCOUNT
    ))
  ;
  IF TIERCOUNT=. THEN TIERCOUNT=0;
  LENGTH MRX_ALT_NO_DED_TIER $7;
  IF COSTSHAREDEDUCTTIEREXEMPT NE ' ' THEN DO;
    MRX_ALT_NO_DED_TIER='0000000';
    IF INDEX(COSTSHAREDEDUCTTIEREXEMPT,'1')>0 THEN
      SUBSTR(MRX_ALT_NO_DED_TIER,2,1)='1';
    IF INDEX(COSTSHAREDEDUCTTIEREXEMPT,'2')>0 THEN
      SUBSTR(MRX_ALT_NO_DED_TIER,3,1)='1';
    IF INDEX(COSTSHAREDEDUCTTIEREXEMPT,'3')>0 THEN
      SUBSTR(MRX_ALT_NO_DED_TIER,4,1)='1';
    IF INDEX(COSTSHAREDEDUCTTIEREXEMPT,'4')>0 THEN
      SUBSTR(MRX_ALT_NO_DED_TIER,5,1)='1';
    IF INDEX(COSTSHAREDEDUCTTIEREXEMPT,'5')>0 THEN
      SUBSTR(MRX_ALT_NO_DED_TIER,6,1)='1';
    IF INDEX(COSTSHAREDEDUCTTIEREXEMPT,'6')>0 THEN
      SUBSTR(MRX_ALT_NO_DED_TIER,7,1)='1';
    IF INDEX(COSTSHAREDEDUCTTIEREXEMPT,'7')>0 THEN
      SUBSTR(MRX_ALT_NO_DED_TIER,1,1)='1';
  END;
RUN;

PROC SQL;
CREATE TABLE PBPMRXT_JSON AS
  SELECT * FROM (
    SELECT
      put(cat(contractId, planId, put(segmentId, z3.)),$11.) AS qid,
      '1' as MRX_TIER_TYPE_ID,
      '1' as MRX_TIER_ID,
      (case
        when rxTier1_rxTier1Details.tierIncludes = 'PD' then '1'
        when rxTier1_rxTier1Details.tierIncludes = 'ED' then '2'
        when rxTier1_rxTier1Details.tierIncludes = 'PE' then '3'
             else ' '
      end) as MRX_TIER_INCLUDES,    
      rxTier1PreIcl_rxTi1PrIcDe.preIclCostShareStructure AS MRX_TIER_CSTSHR_STRUCT_TYPE,
      rxTier1PreIcl_rxTi1PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSTD_COINS_1M,
      rxTier1PreIcl_rxTi1PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSSTD_COINS_1M,
      rxTier1PreIcl_rxTi1PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSTD_COPAY_1M,
      rxTier1PreIcl_rxTi1PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSSTD_COPAY_1M,
      rxTier1PreIcl_rxTi1PrIcDe.preIclPrefRetaOneMontCoin AS MRX_TIER_RSPFD_COINS_1M,
      rxTier1PreIcl_rxTi1PrIcDe.preIclPrefRetaOneMontCopa AS MRX_TIER_RSPFD_COPAY_1M,
      rxTier1PreIcl_rxTi1PrIcDe.preIclOutOfNetworkCoinsurance AS MRX_TIER_OONP_COINS_1M,
      rxTier1PreIcl_rxTi1PrIcDe.preIclOutOfNetworkCopayment AS MRX_TIER_OONP_COPAY_1M,
      rxinti1pric_rxinti1pricde.insuPreIclRetaOneMontCopa AS MRX_TIER_IRA_RSTD_COPAY_1M,
      rxinti1pric_rxinti1pricde.insPreIclPreRetOneMonCop AS MRX_TIER_IRA_RSPFD_COPAY_1M
    FROM
      jsonlib.root
      LEFT JOIN jsonlib.pbp AS pbp ON
        root.ordinal_root = pbp.ordinal_root
      LEFT JOIN jsonlib.pbp_rx AS pbp_rx ON
        pbp.ordinal_pbp = pbp_rx.ordinal_pbp
      LEFT JOIN jsonlib.rxDetails_rxSetup AS rxDetails_rxSetup ON
        pbp_rx.ordinal_rx = rxDetails_rxSetup.ordinal_rxDetails
      LEFT JOIN jsonlib.rxSetup_rxTiers AS rxSetup_rxTiers ON
        rxDetails_rxSetup.ordinal_rxSetup = rxSetup_rxTiers.ordinal_rxSetup
      LEFT JOIN jsonlib.rxTiers_rxTier1 AS rxTiers_rxTier1 ON
        rxSetup_rxTiers.ordinal_rxTiers = rxTiers_rxTier1.ordinal_rxTiers
      LEFT JOIN jsonlib.rxTier1_rxTier1Details AS rxTier1_rxTier1Details ON
        rxTiers_rxTier1.ordinal_rxTier1 = rxTier1_rxTier1Details.ordinal_rxTier1
      LEFT JOIN jsonlib.rxTier1_rxTier1PreIcl AS rxTier1_rxTier1PreIcl ON
        rxTiers_rxTier1.ordinal_rxTier1 = rxTier1_rxTier1PreIcl.ordinal_rxTier1
      LEFT JOIN jsonlib.rxTier1PreIcl_rxTi1PrIcDe AS rxTier1PreIcl_rxTi1PrIcDe ON
        rxTier1_rxTier1PreIcl.ordinal_rxTier1PreIcl = rxTier1PreIcl_rxTi1PrIcDe.ordinal_rxTier1PreIcl
      LEFT JOIN jsonlib.rxsetup_rxinsulin AS rxsetup_rxinsulin ON
        rxdetails_rxsetup.ordinal_rxSetup = rxsetup_rxinsulin.ordinal_rxSetup
      LEFT JOIN jsonlib.rxinsulin_rxinti1pric AS rxinsulin_rxinti1pric ON
        rxsetup_rxinsulin.ordinal_rxInsulin = rxinsulin_rxinti1pric.ordinal_rxInsulin
      LEFT JOIN jsonlib.rxinti1pric_rxinti1pricde AS rxinti1pric_rxinti1pricde ON
        rxinsulin_rxinti1pric.ordinal_rxInTi1PrIc = rxinti1pric_rxinti1pricde.ordinal_rxInTi1PrIc
    UNION
    SELECT
      put(cat(contractId, planId, put(segmentId, z3.)),$11.) AS qid,
      '1' as MRX_TIER_TYPE_ID,
      '2' as MRX_TIER_ID,
      (case
        when rxTier2_rxTier2Details.tierIncludes = 'PD' then '1'
        when rxTier2_rxTier2Details.tierIncludes = 'ED' then '2'
        when rxTier2_rxTier2Details.tierIncludes = 'PE' then '3'
        else ' '
      end) as MRX_TIER_INCLUDES, 
      rxTier2PreIcl_rxTi2PrIcDe.preIclCostShareStructure AS MRX_TIER_CSTSHR_STRUCT_TYPE,
      rxTier2PreIcl_rxTi2PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSTD_COINS_1M,
      rxTier2PreIcl_rxTi2PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSSTD_COINS_1M,
      rxTier2PreIcl_rxTi2PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSTD_COPAY_1M,
      rxTier2PreIcl_rxTi2PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSSTD_COPAY_1M,
      rxTier2PreIcl_rxTi2PrIcDe.preIclPrefRetaOneMontCoin AS MRX_TIER_RSPFD_COINS_1M,
      rxTier2PreIcl_rxTi2PrIcDe.preIclPrefRetaOneMontCopa AS MRX_TIER_RSPFD_COPAY_1M,
      rxTier2PreIcl_rxTi2PrIcDe.preIclOutOfNetworkCoinsurance AS MRX_TIER_OONP_COINS_1M,
      rxTier2PreIcl_rxTi2PrIcDe.preIclOutOfNetworkCopayment AS MRX_TIER_OONP_COPAY_1M,
      rxinti2pric_rxinti2pricde.insuPreIclRetaOneMontCopa AS MRX_TIER_IRA_RSTD_COPAY_1M,
      rxinti2pric_rxinti2pricde.insPreIclPreRetOneMonCop AS MRX_TIER_IRA_RSPFD_COPAY_1M
    FROM
      jsonlib.root
      LEFT JOIN jsonlib.pbp AS pbp ON
        root.ordinal_root = pbp.ordinal_root
      LEFT JOIN jsonlib.pbp_rx AS pbp_rx ON
        pbp.ordinal_pbp = pbp_rx.ordinal_pbp
      LEFT JOIN jsonlib.rxDetails_rxSetup AS rxDetails_rxSetup ON
        pbp_rx.ordinal_rx = rxDetails_rxSetup.ordinal_rxDetails
      LEFT JOIN jsonlib.rxSetup_rxTiers AS rxSetup_rxTiers ON
        rxDetails_rxSetup.ordinal_rxSetup = rxSetup_rxTiers.ordinal_rxSetup
      LEFT JOIN jsonlib.rxTiers_rxTier2 AS rxTiers_rxTier2 ON
        rxSetup_rxTiers.ordinal_rxTiers = rxTiers_rxTier2.ordinal_rxTiers
      LEFT JOIN jsonlib.rxTier2_rxTier2Details AS rxTier2_rxTier2Details ON
        rxTiers_rxTier2.ordinal_rxTier2 = rxTier2_rxTier2Details.ordinal_rxTier2
      LEFT JOIN jsonlib.rxTier2_rxTier2PreIcl AS rxTier2_rxTier2PreIcl ON
        rxTiers_rxTier2.ordinal_rxTier2 = rxTier2_rxTier2PreIcl.ordinal_rxTier2
      LEFT JOIN jsonlib.rxTier2PreIcl_rxTi2PrIcDe AS rxTier2PreIcl_rxTi2PrIcDe ON
        rxTier2_rxTier2PreIcl.ordinal_rxTier2PreIcl = rxTier2PreIcl_rxTi2PrIcDe.ordinal_rxTier2PreIcl
      LEFT JOIN jsonlib.rxsetup_rxinsulin AS rxsetup_rxinsulin ON
        rxdetails_rxsetup.ordinal_rxSetup = rxsetup_rxinsulin.ordinal_rxSetup
      LEFT JOIN jsonlib.rxinsulin_rxinti2pric AS rxinsulin_rxinti2pric ON
        rxsetup_rxinsulin.ordinal_rxInsulin = rxinsulin_rxinti2pric.ordinal_rxInsulin
      LEFT JOIN jsonlib.rxinti2pric_rxinti2pricde AS rxinti2pric_rxinti2pricde ON
        rxinsulin_rxinti2pric.ordinal_rxInTi2PrIc = rxinti2pric_rxinti2pricde.ordinal_rxInTi2PrIc
    UNION
    SELECT
      put(cat(contractId, planId, put(segmentId, z3.)),$11.) AS qid,
      '1' as MRX_TIER_TYPE_ID,
      '3' as MRX_TIER_ID,
      (case
        when rxTier3_rxTier3Details.tierIncludes = 'PD' then '1'
        when rxTier3_rxTier3Details.tierIncludes = 'ED' then '2'
        when rxTier3_rxTier3Details.tierIncludes = 'PE' then '3'
        else ' '
      end) as MRX_TIER_INCLUDES,
      rxTier3PreIcl_rxTi3PrIcDe.preIclCostShareStructure AS MRX_TIER_CSTSHR_STRUCT_TYPE,
      rxTier3PreIcl_rxTi3PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSTD_COINS_1M,
      rxTier3PreIcl_rxTi3PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSSTD_COINS_1M,
      rxTier3PreIcl_rxTi3PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSTD_COPAY_1M,
      rxTier3PreIcl_rxTi3PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSSTD_COPAY_1M,
      rxTier3PreIcl_rxTi3PrIcDe.preIclPrefRetaOneMontCoin AS MRX_TIER_RSPFD_COINS_1M,
      rxTier3PreIcl_rxTi3PrIcDe.preIclPrefRetaOneMontCopa AS MRX_TIER_RSPFD_COPAY_1M,
      rxTier3PreIcl_rxTi3PrIcDe.preIclOutOfNetworkCoinsurance AS MRX_TIER_OONP_COINS_1M,
      rxTier3PreIcl_rxTi3PrIcDe.preIclOutOfNetworkCopayment AS MRX_TIER_OONP_COPAY_1M,
      rxinti3pric_rxinti3pricde.insuPreIclRetaOneMontCopa AS MRX_TIER_IRA_RSTD_COPAY_1M,
      rxinti3pric_rxinti3pricde.insPreIclPreRetOneMonCop AS MRX_TIER_IRA_RSPFD_COPAY_1M
    FROM
      jsonlib.root
      LEFT JOIN jsonlib.pbp AS pbp ON
        root.ordinal_root = pbp.ordinal_root
      LEFT JOIN jsonlib.pbp_rx AS pbp_rx ON
        pbp.ordinal_pbp = pbp_rx.ordinal_pbp
      LEFT JOIN jsonlib.rxDetails_rxSetup AS rxDetails_rxSetup ON
        pbp_rx.ordinal_rx = rxDetails_rxSetup.ordinal_rxDetails
      LEFT JOIN jsonlib.rxSetup_rxTiers AS rxSetup_rxTiers ON
        rxDetails_rxSetup.ordinal_rxSetup = rxSetup_rxTiers.ordinal_rxSetup
      LEFT JOIN jsonlib.rxTiers_rxTier3 AS rxTiers_rxTier3 ON
        rxSetup_rxTiers.ordinal_rxTiers = rxTiers_rxTier3.ordinal_rxTiers
      LEFT JOIN jsonlib.rxTier3_rxTier3Details AS rxTier3_rxTier3Details ON
        rxTiers_rxTier3.ordinal_rxTier3 = rxTier3_rxTier3Details.ordinal_rxTier3
      LEFT JOIN jsonlib.rxTier3_rxTier3PreIcl AS rxTier3_rxTier3PreIcl ON
        rxTiers_rxTier3.ordinal_rxTier3 = rxTier3_rxTier3PreIcl.ordinal_rxTier3
      LEFT JOIN jsonlib.rxTier3PreIcl_rxTi3PrIcDe AS rxTier3PreIcl_rxTi3PrIcDe ON
        rxTier3_rxTier3PreIcl.ordinal_rxTier3PreIcl = rxTier3PreIcl_rxTi3PrIcDe.ordinal_rxTier3PreIcl
      LEFT JOIN jsonlib.rxsetup_rxinsulin AS rxsetup_rxinsulin ON
        rxdetails_rxsetup.ordinal_rxSetup = rxsetup_rxinsulin.ordinal_rxSetup
      LEFT JOIN jsonlib.rxinsulin_rxinti3pric AS rxinsulin_rxinti3pric ON
        rxsetup_rxinsulin.ordinal_rxInsulin = rxinsulin_rxinti3pric.ordinal_rxInsulin
      LEFT JOIN jsonlib.rxinti3pric_rxinti3pricde AS rxinti3pric_rxinti3pricde ON
        rxinsulin_rxinti3pric.ordinal_rxInTi3PrIc = rxinti3pric_rxinti3pricde.ordinal_rxInTi3PrIc
    UNION
    SELECT
      put(cat(contractId, planId, put(segmentId, z3.)),$11.) AS qid,
      '1' as MRX_TIER_TYPE_ID,
      '4' as MRX_TIER_ID,
      (case
        when rxTier4_rxTier4Details.tierIncludes = 'PD' then '1'
        when rxTier4_rxTier4Details.tierIncludes = 'ED' then '2'
        when rxTier4_rxTier4Details.tierIncludes = 'PE' then '3'
        else ' '
      end) as MRX_TIER_INCLUDES,
      rxTier4PreIcl_rxTi4PrIcDe.preIclCostShareStructure AS MRX_TIER_CSTSHR_STRUCT_TYPE,
      rxTier4PreIcl_rxTi4PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSTD_COINS_1M,
      rxTier4PreIcl_rxTi4PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSSTD_COINS_1M,
      rxTier4PreIcl_rxTi4PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSTD_COPAY_1M,
      rxTier4PreIcl_rxTi4PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSSTD_COPAY_1M,
      rxTier4PreIcl_rxTi4PrIcDe.preIclPrefRetaOneMontCoin AS MRX_TIER_RSPFD_COINS_1M,
      rxTier4PreIcl_rxTi4PrIcDe.preIclPrefRetaOneMontCopa AS MRX_TIER_RSPFD_COPAY_1M,
      rxTier4PreIcl_rxTi4PrIcDe.preIclOutOfNetworkCoinsurance AS MRX_TIER_OONP_COINS_1M,
      rxTier4PreIcl_rxTi4PrIcDe.preIclOutOfNetworkCopayment AS MRX_TIER_OONP_COPAY_1M,
      rxinti4pric_rxinti4pricde.insuPreIclRetaOneMontCopa AS MRX_TIER_IRA_RSTD_COPAY_1M,
      rxinti4pric_rxinti4pricde.insPreIclPreRetOneMonCop AS MRX_TIER_IRA_RSPFD_COPAY_1M
    FROM
      jsonlib.root
      LEFT JOIN jsonlib.pbp AS pbp ON
        root.ordinal_root = pbp.ordinal_root
      LEFT JOIN jsonlib.pbp_rx AS pbp_rx ON
        pbp.ordinal_pbp = pbp_rx.ordinal_pbp
      LEFT JOIN jsonlib.rxDetails_rxSetup AS rxDetails_rxSetup ON
        pbp_rx.ordinal_rx = rxDetails_rxSetup.ordinal_rxDetails
      LEFT JOIN jsonlib.rxSetup_rxTiers AS rxSetup_rxTiers ON
        rxDetails_rxSetup.ordinal_rxSetup = rxSetup_rxTiers.ordinal_rxSetup
      LEFT JOIN jsonlib.rxTiers_rxTier4 AS rxTiers_rxTier4 ON
        rxSetup_rxTiers.ordinal_rxTiers = rxTiers_rxTier4.ordinal_rxTiers
      LEFT JOIN jsonlib.rxTier4_rxTier4Details AS rxTier4_rxTier4Details ON
        rxTiers_rxTier4.ordinal_rxTier4 = rxTier4_rxTier4Details.ordinal_rxTier4
      LEFT JOIN jsonlib.rxTier4_rxTier4PreIcl AS rxTier4_rxTier4PreIcl ON
        rxTiers_rxTier4.ordinal_rxTier4 = rxTier4_rxTier4PreIcl.ordinal_rxTier4
      LEFT JOIN jsonlib.rxTier4PreIcl_rxTi4PrIcDe AS rxTier4PreIcl_rxTi4PrIcDe ON
        rxTier4_rxTier4PreIcl.ordinal_rxTier4PreIcl = rxTier4PreIcl_rxTi4PrIcDe.ordinal_rxTier4PreIcl
      LEFT JOIN jsonlib.rxsetup_rxinsulin AS rxsetup_rxinsulin ON
        rxdetails_rxsetup.ordinal_rxSetup = rxsetup_rxinsulin.ordinal_rxSetup
      LEFT JOIN jsonlib.rxinsulin_rxinti4pric AS rxinsulin_rxinti4pric ON
        rxsetup_rxinsulin.ordinal_rxInsulin = rxinsulin_rxinti4pric.ordinal_rxInsulin
      LEFT JOIN jsonlib.rxinti4pric_rxinti4pricde AS rxinti4pric_rxinti4pricde ON
        rxinsulin_rxinti4pric.ordinal_rxInTi4PrIc = rxinti4pric_rxinti4pricde.ordinal_rxInTi4PrIc          
    UNION
    SELECT
      put(cat(contractId, planId, put(segmentId, z3.)),$11.) AS qid,
      '1' as MRX_TIER_TYPE_ID,
      '5' as MRX_TIER_ID,
      (case
        when rxTier5_rxTier5Details.tierIncludes = 'PD' then '1'
        when rxTier5_rxTier5Details.tierIncludes = 'ED' then '2'
        when rxTier5_rxTier5Details.tierIncludes = 'PE' then '3'
        else ' '
      end) as MRX_TIER_INCLUDES,
      rxTier5PreIcl_rxTi5PrIcDe.preIclCostShareStructure AS MRX_TIER_CSTSHR_STRUCT_TYPE,
      rxTier5PreIcl_rxTi5PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSTD_COINS_1M,
      rxTier5PreIcl_rxTi5PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSSTD_COINS_1M,
      rxTier5PreIcl_rxTi5PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSTD_COPAY_1M,
      rxTier5PreIcl_rxTi5PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSSTD_COPAY_1M,
      rxTier5PreIcl_rxTi5PrIcDe.preIclPrefRetaOneMontCoin AS MRX_TIER_RSPFD_COINS_1M,
      rxTier5PreIcl_rxTi5PrIcDe.preIclPrefRetaOneMontCopa AS MRX_TIER_RSPFD_COPAY_1M,
      rxTier5PreIcl_rxTi5PrIcDe.preIclOutOfNetworkCoinsurance AS MRX_TIER_OONP_COINS_1M,
      rxTier5PreIcl_rxTi5PrIcDe.preIclOutOfNetworkCopayment AS MRX_TIER_OONP_COPAY_1M,
      rxinti5pric_rxinti5pricde.insuPreIclRetaOneMontCopa AS MRX_TIER_IRA_RSTD_COPAY_1M,
      rxinti5pric_rxinti5pricde.insPreIclPreRetOneMonCop AS MRX_TIER_IRA_RSPFD_COPAY_1M
    FROM
      jsonlib.root
      LEFT JOIN jsonlib.pbp AS pbp ON
        root.ordinal_root = pbp.ordinal_root
      LEFT JOIN jsonlib.pbp_rx AS pbp_rx ON
        pbp.ordinal_pbp = pbp_rx.ordinal_pbp
      LEFT JOIN jsonlib.rxDetails_rxSetup AS rxDetails_rxSetup ON
        pbp_rx.ordinal_rx = rxDetails_rxSetup.ordinal_rxDetails
      LEFT JOIN jsonlib.rxSetup_rxTiers AS rxSetup_rxTiers ON
        rxDetails_rxSetup.ordinal_rxSetup = rxSetup_rxTiers.ordinal_rxSetup
      LEFT JOIN jsonlib.rxTiers_rxTier5 AS rxTiers_rxTier5 ON
        rxSetup_rxTiers.ordinal_rxTiers = rxTiers_rxTier5.ordinal_rxTiers
      LEFT JOIN jsonlib.rxTier5_rxTier5Details AS rxTier5_rxTier5Details ON
        rxTiers_rxTier5.ordinal_rxTier5 = rxTier5_rxTier5Details.ordinal_rxTier5
      LEFT JOIN jsonlib.rxTier5_rxTier5PreIcl AS rxTier5_rxTier5PreIcl ON
        rxTiers_rxTier5.ordinal_rxTier5 = rxTier5_rxTier5PreIcl.ordinal_rxTier5
      LEFT JOIN jsonlib.rxTier5PreIcl_rxTi5PrIcDe AS rxTier5PreIcl_rxTi5PrIcDe ON
        rxTier5_rxTier5PreIcl.ordinal_rxTier5PreIcl = rxTier5PreIcl_rxTi5PrIcDe.ordinal_rxTier5PreIcl
      LEFT JOIN jsonlib.rxsetup_rxinsulin AS rxsetup_rxinsulin ON
        rxdetails_rxsetup.ordinal_rxSetup = rxsetup_rxinsulin.ordinal_rxSetup
      LEFT JOIN jsonlib.rxinsulin_rxinti5pric AS rxinsulin_rxinti5pric ON
        rxsetup_rxinsulin.ordinal_rxInsulin = rxinsulin_rxinti5pric.ordinal_rxInsulin
      LEFT JOIN jsonlib.rxinti5pric_rxinti5pricde AS rxinti5pric_rxinti5pricde ON
        rxinsulin_rxinti5pric.ordinal_rxInTi5PrIc = rxinti5pric_rxinti5pricde.ordinal_rxInTi5PrIc
    UNION
    SELECT
      put(cat(contractId, planId, put(segmentId, z3.)),$11.) AS qid,
      '1' as MRX_TIER_TYPE_ID,
      '6' as MRX_TIER_ID,
      (case
        when rxTier6_rxTier6Details.tierIncludes = 'PD' then '1'
        when rxTier6_rxTier6Details.tierIncludes = 'ED' then '2'
        when rxTier6_rxTier6Details.tierIncludes = 'PE' then '3'
        else ' '
      end) as MRX_TIER_INCLUDES,    
      rxTier6PreIcl_rxTi6PrIcDe.preIclCostShareStructure AS MRX_TIER_CSTSHR_STRUCT_TYPE,
      rxTier6PreIcl_rxTi6PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSTD_COINS_1M,
      rxTier6PreIcl_rxTi6PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSSTD_COINS_1M,
      rxTier6PreIcl_rxTi6PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSTD_COPAY_1M,
      rxTier6PreIcl_rxTi6PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSSTD_COPAY_1M,
      rxTier6PreIcl_rxTi6PrIcDe.preIclPrefRetaOneMontCoin AS MRX_TIER_RSPFD_COINS_1M,
      rxTier6PreIcl_rxTi6PrIcDe.preIclPrefRetaOneMontCopa AS MRX_TIER_RSPFD_COPAY_1M,
      rxTier6PreIcl_rxTi6PrIcDe.preIclOutOfNetworkCoinsurance AS MRX_TIER_OONP_COINS_1M,
      rxTier6PreIcl_rxTi6PrIcDe.preIclOutOfNetworkCopayment AS MRX_TIER_OONP_COPAY_1M,
      rxinti6pric_rxinti6pricde.insuPreIclRetaOneMontCopa AS MRX_TIER_IRA_RSTD_COPAY_1M,
      rxinti6pric_rxinti6pricde.insPreIclPreRetOneMonCop AS MRX_TIER_IRA_RSPFD_COPAY_1M
    FROM
      jsonlib.root
      LEFT JOIN jsonlib.pbp AS pbp ON
        root.ordinal_root = pbp.ordinal_root
      LEFT JOIN jsonlib.pbp_rx AS pbp_rx ON
        pbp.ordinal_pbp = pbp_rx.ordinal_pbp
      LEFT JOIN jsonlib.rxDetails_rxSetup AS rxDetails_rxSetup ON
        pbp_rx.ordinal_rx = rxDetails_rxSetup.ordinal_rxDetails
      LEFT JOIN jsonlib.rxSetup_rxTiers AS rxSetup_rxTiers ON
        rxDetails_rxSetup.ordinal_rxSetup = rxSetup_rxTiers.ordinal_rxSetup
      LEFT JOIN jsonlib.rxTiers_rxTier6 AS rxTiers_rxTier6 ON
        rxSetup_rxTiers.ordinal_rxTiers = rxTiers_rxTier6.ordinal_rxTiers
      LEFT JOIN jsonlib.rxTier6_rxTier6Details AS rxTier6_rxTier6Details ON
        rxTiers_rxTier6.ordinal_rxTier6 = rxTier6_rxTier6Details.ordinal_rxTier6
      LEFT JOIN jsonlib.rxTier6_rxTier6PreIcl AS rxTier6_rxTier6PreIcl ON
        rxTiers_rxTier6.ordinal_rxTier6 = rxTier6_rxTier6PreIcl.ordinal_rxTier6
      LEFT JOIN jsonlib.rxTier6PreIcl_rxTi6PrIcDe AS rxTier6PreIcl_rxTi6PrIcDe ON
        rxTier6_rxTier6PreIcl.ordinal_rxTier6PreIcl = rxTier6PreIcl_rxTi6PrIcDe.ordinal_rxTier6PreIcl
      LEFT JOIN jsonlib.rxsetup_rxinsulin AS rxsetup_rxinsulin ON
        rxdetails_rxsetup.ordinal_rxSetup = rxsetup_rxinsulin.ordinal_rxSetup
      LEFT JOIN jsonlib.rxinsulin_rxinti6pric AS rxinsulin_rxinti6pric ON
        rxsetup_rxinsulin.ordinal_rxInsulin = rxinsulin_rxinti6pric.ordinal_rxInsulin
      LEFT JOIN jsonlib.rxinti6pric_rxinti6pricde AS rxinti6pric_rxinti6pricde ON
        rxinsulin_rxinti6pric.ordinal_rxInTi6PrIc = rxinti6pric_rxinti6pricde.ordinal_rxInTi6PrIc
    UNION
    SELECT
      put(cat(contractId, planId, put(segmentId, z3.)),$11.) AS qid,
      '1' as MRX_TIER_TYPE_ID,
      '7' as MRX_TIER_ID,
      (case
        when rxTier7_rxTier7Details.tierIncludes = 'PD' then '1'
        when rxTier7_rxTier7Details.tierIncludes = 'ED' then '2'
        when rxTier7_rxTier7Details.tierIncludes = 'PE' then '3'
        else ' '
      end) as MRX_TIER_INCLUDES, 
      rxTier7PreIcl_rxTi7PrIcDe.preIclCostShareStructure AS MRX_TIER_CSTSHR_STRUCT_TYPE,
      rxTier7PreIcl_rxTi7PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSTD_COINS_1M,
      rxTier7PreIcl_rxTi7PrIcDe.preIclRetaiOneMonthCoins AS MRX_TIER_RSSTD_COINS_1M,
      rxTier7PreIcl_rxTi7PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSTD_COPAY_1M,
      rxTier7PreIcl_rxTi7PrIcDe.preIclRetailOneMonthCopayment AS MRX_TIER_RSSTD_COPAY_1M,
      rxTier7PreIcl_rxTi7PrIcDe.preIclPrefRetaOneMontCoin AS MRX_TIER_RSPFD_COINS_1M,
      rxTier7PreIcl_rxTi7PrIcDe.preIclPrefRetaOneMontCopa AS MRX_TIER_RSPFD_COPAY_1M,
      rxTier7PreIcl_rxTi7PrIcDe.preIclOutOfNetworkCoinsurance AS MRX_TIER_OONP_COINS_1M,
      rxTier7PreIcl_rxTi7PrIcDe.preIclOutOfNetworkCopayment AS MRX_TIER_OONP_COPAY_1M,
      rxinti7pric_rxinti7pricde.insuPreIclRetaOneMontCopa AS MRX_TIER_IRA_RSTD_COPAY_1M,
      rxinti7pric_rxinti7pricde.insPreIclPreRetOneMonCop AS MRX_TIER_IRA_RSPFD_COPAY_1M
    FROM
      jsonlib.root
      LEFT JOIN jsonlib.pbp AS pbp ON
        root.ordinal_root = pbp.ordinal_root
      LEFT JOIN jsonlib.pbp_rx AS pbp_rx ON
        pbp.ordinal_pbp = pbp_rx.ordinal_pbp
      LEFT JOIN jsonlib.rxDetails_rxSetup AS rxDetails_rxSetup ON
        pbp_rx.ordinal_rx = rxDetails_rxSetup.ordinal_rxDetails
      LEFT JOIN jsonlib.rxSetup_rxTiers AS rxSetup_rxTiers ON
        rxDetails_rxSetup.ordinal_rxSetup = rxSetup_rxTiers.ordinal_rxSetup
      LEFT JOIN jsonlib.rxTiers_rxTier7 AS rxTiers_rxTier7 ON
        rxSetup_rxTiers.ordinal_rxTiers = rxTiers_rxTier7.ordinal_rxTiers
      LEFT JOIN jsonlib.rxTier7_rxTier7Details AS rxTier7_rxTier7Details ON
        rxTiers_rxTier7.ordinal_rxTier7 = rxTier7_rxTier7Details.ordinal_rxTier7
      LEFT JOIN jsonlib.rxTier7_rxTier7PreIcl AS rxTier7_rxTier7PreIcl ON
        rxTiers_rxTier7.ordinal_rxTier7 = rxTier7_rxTier7PreIcl.ordinal_rxTier7
      LEFT JOIN jsonlib.rxTier7PreIcl_rxTi7PrIcDe AS rxTier7PreIcl_rxTi7PrIcDe ON
        rxTier7_rxTier7PreIcl.ordinal_rxTier7PreIcl = rxTier7PreIcl_rxTi7PrIcDe.ordinal_rxTier7PreIcl
      LEFT JOIN jsonlib.rxsetup_rxinsulin AS rxsetup_rxinsulin ON
        rxdetails_rxsetup.ordinal_rxSetup = rxsetup_rxinsulin.ordinal_rxSetup
      LEFT JOIN jsonlib.rxinsulin_rxinti7pric AS rxinsulin_rxinti7pric ON
        rxsetup_rxinsulin.ordinal_rxInsulin = rxinsulin_rxinti7pric.ordinal_rxInsulin
      LEFT JOIN jsonlib.rxinti7pric_rxinti7pricde AS rxinti7pric_rxinti7pricde ON
        rxinsulin_rxinti7pric.ordinal_rxInTi7PrIc = rxinti7pric_rxinti7pricde.ordinal_rxInTi7PrIc
    ) MAIN
    WHERE MRX_TIER_CSTSHR_STRUCT_TYPE IS NOT NULL
    ORDER BY QID, MRX_TIER_TYPE_ID, MRX_TIER_ID
    ;
   QUIT;
   RUN;

DATA _NULL_;
CALL SYMPUT('NUM',TRIM(LEFT(PUT(NUMOBS,4.))));
STOP;
SET PBPMRXT_JSON NOBS=NUMOBS;
STOP;
RUN;

%MACRO EMPTYTIER;
  %IF &NUM = 0 %THEN %DO;
    DATA PBPMRXT_JSON;
      SET PBP_JSON (KEEP=QID)
          PBPMRXT_JSON;
    RUN;
  %END;
%MEND;

%EMPTYTIER;
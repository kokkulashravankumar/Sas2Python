* PROGRAM NAME: JSON_IMPORT_PBPB14_2.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB14_2();
PROC SQL;
CREATE TABLE PBPB14_2_JSON AS
SELECT
	segment_0.qid,
     CASE WHEN PBP_B14E_COINS_EHC_24 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COINS_EHC_0 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COINS_EHC_14 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COINS_EHC_17 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COINS_EHC_22 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B14E_COINS_EHC,
     CASE WHEN PBP_B14E_DED_EHC_25 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_DED_EHC_1 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_DED_EHC_5 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_DED_EHC_18 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_DED_EHC_20 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B14E_DED_EHC,
     CASE WHEN PBP_B14E_COPAY_EHC_23 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COPAY_EHC_2 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COPAY_EHC_9 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COPAY_EHC_16 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_COPAY_EHC_19 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B14E_COPAY_EHC,     
     CASE WHEN PBP_B14E_MAXENR_EHC_26 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_MAXENR_EHC_3 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_MAXENR_EHC_10 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_MAXENR_EHC_15 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B14E_MAXENR_EHC_21 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B14E_MAXENR_EHC, 
	COALESCE(PBP_B14E_COPAY_AMT_MIN_DIAB_4, PBP_B14E_COPAY_AMT_MIN_DIAB_28) AS PBP_B14E_COPAY_AMT_MIN_DIAB,
	PBP_B14E_MAXENR_PER_DIAB_6 AS PBP_B14E_MAXENR_PER_DIAB,
	COALESCE(PBP_B14E_COPAY_AMT_MAX_DIAB_7, PBP_B14E_COPAY_AMT_MAX_DIAB_29) AS PBP_B14E_COPAY_AMT_MAX_DIAB,
	PBP_B14E_DED_AMT_DIAB_8 AS PBP_B14E_DED_AMT_DIAB,
	PBP_B14E_MAXENR_AMT_DIAB_11 AS PBP_B14E_MAXENR_AMT_DIAB,
	COALESCE(PBP_B14E_COINS_PCT_MAX_DIAB_12, PBP_B14E_COINS_PCT_MAX_DIAB_30) AS PBP_B14E_COINS_PCT_MAX_DIAB,
	COALESCE(PBP_B14E_COINS_PCT_MIN_DIAB_13, PBP_B14E_COINS_PCT_MIN_DIAB_27) AS PBP_B14E_COINS_PCT_MIN_DIAB
FROM
	(
	SELECT
		qid,
		MAX(PBP_B14E_COINS_EHC_0) AS PBP_B14E_COINS_EHC_0,
		MAX(PBP_B14E_DED_EHC_1) AS PBP_B14E_DED_EHC_1,
		MAX(PBP_B14E_COPAY_EHC_2) AS PBP_B14E_COPAY_EHC_2,
		MAX(PBP_B14E_MAXENR_EHC_3) AS PBP_B14E_MAXENR_EHC_3,
		MAX(PBP_B14E_COPAY_AMT_MIN_DIAB_4) AS PBP_B14E_COPAY_AMT_MIN_DIAB_4,
		MAX(PBP_B14E_DED_EHC_5) AS PBP_B14E_DED_EHC_5,
		MAX(PBP_B14E_MAXENR_PER_DIAB_6) AS PBP_B14E_MAXENR_PER_DIAB_6,
		MAX(PBP_B14E_COPAY_AMT_MAX_DIAB_7) AS PBP_B14E_COPAY_AMT_MAX_DIAB_7,
		MAX(PBP_B14E_DED_AMT_DIAB_8) AS PBP_B14E_DED_AMT_DIAB_8,
		MAX(PBP_B14E_COPAY_EHC_9) AS PBP_B14E_COPAY_EHC_9,
		MAX(PBP_B14E_MAXENR_EHC_10) AS PBP_B14E_MAXENR_EHC_10,
		MAX(PBP_B14E_MAXENR_AMT_DIAB_11) AS PBP_B14E_MAXENR_AMT_DIAB_11,
		MAX(PBP_B14E_COINS_PCT_MAX_DIAB_12) AS PBP_B14E_COINS_PCT_MAX_DIAB_12,
		MAX(PBP_B14E_COINS_PCT_MIN_DIAB_13) AS PBP_B14E_COINS_PCT_MIN_DIAB_13,
		MAX(PBP_B14E_COINS_EHC_14) AS PBP_B14E_COINS_EHC_14,
		MAX(PBP_B14E_MAXENR_EHC_15) AS PBP_B14E_MAXENR_EHC_15,
		MAX(PBP_B14E_COPAY_EHC_16) AS PBP_B14E_COPAY_EHC_16,
		MAX(PBP_B14E_COINS_EHC_17) AS PBP_B14E_COINS_EHC_17,
		MAX(PBP_B14E_DED_EHC_18) AS PBP_B14E_DED_EHC_18,
		MAX(PBP_B14E_COPAY_EHC_19) AS PBP_B14E_COPAY_EHC_19,
		MAX(PBP_B14E_DED_EHC_20) AS PBP_B14E_DED_EHC_20,
		MAX(PBP_B14E_MAXENR_EHC_21) AS PBP_B14E_MAXENR_EHC_21,
		MAX(PBP_B14E_COINS_EHC_22) AS PBP_B14E_COINS_EHC_22,
		MAX(PBP_B14E_COPAY_EHC_23) AS PBP_B14E_COPAY_EHC_23,
		MAX(PBP_B14E_COINS_EHC_24) AS PBP_B14E_COINS_EHC_24,
		MAX(PBP_B14E_DED_EHC_25) AS PBP_B14E_DED_EHC_25,
		MAX(PBP_B14E_MAXENR_EHC_26) AS PBP_B14E_MAXENR_EHC_26,
		MAX(PBP_B14E_COINS_PCT_MIN_DIAB_27) AS PBP_B14E_COINS_PCT_MIN_DIAB_27,
		MAX(PBP_B14E_COPAY_AMT_MIN_DIAB_28) AS PBP_B14E_COPAY_AMT_MIN_DIAB_28,
		MAX(PBP_B14E_COPAY_AMT_MAX_DIAB_29) AS PBP_B14E_COPAY_AMT_MAX_DIAB_29,
		MAX(PBP_B14E_COINS_PCT_MAX_DIAB_30) AS PBP_B14E_COINS_PCT_MAX_DIAB_30
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COINS_EHC_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B14E_DED_EHC_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COPAY_EHC_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B14E_MAXENR_EHC_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B14E_COPAY_AMT_MIN_DIAB_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B14E_DED_EHC_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B14E_MAXENR_PER_DIAB_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
				ELSE ''
			END AS PBP_B14E_COPAY_AMT_MAX_DIAB_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B14E_DED_AMT_DIAB_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COPAY_EHC_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B14E_MAXENR_EHC_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B14E_MAXENR_AMT_DIAB_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
				ELSE ''
			END AS PBP_B14E_COINS_PCT_MAX_DIAB_12,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B14E_COINS_PCT_MIN_DIAB_13,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COINS_EHC_14,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B14E_MAXENR_EHC_15,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COPAY_EHC_16,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COINS_EHC_17,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B14E_DED_EHC_18,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e4')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COPAY_EHC_19,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e4')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B14E_DED_EHC_20,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e4')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B14E_MAXENR_EHC_21,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e4')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COINS_EHC_22,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e5')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COPAY_EHC_23,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e5')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B14E_COINS_EHC_24,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e5')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B14E_DED_EHC_25,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e5')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B14E_MAXENR_EHC_26,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B14E_COINS_PCT_MIN_DIAB_27,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B14E_COPAY_AMT_MIN_DIAB_28,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B14E_COPAY_AMT_MAX_DIAB_29,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('14e2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B14E_COINS_PCT_MAX_DIAB_30
		FROM
			jsonlib.root
		LEFT JOIN jsonlib.pbp AS pbp ON
			root.ordinal_root = pbp.ordinal_root
		LEFT JOIN jsonlib.pbp_benefitDetails AS pbp_benefitDetails ON
			pbp.ordinal_pbp = pbp_benefitDetails.ordinal_pbp
		LEFT JOIN jsonlib.benefitDetails_benefDetaiInfo AS benefitDetails_benefDetaiInfo ON
			pbp_benefitDetails.ordinal_benefitDetails = benefitDetails_benefDetaiInfo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefDetaiInfo_benefitDetails AS benefDetaiInfo_benefitDetails ON
			benefitDetails_benefDetaiInfo.ordinal_benefDetaiInfo = benefDetaiInfo_benefitDetails.ordinal_benefDetaiInfo
		LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;



* PROGRAM NAME: JSON_IMPORT_PBPB17.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB17();
PROC SQL;
CREATE TABLE PBPB17_JSON AS
SELECT
	segment_0.qid,
	COALESCE(PBP_B17A_COPAY_AMT_MC_MIN_0, PBP_B17A_COPAY_AMT_MC_MIN_12) AS PBP_B17A_COPAY_AMT_MC_MIN,
	PBP_B17A_MAXENR_AMT_1 AS PBP_B17A_MAXENR_AMT,
	PBP_B17A_DED_AMT_2 AS PBP_B17A_DED_AMT,
	COALESCE(PBP_B17A_MAXENR_PER_3, PBP_B17A_MAXENR_PER_13) AS PBP_B17A_MAXENR_PER,
	COALESCE(PBP_B17A_COINS_PCT_MC_MIN_6, PBP_B17A_COINS_PCT_MC_MIN_11) AS PBP_B17A_COINS_PCT_MC_MIN,
	CASE WHEN PBP_B17A_COPAY_EHC_9 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B17A_COPAY_EHC_4 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B17A_COPAY_EHC_7 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B17A_COPAY_EHC,
	CASE WHEN PBP_B17A_COINS_EHC_10 IN ('1','3') THEN '1' ELSE '0' END ||              
		  CASE WHEN PBP_B17A_COINS_EHC_5 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B17A_COINS_EHC_8 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B17A_COINS_EHC 
FROM
	(
	SELECT
		qid,
		MAX(PBP_B17A_COPAY_AMT_MC_MIN_0) AS PBP_B17A_COPAY_AMT_MC_MIN_0,
		MAX(PBP_B17A_MAXENR_AMT_1) AS PBP_B17A_MAXENR_AMT_1,
		MAX(PBP_B17A_DED_AMT_2) AS PBP_B17A_DED_AMT_2,
		MAX(PBP_B17A_MAXENR_PER_3) AS PBP_B17A_MAXENR_PER_3,
		MAX(PBP_B17A_COPAY_EHC_4) AS PBP_B17A_COPAY_EHC_4,
		MAX(PBP_B17A_COINS_EHC_5) AS PBP_B17A_COINS_EHC_5,
		MAX(PBP_B17A_COINS_PCT_MC_MIN_6) AS PBP_B17A_COINS_PCT_MC_MIN_6,
		MAX(PBP_B17A_COPAY_EHC_7) AS PBP_B17A_COPAY_EHC_7,
		MAX(PBP_B17A_COINS_EHC_8) AS PBP_B17A_COINS_EHC_8,
		MAX(PBP_B17A_COPAY_EHC_9) AS PBP_B17A_COPAY_EHC_9,
		MAX(PBP_B17A_COINS_EHC_10) AS PBP_B17A_COINS_EHC_10,
		MAX(PBP_B17A_COINS_PCT_MC_MIN_11) AS PBP_B17A_COINS_PCT_MC_MIN_11,
		MAX(PBP_B17A_COPAY_AMT_MC_MIN_12) AS PBP_B17A_COPAY_AMT_MC_MIN_12,
		MAX(PBP_B17A_MAXENR_PER_13) AS PBP_B17A_MAXENR_PER_13
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B17A_COPAY_AMT_MC_MIN_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B17A_MAXENR_AMT_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B17A_DED_AMT_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B17A_MAXENR_PER_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B17A_COPAY_EHC_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B17A_COINS_EHC_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B17A_COINS_PCT_MC_MIN_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B17A_COPAY_EHC_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B17A_COINS_EHC_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B17A_COPAY_EHC_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B17A_COINS_EHC_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B17A_COINS_PCT_MC_MIN_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B17A_COPAY_AMT_MC_MIN_12,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('17a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B17A_MAXENR_PER_13
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
		LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;



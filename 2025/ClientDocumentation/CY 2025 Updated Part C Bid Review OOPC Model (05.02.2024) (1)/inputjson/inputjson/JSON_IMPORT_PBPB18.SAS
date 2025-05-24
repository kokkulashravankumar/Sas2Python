* PROGRAM NAME: JSON_IMPORT_PBPB18.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB18();
PROC SQL;
CREATE TABLE PBPB18_JSON AS
SELECT
	segment_0.qid,

	CASE WHEN PBP_B18A_COPAY_EHC_2 IN ('1','3') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B18A_COPAY_EHC_4 IN ('1','3') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B18A_COPAY_EHC_1 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B18A_COPAY_EHC,

		
	CASE WHEN PBP_B18A_COINS_EHC_3 IN ('1','3') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B18A_COINS_EHC_8 IN ('1','3') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B18A_COINS_EHC_0 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B18A_COINS_EHC,

	PBP_B18A_DED_AMT_5 AS PBP_B18A_DED_AMT,
	PBP_B18A_MAXENR_PER_6 AS PBP_B18A_MAXENR_PER,
	COALESCE(PBP_B18A_COPAY_AMT_7, PBP_B18A_COPAY_AMT_12) AS PBP_B18A_COPAY_AMT,
	PBP_B18A_MAXENR_AMT_9 AS PBP_B18A_MAXENR_AMT,
	COALESCE(PBP_B18A_MED_COINS_PCT_10, PBP_B18A_MED_COINS_PCT_11) AS PBP_B18A_MED_COINS_PCT
FROM
	(
	SELECT
		qid,
		MAX(PBP_B18A_COINS_EHC_0) AS PBP_B18A_COINS_EHC_0,
		MAX(PBP_B18A_COPAY_EHC_1) AS PBP_B18A_COPAY_EHC_1,
		MAX(PBP_B18A_COPAY_EHC_2) AS PBP_B18A_COPAY_EHC_2,
		MAX(PBP_B18A_COINS_EHC_3) AS PBP_B18A_COINS_EHC_3,
		MAX(PBP_B18A_COPAY_EHC_4) AS PBP_B18A_COPAY_EHC_4,
		MAX(PBP_B18A_DED_AMT_5) AS PBP_B18A_DED_AMT_5,
		MAX(PBP_B18A_MAXENR_PER_6) AS PBP_B18A_MAXENR_PER_6,
		MAX(PBP_B18A_COPAY_AMT_7) AS PBP_B18A_COPAY_AMT_7,
		MAX(PBP_B18A_COINS_EHC_8) AS PBP_B18A_COINS_EHC_8,
		MAX(PBP_B18A_MAXENR_AMT_9) AS PBP_B18A_MAXENR_AMT_9,
		MAX(PBP_B18A_MED_COINS_PCT_10) AS PBP_B18A_MED_COINS_PCT_10,
		MAX(PBP_B18A_MED_COINS_PCT_11) AS PBP_B18A_MED_COINS_PCT_11,
		MAX(PBP_B18A_COPAY_AMT_12) AS PBP_B18A_COPAY_AMT_12
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B18A_COINS_EHC_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B18A_COPAY_EHC_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B18A_COPAY_EHC_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B18A_COINS_EHC_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B18A_COPAY_EHC_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B18A_DED_AMT_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B18A_MAXENR_PER_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B18A_COPAY_AMT_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B18A_COINS_EHC_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B18A_MAXENR_AMT_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B18A_MED_COINS_PCT_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B18A_MED_COINS_PCT_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('18a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B18A_COPAY_AMT_12
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
		LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;


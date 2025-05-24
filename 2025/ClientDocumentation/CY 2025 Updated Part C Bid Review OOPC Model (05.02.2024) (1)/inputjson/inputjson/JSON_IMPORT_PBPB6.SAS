* PROGRAM NAME: JSON_IMPORT_PBPB6.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB6();
PROC SQL;
CREATE TABLE PBPB6_JSON AS
SELECT
	segment_0.qid,
	COALESCE(PBP_B6_COPAY_MC_AMT_MIN_0, PBP_B6_COPAY_MC_AMT_MIN_6) AS PBP_B6_COPAY_MC_AMT_MIN,
	COALESCE(PBP_B6_COINS_PCT_MC_MIN_1, PBP_B6_COINS_PCT_MC_MIN_5) AS PBP_B6_COINS_PCT_MC_MIN,
	PBP_B6_MAXENR_PER_2 AS PBP_B6_MAXENR_PER,
	PBP_B6_DED_AMT_3 AS PBP_B6_DED_AMT,
	PBP_B6_MAXENR_AMT_4 AS PBP_B6_MAXENR_AMT
FROM
	(
	SELECT
		qid,
		MAX(PBP_B6_COPAY_MC_AMT_MIN_0) AS PBP_B6_COPAY_MC_AMT_MIN_0,
		MAX(PBP_B6_COINS_PCT_MC_MIN_1) AS PBP_B6_COINS_PCT_MC_MIN_1,
		MAX(PBP_B6_MAXENR_PER_2) AS PBP_B6_MAXENR_PER_2,
		MAX(PBP_B6_DED_AMT_3) AS PBP_B6_DED_AMT_3,
		MAX(PBP_B6_MAXENR_AMT_4) AS PBP_B6_MAXENR_AMT_4,
		MAX(PBP_B6_COINS_PCT_MC_MIN_5) AS PBP_B6_COINS_PCT_MC_MIN_5,
		MAX(PBP_B6_COPAY_MC_AMT_MIN_6) AS PBP_B6_COPAY_MC_AMT_MIN_6
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('6')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B6_COPAY_MC_AMT_MIN_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('6')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B6_COINS_PCT_MC_MIN_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('6')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B6_MAXENR_PER_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('6')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B6_DED_AMT_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('6')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B6_MAXENR_AMT_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('6')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B6_COINS_PCT_MC_MIN_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('6')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B6_COPAY_MC_AMT_MIN_6
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
		LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;



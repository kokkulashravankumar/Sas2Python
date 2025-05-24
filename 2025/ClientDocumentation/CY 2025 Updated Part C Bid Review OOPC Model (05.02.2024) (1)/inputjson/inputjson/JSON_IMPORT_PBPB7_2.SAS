* PROGRAM NAME: JSON_IMPORT_PBPB7_2.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB7_2();
PROC SQL;
CREATE TABLE PBPB7_2_JSON AS
SELECT
	segment_0.qid,
	COALESCE(PBP_B7K_COPAY_MC_AMT_0, PBP_B7K_COPAY_MC_AMT_6) AS PBP_B7K_COPAY_MC_AMT,
	PBP_B7K_MAXENR_PER_1 AS PBP_B7K_MAXENR_PER,
	COALESCE(PBP_B7K_COINS_PCT_MC_2, PBP_B7K_COINS_PCT_MC_5) AS PBP_B7K_COINS_PCT_MC,
	PBP_B7K_MAXENR_AMT_3 AS PBP_B7K_MAXENR_AMT,
	PBP_B7K_DED_AMT_4 AS PBP_B7K_DED_AMT
FROM
	(
	SELECT
		qid,
		MAX(PBP_B7K_COPAY_MC_AMT_0) AS PBP_B7K_COPAY_MC_AMT_0,
		MAX(PBP_B7K_MAXENR_PER_1) AS PBP_B7K_MAXENR_PER_1,
		MAX(PBP_B7K_COINS_PCT_MC_2) AS PBP_B7K_COINS_PCT_MC_2,
		MAX(PBP_B7K_MAXENR_AMT_3) AS PBP_B7K_MAXENR_AMT_3,
		MAX(PBP_B7K_DED_AMT_4) AS PBP_B7K_DED_AMT_4,
		MAX(PBP_B7K_COINS_PCT_MC_5) AS PBP_B7K_COINS_PCT_MC_5,
		MAX(PBP_B7K_COPAY_MC_AMT_6) AS PBP_B7K_COPAY_MC_AMT_6
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7k')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B7K_COPAY_MC_AMT_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7k')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B7K_MAXENR_PER_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7k')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B7K_COINS_PCT_MC_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7k')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B7K_MAXENR_AMT_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7k')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B7K_DED_AMT_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7k')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B7K_COINS_PCT_MC_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7k')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B7K_COPAY_MC_AMT_6
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
		LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;



* PROGRAM NAME: JSON_IMPORT_PBPB4.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB4();
PROC SQL;
CREATE TABLE PBPB4_JSON AS
SELECT
	segment_0.qid,
	COALESCE(PBP_B4A_COINS_PCT_MC_MIN_0, PBP_B4A_COINS_PCT_MC_MIN_11) AS PBP_B4A_COINS_PCT_MC_MIN,
	COALESCE(PBP_B4A_COPAY_AMT_MC_MIN_1, PBP_B4A_COPAY_AMT_MC_MIN_13) AS PBP_B4A_COPAY_AMT_MC_MIN,
	PBP_B4A_MAXENR_PER_2 AS PBP_B4A_MAXENR_PER,
	PBP_B4A_MAXENR_AMT_3 AS PBP_B4A_MAXENR_AMT,
	PBP_B4A_MAX_VISIT_4 AS PBP_B4A_MAX_VISIT,
	PBP_B4B_MAX_VISIT_5 AS PBP_B4B_MAX_VISIT,
	PBP_B4B_MAXENR_PER_6 AS PBP_B4B_MAXENR_PER,
	PBP_B4B_MAXENR_TYPE_7 AS PBP_B4B_MAXENR_TYPE,
	COALESCE(PBP_B4B_COPAY_AMT_MC_MIN_8, PBP_B4B_COPAY_AMT_MC_MIN_14) AS PBP_B4B_COPAY_AMT_MC_MIN,
	COALESCE(PBP_B4B_COINS_PCT_MC_MIN_9, PBP_B4B_COINS_PCT_MC_MIN_12) AS PBP_B4B_COINS_PCT_MC_MIN,
	PBP_B4B_MAXENR_AMT_10 AS PBP_B4B_MAXENR_AMT
FROM
	(
	SELECT
		qid,
		MAX(PBP_B4A_COINS_PCT_MC_MIN_0) AS PBP_B4A_COINS_PCT_MC_MIN_0,
		MAX(PBP_B4A_COPAY_AMT_MC_MIN_1) AS PBP_B4A_COPAY_AMT_MC_MIN_1,
		MAX(PBP_B4A_MAXENR_PER_2) AS PBP_B4A_MAXENR_PER_2,
		MAX(PBP_B4A_MAXENR_AMT_3) AS PBP_B4A_MAXENR_AMT_3,
		MAX(PBP_B4A_MAX_VISIT_4) AS PBP_B4A_MAX_VISIT_4,
		MAX(PBP_B4B_MAX_VISIT_5) AS PBP_B4B_MAX_VISIT_5,
		MAX(PBP_B4B_MAXENR_PER_6) AS PBP_B4B_MAXENR_PER_6,
		MAX(PBP_B4B_MAXENR_TYPE_7) AS PBP_B4B_MAXENR_TYPE_7,
		MAX(PBP_B4B_COPAY_AMT_MC_MIN_8) AS PBP_B4B_COPAY_AMT_MC_MIN_8,
		MAX(PBP_B4B_COINS_PCT_MC_MIN_9) AS PBP_B4B_COINS_PCT_MC_MIN_9,
		MAX(PBP_B4B_MAXENR_AMT_10) AS PBP_B4B_MAXENR_AMT_10,
		MAX(PBP_B4A_COINS_PCT_MC_MIN_11) AS PBP_B4A_COINS_PCT_MC_MIN_11,
		MAX(PBP_B4B_COINS_PCT_MC_MIN_12) AS PBP_B4B_COINS_PCT_MC_MIN_12,
		MAX(PBP_B4A_COPAY_AMT_MC_MIN_13) AS PBP_B4A_COPAY_AMT_MC_MIN_13,
		MAX(PBP_B4B_COPAY_AMT_MC_MIN_14) AS PBP_B4B_COPAY_AMT_MC_MIN_14
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CoiAdWaTiCom_CoiAdWaTiComCoi.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B4A_COINS_PCT_MC_MIN_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CopAdWaTiCom_CopAdWaTiComCop.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B4A_COPAY_AMT_MC_MIN_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B4A_MAXENR_PER_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B4A_MAXENR_AMT_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CoiAdWaTiCom_CoiAdWaTiComCoi.bdMaxPerVisitAmount)
				ELSE ''
			END AS PBP_B4A_MAX_VISIT_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CoiAdWaTiCom_CoiAdWaTiComCoi.bdMaxPerVisitAmount)
				ELSE ''
			END AS PBP_B4B_MAX_VISIT_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopPeriodicity)
				ELSE ''
			END AS PBP_B4B_MAXENR_PER_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeRadioGroup)
				ELSE ''
			END AS PBP_B4B_MAXENR_TYPE_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CopAdWaTiCom_CopAdWaTiComCop.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B4B_COPAY_AMT_MC_MIN_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CoiAdWaTiCom_CoiAdWaTiComCoi.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B4B_COINS_PCT_MC_MIN_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopAmt)
				ELSE ''
			END AS PBP_B4B_MAXENR_AMT_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CoiAdWaTiCom_CoiAdWaTiComCoi.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B4A_COINS_PCT_MC_MIN_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CoiAdWaTiCom_CoiAdWaTiComCoi.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B4B_COINS_PCT_MC_MIN_12,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CopAdWaTiCom_CopAdWaTiComCop.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B4A_COPAY_AMT_MC_MIN_13,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('4b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CopAdWaTiCom_CopAdWaTiComCop.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B4B_COPAY_AMT_MC_MIN_14
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
		LEFT JOIN jsonlib.benefitDetails_CoiAdWaTiCom AS benefitDetails_CoiAdWaTiCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoiAdWaTiCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.CoiAdWaTiCom_CoiAdWaTiComCoi AS CoiAdWaTiCom_CoiAdWaTiComCoi ON
			benefitDetails_CoiAdWaTiCom.ordinal_CoiAdWaTiCom = CoiAdWaTiCom_CoiAdWaTiComCoi.ordinal_CoiAdWaTiCom
		LEFT JOIN jsonlib.benefitDetails_CopAdWaTiCom AS benefitDetails_CopAdWaTiCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopAdWaTiCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.CopAdWaTiCom_CopAdWaTiComCop AS CopAdWaTiCom_CopAdWaTiComCop ON
			benefitDetails_CopAdWaTiCom.ordinal_CopAdWaTiCom = CopAdWaTiCom_CopAdWaTiComCop.ordinal_CopAdWaTiCom
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooCosTypCom AS benefitDetails_MooCosTypCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooCosTypCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;

QUIT;
RUN;

%MEND;


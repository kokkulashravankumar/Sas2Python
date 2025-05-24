* PROGRAM NAME: JSON_IMPORT_PBPB20.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB20();
PROC SQL;
CREATE TABLE PBPB20_JSON AS
SELECT
	segment_0.qid,
	PBP_B20_COINS_MC_PCT_0 AS PBP_B20_COINS_MC_PCT,
	PBP_B20_B_COINS_MIN_PCT_1 AS PBP_B20_B_COINS_MIN_PCT,
	PBP_B20_DED_AMT_2 AS PBP_B20_DED_AMT,
	PBP_B20_COPAY_MIN_MC_AMT_3 AS PBP_B20_COPAY_MIN_MC_AMT,
	PBP_B20_B_MIN_COPAY_4 AS PBP_B20_B_MIN_COPAY,
	
     CASE WHEN PBP_B20_COINS_MC_EHC_5 IN ('1','3') THEN '1' ELSE '0' END ||
       CASE WHEN PBP_B20_COINS_MC_EHC_7_1 IN ('1','3') THEN '1' ELSE '0' END ||
       CASE WHEN PBP_B20_COINS_MC_EHC_7 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B20_COINS_MC_EHC,
     CASE WHEN PBP_B20_COPAY_MC_EHC_6 IN ('1','3') THEN '1' ELSE '0' END ||
       CASE WHEN PBP_B20_COPAY_MC_EHC_8_1 IN ('1','3') THEN '1' ELSE '0' END ||
       CASE WHEN PBP_B20_COPAY_MC_EHC_8 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B20_COPAY_MC_EHC
FROM
	(
	SELECT
		qid,
		MAX(PBP_B20_COINS_MC_PCT_0) AS PBP_B20_COINS_MC_PCT_0,
		MAX(PBP_B20_B_COINS_MIN_PCT_1) AS PBP_B20_B_COINS_MIN_PCT_1,
		MAX(PBP_B20_DED_AMT_2) AS PBP_B20_DED_AMT_2,
		MAX(PBP_B20_COPAY_MIN_MC_AMT_3) AS PBP_B20_COPAY_MIN_MC_AMT_3,
		MAX(PBP_B20_B_MIN_COPAY_4) AS PBP_B20_B_MIN_COPAY_4,
		MAX(PBP_B20_COINS_MC_EHC_5) AS PBP_B20_COINS_MC_EHC_5,
		MAX(PBP_B20_COPAY_MC_EHC_6) AS PBP_B20_COPAY_MC_EHC_6,
		MAX(PBP_B20_COINS_MC_EHC_7) AS PBP_B20_COINS_MC_EHC_7,
		MAX(PBP_B20_COINS_MC_EHC_7_1) AS PBP_B20_COINS_MC_EHC_7_1,
		MAX(PBP_B20_COPAY_MC_EHC_8) AS PBP_B20_COPAY_MC_EHC_8,
		MAX(PBP_B20_COPAY_MC_EHC_8_1) AS PBP_B20_COPAY_MC_EHC_8_1
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCoiCom.bdCoinsPartBChemoRadMinAmount)
				ELSE ''
			END AS PBP_B20_COINS_MC_PCT_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCoiCom.bdCoinsPartBOtherMinAmount)
				ELSE ''
			END AS PBP_B20_B_COINS_MIN_PCT_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_prDrBeDeAmCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B20_DED_AMT_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCopCom.bdCopayPartBChemoRadMinAmount)
				ELSE ''
			END AS PBP_B20_COPAY_MIN_MC_AMT_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCopCom.bdCopayPartBOtherMinAmount)
				ELSE ''
			END AS PBP_B20_B_MIN_COPAY_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCoiCom.bdCoinsPartBOtherYesNo)
				ELSE ''
			END AS PBP_B20_COINS_MC_EHC_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCopCom.bdCopayPartBOtherYesNo)
				ELSE ''
			END AS PBP_B20_COPAY_MC_EHC_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCoiCom.bdCoinsPartBChemoRadYesNo)
				ELSE ''
			END AS PBP_B20_COINS_MC_EHC_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCoiCom.bdCoinsPartBInsulinDrugsYesNo)
				ELSE ''
			END AS PBP_B20_COINS_MC_EHC_7_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCopCom.bdCopayPartBChemoRadYesNo)
				ELSE ''
			END AS PBP_B20_COPAY_MC_EHC_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('20')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(PreDruBenCom_preDruBenCopCom.bdCopayPartBInsulinDrugsYesNo)
				ELSE ''
			END AS PBP_B20_COPAY_MC_EHC_8_1
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
		LEFT JOIN jsonlib.PreDruBenCom_preDruBenCoiCom AS PreDruBenCom_preDruBenCoiCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = PreDruBenCom_preDruBenCoiCom.ordinal_PreDruBenCom
		LEFT JOIN jsonlib.PreDruBenCom_prDrBeDeAmCom AS PreDruBenCom_prDrBeDeAmCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = PreDruBenCom_prDrBeDeAmCom.ordinal_PreDruBenCom
		LEFT JOIN jsonlib.PreDruBenCom_preDruBenCopCom AS PreDruBenCom_preDruBenCopCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = PreDruBenCom_preDruBenCopCom.ordinal_PreDruBenCom ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;


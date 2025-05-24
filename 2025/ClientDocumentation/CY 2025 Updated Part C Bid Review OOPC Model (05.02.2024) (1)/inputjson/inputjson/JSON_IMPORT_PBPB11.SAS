* PROGRAM NAME: JSON_IMPORT_PBPB11.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB11();
PROC SQL;
CREATE TABLE PBPB11_JSON AS
SELECT
	segment_0.qid,
	PBP_B11A_MAXENR_AMT_0 AS PBP_B11A_MAXENR_AMT,
	PBP_B11A_MAXENR_PER_1 AS PBP_B11A_MAXENR_PER,
	COALESCE(PBP_B11A_COINS_PCT_MC_2, PBP_B11A_COINS_PCT_MC_17) AS PBP_B11A_COINS_PCT_MC,
	COALESCE(PBP_B11A_COPAY_MC_AMT_3, PBP_B11A_COPAY_MC_AMT_20) AS PBP_B11A_COPAY_MC_AMT,
	PBP_B11A_DED_AMT_4 AS PBP_B11A_DED_AMT,
	COALESCE(PBP_B11B_COPAY_MCMIN_AMT_5, PBP_B11B_COPAY_MCMIN_AMT_21) AS PBP_B11B_COPAY_MCMIN_AMT,

     CASE WHEN PBP_B11B_COINS_EHC_10 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B11B_COINS_EHC_6 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B11B_COINS_EHC,
     CASE WHEN PBP_B11B_COPAY_EHC_11 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B11B_COPAY_EHC_7 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B11B_COPAY_EHC,

	COALESCE(PBP_B11B_COINS_PCT_MC_8, PBP_B11B_COINS_PCT_MC_18) AS PBP_B11B_COINS_PCT_MC,
	COALESCE(PBP_B11B_COINS_PCT_MCMS_MIN_9, PBP_B11B_COINS_PCT_MCMS_MIN_19) AS PBP_B11B_COINS_PCT_MCMS_MIN,
	COALESCE(PBP_B11B_COPAY_MCMS_MIN_AMT_12, PBP_B11B_COPAY_MCMS_MIN_AMT_22) AS PBP_B11B_COPAY_MCMS_MIN_AMT,
	PBP_B11B_MAXENR_COST_TYPE_13 AS PBP_B11B_MAXENR_COST_TYPE,
	PBP_B11B_MAXENR_AMT_14 AS PBP_B11B_MAXENR_AMT,
	PBP_B11B_DED_AMT_15 AS PBP_B11B_DED_AMT,
	PBP_B11B_MAXENR_PER_16 AS PBP_B11B_MAXENR_PER
FROM
	(
	SELECT
		qid,
		MAX(PBP_B11A_MAXENR_AMT_0) AS PBP_B11A_MAXENR_AMT_0,
		MAX(PBP_B11A_MAXENR_PER_1) AS PBP_B11A_MAXENR_PER_1,
		MAX(PBP_B11A_COINS_PCT_MC_2) AS PBP_B11A_COINS_PCT_MC_2,
		MAX(PBP_B11A_COPAY_MC_AMT_3) AS PBP_B11A_COPAY_MC_AMT_3,
		MAX(PBP_B11A_DED_AMT_4) AS PBP_B11A_DED_AMT_4,
		MAX(PBP_B11B_COPAY_MCMIN_AMT_5) AS PBP_B11B_COPAY_MCMIN_AMT_5,
		MAX(PBP_B11B_COINS_EHC_6) AS PBP_B11B_COINS_EHC_6,
		MAX(PBP_B11B_COPAY_EHC_7) AS PBP_B11B_COPAY_EHC_7,
		MAX(PBP_B11B_COINS_PCT_MC_8) AS PBP_B11B_COINS_PCT_MC_8,
		MAX(PBP_B11B_COINS_PCT_MCMS_MIN_9) AS PBP_B11B_COINS_PCT_MCMS_MIN_9,
		MAX(PBP_B11B_COINS_EHC_10) AS PBP_B11B_COINS_EHC_10,
		MAX(PBP_B11B_COPAY_EHC_11) AS PBP_B11B_COPAY_EHC_11,
		MAX(PBP_B11B_COPAY_MCMS_MIN_AMT_12) AS PBP_B11B_COPAY_MCMS_MIN_AMT_12,
		MAX(PBP_B11B_MAXENR_COST_TYPE_13) AS PBP_B11B_MAXENR_COST_TYPE_13,
		MAX(PBP_B11B_MAXENR_AMT_14) AS PBP_B11B_MAXENR_AMT_14,
		MAX(PBP_B11B_DED_AMT_15) AS PBP_B11B_DED_AMT_15,
		MAX(PBP_B11B_MAXENR_PER_16) AS PBP_B11B_MAXENR_PER_16,
		MAX(PBP_B11A_COINS_PCT_MC_17) AS PBP_B11A_COINS_PCT_MC_17,
		MAX(PBP_B11B_COINS_PCT_MC_18) AS PBP_B11B_COINS_PCT_MC_18,
		MAX(PBP_B11B_COINS_PCT_MCMS_MIN_19) AS PBP_B11B_COINS_PCT_MCMS_MIN_19,
		MAX(PBP_B11A_COPAY_MC_AMT_20) AS PBP_B11A_COPAY_MC_AMT_20,
		MAX(PBP_B11B_COPAY_MCMIN_AMT_21) AS PBP_B11B_COPAY_MCMIN_AMT_21,
		MAX(PBP_B11B_COPAY_MCMS_MIN_AMT_22) AS PBP_B11B_COPAY_MCMS_MIN_AMT_22
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B11A_MAXENR_AMT_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B11A_MAXENR_PER_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B11A_COINS_PCT_MC_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B11A_COPAY_MC_AMT_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B11A_DED_AMT_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B11B_COPAY_MCMIN_AMT_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B11B_COINS_EHC_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B11B_COPAY_EHC_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B11B_COINS_PCT_MC_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B11B_COINS_PCT_MCMS_MIN_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B11B_COINS_EHC_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B11B_COPAY_EHC_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B11B_COPAY_MCMS_MIN_AMT_12,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeRadioGroup)
				ELSE ''
			END AS PBP_B11B_MAXENR_COST_TYPE_13,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopAmt)
				ELSE ''
			END AS PBP_B11B_MAXENR_AMT_14,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B11B_DED_AMT_15,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopPeriodicity)
				ELSE ''
			END AS PBP_B11B_MAXENR_PER_16,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B11A_COINS_PCT_MC_17,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B11B_COINS_PCT_MC_18,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B11B_COINS_PCT_MCMS_MIN_19,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B11A_COPAY_MC_AMT_20,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B11B_COPAY_MCMIN_AMT_21,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('11b2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B11B_COPAY_MCMS_MIN_AMT_22
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
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooCosTypCom AS benefitDetails_MooCosTypCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooCosTypCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;



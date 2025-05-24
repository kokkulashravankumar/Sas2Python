* PROGRAM NAME: JSON_IMPORT_PBPB9.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB9();
PROC SQL;
CREATE TABLE PBPB9_JSON AS
SELECT
	segment_0.qid,
	COALESCE(PBP_B9A_COPAY_OBS_AMT_MIN_2, PBP_B9A_COPAY_OBS_AMT_MIN_29) AS PBP_B9A_COPAY_OBS_AMT_MIN,
	COALESCE(PBP_B9A_COINS_OHS_PCT_MIN_3, PBP_B9A_COINS_OHS_PCT_MIN_26) AS PBP_B9A_COINS_OHS_PCT_MIN,
	PBP_B9A_MAXENR_OHS_AMT_4 AS PBP_B9A_MAXENR_OHS_AMT,

	CASE WHEN PBP_B9A_COPAY_EHC_19 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B9A_COPAY_EHC_5 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B9A_COPAY_EHC,
     CASE WHEN PBP_B9A_DED_EHC_18 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B9A_DED_EHC_6 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B9A_DED_EHC,

	COALESCE(PBP_B9A_COPAY_OHS_AMT_MIN_7, PBP_B9A_COPAY_OHS_AMT_MIN_30) AS PBP_B9A_COPAY_OHS_AMT_MIN,

	CASE WHEN PBP_B9A_COINS_EHC_14 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B9A_COINS_EHC_8 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B9A_COINS_EHC,

	PBP_B9A_MAXENR_OHS_PER_9 AS PBP_B9A_MAXENR_OHS_PER,
	PBP_B9A_DED_OHS_AMT_10 AS PBP_B9A_DED_OHS_AMT,

    CASE WHEN PBP_B9A_MAXENR_EHC_13 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B9A_MAXENR_EHC_11 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B9A_MAXENR_EHC,
	
	PBP_B9A_MAXENR_OBS_AMT_12 AS PBP_B9A_MAXENR_OBS_AMT,
	PBP_B9A_MAXENR_OBS_PER_15 AS PBP_B9A_MAXENR_OBS_PER,
	PBP_B9A_DED_OBS_AMT_16 AS PBP_B9A_DED_OBS_AMT,
	COALESCE(PBP_B9A_COINS_OBS_PCT_MIN_17, PBP_B9A_COINS_OBS_PCT_MIN_27) AS PBP_B9A_COINS_OBS_PCT_MIN,
	COALESCE(PBP_B9B_COINS_PCT_MC_20, PBP_B9B_COINS_PCT_MC_28) AS PBP_B9B_COINS_PCT_MC,
	COALESCE(PBP_B9B_COPAY_MC_AMT_21, PBP_B9B_COPAY_MC_AMT_31) AS PBP_B9B_COPAY_MC_AMT,
	PBP_B9B_MAXENR_TYPE_22 AS PBP_B9B_MAXENR_TYPE,
	PBP_B9B_MAXENR_AMT_23 AS PBP_B9B_MAXENR_AMT,
	PBP_B9B_DED_AMT_24 AS PBP_B9B_DED_AMT,
	PBP_B9B_MAXENR_PER_25 AS PBP_B9B_MAXENR_PER
FROM
	(
	SELECT
		qid,
		MAX(PBP_B9A_COPAY_OBS_AMT_MIN_2) AS PBP_B9A_COPAY_OBS_AMT_MIN_2,
		MAX(PBP_B9A_COINS_OHS_PCT_MIN_3) AS PBP_B9A_COINS_OHS_PCT_MIN_3,
		MAX(PBP_B9A_MAXENR_OHS_AMT_4) AS PBP_B9A_MAXENR_OHS_AMT_4,
		MAX(PBP_B9A_COPAY_EHC_5) AS PBP_B9A_COPAY_EHC_5,
		MAX(PBP_B9A_DED_EHC_6) AS PBP_B9A_DED_EHC_6,
		MAX(PBP_B9A_COPAY_OHS_AMT_MIN_7) AS PBP_B9A_COPAY_OHS_AMT_MIN_7,
		MAX(PBP_B9A_COINS_EHC_8) AS PBP_B9A_COINS_EHC_8,
		MAX(PBP_B9A_MAXENR_OHS_PER_9) AS PBP_B9A_MAXENR_OHS_PER_9,
		MAX(PBP_B9A_DED_OHS_AMT_10) AS PBP_B9A_DED_OHS_AMT_10,
		MAX(PBP_B9A_MAXENR_EHC_11) AS PBP_B9A_MAXENR_EHC_11,
		MAX(PBP_B9A_MAXENR_OBS_AMT_12) AS PBP_B9A_MAXENR_OBS_AMT_12,
		MAX(PBP_B9A_MAXENR_EHC_13) AS PBP_B9A_MAXENR_EHC_13,
		MAX(PBP_B9A_COINS_EHC_14) AS PBP_B9A_COINS_EHC_14,
		MAX(PBP_B9A_MAXENR_OBS_PER_15) AS PBP_B9A_MAXENR_OBS_PER_15,
		MAX(PBP_B9A_DED_OBS_AMT_16) AS PBP_B9A_DED_OBS_AMT_16,
		MAX(PBP_B9A_COINS_OBS_PCT_MIN_17) AS PBP_B9A_COINS_OBS_PCT_MIN_17,
		MAX(PBP_B9A_DED_EHC_18) AS PBP_B9A_DED_EHC_18,
		MAX(PBP_B9A_COPAY_EHC_19) AS PBP_B9A_COPAY_EHC_19,
		MAX(PBP_B9B_COINS_PCT_MC_20) AS PBP_B9B_COINS_PCT_MC_20,
		MAX(PBP_B9B_COPAY_MC_AMT_21) AS PBP_B9B_COPAY_MC_AMT_21,
		MAX(PBP_B9B_MAXENR_TYPE_22) AS PBP_B9B_MAXENR_TYPE_22,
		MAX(PBP_B9B_MAXENR_AMT_23) AS PBP_B9B_MAXENR_AMT_23,
		MAX(PBP_B9B_DED_AMT_24) AS PBP_B9B_DED_AMT_24,
		MAX(PBP_B9B_MAXENR_PER_25) AS PBP_B9B_MAXENR_PER_25,
		MAX(PBP_B9A_COINS_OHS_PCT_MIN_26) AS PBP_B9A_COINS_OHS_PCT_MIN_26,
		MAX(PBP_B9A_COINS_OBS_PCT_MIN_27) AS PBP_B9A_COINS_OBS_PCT_MIN_27,
		MAX(PBP_B9B_COINS_PCT_MC_28) AS PBP_B9B_COINS_PCT_MC_28,
		MAX(PBP_B9A_COPAY_OBS_AMT_MIN_29) AS PBP_B9A_COPAY_OBS_AMT_MIN_29,
		MAX(PBP_B9A_COPAY_OHS_AMT_MIN_30) AS PBP_B9A_COPAY_OHS_AMT_MIN_30,
		MAX(PBP_B9B_COPAY_MC_AMT_31) AS PBP_B9B_COPAY_MC_AMT_31
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CopObsSerCom_bdcopayCompone.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B9A_COPAY_OBS_AMT_MIN_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B9A_COINS_OHS_PCT_MIN_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B9A_MAXENR_OHS_AMT_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B9A_COPAY_EHC_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B9A_DED_EHC_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B9A_COPAY_OHS_AMT_MIN_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B9A_COINS_EHC_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B9A_MAXENR_OHS_PER_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B9A_DED_OHS_AMT_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B9A_MAXENR_EHC_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B9A_MAXENR_OBS_AMT_12,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B9A_MAXENR_EHC_13,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B9A_COINS_EHC_14,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B9A_MAXENR_OBS_PER_15,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B9A_DED_OBS_AMT_16,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B9A_COINS_OBS_PCT_MIN_17,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B9A_DED_EHC_18,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CopObsSerCom_bdcopayCompone.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B9A_COPAY_EHC_19,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B9B_COINS_PCT_MC_20,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B9B_COPAY_MC_AMT_21,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeRadioGroup)
				ELSE ''
			END AS PBP_B9B_MAXENR_TYPE_22,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopAmt)
				ELSE ''
			END AS PBP_B9B_MAXENR_AMT_23,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B9B_DED_AMT_24,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopPeriodicity)
				ELSE ''
			END AS PBP_B9B_MAXENR_PER_25,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B9A_COINS_OHS_PCT_MIN_26,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B9A_COINS_OBS_PCT_MIN_27,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B9B_COINS_PCT_MC_28,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(CopObsSerCom_bdcopayCompone.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B9A_COPAY_OBS_AMT_MIN_29,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B9A_COPAY_OHS_AMT_MIN_30,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('9b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B9B_COPAY_MC_AMT_31
		FROM
			jsonlib.root
		LEFT JOIN jsonlib.pbp AS pbp ON
			root.ordinal_root = pbp.ordinal_root
		LEFT JOIN jsonlib.pbp_priAutAndRef AS pbp_priAutAndRef ON
			pbp.ordinal_pbp = pbp_priAutAndRef.ordinal_pbp
		LEFT JOIN jsonlib.priAutAndRef_priorAuthori AS priAutAndRef_priorAuthori ON
			pbp_priAutAndRef.ordinal_priAutAndRef = priAutAndRef_priorAuthori.ordinal_priAutAndRef
		LEFT JOIN jsonlib.priorAuthori_priorAuthoDetai AS priorAuthori_priorAuthoDetai ON
			priAutAndRef_priorAuthori.ordinal_priorAuthori = priorAuthori_priorAuthoDetai.ordinal_priorAuthori
		LEFT JOIN jsonlib.priorAuthoDetai_inNeAuSeCaSe AS priorAuthoDetai_inNeAuSeCaSe ON
			priorAuthori_priorAuthoDetai.ordinal_priorAuthoDetai = priorAuthoDetai_inNeAuSeCaSe.ordinal_priorAuthoDetai
		LEFT JOIN jsonlib.inNeAuSeCaSe_medicare AS inNeAuSeCaSe_medicare ON
			priorAuthoDetai_inNeAuSeCaSe.ordinal_inNeAuSeCaSe = inNeAuSeCaSe_medicare.ordinal_inNeAuSeCaSe
		LEFT JOIN jsonlib.pbp_benefitDetails AS pbp_benefitDetails ON
			pbp.ordinal_pbp = pbp_benefitDetails.ordinal_pbp
		LEFT JOIN jsonlib.benefitDetails_benefDetaiInfo AS benefitDetails_benefDetaiInfo ON
			pbp_benefitDetails.ordinal_benefitDetails = benefitDetails_benefDetaiInfo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefDetaiInfo_benefitDetails AS benefDetaiInfo_benefitDetails ON
			benefitDetails_benefDetaiInfo.ordinal_benefDetaiInfo = benefDetaiInfo_benefitDetails.ordinal_benefDetaiInfo
		LEFT JOIN jsonlib.benefitDetails_CopObsSerCom AS benefitDetails_CopObsSerCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopObsSerCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.CopObsSerCom_bdcopayCompone AS CopObsSerCom_bdcopayCompone ON
			benefitDetails_CopObsSerCom.ordinal_CopObsSerCom = CopObsSerCom_bdcopayCompone.ordinal_CopObsSerCom
		LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
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



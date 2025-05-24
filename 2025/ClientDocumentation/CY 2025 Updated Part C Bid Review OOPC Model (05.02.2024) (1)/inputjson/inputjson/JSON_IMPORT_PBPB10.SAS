* PROGRAM NAME: JSON_IMPORT_PBPB10.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB10();
PROC SQL;
CREATE TABLE PBPB10_JSON AS
SELECT
	segment_0.qid,
	PBP_B10A_COINS_WAV_YN_0 AS PBP_B10A_COINS_WAV_YN,
	PBP_B10A_DED_GAS_AMT_1 AS PBP_B10A_DED_GAS_AMT,
	PBP_B10A_MAXENR_GAS_AMT_2 AS PBP_B10A_MAXENR_GAS_AMT,

	CASE WHEN PBP_B10A_DED_EHC_16 IN ('1') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B10A_DED_EHC_3 IN ('1') THEN '1' ELSE '0' END AS PBP_B10A_DED_EHC,
     CASE WHEN PBP_B10A_MAXENR_EHC_21 IN ('1') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B10A_MAXENR_EHC_4 IN ('1') THEN '1' ELSE '0' END AS PBP_B10A_MAXENR_EHC,
     CASE WHEN PBP_B10A_COPAY_EHC_12 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B10A_COPAY_EHC_5 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B10A_COPAY_EHC,

	COALESCE(PBP_B10A_COINS_GAS_PCT_MIN_6, PBP_B10A_COINS_GAS_PCT_MIN_24) AS PBP_B10A_COINS_GAS_PCT_MIN,
	COALESCE(PBP_B10A_COINS_GAS_PCT_MAX_7, PBP_B10A_COINS_GAS_PCT_MAX_29) AS PBP_B10A_COINS_GAS_PCT_MAX,
	COALESCE(PBP_B10A_COPAY_GAS_AMT_MIN_8, PBP_B10A_COPAY_GAS_AMT_MIN_26) AS PBP_B10A_COPAY_GAS_AMT_MIN,
	COALESCE(PBP_B10A_COPAY_GAS_AMT_MAX_9, PBP_B10A_COPAY_GAS_AMT_MAX_30) AS PBP_B10A_COPAY_GAS_AMT_MAX,
	
	PBP_B10A_MAXENR_GAS_PER_10 AS PBP_B10A_MAXENR_GAS_PER,
	
	CASE WHEN PBP_B10A_COINS_EHC_14 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B10A_COINS_EHC_11 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B10A_COINS_EHC,

	PBP_B10A_MAXENR_AAS_AMT_13 AS PBP_B10A_MAXENR_AAS_AMT,
	COALESCE(PBP_B10A_COINS_AAS_PCT_MIN_15, PBP_B10A_COINS_AAS_PCT_MIN_25) AS PBP_B10A_COINS_AAS_PCT_MIN,
	COALESCE(PBP_B10A_COINS_AAS_PCT_MAX_17, PBP_B10A_COINS_AAS_PCT_MAX_31) AS PBP_B10A_COINS_AAS_PCT_MAX,
	PBP_B10A_MAXENR_AAS_PER_18 AS PBP_B10A_MAXENR_AAS_PER,
	PBP_B10A_DED_AAS_AMT_19 AS PBP_B10A_DED_AAS_AMT,
	COALESCE(PBP_B10A_COPAY_AAS_AMT_MIN_20, PBP_B10A_COPAY_AAS_AMT_MIN_27) AS PBP_B10A_COPAY_AAS_AMT_MIN,
	COALESCE(PBP_B10A_COPAY_AAS_AMT_MAX_22, PBP_B10A_COPAY_AAS_AMT_MAX_28) AS PBP_B10A_COPAY_AAS_AMT_MAX,
	PBP_B10A_COPAY_WAV_YN_23 AS PBP_B10A_COPAY_WAV_YN
FROM
	(
	SELECT
		qid,
		MAX(PBP_B10A_COINS_WAV_YN_0) AS PBP_B10A_COINS_WAV_YN_0,
		MAX(PBP_B10A_DED_GAS_AMT_1) AS PBP_B10A_DED_GAS_AMT_1,
		MAX(PBP_B10A_MAXENR_GAS_AMT_2) AS PBP_B10A_MAXENR_GAS_AMT_2,
		MAX(PBP_B10A_DED_EHC_3) AS PBP_B10A_DED_EHC_3,
		MAX(PBP_B10A_MAXENR_EHC_4) AS PBP_B10A_MAXENR_EHC_4,
		MAX(PBP_B10A_COPAY_EHC_5) AS PBP_B10A_COPAY_EHC_5,
		MAX(PBP_B10A_COINS_GAS_PCT_MIN_6) AS PBP_B10A_COINS_GAS_PCT_MIN_6,
		MAX(PBP_B10A_COINS_GAS_PCT_MAX_7) AS PBP_B10A_COINS_GAS_PCT_MAX_7,
		MAX(PBP_B10A_COPAY_GAS_AMT_MIN_8) AS PBP_B10A_COPAY_GAS_AMT_MIN_8,
		MAX(PBP_B10A_COPAY_GAS_AMT_MAX_9) AS PBP_B10A_COPAY_GAS_AMT_MAX_9,
		MAX(PBP_B10A_MAXENR_GAS_PER_10) AS PBP_B10A_MAXENR_GAS_PER_10,
		MAX(PBP_B10A_COINS_EHC_11) AS PBP_B10A_COINS_EHC_11,
		MAX(PBP_B10A_COPAY_EHC_12) AS PBP_B10A_COPAY_EHC_12,
		MAX(PBP_B10A_MAXENR_AAS_AMT_13) AS PBP_B10A_MAXENR_AAS_AMT_13,
		MAX(PBP_B10A_COINS_EHC_14) AS PBP_B10A_COINS_EHC_14,
		MAX(PBP_B10A_COINS_AAS_PCT_MIN_15) AS PBP_B10A_COINS_AAS_PCT_MIN_15,
		MAX(PBP_B10A_DED_EHC_16) AS PBP_B10A_DED_EHC_16,
		MAX(PBP_B10A_COINS_AAS_PCT_MAX_17) AS PBP_B10A_COINS_AAS_PCT_MAX_17,
		MAX(PBP_B10A_MAXENR_AAS_PER_18) AS PBP_B10A_MAXENR_AAS_PER_18,
		MAX(PBP_B10A_DED_AAS_AMT_19) AS PBP_B10A_DED_AAS_AMT_19,
		MAX(PBP_B10A_COPAY_AAS_AMT_MIN_20) AS PBP_B10A_COPAY_AAS_AMT_MIN_20,
		MAX(PBP_B10A_MAXENR_EHC_21) AS PBP_B10A_MAXENR_EHC_21,
		MAX(PBP_B10A_COPAY_AAS_AMT_MAX_22) AS PBP_B10A_COPAY_AAS_AMT_MAX_22,
		MAX(PBP_B10A_COPAY_WAV_YN_23) AS PBP_B10A_COPAY_WAV_YN_23,
		MAX(PBP_B10A_COINS_GAS_PCT_MIN_24) AS PBP_B10A_COINS_GAS_PCT_MIN_24,
		MAX(PBP_B10A_COINS_AAS_PCT_MIN_25) AS PBP_B10A_COINS_AAS_PCT_MIN_25,
		MAX(PBP_B10A_COPAY_GAS_AMT_MIN_26) AS PBP_B10A_COPAY_GAS_AMT_MIN_26,
		MAX(PBP_B10A_COPAY_AAS_AMT_MIN_27) AS PBP_B10A_COPAY_AAS_AMT_MIN_27,
		MAX(PBP_B10A_COPAY_AAS_AMT_MAX_28) AS PBP_B10A_COPAY_AAS_AMT_MAX_28,
		MAX(PBP_B10A_COINS_GAS_PCT_MAX_29) AS PBP_B10A_COINS_GAS_PCT_MAX_29,
		MAX(PBP_B10A_COPAY_GAS_AMT_MAX_30) AS PBP_B10A_COPAY_GAS_AMT_MAX_30,
		MAX(PBP_B10A_COINS_AAS_PCT_MAX_31) AS PBP_B10A_COINS_AAS_PCT_MAX_31
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoiAdWaHoCom.admittedHopsitalYesNo)
				ELSE ''
			END AS PBP_B10A_COINS_WAV_YN_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B10A_DED_GAS_AMT_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B10A_MAXENR_GAS_AMT_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B10A_DED_EHC_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B10A_MAXENR_EHC_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B10A_COPAY_EHC_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B10A_COINS_GAS_PCT_MIN_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
				ELSE ''
			END AS PBP_B10A_COINS_GAS_PCT_MAX_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_GAS_AMT_MIN_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_GAS_AMT_MAX_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B10A_MAXENR_GAS_PER_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B10A_COINS_EHC_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B10A_COPAY_EHC_12,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B10A_MAXENR_AAS_AMT_13,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS PBP_B10A_COINS_EHC_14,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B10A_COINS_AAS_PCT_MIN_15,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleYesNo)
				ELSE ''
			END AS PBP_B10A_DED_EHC_16,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
				ELSE ''
			END AS PBP_B10A_COINS_AAS_PCT_MAX_17,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B10A_MAXENR_AAS_PER_18,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B10A_DED_AAS_AMT_19,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_AAS_AMT_MIN_20,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmountYesNo)
				ELSE ''
			END AS PBP_B10A_MAXENR_EHC_21,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_AAS_AMT_MAX_22,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopAdWaHoCom.admittedHopsitalYesNo)
				ELSE ''
			END AS PBP_B10A_COPAY_WAV_YN_23,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B10A_COINS_GAS_PCT_MIN_24,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B10A_COINS_AAS_PCT_MIN_25,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_GAS_AMT_MIN_26,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_AAS_AMT_MIN_27,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_AAS_AMT_MAX_28,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B10A_COINS_GAS_PCT_MAX_29,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS PBP_B10A_COPAY_GAS_AMT_MAX_30,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('10a2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS PBP_B10A_COINS_AAS_PCT_MAX_31
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
		LEFT JOIN jsonlib.benefitDetails_CoiAdWaHoCom AS benefitDetails_CoiAdWaHoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoiAdWaHoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_CopAdWaHoCom AS benefitDetails_CopAdWaHoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopAdWaHoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;



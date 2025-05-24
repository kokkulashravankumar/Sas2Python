* PROGRAM NAME: JSON_IMPORT_PBPB8.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB8();
PROC SQL;
CREATE TABLE PBPB8_JSON AS
SELECT
	segment_0.qid,
	PBP_B8A_MAXENR_PER_0 AS PBP_B8A_MAXENR_PER,
	PBP_B8A_MAXENR_AMT_1 AS PBP_B8A_MAXENR_AMT,
	PBP_B8A_COPAY_MAX_YN_2 AS PBP_B8A_COPAY_MAX_YN,
	PBP_B8A_DED_AMT_3 AS PBP_B8A_DED_AMT,
	COALESCE(PBP_B8A_COINS_PCT_DMC_4, PBP_B8A_COINS_PCT_DMC_38) AS PBP_B8A_COINS_PCT_DMC,
	COALESCE(PBP_B8A_COPAY_MAX_DMC_AMT_5, PBP_B8A_COPAY_MAX_DMC_AMT_50) AS PBP_B8A_COPAY_MAX_DMC_AMT,

	CASE WHEN PBP_B8A_COINS_EHC_15 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B8A_COINS_EHC_6 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B8A_COINS_EHC,

	COALESCE(PBP_B8A_COPAY_MIN_DMC_AMT_7, PBP_B8A_COPAY_MIN_DMC_AMT_43) AS PBP_B8A_COPAY_MIN_DMC_AMT,
	COALESCE(PBP_B8A_COINS_PCT_DMC_MAX_8, PBP_B8A_COINS_PCT_DMC_MAX_51) AS PBP_B8A_COINS_PCT_DMC_MAX,

    CASE WHEN PBP_B8A_COPAY_EHC_11 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B8A_COPAY_EHC_9 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B8A_COPAY_EHC,

	COALESCE(PBP_B8A_COINS_PCT_LAB_10, PBP_B8A_COINS_PCT_LAB_39) AS PBP_B8A_COINS_PCT_LAB,
	COALESCE(PBP_B8A_COINS_PCT_LAB_MAX_12, PBP_B8A_COINS_PCT_LAB_MAX_48) AS PBP_B8A_COINS_PCT_LAB_MAX,
	COALESCE(PBP_B8A_LAB_COPAY_AMT_MAX_13, PBP_B8A_LAB_COPAY_AMT_MAX_49) AS PBP_B8A_LAB_COPAY_AMT_MAX,
	COALESCE(PBP_B8A_LAB_COPAY_AMT_14, PBP_B8A_LAB_COPAY_AMT_44) AS PBP_B8A_LAB_COPAY_AMT,
	PBP_B8B_MAXENR_AMT_16 AS PBP_B8B_MAXENR_AMT,
	PBP_B8B_COPAY_MAX_YN_17 AS PBP_B8B_COPAY_MAX_YN,
	PBP_B8B_MAXENR_PER_18 AS PBP_B8B_MAXENR_PER,
	PBP_B8B_DED_AMT_19 AS PBP_B8B_DED_AMT,
	COALESCE(PBP_B8B_COINS_PCT_DRS_MAX_20, PBP_B8B_COINS_PCT_DRS_MAX_52) AS PBP_B8B_COINS_PCT_DRS_MAX,
	COALESCE(PBP_B8B_COINS_PCT_DRS_21, PBP_B8B_COINS_PCT_DRS_40) AS PBP_B8B_COINS_PCT_DRS,

    CASE WHEN PBP_B8B_COPAY_EHC_33 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B8B_COPAY_EHC_22 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B8B_COPAY_EHC_30 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B8B_COPAY_EHC,

	COALESCE(PBP_B8B_COPAY_AMT_DRS_23, PBP_B8B_COPAY_AMT_DRS_45) AS PBP_B8B_COPAY_AMT_DRS,

	  CASE WHEN PBP_B8B_COINS_EHC_36 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B8B_COINS_EHC_24 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B8B_COINS_EHC_29 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B8B_COINS_EHC,
	COALESCE(PBP_B8B_COPAY_AMT_DRS_MAX_25, PBP_B8B_COPAY_AMT_DRS_MAX_53) AS PBP_B8B_COPAY_AMT_DRS_MAX,
	COALESCE(PBP_B8B_COPAY_AMT_TMC_26, PBP_B8B_COPAY_AMT_TMC_46) AS PBP_B8B_COPAY_AMT_TMC,
	COALESCE(PBP_B8B_COINS_PCT_TMC_27, PBP_B8B_COINS_PCT_TMC_41) AS PBP_B8B_COINS_PCT_TMC,
	COALESCE(PBP_B8B_COINS_PCT_TMC_MAX_28, PBP_B8B_COINS_PCT_TMC_MAX_54) AS PBP_B8B_COINS_PCT_TMC_MAX,
	COALESCE(PBP_B8B_COPAY_AMT_TMC_MAX_31, PBP_B8B_COPAY_AMT_TMC_MAX_55) AS PBP_B8B_COPAY_AMT_TMC_MAX,
	
	COALESCE(PBP_B8B_COPAY_MC_AMT_MAX_32, PBP_B8B_COPAY_MC_AMT_MAX_56) AS PBP_B8B_COPAY_MC_AMT_MAX,
	COALESCE(PBP_B8B_COINS_PCT_CMC_MAX_34, PBP_B8B_COINS_PCT_CMC_MAX_57) AS PBP_B8B_COINS_PCT_CMC_MAX,
	
	COALESCE(PBP_B8B_COPAY_MC_AMT_35, PBP_B8B_COPAY_MC_AMT_47) AS PBP_B8B_COPAY_MC_AMT,
	COALESCE(PBP_B8B_COINS_PCT_CMC_37, PBP_B8B_COINS_PCT_CMC_42) AS PBP_B8B_COINS_PCT_CMC
FROM
	(
	SELECT
		qid,
		MAX(PBP_B8A_MAXENR_PER_0) AS PBP_B8A_MAXENR_PER_0,
		MAX(PBP_B8A_MAXENR_AMT_1) AS PBP_B8A_MAXENR_AMT_1,
		MAX(PBP_B8A_COPAY_MAX_YN_2) AS PBP_B8A_COPAY_MAX_YN_2
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B8A_MAXENR_PER_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B8A_MAXENR_AMT_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopMuSeLoCom.bdMaximumCopayYesNo)
				ELSE ''
			END AS PBP_B8A_COPAY_MAX_YN_2
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
		LEFT JOIN jsonlib.benefitDetails_CopMuSeLoCom AS benefitDetails_CopMuSeLoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopMuSeLoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_DED_AMT_3) AS PBP_B8A_DED_AMT_3,
		MAX(PBP_B8A_COINS_PCT_DMC_4) AS PBP_B8A_COINS_PCT_DMC_4,
		MAX(PBP_B8A_COPAY_MAX_DMC_AMT_5) AS PBP_B8A_COPAY_MAX_DMC_AMT_5
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
					ELSE ''
				END AS PBP_B8A_DED_AMT_3,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B8A_COINS_PCT_DMC_4,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
							ELSE ''
						END AS PBP_B8A_COPAY_MAX_DMC_AMT_5
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
					LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
					LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
					LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_1 ON
	segment_0.qid = segment_1.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_COINS_EHC_6) AS PBP_B8A_COINS_EHC_6,
		MAX(PBP_B8A_COPAY_MIN_DMC_AMT_7) AS PBP_B8A_COPAY_MIN_DMC_AMT_7,
		MAX(PBP_B8A_COINS_PCT_DMC_MAX_8) AS PBP_B8A_COINS_PCT_DMC_MAX_8
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B8A_COINS_EHC_6,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
						ELSE ''
					END AS PBP_B8A_COPAY_MIN_DMC_AMT_7,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
							ELSE ''
						END AS PBP_B8A_COINS_PCT_DMC_MAX_8
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_2 ON
	segment_0.qid = segment_2.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_COPAY_EHC_9) AS PBP_B8A_COPAY_EHC_9,
		MAX(PBP_B8A_COINS_PCT_LAB_10) AS PBP_B8A_COINS_PCT_LAB_10,
		MAX(PBP_B8A_COPAY_EHC_11) AS PBP_B8A_COPAY_EHC_11
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B8A_COPAY_EHC_9,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B8A_COINS_PCT_LAB_10,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
							ELSE ''
						END AS PBP_B8A_COPAY_EHC_11
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_3 ON
	segment_0.qid = segment_3.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_COINS_PCT_LAB_MAX_12) AS PBP_B8A_COINS_PCT_LAB_MAX_12,
		MAX(PBP_B8A_LAB_COPAY_AMT_MAX_13) AS PBP_B8A_LAB_COPAY_AMT_MAX_13,
		MAX(PBP_B8A_LAB_COPAY_AMT_14) AS PBP_B8A_LAB_COPAY_AMT_14
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
					ELSE ''
				END AS PBP_B8A_COINS_PCT_LAB_MAX_12,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
						ELSE ''
					END AS PBP_B8A_LAB_COPAY_AMT_MAX_13,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
							ELSE ''
						END AS PBP_B8A_LAB_COPAY_AMT_14
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_4 ON
	segment_0.qid = segment_4.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_COINS_EHC_15) AS PBP_B8A_COINS_EHC_15,
		MAX(PBP_B8B_MAXENR_AMT_16) AS PBP_B8B_MAXENR_AMT_16,
		MAX(PBP_B8B_COPAY_MAX_YN_17) AS PBP_B8B_COPAY_MAX_YN_17
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B8A_COINS_EHC_15,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
						ELSE ''
					END AS PBP_B8B_MAXENR_AMT_16,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CMSL8BC.bdMaximumCopayYesNo8B)
							ELSE ''
						END AS PBP_B8B_COPAY_MAX_YN_17
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
					LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
					LEFT JOIN jsonlib.benefitDetails_CMSL8BC AS benefitDetails_CMSL8BC ON
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CMSL8BC.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_5 ON
	segment_0.qid = segment_5.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_MAXENR_PER_18) AS PBP_B8B_MAXENR_PER_18,
		MAX(PBP_B8B_DED_AMT_19) AS PBP_B8B_DED_AMT_19,
		MAX(PBP_B8B_COINS_PCT_DRS_MAX_20) AS PBP_B8B_COINS_PCT_DRS_MAX_20
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
					ELSE ''
				END AS PBP_B8B_MAXENR_PER_18,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
						ELSE ''
					END AS PBP_B8B_DED_AMT_19,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
							ELSE ''
						END AS PBP_B8B_COINS_PCT_DRS_MAX_20
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
					LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
					LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_6 ON
	segment_0.qid = segment_6.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COINS_PCT_DRS_21) AS PBP_B8B_COINS_PCT_DRS_21,
		MAX(PBP_B8B_COPAY_EHC_22) AS PBP_B8B_COPAY_EHC_22,
		MAX(PBP_B8B_COPAY_AMT_DRS_23) AS PBP_B8B_COPAY_AMT_DRS_23
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B8B_COINS_PCT_DRS_21,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B8B_COPAY_EHC_22,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
							ELSE ''
						END AS PBP_B8B_COPAY_AMT_DRS_23
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_7 ON
	segment_0.qid = segment_7.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COINS_EHC_24) AS PBP_B8B_COINS_EHC_24,
		MAX(PBP_B8B_COPAY_AMT_DRS_MAX_25) AS PBP_B8B_COPAY_AMT_DRS_MAX_25,
		MAX(PBP_B8B_COPAY_AMT_TMC_26) AS PBP_B8B_COPAY_AMT_TMC_26
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B8B_COINS_EHC_24,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
						ELSE ''
					END AS PBP_B8B_COPAY_AMT_DRS_MAX_25,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
							ELSE ''
						END AS PBP_B8B_COPAY_AMT_TMC_26
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_8 ON
	segment_0.qid = segment_8.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COINS_PCT_TMC_27) AS PBP_B8B_COINS_PCT_TMC_27,
		MAX(PBP_B8B_COINS_PCT_TMC_MAX_28) AS PBP_B8B_COINS_PCT_TMC_MAX_28,
		MAX(PBP_B8B_COINS_EHC_29) AS PBP_B8B_COINS_EHC_29
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B8B_COINS_PCT_TMC_27,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
						ELSE ''
					END AS PBP_B8B_COINS_PCT_TMC_MAX_28,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
							ELSE ''
						END AS PBP_B8B_COINS_EHC_29
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_9 ON
	segment_0.qid = segment_9.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COPAY_EHC_30) AS PBP_B8B_COPAY_EHC_30,
		MAX(PBP_B8B_COPAY_AMT_TMC_MAX_31) AS PBP_B8B_COPAY_AMT_TMC_MAX_31,
		MAX(PBP_B8B_COPAY_MC_AMT_MAX_32) AS PBP_B8B_COPAY_MC_AMT_MAX_32
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B8B_COPAY_EHC_30,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
						ELSE ''
					END AS PBP_B8B_COPAY_AMT_TMC_MAX_31,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMaxAmount)
							ELSE ''
						END AS PBP_B8B_COPAY_MC_AMT_MAX_32
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_10 ON
	segment_0.qid = segment_10.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COPAY_EHC_33) AS PBP_B8B_COPAY_EHC_33,
		MAX(PBP_B8B_COINS_PCT_CMC_MAX_34) AS PBP_B8B_COINS_PCT_CMC_MAX_34,
		MAX(PBP_B8B_COPAY_MC_AMT_35) AS PBP_B8B_COPAY_MC_AMT_35
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B8B_COPAY_EHC_33,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMaxAmount)
						ELSE ''
					END AS PBP_B8B_COINS_PCT_CMC_MAX_34,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
							ELSE ''
						END AS PBP_B8B_COPAY_MC_AMT_35
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_11 ON
	segment_0.qid = segment_11.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COINS_EHC_36) AS PBP_B8B_COINS_EHC_36,
		MAX(PBP_B8B_COINS_PCT_CMC_37) AS PBP_B8B_COINS_PCT_CMC_37,
		MAX(PBP_B8A_COINS_PCT_DMC_38) AS PBP_B8A_COINS_PCT_DMC_38
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B8B_COINS_EHC_36,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B8B_COINS_PCT_CMC_37,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
							ELSE ''
						END AS PBP_B8A_COINS_PCT_DMC_38
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_12 ON
	segment_0.qid = segment_12.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_COINS_PCT_LAB_39) AS PBP_B8A_COINS_PCT_LAB_39,
		MAX(PBP_B8B_COINS_PCT_DRS_40) AS PBP_B8B_COINS_PCT_DRS_40,
		MAX(PBP_B8B_COINS_PCT_TMC_41) AS PBP_B8B_COINS_PCT_TMC_41
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B8A_COINS_PCT_LAB_39,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
						ELSE ''
					END AS PBP_B8B_COINS_PCT_DRS_40,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
							ELSE ''
						END AS PBP_B8B_COINS_PCT_TMC_41
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_13 ON
	segment_0.qid = segment_13.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COINS_PCT_CMC_42) AS PBP_B8B_COINS_PCT_CMC_42,
		MAX(PBP_B8A_COPAY_MIN_DMC_AMT_43) AS PBP_B8A_COPAY_MIN_DMC_AMT_43,
		MAX(PBP_B8A_LAB_COPAY_AMT_44) AS PBP_B8A_LAB_COPAY_AMT_44
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B8B_COINS_PCT_CMC_42,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
						ELSE ''
					END AS PBP_B8A_COPAY_MIN_DMC_AMT_43,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B8A_LAB_COPAY_AMT_44
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_14 ON
	segment_0.qid = segment_14.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COPAY_AMT_DRS_45) AS PBP_B8B_COPAY_AMT_DRS_45,
		MAX(PBP_B8B_COPAY_AMT_TMC_46) AS PBP_B8B_COPAY_AMT_TMC_46,
		MAX(PBP_B8B_COPAY_MC_AMT_47) AS PBP_B8B_COPAY_MC_AMT_47
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
					ELSE ''
				END AS PBP_B8B_COPAY_AMT_DRS_45,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
						ELSE ''
					END AS PBP_B8B_COPAY_AMT_TMC_46,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B8B_COPAY_MC_AMT_47
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_15 ON
	segment_0.qid = segment_15.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_COINS_PCT_LAB_MAX_48) AS PBP_B8A_COINS_PCT_LAB_MAX_48,
		MAX(PBP_B8A_LAB_COPAY_AMT_MAX_49) AS PBP_B8A_LAB_COPAY_AMT_MAX_49,
		MAX(PBP_B8A_COPAY_MAX_DMC_AMT_50) AS PBP_B8A_COPAY_MAX_DMC_AMT_50
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B8A_COINS_PCT_LAB_MAX_48,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
						ELSE ''
					END AS PBP_B8A_LAB_COPAY_AMT_MAX_49,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B8A_COPAY_MAX_DMC_AMT_50
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_16 ON
	segment_0.qid = segment_16.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8A_COINS_PCT_DMC_MAX_51) AS PBP_B8A_COINS_PCT_DMC_MAX_51,
		MAX(PBP_B8B_COINS_PCT_DRS_MAX_52) AS PBP_B8B_COINS_PCT_DRS_MAX_52,
		MAX(PBP_B8B_COPAY_AMT_DRS_MAX_53) AS PBP_B8B_COPAY_AMT_DRS_MAX_53
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8a1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B8A_COINS_PCT_DMC_MAX_51,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
						ELSE ''
					END AS PBP_B8B_COINS_PCT_DRS_MAX_52,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B8B_COPAY_AMT_DRS_MAX_53
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
						benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_17 ON
	segment_0.qid = segment_17.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COINS_PCT_TMC_MAX_54) AS PBP_B8B_COINS_PCT_TMC_MAX_54,
		MAX(PBP_B8B_COPAY_AMT_TMC_MAX_55) AS PBP_B8B_COPAY_AMT_TMC_MAX_55
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B8B_COINS_PCT_TMC_MAX_54,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
						ELSE ''
					END AS PBP_B8B_COPAY_AMT_TMC_MAX_55
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
					benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_18 ON
	segment_0.qid = segment_18.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B8B_COPAY_MC_AMT_MAX_56) AS PBP_B8B_COPAY_MC_AMT_MAX_56,
		MAX(PBP_B8B_COINS_PCT_CMC_MAX_57) AS PBP_B8B_COINS_PCT_CMC_MAX_57
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
					ELSE ''
				END AS PBP_B8B_COPAY_MC_AMT_MAX_56,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('8b3')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
						ELSE ''
					END AS PBP_B8B_COINS_PCT_CMC_MAX_57
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
					benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_19 ON
	segment_0.qid = segment_19.qid;
QUIT;
RUN;

%MEND;


	
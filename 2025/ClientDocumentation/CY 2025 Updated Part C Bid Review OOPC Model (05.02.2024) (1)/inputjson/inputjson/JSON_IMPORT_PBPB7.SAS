* PROGRAM NAME: JSON_IMPORT_PBPB7.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB7();
PROC SQL;
CREATE TABLE PBPB7_JSON AS
SELECT
	segment_0.qid,
	coalesce(PBP_B7A_COPAY_AMT_MC_MIN_0, PBP_B7A_COPAY_AMT_MC_MIN_72) AS PBP_B7A_COPAY_AMT_MC_MIN,
	PBP_B7A_MAXENR_PER_1 AS PBP_B7A_MAXENR_PER,
	coalesce(PBP_B7A_COINS_PCT_MC_MIN_2, PBP_B7A_COINS_PCT_MC_MIN_63) AS PBP_B7A_COINS_PCT_MC_MIN,
	PBP_B7A_MAXENR_AMT_3 AS PBP_B7A_MAXENR_AMT,
	PBP_B7A_DED_AMT_4 AS PBP_B7A_DED_AMT,
	PBP_B7B_MAXENR_AMT_5 AS PBP_B7B_MAXENR_AMT,
	coalesce(PBP_B7B_COPAY_MC_AMT_MIN_6, PBP_B7B_COPAY_MC_AMT_MIN_73) AS PBP_B7B_COPAY_MC_AMT_MIN,
	PBP_B7B_DED_AMT_7 AS PBP_B7B_DED_AMT,
	PBP_B7B_MAXENR_PER_8 AS PBP_B7B_MAXENR_PER,
        CASE WHEN PBP_B7B_COPAY_EHC_60 IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7B_COPAY_EHC_9 IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7B_COPAY_EHC_57 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7B_COPAY_EHC,
	coalesce(PBP_B7B_COINS_PCT_MC_MIN_10, PBP_B7B_COINS_PCT_MC_MIN_64) AS PBP_B7B_COINS_PCT_MC_MIN,
        CASE WHEN PBP_B7B_COINS_EHC_59 IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7B_COINS_EHC_11 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN PBP_B7B_COINS_EHC_58 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7B_COINS_EHC,
	PBP_B7C_MAXENR_AMT_12 AS PBP_B7C_MAXENR_AMT,
	coalesce(PBP_B7C_COINS_PCT_MC_MIN_13, PBP_B7C_COINS_PCT_MC_MIN_65) AS PBP_B7C_COINS_PCT_MC_MIN,
	coalesce(PBP_B7C_COPAY_MC_AMT_MIN_14, PBP_B7C_COPAY_MC_AMT_MIN_74) AS PBP_B7C_COPAY_MC_AMT_MIN,
	PBP_B7C_DED_AMT_15 AS PBP_B7C_DED_AMT,
	PBP_B7C_MAXENR_PER_16 AS PBP_B7C_MAXENR_PER,
	coalesce(PBP_B7D_COPAY_AMT_MC_MIN_17, PBP_B7D_COPAY_AMT_MC_MIN_75) AS PBP_B7D_COPAY_AMT_MC_MIN,
	PBP_B7D_MAXENR_PER_18 AS PBP_B7D_MAXENR_PER,
	coalesce(PBP_B7D_COINS_PCT_MC_MIN_19, PBP_B7D_COINS_PCT_MC_MIN_66) AS PBP_B7D_COINS_PCT_MC_MIN,
	PBP_B7D_DED_AMT_20 AS PBP_B7D_DED_AMT,
	PBP_B7D_MAXENR_AMT_21 AS PBP_B7D_MAXENR_AMT,
	PBP_B7E_MAXENR_PER_22 AS PBP_B7E_MAXENR_PER,
	PBP_B7E_MAXENR_AMT_23 AS PBP_B7E_MAXENR_AMT,
	PBP_B7E_DED_AMT_24 AS PBP_B7E_DED_AMT,
	COALESCE(PBP_B7E_COINS_MCIS_MINPCT_25, PBP_B7E_COINS_MCIS_MINPCT_67) AS PBP_B7E_COINS_MCIS_MINPCT,
        CASE WHEN PBP_B7E_COPAY_EHC_30 IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7E_COPAY_EHC_26  IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7E_COPAY_EHC,
	COALESCE(PBP_B7E_COPAY_MCIS_MINAMT_27, PBP_B7E_COPAY_MCIS_MINAMT_76) AS PBP_B7E_COPAY_MCIS_MINAMT,
	CASE WHEN PBP_B7E_COINS_EHC_29 IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7E_COINS_EHC_28 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7E_COINS_EHC,
	PBP_B7F_MAXENR_PER_31 AS PBP_B7F_MAXENR_PER,
	CASE WHEN PBP_B7F_COINS_EHC_61 IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7F_COINS_EHC_32  IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7F_COINS_EHC,
	coalesce(PBP_B7F_COINS_PCT_MC_MIN_33, PBP_B7F_COINS_PCT_MC_MIN_68) AS PBP_B7F_COINS_PCT_MC_MIN,
        CASE WHEN PBP_B7F_COPAY_EHC_62  IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7F_COPAY_EHC_34 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7F_COPAY_EHC,
	PBP_B7F_DED_AMT_35 AS PBP_B7F_DED_AMT,
	coalesce(PBP_B7F_COPAY_MC_AMT_MIN_36, PBP_B7F_COPAY_MC_AMT_MIN_77) AS PBP_B7F_COPAY_MC_AMT_MIN,
	PBP_B7F_MAXENR_AMT_37 AS PBP_B7F_MAXENR_AMT,
	PBP_B7G_MAXENR_PER_38 AS PBP_B7G_MAXENR_PER,
	PBP_B7G_DED_AMT_39 AS PBP_B7G_DED_AMT,
	COALESCE(PBP_B7G_COPAY_MC_AMT_40, PBP_B7G_COPAY_MC_AMT_78) AS PBP_B7G_COPAY_MC_AMT,
	PBP_B7G_MAXENR_AMT_41 AS PBP_B7G_MAXENR_AMT,
	COALESCE(PBP_B7G_COINS_PCT_MC_42, PBP_B7G_COINS_PCT_MC_69) AS PBP_B7G_COINS_PCT_MC,
	PBP_B7H_MAXENR_PER_43 AS PBP_B7H_MAXENR_PER,
	PBP_B7H_DED_AMT_44 AS PBP_B7H_DED_AMT,
	PBP_B7H_MAXENR_AMT_45 AS PBP_B7H_MAXENR_AMT,
        CASE WHEN PBP_B7H_COPAY_EHC_51 IN ('1','3') THEN '1' ELSE '0' END ||
	CASE WHEN PBP_B7H_COPAY_EHC_46 IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7H_COPAY_EHC,
	COALESCE(PBP_B7H_COPAY_MCIS_MINAMT_48, PBP_B7H_COPAY_MCIS_MINAMT_79) AS PBP_B7H_COPAY_MCIS_MINAMT,
	COALESCE(PBP_B7H_COINS_MCIS_MINPCT_49, PBP_B7H_COINS_MCIS_MINPCT_70) AS PBP_B7H_COINS_MCIS_MINPCT,

	CASE WHEN PBP_B7H_COINS_EHC_50 IN ('1','3') THEN '1' ELSE '0' END ||
	  CASE WHEN PBP_B7H_COINS_EHC_47  IN ('1','3') THEN '1' ELSE '0' END AS PBP_B7H_COINS_EHC,
	PBP_B7I_DED_AMT_52 AS PBP_B7I_DED_AMT,
	coalesce(PBP_B7I_COPAY_MC_AMT_MIN_53, PBP_B7I_COPAY_MC_AMT_MIN_80) AS PBP_B7I_COPAY_MC_AMT_MIN,
	PBP_B7I_MAXENR_AMT_54 AS PBP_B7I_MAXENR_AMT,
	PBP_B7I_MAXENR_PER_55 AS PBP_B7I_MAXENR_PER,
	coalesce(PBP_B7I_COINS_PCT_MC_MIN_56, PBP_B7I_COINS_PCT_MC_MIN_71) AS PBP_B7I_COINS_PCT_MC_MIN
FROM
	(
	SELECT
		qid,
		MAX(PBP_B7A_COPAY_AMT_MC_MIN_0) AS PBP_B7A_COPAY_AMT_MC_MIN_0,
		MAX(PBP_B7A_MAXENR_PER_1) AS PBP_B7A_MAXENR_PER_1,
		MAX(PBP_B7A_COINS_PCT_MC_MIN_2) AS PBP_B7A_COINS_PCT_MC_MIN_2,
		MAX(PBP_B7A_MAXENR_AMT_3) AS PBP_B7A_MAXENR_AMT_3,
		MAX(PBP_B7A_DED_AMT_4) AS PBP_B7A_DED_AMT_4
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS PBP_B7A_COPAY_AMT_MC_MIN_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS PBP_B7A_MAXENR_PER_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS PBP_B7A_COINS_PCT_MC_MIN_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS PBP_B7A_MAXENR_AMT_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7a')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS PBP_B7A_DED_AMT_4
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
		qid) segment_0
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7B_MAXENR_AMT_5) AS PBP_B7B_MAXENR_AMT_5,
		MAX(PBP_B7B_COPAY_MC_AMT_MIN_6) AS PBP_B7B_COPAY_MC_AMT_MIN_6,
		MAX(PBP_B7B_DED_AMT_7) AS PBP_B7B_DED_AMT_7,
		MAX(PBP_B7B_MAXENR_PER_8) AS PBP_B7B_MAXENR_PER_8
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
					ELSE ''
				END AS PBP_B7B_MAXENR_AMT_5,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
						ELSE ''
					END AS PBP_B7B_COPAY_MC_AMT_MIN_6,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
							ELSE ''
						END AS PBP_B7B_DED_AMT_7,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
								ELSE ''
							END AS PBP_B7B_MAXENR_PER_8
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
						LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
						LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_1 ON
	segment_0.qid = segment_1.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7B_COPAY_EHC_9) AS PBP_B7B_COPAY_EHC_9,
		MAX(PBP_B7B_COINS_PCT_MC_MIN_10) AS PBP_B7B_COINS_PCT_MC_MIN_10,
		MAX(PBP_B7B_COINS_EHC_11) AS PBP_B7B_COINS_EHC_11,
		MAX(PBP_B7C_MAXENR_AMT_12) AS PBP_B7C_MAXENR_AMT_12
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B7B_COPAY_EHC_9,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B7B_COINS_PCT_MC_MIN_10,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
							ELSE ''
						END AS PBP_B7B_COINS_EHC_11,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7c')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
								ELSE ''
							END AS PBP_B7C_MAXENR_AMT_12
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
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_2 ON
	segment_0.qid = segment_2.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7C_COINS_PCT_MC_MIN_13) AS PBP_B7C_COINS_PCT_MC_MIN_13,
		MAX(PBP_B7C_COPAY_MC_AMT_MIN_14) AS PBP_B7C_COPAY_MC_AMT_MIN_14,
		MAX(PBP_B7C_DED_AMT_15) AS PBP_B7C_DED_AMT_15,
		MAX(PBP_B7C_MAXENR_PER_16) AS PBP_B7C_MAXENR_PER_16
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7c')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B7C_COINS_PCT_MC_MIN_13,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7c')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
						ELSE ''
					END AS PBP_B7C_COPAY_MC_AMT_MIN_14,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7c')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
							ELSE ''
						END AS PBP_B7C_DED_AMT_15,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7c')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
								ELSE ''
							END AS PBP_B7C_MAXENR_PER_16
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
		qid) segment_3 ON
	segment_0.qid = segment_3.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7D_COPAY_AMT_MC_MIN_17) AS PBP_B7D_COPAY_AMT_MC_MIN_17,
		MAX(PBP_B7D_MAXENR_PER_18) AS PBP_B7D_MAXENR_PER_18,
		MAX(PBP_B7D_COINS_PCT_MC_MIN_19) AS PBP_B7D_COINS_PCT_MC_MIN_19,
		MAX(PBP_B7D_DED_AMT_20) AS PBP_B7D_DED_AMT_20
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7d')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
					ELSE ''
				END AS PBP_B7D_COPAY_AMT_MC_MIN_17,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7d')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
						ELSE ''
					END AS PBP_B7D_MAXENR_PER_18,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7d')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
							ELSE ''
						END AS PBP_B7D_COINS_PCT_MC_MIN_19,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7d')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
								ELSE ''
							END AS PBP_B7D_DED_AMT_20
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
		qid) segment_4 ON
	segment_0.qid = segment_4.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7D_MAXENR_AMT_21) AS PBP_B7D_MAXENR_AMT_21,
		MAX(PBP_B7E_MAXENR_PER_22) AS PBP_B7E_MAXENR_PER_22,
		MAX(PBP_B7E_MAXENR_AMT_23) AS PBP_B7E_MAXENR_AMT_23,
		MAX(PBP_B7E_DED_AMT_24) AS PBP_B7E_DED_AMT_24
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7d')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
					ELSE ''
				END AS PBP_B7D_MAXENR_AMT_21,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
						ELSE ''
					END AS PBP_B7E_MAXENR_PER_22,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
							ELSE ''
						END AS PBP_B7E_MAXENR_AMT_23,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
								ELSE ''
							END AS PBP_B7E_DED_AMT_24
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
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_5 ON
	segment_0.qid = segment_5.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7E_COINS_MCIS_MINPCT_25) AS PBP_B7E_COINS_MCIS_MINPCT_25,
		MAX(PBP_B7E_COPAY_EHC_26) AS PBP_B7E_COPAY_EHC_26,
		MAX(PBP_B7E_COPAY_MCIS_MINAMT_27) AS PBP_B7E_COPAY_MCIS_MINAMT_27,
		MAX(PBP_B7E_COINS_EHC_28) AS PBP_B7E_COINS_EHC_28
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B7E_COINS_MCIS_MINPCT_25,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B7E_COPAY_EHC_26,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
							ELSE ''
						END AS PBP_B7E_COPAY_MCIS_MINAMT_27,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
								ELSE ''
							END AS PBP_B7E_COINS_EHC_28
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
		qid) segment_6 ON
	segment_0.qid = segment_6.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7E_COINS_EHC_29) AS PBP_B7E_COINS_EHC_29,
		MAX(PBP_B7E_COPAY_EHC_30) AS PBP_B7E_COPAY_EHC_30,
		MAX(PBP_B7F_MAXENR_PER_31) AS PBP_B7F_MAXENR_PER_31,
		MAX(PBP_B7F_COINS_EHC_32) AS PBP_B7F_COINS_EHC_32
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B7E_COINS_EHC_29,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B7E_COPAY_EHC_30,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
							ELSE ''
						END AS PBP_B7F_MAXENR_PER_31,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
								ELSE ''
							END AS PBP_B7F_COINS_EHC_32
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
						LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_7 ON
	segment_0.qid = segment_7.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7F_COINS_PCT_MC_MIN_33) AS PBP_B7F_COINS_PCT_MC_MIN_33,
		MAX(PBP_B7F_COPAY_EHC_34) AS PBP_B7F_COPAY_EHC_34,
		MAX(PBP_B7F_DED_AMT_35) AS PBP_B7F_DED_AMT_35,
		MAX(PBP_B7F_COPAY_MC_AMT_MIN_36) AS PBP_B7F_COPAY_MC_AMT_MIN_36
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B7F_COINS_PCT_MC_MIN_33,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B7F_COPAY_EHC_34,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
							ELSE ''
						END AS PBP_B7F_DED_AMT_35,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
								ELSE ''
							END AS PBP_B7F_COPAY_MC_AMT_MIN_36
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
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_8 ON
	segment_0.qid = segment_8.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7F_MAXENR_AMT_37) AS PBP_B7F_MAXENR_AMT_37,
		MAX(PBP_B7G_MAXENR_PER_38) AS PBP_B7G_MAXENR_PER_38,
		MAX(PBP_B7G_DED_AMT_39) AS PBP_B7G_DED_AMT_39,
		MAX(PBP_B7G_COPAY_MC_AMT_40) AS PBP_B7G_COPAY_MC_AMT_40
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
					ELSE ''
				END AS PBP_B7F_MAXENR_AMT_37,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7g')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
						ELSE ''
					END AS PBP_B7G_MAXENR_PER_38,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7g')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
							ELSE ''
						END AS PBP_B7G_DED_AMT_39,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7g')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
								ELSE ''
							END AS PBP_B7G_COPAY_MC_AMT_40
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
						LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_9 ON
	segment_0.qid = segment_9.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7G_MAXENR_AMT_41) AS PBP_B7G_MAXENR_AMT_41,
		MAX(PBP_B7G_COINS_PCT_MC_42) AS PBP_B7G_COINS_PCT_MC_42,
		MAX(PBP_B7H_MAXENR_PER_43) AS PBP_B7H_MAXENR_PER_43,
		MAX(PBP_B7H_DED_AMT_44) AS PBP_B7H_DED_AMT_44
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7g')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
					ELSE ''
				END AS PBP_B7G_MAXENR_AMT_41,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7g')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B7G_COINS_PCT_MC_42,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
							ELSE ''
						END AS PBP_B7H_MAXENR_PER_43,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
								ELSE ''
							END AS PBP_B7H_DED_AMT_44
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
						LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_10 ON
	segment_0.qid = segment_10.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7H_MAXENR_AMT_45) AS PBP_B7H_MAXENR_AMT_45,
		MAX(PBP_B7H_COPAY_EHC_46) AS PBP_B7H_COPAY_EHC_46,
		MAX(PBP_B7H_COINS_EHC_47) AS PBP_B7H_COINS_EHC_47,
		MAX(PBP_B7H_COPAY_MCIS_MINAMT_48) AS PBP_B7H_COPAY_MCIS_MINAMT_48
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
					ELSE ''
				END AS PBP_B7H_MAXENR_AMT_45,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B7H_COPAY_EHC_46,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
							ELSE ''
						END AS PBP_B7H_COINS_EHC_47,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
								ELSE ''
							END AS PBP_B7H_COPAY_MCIS_MINAMT_48
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
		MAX(PBP_B7H_COINS_MCIS_MINPCT_49) AS PBP_B7H_COINS_MCIS_MINPCT_49,
		MAX(PBP_B7H_COINS_EHC_50) AS PBP_B7H_COINS_EHC_50,
		MAX(PBP_B7H_COPAY_EHC_51) AS PBP_B7H_COPAY_EHC_51,
		MAX(PBP_B7I_DED_AMT_52) AS PBP_B7I_DED_AMT_52
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B7H_COINS_MCIS_MINPCT_49,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B7H_COINS_EHC_50,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
							ELSE ''
						END AS PBP_B7H_COPAY_EHC_51,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7i')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
								ELSE ''
							END AS PBP_B7I_DED_AMT_52
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
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_12 ON
	segment_0.qid = segment_12.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7I_COPAY_MC_AMT_MIN_53) AS PBP_B7I_COPAY_MC_AMT_MIN_53,
		MAX(PBP_B7I_MAXENR_AMT_54) AS PBP_B7I_MAXENR_AMT_54,
		MAX(PBP_B7I_MAXENR_PER_55) AS PBP_B7I_MAXENR_PER_55,
		MAX(PBP_B7I_COINS_PCT_MC_MIN_56) AS PBP_B7I_COINS_PCT_MC_MIN_56
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7i')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
					ELSE ''
				END AS PBP_B7I_COPAY_MC_AMT_MIN_53,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7i')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
						ELSE ''
					END AS PBP_B7I_MAXENR_AMT_54,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7i')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
							ELSE ''
						END AS PBP_B7I_MAXENR_PER_55,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7i')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
								ELSE ''
							END AS PBP_B7I_COINS_PCT_MC_MIN_56
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
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_13 ON
	segment_0.qid = segment_13.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7B_COPAY_EHC_57) AS PBP_B7B_COPAY_EHC_57,
		MAX(PBP_B7B_COINS_EHC_58) AS PBP_B7B_COINS_EHC_58,
		MAX(PBP_B7B_COINS_EHC_59) AS PBP_B7B_COINS_EHC_59,
		MAX(PBP_B7B_COPAY_EHC_60) AS PBP_B7B_COPAY_EHC_60
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B7B_COPAY_EHC_57,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B7B_COINS_EHC_58,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
							ELSE ''
						END AS PBP_B7B_COINS_EHC_59,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
								ELSE ''
							END AS PBP_B7B_COPAY_EHC_60
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
		qid) segment_14 ON
	segment_0.qid = segment_14.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7F_COINS_EHC_61) AS PBP_B7F_COINS_EHC_61,
		MAX(PBP_B7F_COPAY_EHC_62) AS PBP_B7F_COPAY_EHC_62,
		MAX(PBP_B7A_COINS_PCT_MC_MIN_63) AS PBP_B7A_COINS_PCT_MC_MIN_63,
		MAX(PBP_B7B_COINS_PCT_MC_MIN_64) AS PBP_B7B_COINS_PCT_MC_MIN_64
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
					ELSE ''
				END AS PBP_B7F_COINS_EHC_61,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
						ELSE ''
					END AS PBP_B7F_COPAY_EHC_62,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7a')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
							ELSE ''
						END AS PBP_B7A_COINS_PCT_MC_MIN_63,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
								ELSE ''
							END AS PBP_B7B_COINS_PCT_MC_MIN_64
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
		qid) segment_15 ON
	segment_0.qid = segment_15.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7C_COINS_PCT_MC_MIN_65) AS PBP_B7C_COINS_PCT_MC_MIN_65,
		MAX(PBP_B7D_COINS_PCT_MC_MIN_66) AS PBP_B7D_COINS_PCT_MC_MIN_66,
		MAX(PBP_B7E_COINS_MCIS_MINPCT_67) AS PBP_B7E_COINS_MCIS_MINPCT_67,
		MAX(PBP_B7F_COINS_PCT_MC_MIN_68) AS PBP_B7F_COINS_PCT_MC_MIN_68
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7c')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B7C_COINS_PCT_MC_MIN_65,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7d')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
						ELSE ''
					END AS PBP_B7D_COINS_PCT_MC_MIN_66,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
							ELSE ''
						END AS PBP_B7E_COINS_MCIS_MINPCT_67,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
								ELSE ''
							END AS PBP_B7F_COINS_PCT_MC_MIN_68
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
		qid) segment_16 ON
	segment_0.qid = segment_16.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7G_COINS_PCT_MC_69) AS PBP_B7G_COINS_PCT_MC_69,
		MAX(PBP_B7H_COINS_MCIS_MINPCT_70) AS PBP_B7H_COINS_MCIS_MINPCT_70,
		MAX(PBP_B7I_COINS_PCT_MC_MIN_71) AS PBP_B7I_COINS_PCT_MC_MIN_71,
		MAX(PBP_B7A_COPAY_AMT_MC_MIN_72) AS PBP_B7A_COPAY_AMT_MC_MIN_72
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7g')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B7G_COINS_PCT_MC_69,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
						ELSE ''
					END AS PBP_B7H_COINS_MCIS_MINPCT_70,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7i')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
							ELSE ''
						END AS PBP_B7I_COINS_PCT_MC_MIN_71,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7a')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
								ELSE ''
							END AS PBP_B7A_COPAY_AMT_MC_MIN_72
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
		MAX(PBP_B7B_COPAY_MC_AMT_MIN_73) AS PBP_B7B_COPAY_MC_AMT_MIN_73,
		MAX(PBP_B7C_COPAY_MC_AMT_MIN_74) AS PBP_B7C_COPAY_MC_AMT_MIN_74,
		MAX(PBP_B7D_COPAY_AMT_MC_MIN_75) AS PBP_B7D_COPAY_AMT_MC_MIN_75,
		MAX(PBP_B7E_COPAY_MCIS_MINAMT_76) AS PBP_B7E_COPAY_MCIS_MINAMT_76
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
					ELSE ''
				END AS PBP_B7B_COPAY_MC_AMT_MIN_73,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7c')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
						ELSE ''
					END AS PBP_B7C_COPAY_MC_AMT_MIN_74,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7d')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B7D_COPAY_AMT_MC_MIN_75,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7e1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
								ELSE ''
							END AS PBP_B7E_COPAY_MCIS_MINAMT_76
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
		qid) segment_18 ON
	segment_0.qid = segment_18.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B7F_COPAY_MC_AMT_MIN_77) AS PBP_B7F_COPAY_MC_AMT_MIN_77,
		MAX(PBP_B7G_COPAY_MC_AMT_78) AS PBP_B7G_COPAY_MC_AMT_78,
		MAX(PBP_B7H_COPAY_MCIS_MINAMT_79) AS PBP_B7H_COPAY_MCIS_MINAMT_79,
		MAX(PBP_B7I_COPAY_MC_AMT_MIN_80) AS PBP_B7I_COPAY_MC_AMT_MIN_80
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7f')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
					ELSE ''
				END AS PBP_B7F_COPAY_MC_AMT_MIN_77,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7g')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
						ELSE ''
					END AS PBP_B7G_COPAY_MC_AMT_78,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7h1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B7H_COPAY_MCIS_MINAMT_79,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('7i')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
								ELSE ''
							END AS PBP_B7I_COPAY_MC_AMT_MIN_80
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
		qid) segment_19 ON
	segment_0.qid = segment_19.qid;
QUIT;
RUN;

%MEND;


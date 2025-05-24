* PROGRAM NAME: JSON_IMPORT_PBPB16.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB16();
PROC SQL;
CREATE TABLE PBPB16_JSON AS
SELECT
	segment_0.qid,
	PBP_B16B_BENDESC_OE_AMO_0 AS PBP_B16B_BENDESC_OE_AMO,
	PBP_B16B_BENDESC_PC_AMO_1 AS PBP_B16B_BENDESC_PC_AMO,
	PBP_B16B_BENDESC_DX_AMO_2 AS PBP_B16B_BENDESC_DX_AMO,
	PBP_B16C_BENDESC_RS_AMO_3 AS PBP_B16C_BENDESC_RS_AMO,
	PBP_B16C_BENDESC_END_AMO_4 AS PBP_B16C_BENDESC_END_AMO,
	PBP_B16C_BENDESC_PERI_AMO_5 AS PBP_B16C_BENDESC_PERI_AMO,
	PBP_B16C_BENDESC_PRF_AMO_7 AS PBP_B16C_BENDESC_PRF_AMO,
	PBP_B16C_BENDESC_OMSG_AMO_7 AS PBP_B16C_BENDESC_OMSG_AMO,
	PBP_B16C_BENDESC_ORTH_AMO_7 AS PBP_B16C_BENDESC_ORTH_AMO,		

	PBP_B16C_MAXENR_CMP_TYPE_9 AS PBP_B16C_MAXENR_CMP_TYPE,
	PBP_B16C_MAXENR_CMP_AMT_9 AS PBP_B16C_MAXENR_CMP_AMT,
	PBP_B16C_MAXENR_CMP_PER_9 AS PBP_B16C_MAXENR_CMP_PER,
	PBP_B16A_COPAY_MC_AMT_MIN_10 AS PBP_B16A_COPAY_MC_AMT_MIN,
	PBP_B16A_COPAY_MC_AMT_93 AS PBP_B16A_COPAY_MC_AMT,
	PBP_B16A_COINS_MC_PCT_MIN_11 AS PBP_B16A_COINS_MC_PCT_MIN,
	PBP_B16A_COINS_MC_PCT_84 as PBP_B16A_COINS_MC_PCT,
	PBP_B16A_MAXENR_MC_PER_12 AS PBP_B16A_MAXENR_MC_PER,
	PBP_B16A_MAXENR_MC_AMT_13 AS PBP_B16A_MAXENR_MC_AMT,

	PBP_B16A_DED_MC_AMT_15 AS PBP_B16A_DED_MC_AMT,
	PBP_B16B_DED_PV_AMT_15 AS PBP_B16B_DED_PV_AMT,
	PBP_B16C_DED_CMP_AMT_15 AS PBP_B16C_DED_CMP_AMT,
	
	PBP_B16B_MAXENR_PV_AMT_17 AS PBP_B16B_MAXENR_PV_AMT,
	PBP_B16B_MAXPLAN_PV_AMT_18 AS PBP_B16B_MAXPLAN_PV_AMT,
	PBP_B16B_MAXPLAN_PV_PER_20 AS PBP_B16B_MAXPLAN_PV_PER,
	PBP_B16B_COPAY_OV_AMT_MAX_21 AS PBP_B16B_COPAY_OV_AMT_MAX,
	PBP_B16B_COPAY_OV_AMT_102 AS PBP_B16B_COPAY_OV_AMT,
	PBP_B16B_MAXENR_PV_PER_22 AS PBP_B16B_MAXENR_PV_PER,
	PBP_B16B_COPAY_OV_AMT_MIN_23 AS PBP_B16B_COPAY_OV_AMT_MIN,
	PBP_B16B_COINS_OV_PCT_MAX_24 AS PBP_B16B_COINS_OV_PCT_MAX,
	PBP_B16B_COINS_OV_PCT_104 AS PBP_B16B_COINS_OV_PCT,
	PBP_B16B_COINS_OV_PCT_MIN_25 AS PBP_B16B_COINS_OV_PCT_MIN,
	PBP_B16B_BENDESC_OE_NUM_26 AS PBP_B16B_BENDESC_OE_NUM,
	PBP_B16B_BENDESC_OE_PER_27 AS PBP_B16B_BENDESC_OE_PER,
	PBP_B16B_COINS_OE_PCT_MIN_28 AS PBP_B16B_COINS_OE_PCT_MIN,
	PBP_B16B_COINS_OE_PCT_85 AS PBP_B16B_COINS_OE_PCT,
	PBP_B16B_COPAY_OE_AMT_MIN_29 AS PBP_B16B_COPAY_OE_AMT_MIN,
	PBP_B16B_COPAY_OE_AMT_94 AS PBP_B16B_COPAY_OE_AMT,

	PBP_B16B_COINS_PC_PCT_MIN_31 AS PBP_B16B_COINS_PC_PCT_MIN,
	PBP_B16B_COINS_PC_PCT_86 AS PBP_B16B_COINS_PC_PCT,
	PBP_B16B_COPAY_PC_AMT_MIN_32 AS PBP_B16B_COPAY_PC_AMT_MIN,
	PBP_B16B_COPAY_PC_AMT_95 AS PBP_B16B_COPAY_PC_AMT,
	PBP_B16B_BENDESC_PC_NUM_33 AS PBP_B16B_BENDESC_PC_NUM,

	PBP_B16B_BENDESC_PC_PER_35 AS PBP_B16B_BENDESC_PC_PER,
	PBP_B16B_COPAY_DX_AMT_MIN_36 AS PBP_B16B_COPAY_DX_AMT_MIN,
	PBP_B16B_COPAY_DX_AMT_96 AS PBP_B16B_COPAY_DX_AMT,
	PBP_B16B_BENDESC_DX_PER_37 AS PBP_B16B_BENDESC_DX_PER,
	PBP_B16B_BENDESC_DX_NUM_38 AS PBP_B16B_BENDESC_DX_NUM,
	PBP_B16B_COINS_DX_PCT_MIN_39 AS PBP_B16B_COINS_DX_PCT_MIN,    
	PBP_B16B_COINS_DX_PCT_87 AS PBP_B16B_COINS_DX_PCT,

	PBP_B16C_MAXPLAN_CMP_AMT_41 AS PBP_B16C_MAXPLAN_CMP_AMT,
	PBP_B16C_MAXPLAN_CMP_TYPE_42 AS PBP_B16C_MAXPLAN_CMP_TYPE,
	PBP_B16C_MAXPLAN_CMP_PER_43 AS PBP_B16C_MAXPLAN_CMP_PER,
	
	PBP_B16C_BENDESC_RS_NUM_47 AS PBP_B16C_BENDESC_RS_NUM,
	PBP_B16C_COINS_RS_PCT_MIN_48 AS PBP_B16C_COINS_RS_PCT_MIN,
	PBP_B16C_COINS_RS_PCT_88 AS PBP_B16C_COINS_RS_PCT,
	PBP_B16C_COPAY_RS_AMT_MIN_49 AS PBP_B16C_COPAY_RS_AMT_MIN,
	PBP_B16C_COPAY_RS_AMT_97 AS PBP_B16C_COPAY_RS_AMT,
	PBP_B16C_BENDESC_RS_PER_50 AS PBP_B16C_BENDESC_RS_PER,
	
	PBP_B16C_BENDESC_END_PER_53 AS PBP_B16C_BENDESC_END_PER,
	PBP_B16C_COINS_END_PCT_MIN_54 AS PBP_B16C_COINS_END_PCT_MIN,
	PBP_B16C_COINS_END_PCT_89 AS PBP_B16C_COINS_END_PCT,
	PBP_B16C_BENDESC_END_NUM_55 AS PBP_B16C_BENDESC_END_NUM,

	PBP_B16C_COPAY_END_AMT_MIN_57 AS PBP_B16C_COPAY_END_AMT_MIN,
	PBP_B16C_COPAY_END_AMT_98 AS PBP_B16C_COPAY_END_AMT,

	PBP_B16C_BENDESC_PERI_PER_59 AS PBP_B16C_BENDESC_PERI_PER,

	PBP_B16C_COINS_PERI_PCT_MIN_62 AS PBP_B16C_COINS_PERI_PCT_MIN,
	PBP_B16C_COINS_PERI_PCT_90 AS PBP_B16C_COINS_PERI_PCT,
	PBP_B16C_COPAY_PERI_AMT_MIN_63 AS PBP_B16C_COPAY_PERI_AMT_MIN,
	PBP_B16C_COPAY_PERI_AMT_99 AS PBP_B16C_COPAY_PERI_AMT,
	PBP_B16C_BENDESC_PERI_NUM_64 AS PBP_B16C_BENDESC_PERI_NUM,


	PBP_B16C_BENDESC_PRF_NUM_71 AS PBP_B16C_BENDESC_PRF_NUM,
	PBP_B16C_BENDESC_OMSG_NUM_71 AS PBP_B16C_BENDESC_OMSG_NUM,
	PBP_B16C_BENDESC_ORTH_NUM_71 AS PBP_B16C_BENDESC_ORTH_NUM,
	PBP_B16C_COPAY_PRF_AMT_MIN_72 AS PBP_B16C_COPAY_PRF_AMT_MIN,
	PBP_B16C_COPAY_PRF_AMT_101 AS PBP_B16C_COPAY_PRF_AMT,
	PBP_B16C_COPAY_OMSG_AMT_MIN_72 AS PBP_B16C_COPAY_OMSG_AMT_MIN,
	PBP_B16C_COPAY_OMSG_AMT_101 AS PBP_B16C_COPAY_OMSG_AMT,
	PBP_B16C_COPAY_ORTH_AMT_MIN_72 AS PBP_B16C_COPAY_ORTH_AMT_MIN,
	PBP_B16C_COPAY_ORTH_AMT_101 AS PBP_B16C_COPAY_ORTH_AMT,
	PBP_B16C_COINS_PRF_PCT_MIN_73 AS PBP_B16C_COINS_PRF_PCT_MIN,
	PBP_B16C_COINS_PRF_PCT_92 AS PBP_B16C_COINS_PRF_PCT,
	PBP_B16C_COINS_OMSG_PCT_MIN_73 AS PBP_B16C_COINS_OMSG_PCT_MIN,
	PBP_B16C_COINS_OMSG_PCT_92 AS PBP_B16C_COINS_OMSG_PCT,
	PBP_B16C_COINS_ORTH_PCT_MIN_73 AS PBP_B16C_COINS_ORTH_PCT_MIN,
	PBP_B16C_COINS_ORTH_PCT_92 AS PBP_B16C_COINS_ORTH_PCT,
	PBP_B16C_BENDESC_PRF_PER_74 AS PBP_B16C_BENDESC_PRF_PER,
	PBP_B16C_BENDESC_OMSG_PER_74 AS PBP_B16C_BENDESC_OMSG_PER,	
	PBP_B16C_BENDESC_ORTH_PER_74 AS PBP_B16C_BENDESC_ORTH_PER,	

	CASE WHEN PBP_B16B_COINS_OV_SVCS_107 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COINS_OV_SVCS_106 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COINS_OV_SVCS_116 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COINS_OV_SVCS_108 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COINS_OV_SVCS_109 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COINS_OV_SVCS_117 IN ('1') THEN '1' ELSE '0' END  AS PBP_B16B_COINS_OV_SVCS,

	CASE WHEN PBP_B16B_COPAY_OV_SVCS_111 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COPAY_OV_SVCS_110 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COPAY_OV_SVCS_114 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COPAY_OV_SVCS_112 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COPAY_OV_SVCS_113 IN ('1') THEN '1' ELSE '0' END ||
		CASE WHEN PBP_B16B_COPAY_OV_SVCS_115 IN ('1') THEN '1' ELSE '0' END AS PBP_B16B_COPAY_OV_SVCS

FROM
	(
	SELECT
		qid,
		MAX(PBP_B16B_BENDESC_OE_AMO_0) AS PBP_B16B_BENDESC_OE_AMO_0,
		MAX(PBP_B16B_BENDESC_PC_AMO_1) AS PBP_B16B_BENDESC_PC_AMO_1,
		MAX(PBP_B16B_BENDESC_DX_AMO_2) AS PBP_B16B_BENDESC_DX_AMO_2,
		MAX(PBP_B16C_BENDESC_RS_AMO_3) AS PBP_B16C_BENDESC_RS_AMO_3,
		MAX(PBP_B16C_BENDESC_END_AMO_4) AS PBP_B16C_BENDESC_END_AMO_4,
		MAX(PBP_B16C_BENDESC_PERI_AMO_5) AS PBP_B16C_BENDESC_PERI_AMO_5
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16b1') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16B_BENDESC_OE_AMO_0,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16b4') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16B_BENDESC_PC_AMO_1,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16b2') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16B_BENDESC_DX_AMO_2,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16c1') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16C_BENDESC_RS_AMO_3,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16c2') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16C_BENDESC_END_AMO_4,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16c3') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16C_BENDESC_PERI_AMO_5
		FROM
			jsonlib.root
		LEFT JOIN jsonlib.pbp AS pbp ON
			root.ordinal_root = pbp.ordinal_root
		LEFT JOIN jsonlib.pbp_benefitOfferin AS pbp_benefitOfferin ON
			pbp.ordinal_pbp = pbp_benefitOfferin.ordinal_pbp
		LEFT JOIN jsonlib.benefitOfferin_nonMedicare AS benefitOfferin_nonMedicare ON
			pbp_benefitOfferin.ordinal_benefitOfferin = benefitOfferin_nonMedicare.ordinal_benefitOfferin
		LEFT JOIN jsonlib.nonMedicare_nonMedBenOffDet AS nonMedicare_nonMedBenOffDet ON
			benefitOfferin_nonMedicare.ordinal_nonMedicare = nonMedicare_nonMedBenOffDet.ordinal_nonMedicare ) main
	GROUP BY
		qid) segment_0
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_BENDESC_PRF_AMO_7) AS PBP_B16C_BENDESC_PRF_AMO_7,
		MAX(PBP_B16C_BENDESC_OMSG_AMO_7) AS PBP_B16C_BENDESC_OMSG_AMO_7,
		MAX(PBP_B16C_BENDESC_ORTH_AMO_7) AS PBP_B16C_BENDESC_ORTH_AMO_7,
		MAX(PBP_B16C_MAXENR_CMP_TYPE_9) AS PBP_B16C_MAXENR_CMP_TYPE_9,
		MAX(PBP_B16C_MAXENR_CMP_AMT_9) AS PBP_B16C_MAXENR_CMP_AMT_9,
		MAX(PBP_B16C_MAXENR_CMP_PER_9) AS PBP_B16C_MAXENR_CMP_PER_9,
		MAX(PBP_B16A_COPAY_MC_AMT_MIN_10) AS PBP_B16A_COPAY_MC_AMT_MIN_10,
		MAX(PBP_B16A_COINS_MC_PCT_MIN_11) AS PBP_B16A_COINS_MC_PCT_MIN_11
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16c7') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16C_BENDESC_PRF_AMO_7,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16c8') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16C_BENDESC_OMSG_AMO_7,
						CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('16c9') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B16C_BENDESC_ORTH_AMO_7,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeRadioGroup)
						ELSE ''
					END AS PBP_B16C_MAXENR_CMP_TYPE_9,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopAmt)
						ELSE ''
					END AS PBP_B16C_MAXENR_CMP_AMT_9,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopPeriodicity)
						ELSE ''
					END AS PBP_B16C_MAXENR_CMP_PER_9,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16a')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
							ELSE ''
						END AS PBP_B16A_COPAY_MC_AMT_MIN_10,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16a')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
								ELSE ''
							END AS PBP_B16A_COINS_MC_PCT_MIN_11
						FROM
							jsonlib.root
						LEFT JOIN jsonlib.pbp AS pbp ON
							root.ordinal_root = pbp.ordinal_root
						LEFT JOIN jsonlib.pbp_benefitOfferin AS pbp_benefitOfferin ON
							pbp.ordinal_pbp = pbp_benefitOfferin.ordinal_pbp
						LEFT JOIN jsonlib.benefitOfferin_nonMedicare AS benefitOfferin_nonMedicare ON
							pbp_benefitOfferin.ordinal_benefitOfferin = benefitOfferin_nonMedicare.ordinal_benefitOfferin
						LEFT JOIN jsonlib.nonMedicare_nonMedBenOffDet AS nonMedicare_nonMedBenOffDet ON
							benefitOfferin_nonMedicare.ordinal_nonMedicare = nonMedicare_nonMedBenOffDet.ordinal_nonMedicare
						LEFT JOIN jsonlib.pbp_benefitDetails AS pbp_benefitDetails ON
							pbp.ordinal_pbp = pbp_benefitDetails.ordinal_pbp
						LEFT JOIN jsonlib.benefitDetails_benefDetaiInfo AS benefitDetails_benefDetaiInfo ON
							pbp_benefitDetails.ordinal_benefitDetails = benefitDetails_benefDetaiInfo.ordinal_benefitDetails
						LEFT JOIN jsonlib.benefDetaiInfo_benefitDetails AS benefDetaiInfo_benefitDetails ON
							benefitDetails_benefDetaiInfo.ordinal_benefDetaiInfo = benefDetaiInfo_benefitDetails.ordinal_benefDetaiInfo
						LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
						LEFT JOIN jsonlib.benefitDetails_MooCosTypCom AS benefitDetails_MooCosTypCom ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooCosTypCom.ordinal_benefitDetails
						LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
							benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_1 ON
	segment_0.qid = segment_1.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16A_MAXENR_MC_PER_12) AS PBP_B16A_MAXENR_MC_PER_12,
		MAX(PBP_B16A_MAXENR_MC_AMT_13) AS PBP_B16A_MAXENR_MC_AMT_13,
		MAX(PBP_B16A_DED_MC_AMT_15) AS PBP_B16A_DED_MC_AMT_15,
		MAX(PBP_B16B_DED_PV_AMT_15) AS PBP_B16B_DED_PV_AMT_15,
		MAX(PBP_B16C_DED_CMP_AMT_15) AS PBP_B16C_DED_CMP_AMT_15,
		MAX(PBP_B16B_MAXENR_PV_AMT_17) AS PBP_B16B_MAXENR_PV_AMT_17
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16a')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
					ELSE ''
				END AS PBP_B16A_MAXENR_MC_PER_12,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16a')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
						ELSE ''
					END AS PBP_B16A_MAXENR_MC_AMT_13,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16a')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
								ELSE ''
							END AS PBP_B16A_DED_MC_AMT_15,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
								ELSE ''
							END AS PBP_B16B_DED_PV_AMT_15,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
								ELSE ''
							END AS PBP_B16C_DED_CMP_AMT_15,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
										ELSE ''
									END AS PBP_B16B_MAXENR_PV_AMT_17
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
								LEFT JOIN jsonlib.benefitDetails_MooCosTypCom AS benefitDetails_MooCosTypCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooCosTypCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_DenCoiCom AS benefitDetails_DenCoiCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCoiCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.DenCoiCom_bdDenCoiSerOpt2 AS DenCoiCom_bdDenCoiSerOpt2 ON
									benefitDetails_DenCoiCom.ordinal_DenCoiCom = DenCoiCom_bdDenCoiSerOpt2.ordinal_DenCoiCom
								LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_2 ON
	segment_0.qid = segment_2.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16B_MAXPLAN_PV_AMT_18) AS PBP_B16B_MAXPLAN_PV_AMT_18,
		MAX(PBP_B16B_MAXPLAN_PV_PER_20) AS PBP_B16B_MAXPLAN_PV_PER_20,
		MAX(PBP_B16B_COPAY_OV_AMT_MAX_21) AS PBP_B16B_COPAY_OV_AMT_MAX_21,
		MAX(PBP_B16B_MAXENR_PV_PER_22) AS PBP_B16B_MAXENR_PV_PER_22,
		MAX(PBP_B16B_COPAY_OV_AMT_MIN_23) AS PBP_B16B_COPAY_OV_AMT_MIN_23
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_PlaBenCovCom.bdMaximumAmount)
					ELSE ''
				END AS PBP_B16B_MAXPLAN_PV_AMT_18,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_PlaBenCovCom.bdCoveragePeriodicity)
							ELSE ''
						END AS PBP_B16B_MAXPLAN_PV_PER_20,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DenCopCom.bdDentalCopaymentMaxAmount)
								ELSE ''
							END AS PBP_B16B_COPAY_OV_AMT_MAX_21,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
									ELSE ''
								END AS PBP_B16B_MAXENR_PV_PER_22,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DenCopCom.bdDentalCopaymentMinAmount)
										ELSE ''
									END AS PBP_B16B_COPAY_OV_AMT_MIN_23
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
								LEFT JOIN jsonlib.benefitDetails_PlaBenCovCom AS benefitDetails_PlaBenCovCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_PlaBenCovCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_DenCopCom AS benefitDetails_DenCopCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCopCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_3 ON
	segment_0.qid = segment_3.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16B_COINS_OV_PCT_MAX_24) AS PBP_B16B_COINS_OV_PCT_MAX_24,
		MAX(PBP_B16B_COINS_OV_PCT_MIN_25) AS PBP_B16B_COINS_OV_PCT_MIN_25,
		MAX(PBP_B16B_BENDESC_OE_NUM_26) AS PBP_B16B_BENDESC_OE_NUM_26,
		MAX(PBP_B16B_BENDESC_OE_PER_27) AS PBP_B16B_BENDESC_OE_PER_27,
		MAX(PBP_B16B_COINS_OE_PCT_MIN_28) AS PBP_B16B_COINS_OE_PCT_MIN_28,
		MAX(PBP_B16B_COPAY_OE_AMT_MIN_29) AS PBP_B16B_COPAY_OE_AMT_MIN_29
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DenCoiCom.bdDentalCoinsuranceMaxPer)
					ELSE ''
				END AS PBP_B16B_COINS_OV_PCT_MAX_24,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DenCoiCom.bdDentalCoinsuranceMinPer)
						ELSE ''
					END AS PBP_B16B_COINS_OV_PCT_MIN_25,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
							ELSE ''
						END AS PBP_B16B_BENDESC_OE_NUM_26,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
								ELSE ''
							END AS PBP_B16B_BENDESC_OE_PER_27,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
									ELSE ''
								END AS PBP_B16B_COINS_OE_PCT_MIN_28,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
										ELSE ''
									END AS PBP_B16B_COPAY_OE_AMT_MIN_29
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
								LEFT JOIN jsonlib.benefitDetails_DenCoiCom AS benefitDetails_DenCoiCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCoiCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails
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
		MAX(PBP_B16B_COINS_PC_PCT_MIN_31) AS PBP_B16B_COINS_PC_PCT_MIN_31,
		MAX(PBP_B16B_COPAY_PC_AMT_MIN_32) AS PBP_B16B_COPAY_PC_AMT_MIN_32,
		MAX(PBP_B16B_BENDESC_PC_NUM_33) AS PBP_B16B_BENDESC_PC_NUM_33,
		MAX(PBP_B16B_BENDESC_PC_PER_35) AS PBP_B16B_BENDESC_PC_PER_35
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b4')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B16B_COINS_PC_PCT_MIN_31,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b4')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
							ELSE ''
						END AS PBP_B16B_COPAY_PC_AMT_MIN_32,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b4')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
								ELSE ''
							END AS PBP_B16B_BENDESC_PC_NUM_33,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b4')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
										ELSE ''
									END AS PBP_B16B_BENDESC_PC_PER_35
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
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_5 ON
	segment_0.qid = segment_5.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16B_COPAY_DX_AMT_MIN_36) AS PBP_B16B_COPAY_DX_AMT_MIN_36,
		MAX(PBP_B16B_BENDESC_DX_PER_37) AS PBP_B16B_BENDESC_DX_PER_37,
		MAX(PBP_B16B_BENDESC_DX_NUM_38) AS PBP_B16B_BENDESC_DX_NUM_38,
		MAX(PBP_B16B_COINS_DX_PCT_MIN_39) AS PBP_B16B_COINS_DX_PCT_MIN_39,
		MAX(PBP_B16C_MAXPLAN_CMP_AMT_41) AS PBP_B16C_MAXPLAN_CMP_AMT_41
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
					ELSE ''
				END AS PBP_B16B_COPAY_DX_AMT_MIN_36,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
						ELSE ''
					END AS PBP_B16B_BENDESC_DX_PER_37,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
							ELSE ''
						END AS PBP_B16B_BENDESC_DX_NUM_38,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
								ELSE ''
							END AS PBP_B16B_COINS_DX_PCT_MIN_39,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MaPlComDeCom.bdMaxPlanComprDentaAmoun)
										ELSE ''
									END AS PBP_B16C_MAXPLAN_CMP_AMT_41
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
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_MaPlComDeCom AS benefitDetails_MaPlComDeCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MaPlComDeCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_6 ON
	segment_0.qid = segment_6.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_MAXPLAN_CMP_TYPE_42) AS PBP_B16C_MAXPLAN_CMP_TYPE_42,
		MAX(PBP_B16C_MAXPLAN_CMP_PER_43) AS PBP_B16C_MAXPLAN_CMP_PER_43,
		MAX(PBP_B16C_BENDESC_RS_NUM_47) AS PBP_B16C_BENDESC_RS_NUM_47
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MaPlComDeCom.bdMaxPlanCompDentRadiOpts)
					ELSE ''
				END AS PBP_B16C_MAXPLAN_CMP_TYPE_42,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_MaPlComDeCom.bdMaxPlanComprDentaPerio)
						ELSE ''
					END AS PBP_B16C_MAXPLAN_CMP_PER_43,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
										ELSE ''
									END AS PBP_B16C_BENDESC_RS_NUM_47
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
								LEFT JOIN jsonlib.benefitDetails_MaPlComDeCom AS benefitDetails_MaPlComDeCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MaPlComDeCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_CoinsCompo AS benefitDetails_CoinsCompo ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoinsCompo.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_7 ON
	segment_0.qid = segment_7.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_COINS_RS_PCT_MIN_48) AS PBP_B16C_COINS_RS_PCT_MIN_48,
		MAX(PBP_B16C_COPAY_RS_AMT_MIN_49) AS PBP_B16C_COPAY_RS_AMT_MIN_49,
		MAX(PBP_B16C_BENDESC_RS_PER_50) AS PBP_B16C_BENDESC_RS_PER_50,
		MAX(PBP_B16C_BENDESC_END_PER_53) AS PBP_B16C_BENDESC_END_PER_53
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B16C_COINS_RS_PCT_MIN_48,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
						ELSE ''
					END AS PBP_B16C_COPAY_RS_AMT_MIN_49,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
							ELSE ''
						END AS PBP_B16C_BENDESC_RS_PER_50,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
										ELSE ''
									END AS PBP_B16C_BENDESC_END_PER_53
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
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_8 ON
	segment_0.qid = segment_8.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_COINS_END_PCT_MIN_54) AS PBP_B16C_COINS_END_PCT_MIN_54,
		MAX(PBP_B16C_BENDESC_END_NUM_55) AS PBP_B16C_BENDESC_END_NUM_55,
		MAX(PBP_B16C_COPAY_END_AMT_MIN_57) AS PBP_B16C_COPAY_END_AMT_MIN_57,
		MAX(PBP_B16C_BENDESC_PERI_PER_59) AS PBP_B16C_BENDESC_PERI_PER_59
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
					ELSE ''
				END AS PBP_B16C_COINS_END_PCT_MIN_54,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
						ELSE ''
					END AS PBP_B16C_BENDESC_END_NUM_55,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
								ELSE ''
							END AS PBP_B16C_COPAY_END_AMT_MIN_57,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c3')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
										ELSE ''
									END AS PBP_B16C_BENDESC_PERI_PER_59
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
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_9 ON
	segment_0.qid = segment_9.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_COINS_PERI_PCT_MIN_62) AS PBP_B16C_COINS_PERI_PCT_MIN_62,
		MAX(PBP_B16C_COPAY_PERI_AMT_MIN_63) AS PBP_B16C_COPAY_PERI_AMT_MIN_63,
		MAX(PBP_B16C_BENDESC_PERI_NUM_64) AS PBP_B16C_BENDESC_PERI_NUM_64
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c3')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
							ELSE ''
						END AS PBP_B16C_COINS_PERI_PCT_MIN_62,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c3')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
								ELSE ''
							END AS PBP_B16C_COPAY_PERI_AMT_MIN_63,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c3')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
									ELSE ''
								END AS PBP_B16C_BENDESC_PERI_NUM_64
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
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_10 ON
	segment_0.qid = segment_10.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_BENDESC_PRF_NUM_71) AS PBP_B16C_BENDESC_PRF_NUM_71,
		MAX(PBP_B16C_BENDESC_OMSG_NUM_71) AS PBP_B16C_BENDESC_OMSG_NUM_71,
		MAX(PBP_B16C_BENDESC_ORTH_NUM_71) AS PBP_B16C_BENDESC_ORTH_NUM_71		
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c7')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
										ELSE ''
									END AS PBP_B16C_BENDESC_PRF_NUM_71,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c8')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
										ELSE ''
									END AS PBP_B16C_BENDESC_OMSG_NUM_71,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c9')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdIndicateNumOfVisit)
										ELSE ''
									END AS PBP_B16C_BENDESC_ORTH_NUM_71
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
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails
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
		MAX(PBP_B16C_COPAY_PRF_AMT_MIN_72) AS PBP_B16C_COPAY_PRF_AMT_MIN_72,
		MAX(PBP_B16C_COPAY_OMSG_AMT_MIN_72) AS PBP_B16C_COPAY_OMSG_AMT_MIN_72,
		MAX(PBP_B16C_COPAY_ORTH_AMT_MIN_72) AS PBP_B16C_COPAY_ORTH_AMT_MIN_72,
		MAX(PBP_B16C_COINS_PRF_PCT_MIN_73) AS PBP_B16C_COINS_PRF_PCT_MIN_73,
		MAX(PBP_B16C_COINS_OMSG_PCT_MIN_73) AS PBP_B16C_COINS_OMSG_PCT_MIN_73,
		MAX(PBP_B16C_COINS_ORTH_PCT_MIN_73) AS PBP_B16C_COINS_ORTH_PCT_MIN_73,
		MAX(PBP_B16C_BENDESC_PRF_PER_74) AS PBP_B16C_BENDESC_PRF_PER_74,
		MAX(PBP_B16C_BENDESC_OMSG_PER_74) AS PBP_B16C_BENDESC_OMSG_PER_74,
		MAX(PBP_B16C_BENDESC_ORTH_PER_74) AS PBP_B16C_BENDESC_ORTH_PER_74
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c7')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
					ELSE ''
				END AS PBP_B16C_COPAY_PRF_AMT_MIN_72,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c8')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
					ELSE ''
				END AS PBP_B16C_COPAY_OMSG_AMT_MIN_72,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c9')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
					ELSE ''
				END AS PBP_B16C_COPAY_ORTH_AMT_MIN_72,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c7')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B16C_COINS_PRF_PCT_MIN_73,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c8')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B16C_COINS_OMSG_PCT_MIN_73,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c9')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
						ELSE ''
					END AS PBP_B16C_COINS_ORTH_PCT_MIN_73,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c7')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
							ELSE ''
						END AS PBP_B16C_BENDESC_PRF_PER_74,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c8')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
							ELSE ''
						END AS PBP_B16C_BENDESC_OMSG_PER_74,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c9')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_BenUnlCom.bdBenefitUnlimitedPeriodicity)
							ELSE ''
						END AS PBP_B16C_BENDESC_ORTH_PER_74
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
								LEFT JOIN jsonlib.benefitDetails_BenUnlCom AS benefitDetails_BenUnlCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_BenUnlCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.benefitDetails_DenCopCom AS benefitDetails_DenCopCom ON
									benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCopCom.ordinal_benefitDetails
								LEFT JOIN jsonlib.DenCopCom_bdDenCopSerOpt2 AS DenCopCom_bdDenCopSerOpt2 ON
									benefitDetails_DenCopCom.ordinal_DenCopCom = DenCopCom_bdDenCopSerOpt2.ordinal_DenCopCom ) main
	GROUP BY
		qid) segment_12 ON
	segment_0.qid = segment_12.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16A_COINS_MC_PCT_84) AS PBP_B16A_COINS_MC_PCT_84,
		MAX(PBP_B16B_COINS_OE_PCT_85) AS PBP_B16B_COINS_OE_PCT_85,
		MAX(PBP_B16B_COINS_PC_PCT_86) AS PBP_B16B_COINS_PC_PCT_86,
		MAX(PBP_B16B_COINS_DX_PCT_87) AS PBP_B16B_COINS_DX_PCT_87,
		MAX(PBP_B16C_COINS_RS_PCT_88) AS PBP_B16C_COINS_RS_PCT_88
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16a')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B16A_COINS_MC_PCT_84,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
						ELSE ''
					END AS PBP_B16B_COINS_OE_PCT_85,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b4')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
							ELSE ''
						END AS PBP_B16B_COINS_PC_PCT_86,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
								ELSE ''
							END AS PBP_B16B_COINS_DX_PCT_87,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
									ELSE ''
								END AS PBP_B16C_COINS_RS_PCT_88
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
		qid) segment_14 ON
	segment_0.qid = segment_14.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_COINS_END_PCT_89) AS PBP_B16C_COINS_END_PCT_89,
		MAX(PBP_B16C_COINS_PERI_PCT_90) AS PBP_B16C_COINS_PERI_PCT_90,
		MAX(PBP_B16C_COINS_PRF_PCT_92) AS PBP_B16C_COINS_PRF_PCT_92,
		MAX(PBP_B16C_COINS_OMSG_PCT_92) AS PBP_B16C_COINS_OMSG_PCT_92,
		MAX(PBP_B16C_COINS_ORTH_PCT_92) AS PBP_B16C_COINS_ORTH_PCT_92,		
		MAX(PBP_B16A_COPAY_MC_AMT_93) AS PBP_B16A_COPAY_MC_AMT_93
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
					ELSE ''
				END AS PBP_B16C_COINS_END_PCT_89,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c3')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
						ELSE ''
					END AS PBP_B16C_COINS_PERI_PCT_90,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c7')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
								ELSE ''
							END AS PBP_B16C_COINS_PRF_PCT_92,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c8')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
								ELSE ''
							END AS PBP_B16C_COINS_OMSG_PCT_92,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c9')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
								ELSE ''
							END AS PBP_B16C_COINS_ORTH_PCT_92,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16a')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
									ELSE ''
								END AS PBP_B16A_COPAY_MC_AMT_93
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
		MAX(PBP_B16B_COPAY_OE_AMT_94) AS PBP_B16B_COPAY_OE_AMT_94,
		MAX(PBP_B16B_COPAY_PC_AMT_95) AS PBP_B16B_COPAY_PC_AMT_95,
		MAX(PBP_B16B_COPAY_DX_AMT_96) AS PBP_B16B_COPAY_DX_AMT_96,
		MAX(PBP_B16C_COPAY_RS_AMT_97) AS PBP_B16C_COPAY_RS_AMT_97,
		MAX(PBP_B16C_COPAY_END_AMT_98) AS PBP_B16C_COPAY_END_AMT_98
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
					ELSE ''
				END AS PBP_B16B_COPAY_OE_AMT_94,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b4')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
						ELSE ''
					END AS PBP_B16B_COPAY_PC_AMT_95,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B16B_COPAY_DX_AMT_96,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
								ELSE ''
							END AS PBP_B16C_COPAY_RS_AMT_97,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
									ELSE ''
								END AS PBP_B16C_COPAY_END_AMT_98
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
		qid) segment_16 ON
	segment_0.qid = segment_16.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16C_COPAY_PERI_AMT_99) AS PBP_B16C_COPAY_PERI_AMT_99,
		MAX(PBP_B16C_COPAY_PRF_AMT_101) AS PBP_B16C_COPAY_PRF_AMT_101,
		MAX(PBP_B16C_COPAY_OMSG_AMT_101) AS PBP_B16C_COPAY_OMSG_AMT_101,
		MAX(PBP_B16C_COPAY_ORTH_AMT_101) AS PBP_B16C_COPAY_ORTH_AMT_101,
		MAX(PBP_B16B_COPAY_OV_AMT_102) AS PBP_B16B_COPAY_OV_AMT_102
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c3')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
					ELSE ''
				END AS PBP_B16C_COPAY_PERI_AMT_99,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c7')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B16C_COPAY_PRF_AMT_101,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c8')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B16C_COPAY_OMSG_AMT_101,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16c9')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
							ELSE ''
						END AS PBP_B16C_COPAY_ORTH_AMT_101,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DenCopCom.bdDentalCopaymentAmount)
								ELSE ''
							END AS PBP_B16B_COPAY_OV_AMT_102
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
							LEFT JOIN jsonlib.benefitDetails_DenCopCom AS benefitDetails_DenCopCom ON
								benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCopCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_17 ON
	segment_0.qid = segment_17.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16B_COINS_OV_PCT_104) AS PBP_B16B_COINS_OV_PCT_104,
		MAX(PBP_B16B_COINS_OV_SVCS_106) AS PBP_B16B_COINS_OV_SVCS_106,
		MAX(PBP_B16B_COINS_OV_SVCS_107) AS PBP_B16B_COINS_OV_SVCS_107,
		MAX(PBP_B16B_COINS_OV_SVCS_108) AS PBP_B16B_COINS_OV_SVCS_108
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_DenCoiCom.bdDentalCoinsurancePer)
					ELSE ''
				END AS PBP_B16B_COINS_OV_PCT_104,
				CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCoiCom_bdDenCoiSerOpt2.dentalXRays)
							ELSE ''
						END AS PBP_B16B_COINS_OV_SVCS_106,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCoiCom_bdDenCoiSerOpt2.oralExams)
								ELSE ''
							END AS PBP_B16B_COINS_OV_SVCS_107,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCoiCom_bdDenCoiSerOpt2.prophylaxisCleaning)
									ELSE ''
								END AS PBP_B16B_COINS_OV_SVCS_108
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
							LEFT JOIN jsonlib.benefitDetails_DenCoiCom AS benefitDetails_DenCoiCom ON
								benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCoiCom.ordinal_benefitDetails
							LEFT JOIN jsonlib.DenCoiCom_bdDenCoiSerOpt2 AS DenCoiCom_bdDenCoiSerOpt2 ON
								benefitDetails_DenCoiCom.ordinal_DenCoiCom = DenCoiCom_bdDenCoiSerOpt2.ordinal_DenCoiCom ) main
	GROUP BY
		qid) segment_18 ON
	segment_0.qid = segment_18.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B16B_COINS_OV_SVCS_109) AS PBP_B16B_COINS_OV_SVCS_109,
		MAX(PBP_B16B_COPAY_OV_SVCS_110) AS PBP_B16B_COPAY_OV_SVCS_110,
		MAX(PBP_B16B_COPAY_OV_SVCS_111) AS PBP_B16B_COPAY_OV_SVCS_111,
		MAX(PBP_B16B_COPAY_OV_SVCS_112) AS PBP_B16B_COPAY_OV_SVCS_112,
		MAX(PBP_B16B_COPAY_OV_SVCS_113) AS PBP_B16B_COPAY_OV_SVCS_113,
		MAX(PBP_B16B_COPAY_OV_SVCS_114) AS PBP_B16B_COPAY_OV_SVCS_114,
		MAX(PBP_B16B_COPAY_OV_SVCS_115) AS PBP_B16B_COPAY_OV_SVCS_115,
		MAX(PBP_B16B_COINS_OV_SVCS_116) AS PBP_B16B_COINS_OV_SVCS_116,
		MAX(PBP_B16B_COINS_OV_SVCS_117) AS PBP_B16B_COINS_OV_SVCS_117
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCoiCom_bdDenCoiSerOpt2.fluorideTreatment)
					ELSE ''
				END AS PBP_B16B_COINS_OV_SVCS_109,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCopCom_bdDenCopSerOpt2.dentalXRays)
						ELSE ''
					END AS PBP_B16B_COPAY_OV_SVCS_110,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCopCom_bdDenCopSerOpt2.oralExams)
							ELSE ''
						END AS PBP_B16B_COPAY_OV_SVCS_111,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCopCom_bdDenCopSerOpt2.prophylaxisCleaning)
								ELSE ''
							END AS PBP_B16B_COPAY_OV_SVCS_112,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCopCom_bdDenCopSerOpt2.fluorideTreatment)
									ELSE ''
								END AS PBP_B16B_COPAY_OV_SVCS_113,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCopCom_bdDenCopSerOpt2.otherDiagnosticDentalServices)
									ELSE ''
								END AS PBP_B16B_COPAY_OV_SVCS_114,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCopCom_bdDenCopSerOpt2.otherPreventiveDentalServices)
									ELSE ''
								END AS PBP_B16B_COPAY_OV_SVCS_115,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCoiCom_bdDenCoiSerOpt2.otherDiagnosticDentalServices)
									ELSE ''
								END AS PBP_B16B_COINS_OV_SVCS_116,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('16b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(DenCoiCom_bdDenCoiSerOpt2.otherPreventiveDentalServices)
									ELSE ''
								END AS PBP_B16B_COINS_OV_SVCS_117
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
							LEFT JOIN jsonlib.benefitDetails_DenCoiCom AS benefitDetails_DenCoiCom ON
								benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCoiCom.ordinal_benefitDetails
							LEFT JOIN jsonlib.DenCoiCom_bdDenCoiSerOpt2 AS DenCoiCom_bdDenCoiSerOpt2 ON
								benefitDetails_DenCoiCom.ordinal_DenCoiCom = DenCoiCom_bdDenCoiSerOpt2.ordinal_DenCoiCom
							LEFT JOIN jsonlib.benefitDetails_DenCopCom AS benefitDetails_DenCopCom ON
								benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DenCopCom.ordinal_benefitDetails
							LEFT JOIN jsonlib.DenCopCom_bdDenCopSerOpt2 AS DenCopCom_bdDenCopSerOpt2 ON
								benefitDetails_DenCopCom.ordinal_DenCopCom = DenCopCom_bdDenCopSerOpt2.ordinal_DenCopCom ) main
	GROUP BY
		qid) segment_19 ON
	segment_0.qid = segment_19.qid;
QUIT;	
RUN;

%MEND;



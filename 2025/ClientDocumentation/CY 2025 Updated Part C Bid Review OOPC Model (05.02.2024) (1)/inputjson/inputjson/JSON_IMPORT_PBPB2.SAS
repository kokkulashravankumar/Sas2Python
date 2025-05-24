* PROGRAM NAME: JSON_IMPORT_PBPB2.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB2();
PROC SQL;
CREATE TABLE PBPB2_JSON AS
SELECT
	segment_0.qid,
	CASE WHEN PBP_B2_BENDESC_YN_01 = '1' OR PBP_B2_BENDESC_YN_02 = '1'
		THEN '1' ELSE '2' END AS PBP_B2_BENDESC_YN,
	PBP_B2_BENDESC_AMO_AD_1 AS PBP_B2_BENDESC_AMO_AD,
	PBP_B2_BENDESC_AMO_NMCS_2 AS PBP_B2_BENDESC_AMO_NMCS,
	PBP_B2_DED_AMT_T1_3 AS PBP_B2_DED_AMT_T1,
	PBP_B2_DED_AMT_T2_4 AS PBP_B2_DED_AMT_T2,
	PBP_B2_DED_AMT_T3_5 AS PBP_B2_DED_AMT_T3,
	PBP_B2_COINS_MCS_PCT_INT2_T1_6 AS PBP_B2_COINS_MCS_PCT_INT2_T1,
	PBP_B2_COINS_MCS_PCT_INT2_T3_7 AS PBP_B2_COINS_MCS_PCT_INT2_T3,
	PBP_B2_COINS_MCS_PCT_INT2_T2_8 AS PBP_B2_COINS_MCS_PCT_INT2_T2,
	PBP_B2_COINS_MCS_ENDD_INT3__9 AS PBP_B2_COINS_MCS_ENDD_INT3_T3,
	PBP_B2_COINS_MCS_ENDD_INT3_10 AS PBP_B2_COINS_MCS_ENDD_INT3_T2,
	PBP_B2_COINS_MCS_ENDD_INT3_11 AS PBP_B2_COINS_MCS_ENDD_INT3_T1,
	PBP_B2_COINS_MCS_PCT_T3_12 AS PBP_B2_COINS_MCS_PCT_T3,
	PBP_B2_COINS_MCS_PCT_T2_13 AS PBP_B2_COINS_MCS_PCT_T2,
	PBP_B2_COINS_MCS_PCT_T1_14 AS PBP_B2_COINS_MCS_PCT_T1,
	PBP_B2_COINS_MCS_BGND_INT2_15 AS PBP_B2_COINS_MCS_BGND_INT2_T3,
	PBP_B2_COINS_MCS_BGND_INT2_16 AS PBP_B2_COINS_MCS_BGND_INT2_T2,
	PBP_B2_COINS_MCS_BGND_INT2_17 AS PBP_B2_COINS_MCS_BGND_INT2_T1,
	PBP_B2_COPAY_MCS_ENDD_INT2_18 AS PBP_B2_COPAY_MCS_ENDD_INT2_T3,
	PBP_B2_COPAY_MCS_ENDD_INT2_19 AS PBP_B2_COPAY_MCS_ENDD_INT2_T2,
	PBP_B2_COPAY_MCS_ENDD_INT2_20 AS PBP_B2_COPAY_MCS_ENDD_INT2_T1,
	PBP_B2_COST_VARY_LOW_TIER_21 AS PBP_B2_COST_VARY_LOW_TIER,
	PBP_B2_COPAY_YN_22 AS PBP_B2_COPAY_YN,
	PBP_B2_MAXENR_AMT_23 AS PBP_B2_MAXENR_AMT,
	PBP_B2_COPAY_MCS_AMT_INT2_24 AS PBP_B2_COPAY_MCS_AMT_INT2_T3,
	PBP_B2_COPAY_MCS_AMT_INT2_25 AS PBP_B2_COPAY_MCS_AMT_INT2_T1,
	PBP_B2_COPAY_MCS_AMT_INT2_26 AS PBP_B2_COPAY_MCS_AMT_INT2_T2,
	PBP_B2_COPAY_MCS_BGND_INT1_27 AS PBP_B2_COPAY_MCS_BGND_INT1_T1,
	PBP_B2_COPAY_MCS_BGND_INT1_28 AS PBP_B2_COPAY_MCS_BGND_INT1_T2,
	PBP_B2_COPAY_MCS_BGND_INT1_29 AS PBP_B2_COPAY_MCS_BGND_INT1_T3,
	PBP_B2_COINS_MCS_ENDD_INT1_30 AS PBP_B2_COINS_MCS_ENDD_INT1_T3,
	PBP_B2_COINS_MCS_ENDD_INT1_31 AS PBP_B2_COINS_MCS_ENDD_INT1_T1,
	PBP_B2_COINS_MCS_ENDD_INT1_32 AS PBP_B2_COINS_MCS_ENDD_INT1_T2,
	PBP_B2_COPAY_MCS_BGND_INT2_33 AS PBP_B2_COPAY_MCS_BGND_INT2_T1,
	PBP_B2_COPAY_MCS_BGND_INT2_34 AS PBP_B2_COPAY_MCS_BGND_INT2_T2,
	PBP_B2_COPAY_MCS_BGND_INT2_35 AS PBP_B2_COPAY_MCS_BGND_INT2_T3,
	PBP_B2_COINS_MCS_PCT_INT1_36 AS PBP_B2_COINS_MCS_PCT_INT1_T1,
	PBP_B2_COINS_MCS_PCT_INT1_37 AS PBP_B2_COINS_MCS_PCT_INT1_T2,
	PBP_B2_COINS_MCS_PCT_INT1_38 AS PBP_B2_COINS_MCS_PCT_INT1_T3,
	PBP_B2_MC_COINS_CSTSHR_YN_39 AS PBP_B2_MC_COINS_CSTSHR_YN_T3,
	PBP_B2_MC_COINS_CSTSHR_YN_40 AS PBP_B2_MC_COINS_CSTSHR_YN_T1,
	PBP_B2_MC_COINS_CSTSHR_YN_41 AS PBP_B2_MC_COINS_CSTSHR_YN_T2,
	PBP_B2_MAXENR_PER_42 AS PBP_B2_MAXENR_PER,
	PBP_B2_COPAY_MCS_AMT_T3_43 AS PBP_B2_COPAY_MCS_AMT_T3,
	PBP_B2_COPAY_MCS_AMT_T1_44 AS PBP_B2_COPAY_MCS_AMT_T1,
	PBP_B2_COPAY_MCS_AMT_T2_45 AS PBP_B2_COPAY_MCS_AMT_T2,
	PBP_B2_COINS_MCS_BGND_INT3_46 AS PBP_B2_COINS_MCS_BGND_INT3_T3,
	PBP_B2_COINS_MCS_BGND_INT3_47 AS PBP_B2_COINS_MCS_BGND_INT3_T2,
	PBP_B2_COINS_MCS_BGND_INT3_48 AS PBP_B2_COINS_MCS_BGND_INT3_T1,
	PBP_B2_COST_DISCHARGE_YN_49 AS PBP_B2_COST_DISCHARGE_YN,
	PBP_B2_COPAY_MCS_AMT_INT3_50 AS PBP_B2_COPAY_MCS_AMT_INT3_T1,
	PBP_B2_COPAY_MCS_AMT_INT3_51 AS PBP_B2_COPAY_MCS_AMT_INT3_T2,
	PBP_B2_COPAY_MCS_AMT_INT3_52 AS PBP_B2_COPAY_MCS_AMT_INT3_T3,
	PBP_B2_COINS_MCS_BGND_INT1_53 AS PBP_B2_COINS_MCS_BGND_INT1_T3,
	PBP_B2_COINS_MCS_BGND_INT1_54 AS PBP_B2_COINS_MCS_BGND_INT1_T1,
	PBP_B2_COINS_MCS_BGND_INT1_55 AS PBP_B2_COINS_MCS_BGND_INT1_T2,
	PBP_B2_MC_COPAY_CSTSHR_YN_56 AS PBP_B2_MC_COPAY_CSTSHR_YN_T3,
	PBP_B2_MC_COPAY_CSTSHR_YN_57 AS PBP_B2_MC_COPAY_CSTSHR_YN_T1,
	PBP_B2_COPAY_MCS_ENDD_INT3_58 AS PBP_B2_COPAY_MCS_ENDD_INT3_T3,
	PBP_B2_MC_COPAY_CSTSHR_YN_59 AS PBP_B2_MC_COPAY_CSTSHR_YN_T2,
	PBP_B2_COPAY_MCS_ENDD_INT3_60 AS PBP_B2_COPAY_MCS_ENDD_INT3_T1,
	PBP_B2_COPAY_MCS_ENDD_INT3_61 AS PBP_B2_COPAY_MCS_ENDD_INT3_T2,
	PBP_B2_COPAY_MCS_AMT_INT1_62 AS PBP_B2_COPAY_MCS_AMT_INT1_T1,
	PBP_B2_COPAY_MCS_AMT_INT1_63 AS PBP_B2_COPAY_MCS_AMT_INT1_T3,
	PBP_B2_COPAY_MCS_AMT_INT1_64 AS PBP_B2_COPAY_MCS_AMT_INT1_T2,
	PBP_B2_COST_VARY_TIERS_YN_65 AS PBP_B2_COST_VARY_TIERS_YN,
	PBP_B2_COINS_YN_66 AS PBP_B2_COINS_YN,
	PBP_B2_COINS_MCS_PCT_INT3_67 AS PBP_B2_COINS_MCS_PCT_INT3_T2,
	PBP_B2_COINS_MCS_PCT_INT3_68 AS PBP_B2_COINS_MCS_PCT_INT3_T1,
	PBP_B2_COINS_MCS_PCT_INT3_69 AS PBP_B2_COINS_MCS_PCT_INT3_T3,
	PBP_B2_COINS_MCS_ENDD_INT2_70 AS PBP_B2_COINS_MCS_ENDD_INT2_T2,
	PBP_B2_COINS_MCS_ENDD_INT2_71 AS PBP_B2_COINS_MCS_ENDD_INT2_T3,
	PBP_B2_COINS_MCS_ENDD_INT2_72 AS PBP_B2_COINS_MCS_ENDD_INT2_T1,
	PBP_B2_COPAY_MCS_BGND_INT3_73 AS PBP_B2_COPAY_MCS_BGND_INT3_T1,
	PBP_B2_COPAY_MCS_BGND_INT3_74 AS PBP_B2_COPAY_MCS_BGND_INT3_T2,
	PBP_B2_COPAY_MCS_BGND_INT3_75 AS PBP_B2_COPAY_MCS_BGND_INT3_T3,
	PBP_B2_COPAY_MCS_ENDD_INT1_76 AS PBP_B2_COPAY_MCS_ENDD_INT1_T3,
	PBP_B2_COPAY_MCS_ENDD_INT1_77 AS PBP_B2_COPAY_MCS_ENDD_INT1_T2,
	PBP_B2_COPAY_MCS_ENDD_INT1_78 AS PBP_B2_COPAY_MCS_ENDD_INT1_T1,
	PBP_B2_HOSP_BEN_PERIOD_79 AS PBP_B2_HOSP_BEN_PERIOD,
	PBP_B2_BENEDESC_LIM_AD_80 AS PBP_B2_BENEDESC_LIM_AD,
	PBP_B2_COINS_AD_ENDD_INT3_81 AS PBP_B2_COINS_AD_ENDD_INT3_T3,
	PBP_B2_COINS_AD_ENDD_INT3_82 AS PBP_B2_COINS_AD_ENDD_INT3_T2,
	PBP_B2_COINS_AD_ENDD_INT3_83 AS PBP_B2_COINS_AD_ENDD_INT3_T1,
	PBP_B2_COPAY_AD_AMT_INT2_T2_84 AS PBP_B2_COPAY_AD_AMT_INT2_T2,
	PBP_B2_COPAY_AD_AMT_INT2_T1_85 AS PBP_B2_COPAY_AD_AMT_INT2_T1,
	PBP_B2_COPAY_AD_AMT_INT2_T3_86 AS PBP_B2_COPAY_AD_AMT_INT2_T3,
	PBP_B2_COINS_AD_PCT_INT1_T1_87 AS PBP_B2_COINS_AD_PCT_INT1_T1,
	PBP_B2_COINS_AD_PCT_INT1_T2_88 AS PBP_B2_COINS_AD_PCT_INT1_T2,
	PBP_B2_COINS_AD_PCT_INT1_T3_89 AS PBP_B2_COINS_AD_PCT_INT1_T3,
	PBP_B2_BENDESC_AD_90 AS PBP_B2_BENDESC_AD,
	PBP_B2_COPAY_AD_ENDD_INT3_91 AS PBP_B2_COPAY_AD_ENDD_INT3_T1,
	PBP_B2_COPAY_AD_ENDD_INT3_92 AS PBP_B2_COPAY_AD_ENDD_INT3_T2,
	PBP_B2_COPAY_AD_ENDD_INT3_93 AS PBP_B2_COPAY_AD_ENDD_INT3_T3,
	PBP_B2_COPAY_AD_BGND_INT1_94 AS PBP_B2_COPAY_AD_BGND_INT1_T3,
	PBP_B2_COPAY_AD_BGND_INT1_95 AS PBP_B2_COPAY_AD_BGND_INT1_T1,
	PBP_B2_COPAY_AD_BGND_INT1_96 AS PBP_B2_COPAY_AD_BGND_INT1_T2,
	PBP_B2_COPAY_AD_AMT_INT1_T1_97 AS PBP_B2_COPAY_AD_AMT_INT1_T1,
	PBP_B2_COPAY_AD_AMT_INT1_T2_98 AS PBP_B2_COPAY_AD_AMT_INT1_T2,
	PBP_B2_COPAY_AD_AMT_INT1_T3_99 AS PBP_B2_COPAY_AD_AMT_INT1_T3,
	PBP_B2_COINS_AD_PCT_INT_100 AS PBP_B2_COINS_AD_PCT_INT3_T3,
	PBP_B2_COINS_AD_PCT_INT_101 AS PBP_B2_COINS_AD_PCT_INT3_T2,
	PBP_B2_COINS_AD_PCT_INT_102 AS PBP_B2_COINS_AD_PCT_INT3_T1,
	PBP_B2_COPAY_AD_ENDD_INT_103 AS PBP_B2_COPAY_AD_ENDD_INT1_T3,
	PBP_B2_COPAY_AD_ENDD_INT_104 AS PBP_B2_COPAY_AD_ENDD_INT1_T1,
	PBP_B2_COPAY_AD_ENDD_INT_105 AS PBP_B2_COPAY_AD_ENDD_INT1_T2,
	PBP_B2_COINS_AD_BGND_INT_106 AS PBP_B2_COINS_AD_BGND_INT2_T3,
	PBP_B2_COINS_AD_BGND_INT_107 AS PBP_B2_COINS_AD_BGND_INT2_T2,
	PBP_B2_COINS_AD_BGND_INT_108 AS PBP_B2_COINS_AD_BGND_INT2_T1,
	PBP_B2_COPAY_AD_BGND_INT_109 AS PBP_B2_COPAY_AD_BGND_INT3_T3,
	PBP_B2_COPAY_AD_BGND_INT_110 AS PBP_B2_COPAY_AD_BGND_INT3_T2,
	PBP_B2_COPAY_AD_BGND_INT_111 AS PBP_B2_COPAY_AD_BGND_INT3_T1,
	PBP_B2_AD_COST_VARY_TIER_112 AS PBP_B2_AD_COST_VARY_TIERS_YN,
	PBP_B2_COINS_AD_PCT_INT_113 AS PBP_B2_COINS_AD_PCT_INT2_T2,
	PBP_B2_COINS_AD_PCT_INT_114 AS PBP_B2_COINS_AD_PCT_INT2_T3,
	PBP_B2_COINS_AD_PCT_INT_115 AS PBP_B2_COINS_AD_PCT_INT2_T1,
	PBP_B2_COINS_AD_BGND_INT_116 AS PBP_B2_COINS_AD_BGND_INT3_T1,
	PBP_B2_COINS_AD_BGND_INT_117 AS PBP_B2_COINS_AD_BGND_INT3_T2,
	PBP_B2_COINS_AD_BGND_INT_118 AS PBP_B2_COINS_AD_BGND_INT3_T3,
	PBP_B2_COINS_AD_ENDD_INT_119 AS PBP_B2_COINS_AD_ENDD_INT2_T3,
	PBP_B2_COINS_AD_ENDD_INT_120 AS PBP_B2_COINS_AD_ENDD_INT2_T2,
	PBP_B2_COINS_AD_ENDD_INT_121 AS PBP_B2_COINS_AD_ENDD_INT2_T1,
	PBP_B2_COINS_AD_BGND_INT_122 AS PBP_B2_COINS_AD_BGND_INT1_T3,
	PBP_B2_COINS_AD_BGND_INT_123 AS PBP_B2_COINS_AD_BGND_INT1_T2,
	PBP_B2_COINS_AD_BGND_INT_124 AS PBP_B2_COINS_AD_BGND_INT1_T1,
	PBP_B2_COPAY_AD_ENDD_INT_125 AS PBP_B2_COPAY_AD_ENDD_INT2_T1,
	PBP_B2_COPAY_AD_ENDD_INT_126 AS PBP_B2_COPAY_AD_ENDD_INT2_T2,
	PBP_B2_COPAY_AD_ENDD_INT_127 AS PBP_B2_COPAY_AD_ENDD_INT2_T3,
	PBP_B2_COPAY_AD_BGND_INT_128 AS PBP_B2_COPAY_AD_BGND_INT2_T3,
	PBP_B2_COPAY_AD_BGND_INT_129 AS PBP_B2_COPAY_AD_BGND_INT2_T2,
	PBP_B2_COPAY_AD_BGND_INT_130 AS PBP_B2_COPAY_AD_BGND_INT2_T1,
	PBP_B2_COINS_AD_ENDD_INT_131 AS PBP_B2_COINS_AD_ENDD_INT1_T3,
	PBP_B2_COINS_AD_ENDD_INT_132 AS PBP_B2_COINS_AD_ENDD_INT1_T1,
	PBP_B2_COINS_AD_ENDD_INT_133 AS PBP_B2_COINS_AD_ENDD_INT1_T2,
	PBP_B2_AD_COST_VARY_LOW__134 AS PBP_B2_AD_COST_VARY_LOW_TIER,
	PBP_B2_COPAY_AD_AMT_INT_135 AS PBP_B2_COPAY_AD_AMT_INT3_T3,
	PBP_B2_COPAY_AD_AMT_INT_136 AS PBP_B2_COPAY_AD_AMT_INT3_T2,
	PBP_B2_COPAY_AD_AMT_INT_137 AS PBP_B2_COPAY_AD_AMT_INT3_T1,
	PBP_B2_COPAY_NMCS_AMT_138 AS PBP_B2_COPAY_NMCS_AMT,
	PBP_B2_COINS_NMCS_ENDD_INT_139 AS PBP_B2_COINS_NMCS_ENDD_INTRVL1,
	PBP_B2_COINS_NMCS_ENDD_INT_140 AS PBP_B2_COINS_NMCS_ENDD_INTRVL3,
	PBP_B2_COINS_NMCS_ENDD_INT_141 AS PBP_B2_COINS_NMCS_ENDD_INTRVL2,
	PBP_B2_COINS_NMCS_BGND_INT_142 AS PBP_B2_COINS_NMCS_BGND_INTRVL3,
	PBP_B2_COINS_NMCS_BGND_INT_143 AS PBP_B2_COINS_NMCS_BGND_INTRVL2,
	PBP_B2_COINS_NMCS_BGND_INT_144 AS PBP_B2_COINS_NMCS_BGND_INTRVL1,
	PBP_B2_COPAY_NMCS_AMT_INT_145 AS PBP_B2_COPAY_NMCS_AMT_INTRVL1,
	PBP_B2_COPAY_NMCS_AMT_INT_146 AS PBP_B2_COPAY_NMCS_AMT_INTRVL2,
	PBP_B2_COPAY_NMCS_AMT_INT_147 AS PBP_B2_COPAY_NMCS_AMT_INTRVL3,
	PBP_B2_COPAY_NMCS_BGND_INT_148 AS PBP_B2_COPAY_NMCS_BGND_INTRVL1,
	PBP_B2_COPAY_NMCS_BGND_INT_149 AS PBP_B2_COPAY_NMCS_BGND_INTRVL2,
	PBP_B2_COPAY_NMCS_BGND_INT_150 AS PBP_B2_COPAY_NMCS_BGND_INTRVL3,
	PBP_B2_COINS_NMCS_PCT_151 AS PBP_B2_COINS_NMCS_PCT,
	PBP_B2_COPAY_NMCS_ENDD_INT_152 AS PBP_B2_COPAY_NMCS_ENDD_INTRVL1,
	PBP_B2_COPAY_NMCS_ENDD_INT_153 AS PBP_B2_COPAY_NMCS_ENDD_INTRVL2,
	PBP_B2_COPAY_NMCS_ENDD_INT_154 AS PBP_B2_COPAY_NMCS_ENDD_INTRVL3,
	PBP_B2_COINS_NMCS_STRUC_YN_155 AS PBP_B2_COINS_NMCS_STRUC_YN,
	PBP_B2_COPAY_NMCS_STRUC_YN_156 AS PBP_B2_COPAY_NMCS_STRUC_YN,
	PBP_B2_COINS_NMCS_PCT_INT_157 AS PBP_B2_COINS_NMCS_PCT_INTRVL3,
	PBP_B2_COINS_NMCS_PCT_INT_158 AS PBP_B2_COINS_NMCS_PCT_INTRVL2,
	PBP_B2_COINS_NMCS_PCT_INT_159 AS PBP_B2_COINS_NMCS_PCT_INTRVL1
FROM
	(
	SELECT
		qid,
		MAX(PBP_B2_BENDESC_YN_01) AS PBP_B2_BENDESC_YN_01,
		MAX(PBP_B2_BENDESC_YN_02) AS PBP_B2_BENDESC_YN_02,
		MAX(PBP_B2_BENDESC_AMO_AD_1) AS PBP_B2_BENDESC_AMO_AD_1,
		MAX(PBP_B2_BENDESC_AMO_NMCS_2) AS PBP_B2_BENDESC_AMO_NMCS_2,
		MAX(PBP_B2_DED_AMT_T1_3) AS PBP_B2_DED_AMT_T1_3,
		MAX(PBP_B2_DED_AMT_T2_4) AS PBP_B2_DED_AMT_T2_4,
		MAX(PBP_B2_DED_AMT_T3_5) AS PBP_B2_DED_AMT_T3_5,
		MAX(PBP_B2_COINS_MCS_PCT_INT2_T1_6) AS PBP_B2_COINS_MCS_PCT_INT2_T1_6,
		MAX(PBP_B2_COINS_MCS_PCT_INT2_T3_7) AS PBP_B2_COINS_MCS_PCT_INT2_T3_7
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('2-1') THEN CATS(nonMedicare_nonMedBenOffDet.boInNetwork)
				ELSE ''
			END AS PBP_B2_BENDESC_YN_01,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('2-2') THEN CATS(nonMedicare_nonMedBenOffDet.boInNetwork)
				ELSE ''
			END AS PBP_B2_BENDESC_YN_02,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('2-1') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B2_BENDESC_AMO_AD_1,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('2-2') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B2_BENDESC_AMO_NMCS_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_PaABTiDeCom.bdPartAandBTier1DeductibleAmt)
				ELSE ''
			END AS PBP_B2_DED_AMT_T1_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_PaABTiDeCom.bdPartAandBTier2DeductibleAmt)
				ELSE ''
			END AS PBP_B2_DED_AMT_T2_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_PaABTiDeCom.bdPartAandBTier3DeductibleAmt)
				ELSE ''
			END AS PBP_B2_DED_AMT_T3_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInter2CoinsPerce)
				ELSE ''
			END AS PBP_B2_COINS_MCS_PCT_INT2_T1_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInter2CoinsPerce)
				ELSE ''
			END AS PBP_B2_COINS_MCS_PCT_INT2_T3_7
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
		LEFT JOIN jsonlib.benefitDetails_PaABTiDeCom AS benefitDetails_PaABTiDeCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_PaABTiDeCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
			benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
		LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
			benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_0
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_MCS_PCT_INT2_T2_8) AS PBP_B2_COINS_MCS_PCT_INT2_T2_8,
		MAX(PBP_B2_COINS_MCS_ENDD_INT3__9) AS PBP_B2_COINS_MCS_ENDD_INT3__9,
		MAX(PBP_B2_COINS_MCS_ENDD_INT3_10) AS PBP_B2_COINS_MCS_ENDD_INT3_10,
		MAX(PBP_B2_COINS_MCS_ENDD_INT3_11) AS PBP_B2_COINS_MCS_ENDD_INT3_11,
		MAX(PBP_B2_COINS_MCS_PCT_T3_12) AS PBP_B2_COINS_MCS_PCT_T3_12,
		MAX(PBP_B2_COINS_MCS_PCT_T2_13) AS PBP_B2_COINS_MCS_PCT_T2_13,
		MAX(PBP_B2_COINS_MCS_PCT_T1_14) AS PBP_B2_COINS_MCS_PCT_T1_14,
		MAX(PBP_B2_COINS_MCS_BGND_INT2_15) AS PBP_B2_COINS_MCS_BGND_INT2_15
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInter2CoinsPerce)
					ELSE ''
				END AS PBP_B2_COINS_MCS_PCT_INT2_T2_8,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval3EndDay)
						ELSE ''
					END AS PBP_B2_COINS_MCS_ENDD_INT3__9,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval3EndDay)
							ELSE ''
						END AS PBP_B2_COINS_MCS_ENDD_INT3_10,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval3EndDay)
								ELSE ''
							END AS PBP_B2_COINS_MCS_ENDD_INT3_11,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier3Percentage)
									ELSE ''
								END AS PBP_B2_COINS_MCS_PCT_T3_12,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier2Percentage)
										ELSE ''
									END AS PBP_B2_COINS_MCS_PCT_T2_13,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier1Percentage)
											ELSE ''
										END AS PBP_B2_COINS_MCS_PCT_T1_14,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval2BeginDay)
												ELSE ''
											END AS PBP_B2_COINS_MCS_BGND_INT2_15
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
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_1 ON
	segment_0.qid = segment_1.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_MCS_BGND_INT2_16) AS PBP_B2_COINS_MCS_BGND_INT2_16,
		MAX(PBP_B2_COINS_MCS_BGND_INT2_17) AS PBP_B2_COINS_MCS_BGND_INT2_17,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT2_18) AS PBP_B2_COPAY_MCS_ENDD_INT2_18,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT2_19) AS PBP_B2_COPAY_MCS_ENDD_INT2_19,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT2_20) AS PBP_B2_COPAY_MCS_ENDD_INT2_20,
		MAX(PBP_B2_COST_VARY_LOW_TIER_21) AS PBP_B2_COST_VARY_LOW_TIER_21,
		MAX(PBP_B2_COPAY_YN_22) AS PBP_B2_COPAY_YN_22,
		MAX(PBP_B2_MAXENR_AMT_23) AS PBP_B2_MAXENR_AMT_23
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval2BeginDay)
					ELSE ''
				END AS PBP_B2_COINS_MCS_BGND_INT2_16,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval2BeginDay)
						ELSE ''
					END AS PBP_B2_COINS_MCS_BGND_INT2_17,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval2EndDay)
							ELSE ''
						END AS PBP_B2_COPAY_MCS_ENDD_INT2_18,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval2EndDay)
								ELSE ''
							END AS PBP_B2_COPAY_MCS_ENDD_INT2_19,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval2EndDay)
									ELSE ''
								END AS PBP_B2_COPAY_MCS_ENDD_INT2_20,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CosShaTieCom.bdLowestCostTier)
										ELSE ''
									END AS PBP_B2_COST_VARY_LOW_TIER_21,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentAmountYesNo)
											ELSE ''
										END AS PBP_B2_COPAY_YN_22,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
												ELSE ''
											END AS PBP_B2_MAXENR_AMT_23
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
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_CosShaTieCom AS benefitDetails_CosShaTieCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CosShaTieCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_2 ON
	segment_0.qid = segment_2.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COPAY_MCS_AMT_INT2_24) AS PBP_B2_COPAY_MCS_AMT_INT2_24,
		MAX(PBP_B2_COPAY_MCS_AMT_INT2_25) AS PBP_B2_COPAY_MCS_AMT_INT2_25,
		MAX(PBP_B2_COPAY_MCS_AMT_INT2_26) AS PBP_B2_COPAY_MCS_AMT_INT2_26,
		MAX(PBP_B2_COPAY_MCS_BGND_INT1_27) AS PBP_B2_COPAY_MCS_BGND_INT1_27,
		MAX(PBP_B2_COPAY_MCS_BGND_INT1_28) AS PBP_B2_COPAY_MCS_BGND_INT1_28,
		MAX(PBP_B2_COPAY_MCS_BGND_INT1_29) AS PBP_B2_COPAY_MCS_BGND_INT1_29,
		MAX(PBP_B2_COINS_MCS_ENDD_INT1_30) AS PBP_B2_COINS_MCS_ENDD_INT1_30,
		MAX(PBP_B2_COINS_MCS_ENDD_INT1_31) AS PBP_B2_COINS_MCS_ENDD_INT1_31
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval2CopaymentAmount)
					ELSE ''
				END AS PBP_B2_COPAY_MCS_AMT_INT2_24,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval2CopaymentAmount)
						ELSE ''
					END AS PBP_B2_COPAY_MCS_AMT_INT2_25,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval2CopaymentAmount)
							ELSE ''
						END AS PBP_B2_COPAY_MCS_AMT_INT2_26,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval1BeginDay)
								ELSE ''
							END AS PBP_B2_COPAY_MCS_BGND_INT1_27,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval1BeginDay)
									ELSE ''
								END AS PBP_B2_COPAY_MCS_BGND_INT1_28,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval1BeginDay)
										ELSE ''
									END AS PBP_B2_COPAY_MCS_BGND_INT1_29,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval1EndDay)
											ELSE ''
										END AS PBP_B2_COINS_MCS_ENDD_INT1_30,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval1EndDay)
												ELSE ''
											END AS PBP_B2_COINS_MCS_ENDD_INT1_31
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
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_3 ON
	segment_0.qid = segment_3.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_MCS_ENDD_INT1_32) AS PBP_B2_COINS_MCS_ENDD_INT1_32,
		MAX(PBP_B2_COPAY_MCS_BGND_INT2_33) AS PBP_B2_COPAY_MCS_BGND_INT2_33,
		MAX(PBP_B2_COPAY_MCS_BGND_INT2_34) AS PBP_B2_COPAY_MCS_BGND_INT2_34,
		MAX(PBP_B2_COPAY_MCS_BGND_INT2_35) AS PBP_B2_COPAY_MCS_BGND_INT2_35,
		MAX(PBP_B2_COINS_MCS_PCT_INT1_36) AS PBP_B2_COINS_MCS_PCT_INT1_36,
		MAX(PBP_B2_COINS_MCS_PCT_INT1_37) AS PBP_B2_COINS_MCS_PCT_INT1_37,
		MAX(PBP_B2_COINS_MCS_PCT_INT1_38) AS PBP_B2_COINS_MCS_PCT_INT1_38,
		MAX(PBP_B2_MC_COINS_CSTSHR_YN_39) AS PBP_B2_MC_COINS_CSTSHR_YN_39
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval1EndDay)
					ELSE ''
				END AS PBP_B2_COINS_MCS_ENDD_INT1_32,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval2BeginDay)
						ELSE ''
					END AS PBP_B2_COPAY_MCS_BGND_INT2_33,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval2BeginDay)
							ELSE ''
						END AS PBP_B2_COPAY_MCS_BGND_INT2_34,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval2BeginDay)
								ELSE ''
							END AS PBP_B2_COPAY_MCS_BGND_INT2_35,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInter1CoinsPerce)
									ELSE ''
								END AS PBP_B2_COINS_MCS_PCT_INT1_36,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInter1CoinsPerce)
										ELSE ''
									END AS PBP_B2_COINS_MCS_PCT_INT1_37,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInter1CoinsPerce)
											ELSE ''
										END AS PBP_B2_COINS_MCS_PCT_INT1_38,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier3YesNo)
												ELSE ''
											END AS PBP_B2_MC_COINS_CSTSHR_YN_39
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
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_4 ON
	segment_0.qid = segment_4.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_MC_COINS_CSTSHR_YN_40) AS PBP_B2_MC_COINS_CSTSHR_YN_40,
		MAX(PBP_B2_MC_COINS_CSTSHR_YN_41) AS PBP_B2_MC_COINS_CSTSHR_YN_41,
		MAX(PBP_B2_MAXENR_PER_42) AS PBP_B2_MAXENR_PER_42,
		MAX(PBP_B2_COPAY_MCS_AMT_T3_43) AS PBP_B2_COPAY_MCS_AMT_T3_43,
		MAX(PBP_B2_COPAY_MCS_AMT_T1_44) AS PBP_B2_COPAY_MCS_AMT_T1_44,
		MAX(PBP_B2_COPAY_MCS_AMT_T2_45) AS PBP_B2_COPAY_MCS_AMT_T2_45,
		MAX(PBP_B2_COINS_MCS_BGND_INT3_46) AS PBP_B2_COINS_MCS_BGND_INT3_46,
		MAX(PBP_B2_COINS_MCS_BGND_INT3_47) AS PBP_B2_COINS_MCS_BGND_INT3_47
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier1YesNo)
					ELSE ''
				END AS PBP_B2_MC_COINS_CSTSHR_YN_40,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier2YesNo)
						ELSE ''
					END AS PBP_B2_MC_COINS_CSTSHR_YN_41,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
							ELSE ''
						END AS PBP_B2_MAXENR_PER_42,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier3Amt)
								ELSE ''
							END AS PBP_B2_COPAY_MCS_AMT_T3_43,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier1Amt)
									ELSE ''
								END AS PBP_B2_COPAY_MCS_AMT_T1_44,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier2Amt)
										ELSE ''
									END AS PBP_B2_COPAY_MCS_AMT_T2_45,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval3BeginDay)
											ELSE ''
										END AS PBP_B2_COINS_MCS_BGND_INT3_46,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval3BeginDay)
												ELSE ''
											END AS PBP_B2_COINS_MCS_BGND_INT3_47
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
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_5 ON
	segment_0.qid = segment_5.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_MCS_BGND_INT3_48) AS PBP_B2_COINS_MCS_BGND_INT3_48,
		MAX(PBP_B2_COST_DISCHARGE_YN_49) AS PBP_B2_COST_DISCHARGE_YN_49,
		MAX(PBP_B2_COPAY_MCS_AMT_INT3_50) AS PBP_B2_COPAY_MCS_AMT_INT3_50,
		MAX(PBP_B2_COPAY_MCS_AMT_INT3_51) AS PBP_B2_COPAY_MCS_AMT_INT3_51,
		MAX(PBP_B2_COPAY_MCS_AMT_INT3_52) AS PBP_B2_COPAY_MCS_AMT_INT3_52,
		MAX(PBP_B2_COINS_MCS_BGND_INT1_53) AS PBP_B2_COINS_MCS_BGND_INT1_53,
		MAX(PBP_B2_COINS_MCS_BGND_INT1_54) AS PBP_B2_COINS_MCS_BGND_INT1_54,
		MAX(PBP_B2_COINS_MCS_BGND_INT1_55) AS PBP_B2_COINS_MCS_BGND_INT1_55
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval3BeginDay)
					ELSE ''
				END AS PBP_B2_COINS_MCS_BGND_INT3_48,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_InHoAcBePeCom.bdMedCostSharingChargeYesNo)
						ELSE ''
					END AS PBP_B2_COST_DISCHARGE_YN_49,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval3CopaymentAmount)
							ELSE ''
						END AS PBP_B2_COPAY_MCS_AMT_INT3_50,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval3CopaymentAmount)
								ELSE ''
							END AS PBP_B2_COPAY_MCS_AMT_INT3_51,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval3CopaymentAmount)
									ELSE ''
								END AS PBP_B2_COPAY_MCS_AMT_INT3_52,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval1BeginDay)
										ELSE ''
									END AS PBP_B2_COINS_MCS_BGND_INT1_53,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval1BeginDay)
											ELSE ''
										END AS PBP_B2_COINS_MCS_BGND_INT1_54,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval1BeginDay)
												ELSE ''
											END AS PBP_B2_COINS_MCS_BGND_INT1_55
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
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.benefitDetails_InHoAcBePeCom AS benefitDetails_InHoAcBePeCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_InHoAcBePeCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_6 ON
	segment_0.qid = segment_6.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_MC_COPAY_CSTSHR_YN_56) AS PBP_B2_MC_COPAY_CSTSHR_YN_56,
		MAX(PBP_B2_MC_COPAY_CSTSHR_YN_57) AS PBP_B2_MC_COPAY_CSTSHR_YN_57,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT3_58) AS PBP_B2_COPAY_MCS_ENDD_INT3_58,
		MAX(PBP_B2_MC_COPAY_CSTSHR_YN_59) AS PBP_B2_MC_COPAY_CSTSHR_YN_59,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT3_60) AS PBP_B2_COPAY_MCS_ENDD_INT3_60,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT3_61) AS PBP_B2_COPAY_MCS_ENDD_INT3_61,
		MAX(PBP_B2_COPAY_MCS_AMT_INT1_62) AS PBP_B2_COPAY_MCS_AMT_INT1_62,
		MAX(PBP_B2_COPAY_MCS_AMT_INT1_63) AS PBP_B2_COPAY_MCS_AMT_INT1_63
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier3YesNo)
					ELSE ''
				END AS PBP_B2_MC_COPAY_CSTSHR_YN_56,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier1YesNo)
						ELSE ''
					END AS PBP_B2_MC_COPAY_CSTSHR_YN_57,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval3EndDay)
							ELSE ''
						END AS PBP_B2_COPAY_MCS_ENDD_INT3_58,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier2YesNo)
								ELSE ''
							END AS PBP_B2_MC_COPAY_CSTSHR_YN_59,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval3EndDay)
									ELSE ''
								END AS PBP_B2_COPAY_MCS_ENDD_INT3_60,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval3EndDay)
										ELSE ''
									END AS PBP_B2_COPAY_MCS_ENDD_INT3_61,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval1CopaymentAmount)
											ELSE ''
										END AS PBP_B2_COPAY_MCS_AMT_INT1_62,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval1CopaymentAmount)
												ELSE ''
											END AS PBP_B2_COPAY_MCS_AMT_INT1_63
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
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom ) main
	GROUP BY
		qid) segment_7 ON
	segment_0.qid = segment_7.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COPAY_MCS_AMT_INT1_64) AS PBP_B2_COPAY_MCS_AMT_INT1_64,
		MAX(PBP_B2_COST_VARY_TIERS_YN_65) AS PBP_B2_COST_VARY_TIERS_YN_65,
		MAX(PBP_B2_COINS_YN_66) AS PBP_B2_COINS_YN_66,
		MAX(PBP_B2_COINS_MCS_PCT_INT3_67) AS PBP_B2_COINS_MCS_PCT_INT3_67,
		MAX(PBP_B2_COINS_MCS_PCT_INT3_68) AS PBP_B2_COINS_MCS_PCT_INT3_68,
		MAX(PBP_B2_COINS_MCS_PCT_INT3_69) AS PBP_B2_COINS_MCS_PCT_INT3_69,
		MAX(PBP_B2_COINS_MCS_ENDD_INT2_70) AS PBP_B2_COINS_MCS_ENDD_INT2_70,
		MAX(PBP_B2_COINS_MCS_ENDD_INT2_71) AS PBP_B2_COINS_MCS_ENDD_INT2_71
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval1CopaymentAmount)
					ELSE ''
				END AS PBP_B2_COPAY_MCS_AMT_INT1_64,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CosShaTieCom.bdCostSharingYesNo)
						ELSE ''
					END AS PBP_B2_COST_VARY_TIERS_YN_65,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsurancePercentageYesNo)
							ELSE ''
						END AS PBP_B2_COINS_YN_66,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInter3CoinsPerce)
								ELSE ''
							END AS PBP_B2_COINS_MCS_PCT_INT3_67,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInter3CoinsPerce)
									ELSE ''
								END AS PBP_B2_COINS_MCS_PCT_INT3_68,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInter3CoinsPerce)
										ELSE ''
									END AS PBP_B2_COINS_MCS_PCT_INT3_69,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval2EndDay)
											ELSE ''
										END AS PBP_B2_COINS_MCS_ENDD_INT2_70,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval2EndDay)
												ELSE ''
											END AS PBP_B2_COINS_MCS_ENDD_INT2_71
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
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_CosShaTieCom AS benefitDetails_CosShaTieCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CosShaTieCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_8 ON
	segment_0.qid = segment_8.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_MCS_ENDD_INT2_72) AS PBP_B2_COINS_MCS_ENDD_INT2_72,
		MAX(PBP_B2_COPAY_MCS_BGND_INT3_73) AS PBP_B2_COPAY_MCS_BGND_INT3_73,
		MAX(PBP_B2_COPAY_MCS_BGND_INT3_74) AS PBP_B2_COPAY_MCS_BGND_INT3_74,
		MAX(PBP_B2_COPAY_MCS_BGND_INT3_75) AS PBP_B2_COPAY_MCS_BGND_INT3_75,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT1_76) AS PBP_B2_COPAY_MCS_ENDD_INT1_76,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT1_77) AS PBP_B2_COPAY_MCS_ENDD_INT1_77,
		MAX(PBP_B2_COPAY_MCS_ENDD_INT1_78) AS PBP_B2_COPAY_MCS_ENDD_INT1_78,
		MAX(PBP_B2_HOSP_BEN_PERIOD_79) AS PBP_B2_HOSP_BEN_PERIOD_79
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval2EndDay)
					ELSE ''
				END AS PBP_B2_COINS_MCS_ENDD_INT2_72,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval3BeginDay)
						ELSE ''
					END AS PBP_B2_COPAY_MCS_BGND_INT3_73,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval3BeginDay)
							ELSE ''
						END AS PBP_B2_COPAY_MCS_BGND_INT3_74,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval3BeginDay)
								ELSE ''
							END AS PBP_B2_COPAY_MCS_BGND_INT3_75,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval1EndDay)
									ELSE ''
								END AS PBP_B2_COPAY_MCS_ENDD_INT1_76,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval1EndDay)
										ELSE ''
									END AS PBP_B2_COPAY_MCS_ENDD_INT1_77,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval1EndDay)
											ELSE ''
										END AS PBP_B2_COPAY_MCS_ENDD_INT1_78,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_InHoAcBePeCom.bdMedInpaHospAcutBenePeri)
												ELSE ''
											END AS PBP_B2_HOSP_BEN_PERIOD_79
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
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_InHoAcBePeCom AS benefitDetails_InHoAcBePeCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_InHoAcBePeCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_9 ON
	segment_0.qid = segment_9.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_BENEDESC_LIM_AD_80) AS PBP_B2_BENEDESC_LIM_AD_80,
		MAX(PBP_B2_COINS_AD_ENDD_INT3_81) AS PBP_B2_COINS_AD_ENDD_INT3_81,
		MAX(PBP_B2_COINS_AD_ENDD_INT3_82) AS PBP_B2_COINS_AD_ENDD_INT3_82,
		MAX(PBP_B2_COINS_AD_ENDD_INT3_83) AS PBP_B2_COINS_AD_ENDD_INT3_83,
		MAX(PBP_B2_COPAY_AD_AMT_INT2_T2_84) AS PBP_B2_COPAY_AD_AMT_INT2_T2_84,
		MAX(PBP_B2_COPAY_AD_AMT_INT2_T1_85) AS PBP_B2_COPAY_AD_AMT_INT2_T1_85,
		MAX(PBP_B2_COPAY_AD_AMT_INT2_T3_86) AS PBP_B2_COPAY_AD_AMT_INT2_T3_86,
		MAX(PBP_B2_COINS_AD_PCT_INT1_T1_87) AS PBP_B2_COINS_AD_PCT_INT1_T1_87
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdUnlAddDay2.bdBenefitUnlimitedYesNo)
					ELSE ''
				END AS PBP_B2_BENEDESC_LIM_AD_80,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval3EndDay)
						ELSE ''
					END AS PBP_B2_COINS_AD_ENDD_INT3_81,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval3EndDay)
							ELSE ''
						END AS PBP_B2_COINS_AD_ENDD_INT3_82,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval3EndDay)
								ELSE ''
							END AS PBP_B2_COINS_AD_ENDD_INT3_83,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval2CopaymentAmount)
									ELSE ''
								END AS PBP_B2_COPAY_AD_AMT_INT2_T2_84,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval2CopaymentAmount)
										ELSE ''
									END AS PBP_B2_COPAY_AD_AMT_INT2_T1_85,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval2CopaymentAmount)
											ELSE ''
										END AS PBP_B2_COPAY_AD_AMT_INT2_T3_86,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInter1CoinsPerce)
												ELSE ''
											END AS PBP_B2_COINS_AD_PCT_INT1_T1_87
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdUnlAddDay2 AS AdDa1a1Com_bdUnlAddDay2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdUnlAddDay2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCoins2 AS AdDa1a1Com_bdTierCoins2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCoins2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment ) main
	GROUP BY
		qid) segment_10 ON
	segment_0.qid = segment_10.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_AD_PCT_INT1_T2_88) AS PBP_B2_COINS_AD_PCT_INT1_T2_88,
		MAX(PBP_B2_COINS_AD_PCT_INT1_T3_89) AS PBP_B2_COINS_AD_PCT_INT1_T3_89,
		MAX(PBP_B2_BENDESC_AD_90) AS PBP_B2_BENDESC_AD_90,
		MAX(PBP_B2_COPAY_AD_ENDD_INT3_91) AS PBP_B2_COPAY_AD_ENDD_INT3_91,
		MAX(PBP_B2_COPAY_AD_ENDD_INT3_92) AS PBP_B2_COPAY_AD_ENDD_INT3_92,
		MAX(PBP_B2_COPAY_AD_ENDD_INT3_93) AS PBP_B2_COPAY_AD_ENDD_INT3_93,
		MAX(PBP_B2_COPAY_AD_BGND_INT1_94) AS PBP_B2_COPAY_AD_BGND_INT1_94,
		MAX(PBP_B2_COPAY_AD_BGND_INT1_95) AS PBP_B2_COPAY_AD_BGND_INT1_95
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInter1CoinsPerce)
					ELSE ''
				END AS PBP_B2_COINS_AD_PCT_INT1_T2_88,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInter1CoinsPerce)
						ELSE ''
					END AS PBP_B2_COINS_AD_PCT_INT1_T3_89,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdUnlAddDay2.bdIndicateNumOfAddDays)
							ELSE ''
						END AS PBP_B2_BENDESC_AD_90,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval3EndDay)
								ELSE ''
							END AS PBP_B2_COPAY_AD_ENDD_INT3_91,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval3EndDay)
									ELSE ''
								END AS PBP_B2_COPAY_AD_ENDD_INT3_92,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval3EndDay)
										ELSE ''
									END AS PBP_B2_COPAY_AD_ENDD_INT3_93,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval1BeginDay)
											ELSE ''
										END AS PBP_B2_COPAY_AD_BGND_INT1_94,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval1BeginDay)
												ELSE ''
											END AS PBP_B2_COPAY_AD_BGND_INT1_95
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCoins2 AS AdDa1a1Com_bdTierCoins2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCoins2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdUnlAddDay2 AS AdDa1a1Com_bdUnlAddDay2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdUnlAddDay2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment ) main
	GROUP BY
		qid) segment_11 ON
	segment_0.qid = segment_11.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COPAY_AD_BGND_INT1_96) AS PBP_B2_COPAY_AD_BGND_INT1_96,
		MAX(PBP_B2_COPAY_AD_AMT_INT1_T1_97) AS PBP_B2_COPAY_AD_AMT_INT1_T1_97,
		MAX(PBP_B2_COPAY_AD_AMT_INT1_T2_98) AS PBP_B2_COPAY_AD_AMT_INT1_T2_98,
		MAX(PBP_B2_COPAY_AD_AMT_INT1_T3_99) AS PBP_B2_COPAY_AD_AMT_INT1_T3_99,
		MAX(PBP_B2_COINS_AD_PCT_INT_100) AS PBP_B2_COINS_AD_PCT_INT_100,
		MAX(PBP_B2_COINS_AD_PCT_INT_101) AS PBP_B2_COINS_AD_PCT_INT_101,
		MAX(PBP_B2_COINS_AD_PCT_INT_102) AS PBP_B2_COINS_AD_PCT_INT_102,
		MAX(PBP_B2_COPAY_AD_ENDD_INT_103) AS PBP_B2_COPAY_AD_ENDD_INT_103
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval1BeginDay)
					ELSE ''
				END AS PBP_B2_COPAY_AD_BGND_INT1_96,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval1CopaymentAmount)
						ELSE ''
					END AS PBP_B2_COPAY_AD_AMT_INT1_T1_97,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval1CopaymentAmount)
							ELSE ''
						END AS PBP_B2_COPAY_AD_AMT_INT1_T2_98,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval1CopaymentAmount)
								ELSE ''
							END AS PBP_B2_COPAY_AD_AMT_INT1_T3_99,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInter3CoinsPerce)
									ELSE ''
								END AS PBP_B2_COINS_AD_PCT_INT_100,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInter3CoinsPerce)
										ELSE ''
									END AS PBP_B2_COINS_AD_PCT_INT_101,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInter3CoinsPerce)
											ELSE ''
										END AS PBP_B2_COINS_AD_PCT_INT_102,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval1EndDay)
												ELSE ''
											END AS PBP_B2_COPAY_AD_ENDD_INT_103
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCoins2 AS AdDa1a1Com_bdTierCoins2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCoins2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins ) main
	GROUP BY
		qid) segment_12 ON
	segment_0.qid = segment_12.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COPAY_AD_ENDD_INT_104) AS PBP_B2_COPAY_AD_ENDD_INT_104,
		MAX(PBP_B2_COPAY_AD_ENDD_INT_105) AS PBP_B2_COPAY_AD_ENDD_INT_105,
		MAX(PBP_B2_COINS_AD_BGND_INT_106) AS PBP_B2_COINS_AD_BGND_INT_106,
		MAX(PBP_B2_COINS_AD_BGND_INT_107) AS PBP_B2_COINS_AD_BGND_INT_107,
		MAX(PBP_B2_COINS_AD_BGND_INT_108) AS PBP_B2_COINS_AD_BGND_INT_108,
		MAX(PBP_B2_COPAY_AD_BGND_INT_109) AS PBP_B2_COPAY_AD_BGND_INT_109,
		MAX(PBP_B2_COPAY_AD_BGND_INT_110) AS PBP_B2_COPAY_AD_BGND_INT_110,
		MAX(PBP_B2_COPAY_AD_BGND_INT_111) AS PBP_B2_COPAY_AD_BGND_INT_111
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval1EndDay)
					ELSE ''
				END AS PBP_B2_COPAY_AD_ENDD_INT_104,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval1EndDay)
						ELSE ''
					END AS PBP_B2_COPAY_AD_ENDD_INT_105,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval2BeginDay)
							ELSE ''
						END AS PBP_B2_COINS_AD_BGND_INT_106,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval2BeginDay)
								ELSE ''
							END AS PBP_B2_COINS_AD_BGND_INT_107,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval2BeginDay)
									ELSE ''
								END AS PBP_B2_COINS_AD_BGND_INT_108,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval3BeginDay)
										ELSE ''
									END AS PBP_B2_COPAY_AD_BGND_INT_109,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval3BeginDay)
											ELSE ''
										END AS PBP_B2_COPAY_AD_BGND_INT_110,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval3BeginDay)
												ELSE ''
											END AS PBP_B2_COPAY_AD_BGND_INT_111
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCoins2 AS AdDa1a1Com_bdTierCoins2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCoins2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment ) main
	GROUP BY
		qid) segment_13 ON
	segment_0.qid = segment_13.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_AD_COST_VARY_TIER_112) AS PBP_B2_AD_COST_VARY_TIER_112,
		MAX(PBP_B2_COINS_AD_PCT_INT_113) AS PBP_B2_COINS_AD_PCT_INT_113,
		MAX(PBP_B2_COINS_AD_PCT_INT_114) AS PBP_B2_COINS_AD_PCT_INT_114,
		MAX(PBP_B2_COINS_AD_PCT_INT_115) AS PBP_B2_COINS_AD_PCT_INT_115,
		MAX(PBP_B2_COINS_AD_BGND_INT_116) AS PBP_B2_COINS_AD_BGND_INT_116,
		MAX(PBP_B2_COINS_AD_BGND_INT_117) AS PBP_B2_COINS_AD_BGND_INT_117,
		MAX(PBP_B2_COINS_AD_BGND_INT_118) AS PBP_B2_COINS_AD_BGND_INT_118,
		MAX(PBP_B2_COINS_AD_ENDD_INT_119) AS PBP_B2_COINS_AD_ENDD_INT_119
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdAddDayCosSha2.bdCostSharingYesNo)
					ELSE ''
				END AS PBP_B2_AD_COST_VARY_TIER_112,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInter2CoinsPerce)
						ELSE ''
					END AS PBP_B2_COINS_AD_PCT_INT_113,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInter2CoinsPerce)
							ELSE ''
						END AS PBP_B2_COINS_AD_PCT_INT_114,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInter2CoinsPerce)
								ELSE ''
							END AS PBP_B2_COINS_AD_PCT_INT_115,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval3BeginDay)
									ELSE ''
								END AS PBP_B2_COINS_AD_BGND_INT_116,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval3BeginDay)
										ELSE ''
									END AS PBP_B2_COINS_AD_BGND_INT_117,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval3BeginDay)
											ELSE ''
										END AS PBP_B2_COINS_AD_BGND_INT_118,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval2EndDay)
												ELSE ''
											END AS PBP_B2_COINS_AD_ENDD_INT_119
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdAddDayCosSha2 AS AdDa1a1Com_bdAddDayCosSha2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdAddDayCosSha2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCoins2 AS AdDa1a1Com_bdTierCoins2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCoins2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins ) main
	GROUP BY
		qid) segment_14 ON
	segment_0.qid = segment_14.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_AD_ENDD_INT_120) AS PBP_B2_COINS_AD_ENDD_INT_120,
		MAX(PBP_B2_COINS_AD_ENDD_INT_121) AS PBP_B2_COINS_AD_ENDD_INT_121,
		MAX(PBP_B2_COINS_AD_BGND_INT_122) AS PBP_B2_COINS_AD_BGND_INT_122,
		MAX(PBP_B2_COINS_AD_BGND_INT_123) AS PBP_B2_COINS_AD_BGND_INT_123,
		MAX(PBP_B2_COINS_AD_BGND_INT_124) AS PBP_B2_COINS_AD_BGND_INT_124,
		MAX(PBP_B2_COPAY_AD_ENDD_INT_125) AS PBP_B2_COPAY_AD_ENDD_INT_125,
		MAX(PBP_B2_COPAY_AD_ENDD_INT_126) AS PBP_B2_COPAY_AD_ENDD_INT_126,
		MAX(PBP_B2_COPAY_AD_ENDD_INT_127) AS PBP_B2_COPAY_AD_ENDD_INT_127
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval2EndDay)
					ELSE ''
				END AS PBP_B2_COINS_AD_ENDD_INT_120,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval2EndDay)
						ELSE ''
					END AS PBP_B2_COINS_AD_ENDD_INT_121,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval1BeginDay)
							ELSE ''
						END AS PBP_B2_COINS_AD_BGND_INT_122,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval1BeginDay)
								ELSE ''
							END AS PBP_B2_COINS_AD_BGND_INT_123,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval1BeginDay)
									ELSE ''
								END AS PBP_B2_COINS_AD_BGND_INT_124,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval2EndDay)
										ELSE ''
									END AS PBP_B2_COPAY_AD_ENDD_INT_125,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval2EndDay)
											ELSE ''
										END AS PBP_B2_COPAY_AD_ENDD_INT_126,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval2EndDay)
												ELSE ''
											END AS PBP_B2_COPAY_AD_ENDD_INT_127
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCoins2 AS AdDa1a1Com_bdTierCoins2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCoins2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment ) main
	GROUP BY
		qid) segment_15 ON
	segment_0.qid = segment_15.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COPAY_AD_BGND_INT_128) AS PBP_B2_COPAY_AD_BGND_INT_128,
		MAX(PBP_B2_COPAY_AD_BGND_INT_129) AS PBP_B2_COPAY_AD_BGND_INT_129,
		MAX(PBP_B2_COPAY_AD_BGND_INT_130) AS PBP_B2_COPAY_AD_BGND_INT_130,
		MAX(PBP_B2_COINS_AD_ENDD_INT_131) AS PBP_B2_COINS_AD_ENDD_INT_131,
		MAX(PBP_B2_COINS_AD_ENDD_INT_132) AS PBP_B2_COINS_AD_ENDD_INT_132,
		MAX(PBP_B2_COINS_AD_ENDD_INT_133) AS PBP_B2_COINS_AD_ENDD_INT_133,
		MAX(PBP_B2_AD_COST_VARY_LOW__134) AS PBP_B2_AD_COST_VARY_LOW__134,
		MAX(PBP_B2_COPAY_AD_AMT_INT_135) AS PBP_B2_COPAY_AD_AMT_INT_135
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval2BeginDay)
					ELSE ''
				END AS PBP_B2_COPAY_AD_BGND_INT_128,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval2BeginDay)
						ELSE ''
					END AS PBP_B2_COPAY_AD_BGND_INT_129,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval2BeginDay)
							ELSE ''
						END AS PBP_B2_COPAY_AD_BGND_INT_130,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval1EndDay)
								ELSE ''
							END AS PBP_B2_COINS_AD_ENDD_INT_131,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval1EndDay)
									ELSE ''
								END AS PBP_B2_COINS_AD_ENDD_INT_132,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval1EndDay)
										ELSE ''
									END AS PBP_B2_COINS_AD_ENDD_INT_133,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdAddDayCosSha2.bdLowestCostTier)
											ELSE ''
										END AS PBP_B2_AD_COST_VARY_LOW__134,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval3CopaymentAmount)
												ELSE ''
											END AS PBP_B2_COPAY_AD_AMT_INT_135
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCoins2 AS AdDa1a1Com_bdTierCoins2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCoins2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdAddDayCosSha2 AS AdDa1a1Com_bdAddDayCosSha2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdAddDayCosSha2.ordinal_AdDa1a1Com ) main
	GROUP BY
		qid) segment_16 ON
	segment_0.qid = segment_16.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COPAY_AD_AMT_INT_136) AS PBP_B2_COPAY_AD_AMT_INT_136,
		MAX(PBP_B2_COPAY_AD_AMT_INT_137) AS PBP_B2_COPAY_AD_AMT_INT_137,
		MAX(PBP_B2_COPAY_NMCS_AMT_138) AS PBP_B2_COPAY_NMCS_AMT_138,
		MAX(PBP_B2_COINS_NMCS_ENDD_INT_139) AS PBP_B2_COINS_NMCS_ENDD_INT_139,
		MAX(PBP_B2_COINS_NMCS_ENDD_INT_140) AS PBP_B2_COINS_NMCS_ENDD_INT_140,
		MAX(PBP_B2_COINS_NMCS_ENDD_INT_141) AS PBP_B2_COINS_NMCS_ENDD_INT_141,
		MAX(PBP_B2_COINS_NMCS_BGND_INT_142) AS PBP_B2_COINS_NMCS_BGND_INT_142,
		MAX(PBP_B2_COINS_NMCS_BGND_INT_143) AS PBP_B2_COINS_NMCS_BGND_INT_143
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval3CopaymentAmount)
					ELSE ''
				END AS PBP_B2_COPAY_AD_AMT_INT_136,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval3CopaymentAmount)
						ELSE ''
					END AS PBP_B2_COPAY_AD_AMT_INT_137,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCopIntCom.bdCopaymentAmt)
							ELSE ''
						END AS PBP_B2_COPAY_NMCS_AMT_138,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval1EndDay)
								ELSE ''
							END AS PBP_B2_COINS_NMCS_ENDD_INT_139,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval3EndDay)
									ELSE ''
								END AS PBP_B2_COINS_NMCS_ENDD_INT_140,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval2EndDay)
										ELSE ''
									END AS PBP_B2_COINS_NMCS_ENDD_INT_141,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval3BeginDay)
											ELSE ''
										END AS PBP_B2_COINS_NMCS_BGND_INT_142,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval2BeginDay)
												ELSE ''
											END AS PBP_B2_COINS_NMCS_BGND_INT_143
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
										LEFT JOIN jsonlib.benefitDetails_AdDa1a1Com AS benefitDetails_AdDa1a1Com ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_AdDa1a1Com.ordinal_benefitDetails
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.benefitDetails_SinCopIntCom AS benefitDetails_SinCopIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCopIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_SinCoiIntCom AS benefitDetails_SinCoiIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCoiIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.SinCoiIntCom_bdCoiDayIntSta2 AS SinCoiIntCom_bdCoiDayIntSta2 ON
											benefitDetails_SinCoiIntCom.ordinal_SinCoiIntCom = SinCoiIntCom_bdCoiDayIntSta2.ordinal_SinCoiIntCom ) main
	GROUP BY
		qid) segment_17 ON
	segment_0.qid = segment_17.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COINS_NMCS_BGND_INT_144) AS PBP_B2_COINS_NMCS_BGND_INT_144,
		MAX(PBP_B2_COPAY_NMCS_AMT_INT_145) AS PBP_B2_COPAY_NMCS_AMT_INT_145,
		MAX(PBP_B2_COPAY_NMCS_AMT_INT_146) AS PBP_B2_COPAY_NMCS_AMT_INT_146,
		MAX(PBP_B2_COPAY_NMCS_AMT_INT_147) AS PBP_B2_COPAY_NMCS_AMT_INT_147,
		MAX(PBP_B2_COPAY_NMCS_BGND_INT_148) AS PBP_B2_COPAY_NMCS_BGND_INT_148,
		MAX(PBP_B2_COPAY_NMCS_BGND_INT_149) AS PBP_B2_COPAY_NMCS_BGND_INT_149,
		MAX(PBP_B2_COPAY_NMCS_BGND_INT_150) AS PBP_B2_COPAY_NMCS_BGND_INT_150,
		MAX(PBP_B2_COINS_NMCS_PCT_151) AS PBP_B2_COINS_NMCS_PCT_151
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval1BeginDay)
					ELSE ''
				END AS PBP_B2_COINS_NMCS_BGND_INT_144,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval1CopaymentAmount)
						ELSE ''
					END AS PBP_B2_COPAY_NMCS_AMT_INT_145,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval2CopaymentAmount)
							ELSE ''
						END AS PBP_B2_COPAY_NMCS_AMT_INT_146,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval3CopaymentAmount)
								ELSE ''
							END AS PBP_B2_COPAY_NMCS_AMT_INT_147,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval1BeginDay)
									ELSE ''
								END AS PBP_B2_COPAY_NMCS_BGND_INT_148,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval2BeginDay)
										ELSE ''
									END AS PBP_B2_COPAY_NMCS_BGND_INT_149,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval3BeginDay)
											ELSE ''
										END AS PBP_B2_COPAY_NMCS_BGND_INT_150,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCoiIntCom.bdCoinsAmt)
												ELSE ''
											END AS PBP_B2_COINS_NMCS_PCT_151
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
										LEFT JOIN jsonlib.benefitDetails_SinCoiIntCom AS benefitDetails_SinCoiIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCoiIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.SinCoiIntCom_bdCoiDayIntSta2 AS SinCoiIntCom_bdCoiDayIntSta2 ON
											benefitDetails_SinCoiIntCom.ordinal_SinCoiIntCom = SinCoiIntCom_bdCoiDayIntSta2.ordinal_SinCoiIntCom
										LEFT JOIN jsonlib.benefitDetails_SinCopIntCom AS benefitDetails_SinCopIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCopIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.SinCopIntCom_bdCopDayIntSta2 AS SinCopIntCom_bdCopDayIntSta2 ON
											benefitDetails_SinCopIntCom.ordinal_SinCopIntCom = SinCopIntCom_bdCopDayIntSta2.ordinal_SinCopIntCom ) main
	GROUP BY
		qid) segment_18 ON
	segment_0.qid = segment_18.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B2_COPAY_NMCS_ENDD_INT_152) AS PBP_B2_COPAY_NMCS_ENDD_INT_152,
		MAX(PBP_B2_COPAY_NMCS_ENDD_INT_153) AS PBP_B2_COPAY_NMCS_ENDD_INT_153,
		MAX(PBP_B2_COPAY_NMCS_ENDD_INT_154) AS PBP_B2_COPAY_NMCS_ENDD_INT_154,
		MAX(PBP_B2_COINS_NMCS_STRUC_YN_155) AS PBP_B2_COINS_NMCS_STRUC_YN_155,
		MAX(PBP_B2_COPAY_NMCS_STRUC_YN_156) AS PBP_B2_COPAY_NMCS_STRUC_YN_156,
		MAX(PBP_B2_COINS_NMCS_PCT_INT_157) AS PBP_B2_COINS_NMCS_PCT_INT_157,
		MAX(PBP_B2_COINS_NMCS_PCT_INT_158) AS PBP_B2_COINS_NMCS_PCT_INT_158,
		MAX(PBP_B2_COINS_NMCS_PCT_INT_159) AS PBP_B2_COINS_NMCS_PCT_INT_159
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval1EndDay)
					ELSE ''
				END AS PBP_B2_COPAY_NMCS_ENDD_INT_152,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval2EndDay)
						ELSE ''
					END AS PBP_B2_COPAY_NMCS_ENDD_INT_153,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval3EndDay)
							ELSE ''
						END AS PBP_B2_COPAY_NMCS_ENDD_INT_154,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCoiIntCom.bdCoinsurancePercentageYesNo)
								ELSE ''
							END AS PBP_B2_COINS_NMCS_STRUC_YN_155,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCopIntCom.bdCopaymentAmountYesNo)
									ELSE ''
								END AS PBP_B2_COPAY_NMCS_STRUC_YN_156,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInter3CoinsPerce)
										ELSE ''
									END AS PBP_B2_COINS_NMCS_PCT_INT_157,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInter2CoinsPerce)
											ELSE ''
										END AS PBP_B2_COINS_NMCS_PCT_INT_158,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('2-2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInter1CoinsPerce)
												ELSE ''
											END AS PBP_B2_COINS_NMCS_PCT_INT_159
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
										LEFT JOIN jsonlib.benefitDetails_SinCopIntCom AS benefitDetails_SinCopIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCopIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.SinCopIntCom_bdCopDayIntSta2 AS SinCopIntCom_bdCopDayIntSta2 ON
											benefitDetails_SinCopIntCom.ordinal_SinCopIntCom = SinCopIntCom_bdCopDayIntSta2.ordinal_SinCopIntCom
										LEFT JOIN jsonlib.benefitDetails_SinCoiIntCom AS benefitDetails_SinCoiIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCoiIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.SinCoiIntCom_bdCoiDayIntSta2 AS SinCoiIntCom_bdCoiDayIntSta2 ON
											benefitDetails_SinCoiIntCom.ordinal_SinCoiIntCom = SinCoiIntCom_bdCoiDayIntSta2.ordinal_SinCoiIntCom ) main
	GROUP BY
		qid) segment_19 ON
	segment_0.qid = segment_19.qid;
QUIT;
RUN;

%MEND;


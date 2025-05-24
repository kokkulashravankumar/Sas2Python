* PROGRAM NAME: JSON_IMPORT_PBPB1_2.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB1_2();
PROC SQL;
CREATE TABLE PBPB1_2_JSON AS
SELECT
	segment_0.qid,
	PBP_B1B_BENDESC_AMO_AD_0 AS PBP_B1B_BENDESC_AMO_AD,
	PBP_B1B_BENDESC_AMO_NMCS_1 AS PBP_B1B_BENDESC_AMO_NMCS,
	PBP_B1B_COINS_MCS_ENDD_INT2__2 AS PBP_B1B_COINS_MCS_ENDD_INT2_T1,
	PBP_B1B_COINS_MCS_ENDD_INT2__3 AS PBP_B1B_COINS_MCS_ENDD_INT2_T2,
	PBP_B1B_COINS_MCS_ENDD_INT2__4 AS PBP_B1B_COINS_MCS_ENDD_INT2_T3,
	PBP_B1B_DED_AMT_T1_5 AS PBP_B1B_DED_AMT_T1,
	PBP_B1B_DED_AMT_T3_6 AS PBP_B1B_DED_AMT_T3,
	PBP_B1B_DED_AMT_T2_7 AS PBP_B1B_DED_AMT_T2,
	PBP_B1B_COPAY_MCS_ENDD_INT1__8 AS PBP_B1B_COPAY_MCS_ENDD_INT1_T1,
	PBP_B1B_COPAY_MCS_ENDD_INT1__9 AS PBP_B1B_COPAY_MCS_ENDD_INT1_T2,
	PBP_B1B_COPAY_MCS_ENDD_INT1_10 AS PBP_B1B_COPAY_MCS_ENDD_INT1_T3,
	PBP_B1B_COINS_MCS_PCT_INT2_11 AS PBP_B1B_COINS_MCS_PCT_INT2_T1,
	PBP_B1B_COINS_MCS_PCT_INT2_12 AS PBP_B1B_COINS_MCS_PCT_INT2_T2,
	PBP_B1B_COINS_MCS_PCT_INT2_13 AS PBP_B1B_COINS_MCS_PCT_INT2_T3,
	PBP_B1B_COINS_MCS_BGND_INT1_14 AS PBP_B1B_COINS_MCS_BGND_INT1_T3,
	PBP_B1B_COINS_MCS_BGND_INT1_15 AS PBP_B1B_COINS_MCS_BGND_INT1_T1,
	PBP_B1B_COINS_MCS_BGND_INT1_16 AS PBP_B1B_COINS_MCS_BGND_INT1_T2,
	PBP_B1B_COST_VARY_TIER_NUM_17 AS PBP_B1B_COST_VARY_TIER_NUM,
	PBP_B1B_MC_COPAY_CSTSHR_YN_18 AS PBP_B1B_MC_COPAY_CSTSHR_YN_T2,
	PBP_B1B_MC_COPAY_CSTSHR_YN_19 AS PBP_B1B_MC_COPAY_CSTSHR_YN_T1,
	PBP_B1B_MC_COPAY_CSTSHR_YN_20 AS PBP_B1B_MC_COPAY_CSTSHR_YN_T3,
	PBP_B1B_COPAY_MCS_AMT_INT2_21 AS PBP_B1B_COPAY_MCS_AMT_INT2_T3,
	PBP_B1B_COPAY_MCS_AMT_INT2_22 AS PBP_B1B_COPAY_MCS_AMT_INT2_T2,
	PBP_B1B_COPAY_MCS_AMT_INT2_23 AS PBP_B1B_COPAY_MCS_AMT_INT2_T1,
	PBP_B1B_COST_VARY_LOW_TIER_24 AS PBP_B1B_COST_VARY_LOW_TIER,
	PBP_B1B_MAXENR_AMT_25 AS PBP_B1B_MAXENR_AMT,
	PBP_B1B_COPAY_MCS_AMT_T3_26 AS PBP_B1B_COPAY_MCS_AMT_T3,
	PBP_B1B_COPAY_MCS_AMT_T1_27 AS PBP_B1B_COPAY_MCS_AMT_T1,
	PBP_B1B_COPAY_MCS_AMT_T2_28 AS PBP_B1B_COPAY_MCS_AMT_T2,
	PBP_B1B_COPAY_MCS_BGND_INT2_29 AS PBP_B1B_COPAY_MCS_BGND_INT2_T1,
	PBP_B1B_COPAY_MCS_BGND_INT2_30 AS PBP_B1B_COPAY_MCS_BGND_INT2_T3,
	PBP_B1B_COPAY_MCS_BGND_INT2_31 AS PBP_B1B_COPAY_MCS_BGND_INT2_T2,
	PBP_B1B_COINS_MCS_ENDD_INT1_32 AS PBP_B1B_COINS_MCS_ENDD_INT1_T2,
	PBP_B1B_COINS_MCS_ENDD_INT1_33 AS PBP_B1B_COINS_MCS_ENDD_INT1_T1,
	PBP_B1B_COINS_MCS_ENDD_INT1_34 AS PBP_B1B_COINS_MCS_ENDD_INT1_T3,
	PBP_B1B_COPAY_MCS_ENDD_INT2_35 AS PBP_B1B_COPAY_MCS_ENDD_INT2_T3,
	PBP_B1B_COPAY_MCS_ENDD_INT2_36 AS PBP_B1B_COPAY_MCS_ENDD_INT2_T2,
	PBP_B1B_COPAY_MCS_ENDD_INT2_37 AS PBP_B1B_COPAY_MCS_ENDD_INT2_T1,
	PBP_B1B_COPAY_MCS_AMT_INT1_38 AS PBP_B1B_COPAY_MCS_AMT_INT1_T3,
	PBP_B1B_COPAY_MCS_AMT_INT1_39 AS PBP_B1B_COPAY_MCS_AMT_INT1_T2,
	PBP_B1B_COPAY_MCS_AMT_INT1_40 AS PBP_B1B_COPAY_MCS_AMT_INT1_T1,
	PBP_B1B_COINS_YN_41 AS PBP_B1B_COINS_YN,
	PBP_B1B_COINS_MCS_BGND_INT3_42 AS PBP_B1B_COINS_MCS_BGND_INT3_T3,
	PBP_B1B_COINS_MCS_BGND_INT3_43 AS PBP_B1B_COINS_MCS_BGND_INT3_T2,
	PBP_B1B_COINS_MCS_BGND_INT3_44 AS PBP_B1B_COINS_MCS_BGND_INT3_T1,
	PBP_B1B_MAXENR_TYPE_45 AS PBP_B1B_MAXENR_TYPE,
	PBP_B1B_COPAY_YN_46 AS PBP_B1B_COPAY_YN,
	PBP_B1B_COST_VARY_TIERS_YN_47 AS PBP_B1B_COST_VARY_TIERS_YN,
	PBP_B1B_COST_DISCHARGE_YN_48 AS PBP_B1B_COST_DISCHARGE_YN,
	PBP_B1B_COPAY_MCS_AMT_INT3_49 AS PBP_B1B_COPAY_MCS_AMT_INT3_T1,
	PBP_B1B_COPAY_MCS_AMT_INT3_50 AS PBP_B1B_COPAY_MCS_AMT_INT3_T2,
	PBP_B1B_COPAY_MCS_AMT_INT3_51 AS PBP_B1B_COPAY_MCS_AMT_INT3_T3,
	PBP_B1B_COINS_MCS_PCT_INT3_52 AS PBP_B1B_COINS_MCS_PCT_INT3_T1,
	PBP_B1B_COINS_MCS_PCT_INT3_53 AS PBP_B1B_COINS_MCS_PCT_INT3_T2,
	PBP_B1B_COINS_MCS_PCT_INT3_54 AS PBP_B1B_COINS_MCS_PCT_INT3_T3,
	PBP_B1B_COINS_MCS_ENDD_INT3_55 AS PBP_B1B_COINS_MCS_ENDD_INT3_T1,
	PBP_B1B_COINS_MCS_ENDD_INT3_56 AS PBP_B1B_COINS_MCS_ENDD_INT3_T2,
	PBP_B1B_COINS_MCS_ENDD_INT3_57 AS PBP_B1B_COINS_MCS_ENDD_INT3_T3,
	PBP_B1B_COPAY_MCS_BGND_INT1_58 AS PBP_B1B_COPAY_MCS_BGND_INT1_T3,
	PBP_B1B_COPAY_MCS_BGND_INT1_59 AS PBP_B1B_COPAY_MCS_BGND_INT1_T2,
	PBP_B1B_COPAY_MCS_BGND_INT1_60 AS PBP_B1B_COPAY_MCS_BGND_INT1_T1,
	PBP_B1B_HOSP_BEN_PERIOD_61 AS PBP_B1B_HOSP_BEN_PERIOD,
	PBP_B1B_COPAY_MCS_ENDD_INT3_62 AS PBP_B1B_COPAY_MCS_ENDD_INT3_T3,
	PBP_B1B_COPAY_MCS_ENDD_INT3_63 AS PBP_B1B_COPAY_MCS_ENDD_INT3_T2,
	PBP_B1B_COPAY_MCS_ENDD_INT3_64 AS PBP_B1B_COPAY_MCS_ENDD_INT3_T1,
	PBP_B1B_COINS_MCS_PCT_INT1_65 AS PBP_B1B_COINS_MCS_PCT_INT1_T3,
	PBP_B1B_COINS_MCS_PCT_INT1_66 AS PBP_B1B_COINS_MCS_PCT_INT1_T2,
	PBP_B1B_COINS_MCS_PCT_INT1_67 AS PBP_B1B_COINS_MCS_PCT_INT1_T1,
	PBP_B1B_MC_COINS_CSTSHR_YN_68 AS PBP_B1B_MC_COINS_CSTSHR_YN_T3,
	PBP_B1B_MC_COINS_CSTSHR_YN_69 AS PBP_B1B_MC_COINS_CSTSHR_YN_T1,
	PBP_B1B_MC_COINS_CSTSHR_YN_70 AS PBP_B1B_MC_COINS_CSTSHR_YN_T2,
	PBP_B1B_COINS_MCS_BGND_INT2_71 AS PBP_B1B_COINS_MCS_BGND_INT2_T2,
	PBP_B1B_COINS_MCS_BGND_INT2_72 AS PBP_B1B_COINS_MCS_BGND_INT2_T3,
	PBP_B1B_COINS_MCS_BGND_INT2_73 AS PBP_B1B_COINS_MCS_BGND_INT2_T1,
	PBP_B1B_COINS_MCS_PCT_T3_74 AS PBP_B1B_COINS_MCS_PCT_T3,
	PBP_B1B_COINS_MCS_PCT_T2_75 AS PBP_B1B_COINS_MCS_PCT_T2,
	PBP_B1B_COINS_MCS_PCT_T1_76 AS PBP_B1B_COINS_MCS_PCT_T1,
	PBP_B1B_MAXENR_PER_77 AS PBP_B1B_MAXENR_PER,
	PBP_B1B_COPAY_MCS_BGND_INT3_78 AS PBP_B1B_COPAY_MCS_BGND_INT3_T1,
	PBP_B1B_COPAY_MCS_BGND_INT3_79 AS PBP_B1B_COPAY_MCS_BGND_INT3_T2,
	PBP_B1B_COPAY_MCS_BGND_INT3_80 AS PBP_B1B_COPAY_MCS_BGND_INT3_T3,
	PBP_B1B_COINS_AD_ENDD_INT2_81 AS PBP_B1B_COINS_AD_ENDD_INT2_T1,
	PBP_B1B_COINS_AD_ENDD_INT2_82 AS PBP_B1B_COINS_AD_ENDD_INT2_T2,
	PBP_B1B_COINS_AD_ENDD_INT2_83 AS PBP_B1B_COINS_AD_ENDD_INT2_T3,
	PBP_B1B_BENDESC_AMT_AD_84 AS PBP_B1B_BENDESC_AMT_AD,
	PBP_B1B_COINS_AD_PCT_INT3_85 AS PBP_B1B_COINS_AD_PCT_INT3_T1,
	PBP_B1B_COINS_AD_PCT_INT3_86 AS PBP_B1B_COINS_AD_PCT_INT3_T2,
	PBP_B1B_COINS_AD_PCT_INT3_87 AS PBP_B1B_COINS_AD_PCT_INT3_T3,
	PBP_B1B_COINS_AD_BGND_INT1_88 AS PBP_B1B_COINS_AD_BGND_INT1_T3,
	PBP_B1B_COINS_AD_BGND_INT1_89 AS PBP_B1B_COINS_AD_BGND_INT1_T1,
	PBP_B1B_COINS_AD_BGND_INT1_90 AS PBP_B1B_COINS_AD_BGND_INT1_T2,
	PBP_B1B_COPAY_AD_BGND_INT1_91 AS PBP_B1B_COPAY_AD_BGND_INT1_T3,
	PBP_B1B_COPAY_AD_BGND_INT1_92 AS PBP_B1B_COPAY_AD_BGND_INT1_T1,
	PBP_B1B_COPAY_AD_BGND_INT1_93 AS PBP_B1B_COPAY_AD_BGND_INT1_T2,
	PBP_B1B_COPAY_AD_ENDD_INT1_94 AS PBP_B1B_COPAY_AD_ENDD_INT1_T2,
	PBP_B1B_COPAY_AD_ENDD_INT1_95 AS PBP_B1B_COPAY_AD_ENDD_INT1_T1,
	PBP_B1B_COPAY_AD_ENDD_INT1_96 AS PBP_B1B_COPAY_AD_ENDD_INT1_T3,
	PBP_B1B_COPAY_AD_AMT_INT2_97 AS PBP_B1B_COPAY_AD_AMT_INT2_T3,
	PBP_B1B_COPAY_AD_AMT_INT2_98 AS PBP_B1B_COPAY_AD_AMT_INT2_T2,
	PBP_B1B_COPAY_AD_AMT_INT2_99 AS PBP_B1B_COPAY_AD_AMT_INT2_T1,
	PBP_B1B_COINS_AD_BGND_INT_100 AS PBP_B1B_COINS_AD_BGND_INT2_T3,
	PBP_B1B_COINS_AD_BGND_INT_101 AS PBP_B1B_COINS_AD_BGND_INT2_T2,
	PBP_B1B_COINS_AD_BGND_INT_102 AS PBP_B1B_COINS_AD_BGND_INT2_T1,
	PBP_B1B_COINS_AD_PCT_INT_103 AS PBP_B1B_COINS_AD_PCT_INT1_T3,
	PBP_B1B_COINS_AD_PCT_INT_104 AS PBP_B1B_COINS_AD_PCT_INT1_T2,
	PBP_B1B_COINS_AD_PCT_INT_105 AS PBP_B1B_COINS_AD_PCT_INT1_T1,
	PBP_B1B_AD_COST_VARY_TIER_106 AS PBP_B1B_AD_COST_VARY_TIERS_YN,
	PBP_B1B_COPAY_AD_BGND_INT_107 AS PBP_B1B_COPAY_AD_BGND_INT3_T1,
	PBP_B1B_COPAY_AD_BGND_INT_108 AS PBP_B1B_COPAY_AD_BGND_INT3_T2,
	PBP_B1B_COPAY_AD_BGND_INT_109 AS PBP_B1B_COPAY_AD_BGND_INT3_T3,
	PBP_B1B_BENDESC_LIM_AD_110 AS PBP_B1B_BENDESC_LIM_AD,
	PBP_B1B_AD_COST_VARY_LOW__111 AS PBP_B1B_AD_COST_VARY_LOW_TIER,
	PBP_B1B_COPAY_AD_ENDD_INT_112 AS PBP_B1B_COPAY_AD_ENDD_INT3_T3,
	PBP_B1B_COPAY_AD_ENDD_INT_113 AS PBP_B1B_COPAY_AD_ENDD_INT3_T1,
	PBP_B1B_COPAY_AD_ENDD_INT_114 AS PBP_B1B_COPAY_AD_ENDD_INT3_T2,
	PBP_B1B_COINS_AD_ENDD_INT_115 AS PBP_B1B_COINS_AD_ENDD_INT3_T1,
	PBP_B1B_COINS_AD_ENDD_INT_116 AS PBP_B1B_COINS_AD_ENDD_INT3_T2,
	PBP_B1B_COINS_AD_ENDD_INT_117 AS PBP_B1B_COINS_AD_ENDD_INT3_T3,
	PBP_B1B_COPAY_AD_AMT_INT_118 AS PBP_B1B_COPAY_AD_AMT_INT3_T1,
	PBP_B1B_COPAY_AD_AMT_INT_119 AS PBP_B1B_COPAY_AD_AMT_INT3_T2,
	PBP_B1B_COPAY_AD_AMT_INT_120 AS PBP_B1B_COPAY_AD_AMT_INT3_T3,
	PBP_B1B_COINS_AD_PCT_INT_121 AS PBP_B1B_COINS_AD_PCT_INT2_T3,
	PBP_B1B_COINS_AD_PCT_INT_122 AS PBP_B1B_COINS_AD_PCT_INT2_T2,
	PBP_B1B_COINS_AD_PCT_INT_123 AS PBP_B1B_COINS_AD_PCT_INT2_T1,
	PBP_B1B_COPAY_AD_BGND_INT_124 AS PBP_B1B_COPAY_AD_BGND_INT2_T1,
	PBP_B1B_COPAY_AD_BGND_INT_125 AS PBP_B1B_COPAY_AD_BGND_INT2_T2,
	PBP_B1B_COPAY_AD_BGND_INT_126 AS PBP_B1B_COPAY_AD_BGND_INT2_T3,
	PBP_B1B_COINS_AD_ENDD_INT_127 AS PBP_B1B_COINS_AD_ENDD_INT1_T3,
	PBP_B1B_COINS_AD_ENDD_INT_128 AS PBP_B1B_COINS_AD_ENDD_INT1_T1,
	PBP_B1B_COINS_AD_ENDD_INT_129 AS PBP_B1B_COINS_AD_ENDD_INT1_T2,
	PBP_B1B_COINS_AD_BGND_INT_130 AS PBP_B1B_COINS_AD_BGND_INT3_T3,
	PBP_B1B_COINS_AD_BGND_INT_131 AS PBP_B1B_COINS_AD_BGND_INT3_T2,
	PBP_B1B_COINS_AD_BGND_INT_132 AS PBP_B1B_COINS_AD_BGND_INT3_T1,
	PBP_B1B_COPAY_AD_ENDD_INT_133 AS PBP_B1B_COPAY_AD_ENDD_INT2_T1,
	PBP_B1B_COPAY_AD_ENDD_INT_134 AS PBP_B1B_COPAY_AD_ENDD_INT2_T3,
	PBP_B1B_COPAY_AD_ENDD_INT_135 AS PBP_B1B_COPAY_AD_ENDD_INT2_T2,
	PBP_B1B_COPAY_AD_AMT_INT_136 AS PBP_B1B_COPAY_AD_AMT_INT1_T3,
	PBP_B1B_COPAY_AD_AMT_INT_137 AS PBP_B1B_COPAY_AD_AMT_INT1_T2,
	PBP_B1B_COPAY_AD_AMT_INT_138 AS PBP_B1B_COPAY_AD_AMT_INT1_T1,
	PBP_B1B_COINS_NMCS_STRU_139 AS PBP_B1B_COINS_NMCS_STRUC_YN,
	PBP_B1B_COPAY_NMCS_AMT_140 AS PBP_B1B_COPAY_NMCS_AMT,
	PBP_B1B_COINS_PCT_NMCS_141 AS PBP_B1B_COINS_PCT_NMCS,
	PBP_B1B_COPAY_NMCS_BGND_NT_142 AS PBP_B1B_COPAY_NMCS_BGND_NTRVL1,
	PBP_B1B_COPAY_NMCS_BGND_NT_143 AS PBP_B1B_COPAY_NMCS_BGND_NTRVL2,
	PBP_B1B_COPAY_NMCS_BGND_NT_144 AS PBP_B1B_COPAY_NMCS_BGND_NTRVL3,
	PBP_B1B_COPAY_NMCS_ENDD_NT_145 AS PBP_B1B_COPAY_NMCS_ENDD_NTRVL1,
	PBP_B1B_COPAY_NMCS_ENDD_NT_146 AS PBP_B1B_COPAY_NMCS_ENDD_NTRVL2,
	PBP_B1B_COPAY_NMCS_ENDD_NT_147 AS PBP_B1B_COPAY_NMCS_ENDD_NTRVL3,
	PBP_B1B_COPAY_NMCS_AMT_INT_148 AS PBP_B1B_COPAY_NMCS_AMT_INTRVL3,
	PBP_B1B_COPAY_NMCS_AMT_INT_149 AS PBP_B1B_COPAY_NMCS_AMT_INTRVL2,
	PBP_B1B_COPAY_NMCS_AMT_INT_150 AS PBP_B1B_COPAY_NMCS_AMT_INTRVL1,
	PBP_B1B_COPAY_NMCS_STRU_151 AS PBP_B1B_COPAY_NMCS_STRUC_YN,
	PBP_B1B_COINS_NMCS_BGND_NT_152 AS PBP_B1B_COINS_NMCS_BGND_NTRVL2,
	PBP_B1B_COINS_NMCS_BGND_NT_153 AS PBP_B1B_COINS_NMCS_BGND_NTRVL1,
	PBP_B1B_COINS_NMCS_BGND_NT_154 AS PBP_B1B_COINS_NMCS_BGND_NTRVL3,
	PBP_B1B_COINS_NMCS_PCT_INT_155 AS PBP_B1B_COINS_NMCS_PCT_INTRVL1,
	PBP_B1B_COINS_NMCS_PCT_INT_156 AS PBP_B1B_COINS_NMCS_PCT_INTRVL2,
	PBP_B1B_COINS_NMCS_PCT_INT_157 AS PBP_B1B_COINS_NMCS_PCT_INTRVL3,
	PBP_B1B_COINS_NMCS_ENDD_NT_158 AS PBP_B1B_COINS_NMCS_ENDD_NTRVL3,
	PBP_B1B_COINS_NMCS_ENDD_NT_159 AS PBP_B1B_COINS_NMCS_ENDD_NTRVL2,
	PBP_B1B_COINS_NMCS_ENDD_NT_160 AS PBP_B1B_COINS_NMCS_ENDD_NTRVL1
FROM
	(
	SELECT
		qid,
		MAX(PBP_B1B_BENDESC_AMO_AD_0) AS PBP_B1B_BENDESC_AMO_AD_0,
		MAX(PBP_B1B_BENDESC_AMO_NMCS_1) AS PBP_B1B_BENDESC_AMO_NMCS_1,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT2__2) AS PBP_B1B_COINS_MCS_ENDD_INT2__2,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT2__3) AS PBP_B1B_COINS_MCS_ENDD_INT2__3,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT2__4) AS PBP_B1B_COINS_MCS_ENDD_INT2__4,
		MAX(PBP_B1B_DED_AMT_T1_5) AS PBP_B1B_DED_AMT_T1_5,
		MAX(PBP_B1B_DED_AMT_T3_6) AS PBP_B1B_DED_AMT_T3_6,
		MAX(PBP_B1B_DED_AMT_T2_7) AS PBP_B1B_DED_AMT_T2_7,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT1__8) AS PBP_B1B_COPAY_MCS_ENDD_INT1__8
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('1b1') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B1B_BENDESC_AMO_AD_0,
			CASE
				WHEN nonMedicare_nonMedBenOffDet.categoryCode IN('1b2') THEN CATS(nonMedicare_nonMedBenOffDet.inNetworkOptionSelected)
				ELSE ''
			END AS PBP_B1B_BENDESC_AMO_NMCS_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval2EndDay)
				ELSE ''
			END AS PBP_B1B_COINS_MCS_ENDD_INT2__2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval2EndDay)
				ELSE ''
			END AS PBP_B1B_COINS_MCS_ENDD_INT2__3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval2EndDay)
				ELSE ''
			END AS PBP_B1B_COINS_MCS_ENDD_INT2__4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_PaABTiDeCom.bdPartAandBTier1DeductibleAmt)
				ELSE ''
			END AS PBP_B1B_DED_AMT_T1_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_PaABTiDeCom.bdPartAandBTier3DeductibleAmt)
				ELSE ''
			END AS PBP_B1B_DED_AMT_T3_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_PaABTiDeCom.bdPartAandBTier2DeductibleAmt)
				ELSE ''
			END AS PBP_B1B_DED_AMT_T2_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval1EndDay)
				ELSE ''
			END AS PBP_B1B_COPAY_MCS_ENDD_INT1__8
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
		LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
			benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
		LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
			benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
		LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
			benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
		LEFT JOIN jsonlib.benefitDetails_PaABTiDeCom AS benefitDetails_PaABTiDeCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_PaABTiDeCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
			benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom ) main
	GROUP BY
		qid) segment_0
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT1__9) AS PBP_B1B_COPAY_MCS_ENDD_INT1__9,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT1_10) AS PBP_B1B_COPAY_MCS_ENDD_INT1_10,
		MAX(PBP_B1B_COINS_MCS_PCT_INT2_11) AS PBP_B1B_COINS_MCS_PCT_INT2_11,
		MAX(PBP_B1B_COINS_MCS_PCT_INT2_12) AS PBP_B1B_COINS_MCS_PCT_INT2_12,
		MAX(PBP_B1B_COINS_MCS_PCT_INT2_13) AS PBP_B1B_COINS_MCS_PCT_INT2_13,
		MAX(PBP_B1B_COINS_MCS_BGND_INT1_14) AS PBP_B1B_COINS_MCS_BGND_INT1_14,
		MAX(PBP_B1B_COINS_MCS_BGND_INT1_15) AS PBP_B1B_COINS_MCS_BGND_INT1_15,
		MAX(PBP_B1B_COINS_MCS_BGND_INT1_16) AS PBP_B1B_COINS_MCS_BGND_INT1_16
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval1EndDay)
					ELSE ''
				END AS PBP_B1B_COPAY_MCS_ENDD_INT1__9,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval1EndDay)
						ELSE ''
					END AS PBP_B1B_COPAY_MCS_ENDD_INT1_10,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInter2CoinsPerce)
							ELSE ''
						END AS PBP_B1B_COINS_MCS_PCT_INT2_11,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInter2CoinsPerce)
								ELSE ''
							END AS PBP_B1B_COINS_MCS_PCT_INT2_12,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInter2CoinsPerce)
									ELSE ''
								END AS PBP_B1B_COINS_MCS_PCT_INT2_13,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval1BeginDay)
										ELSE ''
									END AS PBP_B1B_COINS_MCS_BGND_INT1_14,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval1BeginDay)
											ELSE ''
										END AS PBP_B1B_COINS_MCS_BGND_INT1_15,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval1BeginDay)
												ELSE ''
											END AS PBP_B1B_COINS_MCS_BGND_INT1_16
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
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_1 ON
	segment_0.qid = segment_1.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COST_VARY_TIER_NUM_17) AS PBP_B1B_COST_VARY_TIER_NUM_17,
		MAX(PBP_B1B_MC_COPAY_CSTSHR_YN_18) AS PBP_B1B_MC_COPAY_CSTSHR_YN_18,
		MAX(PBP_B1B_MC_COPAY_CSTSHR_YN_19) AS PBP_B1B_MC_COPAY_CSTSHR_YN_19,
		MAX(PBP_B1B_MC_COPAY_CSTSHR_YN_20) AS PBP_B1B_MC_COPAY_CSTSHR_YN_20,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT2_21) AS PBP_B1B_COPAY_MCS_AMT_INT2_21,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT2_22) AS PBP_B1B_COPAY_MCS_AMT_INT2_22,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT2_23) AS PBP_B1B_COPAY_MCS_AMT_INT2_23,
		MAX(PBP_B1B_COST_VARY_LOW_TIER_24) AS PBP_B1B_COST_VARY_LOW_TIER_24
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CosShaTieCom.bdNumberOfTiers)
					ELSE ''
				END AS PBP_B1B_COST_VARY_TIER_NUM_17,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier2YesNo)
						ELSE ''
					END AS PBP_B1B_MC_COPAY_CSTSHR_YN_18,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier1YesNo)
							ELSE ''
						END AS PBP_B1B_MC_COPAY_CSTSHR_YN_19,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier3YesNo)
								ELSE ''
							END AS PBP_B1B_MC_COPAY_CSTSHR_YN_20,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval2CopaymentAmount)
									ELSE ''
								END AS PBP_B1B_COPAY_MCS_AMT_INT2_21,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval2CopaymentAmount)
										ELSE ''
									END AS PBP_B1B_COPAY_MCS_AMT_INT2_22,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval2CopaymentAmount)
											ELSE ''
										END AS PBP_B1B_COPAY_MCS_AMT_INT2_23,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CosShaTieCom.bdLowestCostTier)
												ELSE ''
											END AS PBP_B1B_COST_VARY_LOW_TIER_24
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
										LEFT JOIN jsonlib.benefitDetails_CosShaTieCom AS benefitDetails_CosShaTieCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CosShaTieCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom ) main
	GROUP BY
		qid) segment_2 ON
	segment_0.qid = segment_2.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_MAXENR_AMT_25) AS PBP_B1B_MAXENR_AMT_25,
		MAX(PBP_B1B_COPAY_MCS_AMT_T3_26) AS PBP_B1B_COPAY_MCS_AMT_T3_26,
		MAX(PBP_B1B_COPAY_MCS_AMT_T1_27) AS PBP_B1B_COPAY_MCS_AMT_T1_27,
		MAX(PBP_B1B_COPAY_MCS_AMT_T2_28) AS PBP_B1B_COPAY_MCS_AMT_T2_28,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT2_29) AS PBP_B1B_COPAY_MCS_BGND_INT2_29,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT2_30) AS PBP_B1B_COPAY_MCS_BGND_INT2_30,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT2_31) AS PBP_B1B_COPAY_MCS_BGND_INT2_31,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT1_32) AS PBP_B1B_COINS_MCS_ENDD_INT1_32
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopAmt)
					ELSE ''
				END AS PBP_B1B_MAXENR_AMT_25,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier3Amt)
						ELSE ''
					END AS PBP_B1B_COPAY_MCS_AMT_T3_26,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier1Amt)
							ELSE ''
						END AS PBP_B1B_COPAY_MCS_AMT_T1_27,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentTier2Amt)
								ELSE ''
							END AS PBP_B1B_COPAY_MCS_AMT_T2_28,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval2BeginDay)
									ELSE ''
								END AS PBP_B1B_COPAY_MCS_BGND_INT2_29,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval2BeginDay)
										ELSE ''
									END AS PBP_B1B_COPAY_MCS_BGND_INT2_30,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval2BeginDay)
											ELSE ''
										END AS PBP_B1B_COPAY_MCS_BGND_INT2_31,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval1EndDay)
												ELSE ''
											END AS PBP_B1B_COINS_MCS_ENDD_INT1_32
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
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_3 ON
	segment_0.qid = segment_3.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT1_33) AS PBP_B1B_COINS_MCS_ENDD_INT1_33,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT1_34) AS PBP_B1B_COINS_MCS_ENDD_INT1_34,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT2_35) AS PBP_B1B_COPAY_MCS_ENDD_INT2_35,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT2_36) AS PBP_B1B_COPAY_MCS_ENDD_INT2_36,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT2_37) AS PBP_B1B_COPAY_MCS_ENDD_INT2_37,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT1_38) AS PBP_B1B_COPAY_MCS_AMT_INT1_38,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT1_39) AS PBP_B1B_COPAY_MCS_AMT_INT1_39,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT1_40) AS PBP_B1B_COPAY_MCS_AMT_INT1_40
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval1EndDay)
					ELSE ''
				END AS PBP_B1B_COINS_MCS_ENDD_INT1_33,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval1EndDay)
						ELSE ''
					END AS PBP_B1B_COINS_MCS_ENDD_INT1_34,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval2EndDay)
							ELSE ''
						END AS PBP_B1B_COPAY_MCS_ENDD_INT2_35,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval2EndDay)
								ELSE ''
							END AS PBP_B1B_COPAY_MCS_ENDD_INT2_36,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval2EndDay)
									ELSE ''
								END AS PBP_B1B_COPAY_MCS_ENDD_INT2_37,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval1CopaymentAmount)
										ELSE ''
									END AS PBP_B1B_COPAY_MCS_AMT_INT1_38,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval1CopaymentAmount)
											ELSE ''
										END AS PBP_B1B_COPAY_MCS_AMT_INT1_39,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval1CopaymentAmount)
												ELSE ''
											END AS PBP_B1B_COPAY_MCS_AMT_INT1_40
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
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom ) main
	GROUP BY
		qid) segment_4 ON
	segment_0.qid = segment_4.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_YN_41) AS PBP_B1B_COINS_YN_41,
		MAX(PBP_B1B_COINS_MCS_BGND_INT3_42) AS PBP_B1B_COINS_MCS_BGND_INT3_42,
		MAX(PBP_B1B_COINS_MCS_BGND_INT3_43) AS PBP_B1B_COINS_MCS_BGND_INT3_43,
		MAX(PBP_B1B_COINS_MCS_BGND_INT3_44) AS PBP_B1B_COINS_MCS_BGND_INT3_44,
		MAX(PBP_B1B_MAXENR_TYPE_45) AS PBP_B1B_MAXENR_TYPE_45,
		MAX(PBP_B1B_COPAY_YN_46) AS PBP_B1B_COPAY_YN_46,
		MAX(PBP_B1B_COST_VARY_TIERS_YN_47) AS PBP_B1B_COST_VARY_TIERS_YN_47,
		MAX(PBP_B1B_COST_DISCHARGE_YN_48) AS PBP_B1B_COST_DISCHARGE_YN_48
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsurancePercentageYesNo)
					ELSE ''
				END AS PBP_B1B_COINS_YN_41,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval3BeginDay)
						ELSE ''
					END AS PBP_B1B_COINS_MCS_BGND_INT3_42,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval3BeginDay)
							ELSE ''
						END AS PBP_B1B_COINS_MCS_BGND_INT3_43,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval3BeginDay)
								ELSE ''
							END AS PBP_B1B_COINS_MCS_BGND_INT3_44,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeRadioGroup)
									ELSE ''
								END AS PBP_B1B_MAXENR_TYPE_45,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCopCom.bdCopaymentAmountYesNo)
										ELSE ''
									END AS PBP_B1B_COPAY_YN_46,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CosShaTieCom.bdCostSharingYesNo)
											ELSE ''
										END AS PBP_B1B_COST_VARY_TIERS_YN_47,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_InHoAcBePeCom.bdMedCostSharingChargeYesNo)
												ELSE ''
											END AS PBP_B1B_COST_DISCHARGE_YN_48
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
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.benefitDetails_MooCosTypCom AS benefitDetails_MooCosTypCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooCosTypCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_CosShaTieCom AS benefitDetails_CosShaTieCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CosShaTieCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_InHoAcBePeCom AS benefitDetails_InHoAcBePeCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_InHoAcBePeCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_5 ON
	segment_0.qid = segment_5.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT3_49) AS PBP_B1B_COPAY_MCS_AMT_INT3_49,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT3_50) AS PBP_B1B_COPAY_MCS_AMT_INT3_50,
		MAX(PBP_B1B_COPAY_MCS_AMT_INT3_51) AS PBP_B1B_COPAY_MCS_AMT_INT3_51,
		MAX(PBP_B1B_COINS_MCS_PCT_INT3_52) AS PBP_B1B_COINS_MCS_PCT_INT3_52,
		MAX(PBP_B1B_COINS_MCS_PCT_INT3_53) AS PBP_B1B_COINS_MCS_PCT_INT3_53,
		MAX(PBP_B1B_COINS_MCS_PCT_INT3_54) AS PBP_B1B_COINS_MCS_PCT_INT3_54,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT3_55) AS PBP_B1B_COINS_MCS_ENDD_INT3_55,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT3_56) AS PBP_B1B_COINS_MCS_ENDD_INT3_56
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval3CopaymentAmount)
					ELSE ''
				END AS PBP_B1B_COPAY_MCS_AMT_INT3_49,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval3CopaymentAmount)
						ELSE ''
					END AS PBP_B1B_COPAY_MCS_AMT_INT3_50,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval3CopaymentAmount)
							ELSE ''
						END AS PBP_B1B_COPAY_MCS_AMT_INT3_51,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInter3CoinsPerce)
								ELSE ''
							END AS PBP_B1B_COINS_MCS_PCT_INT3_52,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInter3CoinsPerce)
									ELSE ''
								END AS PBP_B1B_COINS_MCS_PCT_INT3_53,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInter3CoinsPerce)
										ELSE ''
									END AS PBP_B1B_COINS_MCS_PCT_INT3_54,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval3EndDay)
											ELSE ''
										END AS PBP_B1B_COINS_MCS_ENDD_INT3_55,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval3EndDay)
												ELSE ''
											END AS PBP_B1B_COINS_MCS_ENDD_INT3_56
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
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_TieCoiCom AS benefitDetails_TieCoiCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCoiCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_6 ON
	segment_0.qid = segment_6.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_MCS_ENDD_INT3_57) AS PBP_B1B_COINS_MCS_ENDD_INT3_57,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT1_58) AS PBP_B1B_COPAY_MCS_BGND_INT1_58,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT1_59) AS PBP_B1B_COPAY_MCS_BGND_INT1_59,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT1_60) AS PBP_B1B_COPAY_MCS_BGND_INT1_60,
		MAX(PBP_B1B_HOSP_BEN_PERIOD_61) AS PBP_B1B_HOSP_BEN_PERIOD_61,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT3_62) AS PBP_B1B_COPAY_MCS_ENDD_INT3_62,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT3_63) AS PBP_B1B_COPAY_MCS_ENDD_INT3_63,
		MAX(PBP_B1B_COPAY_MCS_ENDD_INT3_64) AS PBP_B1B_COPAY_MCS_ENDD_INT3_64
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval3EndDay)
					ELSE ''
				END AS PBP_B1B_COINS_MCS_ENDD_INT3_57,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval1BeginDay)
						ELSE ''
					END AS PBP_B1B_COPAY_MCS_BGND_INT1_58,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval1BeginDay)
							ELSE ''
						END AS PBP_B1B_COPAY_MCS_BGND_INT1_59,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval1BeginDay)
								ELSE ''
							END AS PBP_B1B_COPAY_MCS_BGND_INT1_60,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_InHoAcBePeCom.bdMedInpaHospAcutBenePeri)
									ELSE ''
								END AS PBP_B1B_HOSP_BEN_PERIOD_61,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval3EndDay)
										ELSE ''
									END AS PBP_B1B_COPAY_MCS_ENDD_INT3_62,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval3EndDay)
											ELSE ''
										END AS PBP_B1B_COPAY_MCS_ENDD_INT3_63,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval3EndDay)
												ELSE ''
											END AS PBP_B1B_COPAY_MCS_ENDD_INT3_64
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
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.benefitDetails_InHoAcBePeCom AS benefitDetails_InHoAcBePeCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_InHoAcBePeCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_7 ON
	segment_0.qid = segment_7.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_MCS_PCT_INT1_65) AS PBP_B1B_COINS_MCS_PCT_INT1_65,
		MAX(PBP_B1B_COINS_MCS_PCT_INT1_66) AS PBP_B1B_COINS_MCS_PCT_INT1_66,
		MAX(PBP_B1B_COINS_MCS_PCT_INT1_67) AS PBP_B1B_COINS_MCS_PCT_INT1_67,
		MAX(PBP_B1B_MC_COINS_CSTSHR_YN_68) AS PBP_B1B_MC_COINS_CSTSHR_YN_68,
		MAX(PBP_B1B_MC_COINS_CSTSHR_YN_69) AS PBP_B1B_MC_COINS_CSTSHR_YN_69,
		MAX(PBP_B1B_MC_COINS_CSTSHR_YN_70) AS PBP_B1B_MC_COINS_CSTSHR_YN_70,
		MAX(PBP_B1B_COINS_MCS_BGND_INT2_71) AS PBP_B1B_COINS_MCS_BGND_INT2_71,
		MAX(PBP_B1B_COINS_MCS_BGND_INT2_72) AS PBP_B1B_COINS_MCS_BGND_INT2_72
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInter1CoinsPerce)
					ELSE ''
				END AS PBP_B1B_COINS_MCS_PCT_INT1_65,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInter1CoinsPerce)
						ELSE ''
					END AS PBP_B1B_COINS_MCS_PCT_INT1_66,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInter1CoinsPerce)
							ELSE ''
						END AS PBP_B1B_COINS_MCS_PCT_INT1_67,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier3YesNo)
								ELSE ''
							END AS PBP_B1B_MC_COINS_CSTSHR_YN_68,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier1YesNo)
									ELSE ''
								END AS PBP_B1B_MC_COINS_CSTSHR_YN_69,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier2YesNo)
										ELSE ''
									END AS PBP_B1B_MC_COINS_CSTSHR_YN_70,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi2DaInSt.bdDayInterval2BeginDay)
											ELSE ''
										END AS PBP_B1B_COINS_MCS_BGND_INT2_71,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi3DaInSt.bdDayInterval2BeginDay)
												ELSE ''
											END AS PBP_B1B_COINS_MCS_BGND_INT2_72
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
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi3DaInSt AS TieCoiCom_bdCoiTi3DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi3DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi2DaInSt AS TieCoiCom_bdCoiTi2DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi2DaInSt.ordinal_TieCoiCom
										LEFT JOIN jsonlib.TieCoiCom_bdCoiTi1DaInSt AS TieCoiCom_bdCoiTi1DaInSt ON
											benefitDetails_TieCoiCom.ordinal_TieCoiCom = TieCoiCom_bdCoiTi1DaInSt.ordinal_TieCoiCom ) main
	GROUP BY
		qid) segment_8 ON
	segment_0.qid = segment_8.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_MCS_BGND_INT2_73) AS PBP_B1B_COINS_MCS_BGND_INT2_73,
		MAX(PBP_B1B_COINS_MCS_PCT_T3_74) AS PBP_B1B_COINS_MCS_PCT_T3_74,
		MAX(PBP_B1B_COINS_MCS_PCT_T2_75) AS PBP_B1B_COINS_MCS_PCT_T2_75,
		MAX(PBP_B1B_COINS_MCS_PCT_T1_76) AS PBP_B1B_COINS_MCS_PCT_T1_76,
		MAX(PBP_B1B_MAXENR_PER_77) AS PBP_B1B_MAXENR_PER_77,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT3_78) AS PBP_B1B_COPAY_MCS_BGND_INT3_78,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT3_79) AS PBP_B1B_COPAY_MCS_BGND_INT3_79,
		MAX(PBP_B1B_COPAY_MCS_BGND_INT3_80) AS PBP_B1B_COPAY_MCS_BGND_INT3_80
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCoiCom_bdCoiTi1DaInSt.bdDayInterval2BeginDay)
					ELSE ''
				END AS PBP_B1B_COINS_MCS_BGND_INT2_73,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier3Percentage)
						ELSE ''
					END AS PBP_B1B_COINS_MCS_PCT_T3_74,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier2Percentage)
							ELSE ''
						END AS PBP_B1B_COINS_MCS_PCT_T2_75,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_TieCoiCom.bdCoinsuranceTier1Percentage)
								ELSE ''
							END AS PBP_B1B_COINS_MCS_PCT_T1_76,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooCosTypCom.moopCostTypeMoopPeriodicity)
									ELSE ''
								END AS PBP_B1B_MAXENR_PER_77,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi1DaInSt.bdDayInterval3BeginDay)
										ELSE ''
									END AS PBP_B1B_COPAY_MCS_BGND_INT3_78,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi2DaInSt.bdDayInterval3BeginDay)
											ELSE ''
										END AS PBP_B1B_COPAY_MCS_BGND_INT3_79,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(TieCopCom_bdCopTi3DaInSt.bdDayInterval3BeginDay)
												ELSE ''
											END AS PBP_B1B_COPAY_MCS_BGND_INT3_80
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
										LEFT JOIN jsonlib.benefitDetails_MooCosTypCom AS benefitDetails_MooCosTypCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooCosTypCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_TieCopCom AS benefitDetails_TieCopCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_TieCopCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.TieCopCom_bdCopTi1DaInSt AS TieCopCom_bdCopTi1DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi1DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi2DaInSt AS TieCopCom_bdCopTi2DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi2DaInSt.ordinal_TieCopCom
										LEFT JOIN jsonlib.TieCopCom_bdCopTi3DaInSt AS TieCopCom_bdCopTi3DaInSt ON
											benefitDetails_TieCopCom.ordinal_TieCopCom = TieCopCom_bdCopTi3DaInSt.ordinal_TieCopCom ) main
	GROUP BY
		qid) segment_9 ON
	segment_0.qid = segment_9.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_AD_ENDD_INT2_81) AS PBP_B1B_COINS_AD_ENDD_INT2_81,
		MAX(PBP_B1B_COINS_AD_ENDD_INT2_82) AS PBP_B1B_COINS_AD_ENDD_INT2_82,
		MAX(PBP_B1B_COINS_AD_ENDD_INT2_83) AS PBP_B1B_COINS_AD_ENDD_INT2_83,
		MAX(PBP_B1B_BENDESC_AMT_AD_84) AS PBP_B1B_BENDESC_AMT_AD_84,
		MAX(PBP_B1B_COINS_AD_PCT_INT3_85) AS PBP_B1B_COINS_AD_PCT_INT3_85,
		MAX(PBP_B1B_COINS_AD_PCT_INT3_86) AS PBP_B1B_COINS_AD_PCT_INT3_86,
		MAX(PBP_B1B_COINS_AD_PCT_INT3_87) AS PBP_B1B_COINS_AD_PCT_INT3_87,
		MAX(PBP_B1B_COINS_AD_BGND_INT1_88) AS PBP_B1B_COINS_AD_BGND_INT1_88
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval2EndDay)
					ELSE ''
				END AS PBP_B1B_COINS_AD_ENDD_INT2_81,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval2EndDay)
						ELSE ''
					END AS PBP_B1B_COINS_AD_ENDD_INT2_82,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval2EndDay)
							ELSE ''
						END AS PBP_B1B_COINS_AD_ENDD_INT2_83,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdUnlAddDay2.bdIndicateNumOfAddDays)
								ELSE ''
							END AS PBP_B1B_BENDESC_AMT_AD_84,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInter3CoinsPerce)
									ELSE ''
								END AS PBP_B1B_COINS_AD_PCT_INT3_85,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInter3CoinsPerce)
										ELSE ''
									END AS PBP_B1B_COINS_AD_PCT_INT3_86,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInter3CoinsPerce)
											ELSE ''
										END AS PBP_B1B_COINS_AD_PCT_INT3_87,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval1BeginDay)
												ELSE ''
											END AS PBP_B1B_COINS_AD_BGND_INT1_88
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
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdUnlAddDay2 AS AdDa1a1Com_bdUnlAddDay2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdUnlAddDay2.ordinal_AdDa1a1Com ) main
	GROUP BY
		qid) segment_10 ON
	segment_0.qid = segment_10.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_AD_BGND_INT1_89) AS PBP_B1B_COINS_AD_BGND_INT1_89,
		MAX(PBP_B1B_COINS_AD_BGND_INT1_90) AS PBP_B1B_COINS_AD_BGND_INT1_90,
		MAX(PBP_B1B_COPAY_AD_BGND_INT1_91) AS PBP_B1B_COPAY_AD_BGND_INT1_91,
		MAX(PBP_B1B_COPAY_AD_BGND_INT1_92) AS PBP_B1B_COPAY_AD_BGND_INT1_92,
		MAX(PBP_B1B_COPAY_AD_BGND_INT1_93) AS PBP_B1B_COPAY_AD_BGND_INT1_93,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT1_94) AS PBP_B1B_COPAY_AD_ENDD_INT1_94,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT1_95) AS PBP_B1B_COPAY_AD_ENDD_INT1_95,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT1_96) AS PBP_B1B_COPAY_AD_ENDD_INT1_96
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval1BeginDay)
					ELSE ''
				END AS PBP_B1B_COINS_AD_BGND_INT1_89,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval1BeginDay)
						ELSE ''
					END AS PBP_B1B_COINS_AD_BGND_INT1_90,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval1BeginDay)
							ELSE ''
						END AS PBP_B1B_COPAY_AD_BGND_INT1_91,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval1BeginDay)
								ELSE ''
							END AS PBP_B1B_COPAY_AD_BGND_INT1_92,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval1BeginDay)
									ELSE ''
								END AS PBP_B1B_COPAY_AD_BGND_INT1_93,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval1EndDay)
										ELSE ''
									END AS PBP_B1B_COPAY_AD_ENDD_INT1_94,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval1EndDay)
											ELSE ''
										END AS PBP_B1B_COPAY_AD_ENDD_INT1_95,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval1EndDay)
												ELSE ''
											END AS PBP_B1B_COPAY_AD_ENDD_INT1_96
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
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment ) main
	GROUP BY
		qid) segment_11 ON
	segment_0.qid = segment_11.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COPAY_AD_AMT_INT2_97) AS PBP_B1B_COPAY_AD_AMT_INT2_97,
		MAX(PBP_B1B_COPAY_AD_AMT_INT2_98) AS PBP_B1B_COPAY_AD_AMT_INT2_98,
		MAX(PBP_B1B_COPAY_AD_AMT_INT2_99) AS PBP_B1B_COPAY_AD_AMT_INT2_99,
		MAX(PBP_B1B_COINS_AD_BGND_INT_100) AS PBP_B1B_COINS_AD_BGND_INT_100,
		MAX(PBP_B1B_COINS_AD_BGND_INT_101) AS PBP_B1B_COINS_AD_BGND_INT_101,
		MAX(PBP_B1B_COINS_AD_BGND_INT_102) AS PBP_B1B_COINS_AD_BGND_INT_102,
		MAX(PBP_B1B_COINS_AD_PCT_INT_103) AS PBP_B1B_COINS_AD_PCT_INT_103,
		MAX(PBP_B1B_COINS_AD_PCT_INT_104) AS PBP_B1B_COINS_AD_PCT_INT_104
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval2CopaymentAmount)
					ELSE ''
				END AS PBP_B1B_COPAY_AD_AMT_INT2_97,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval2CopaymentAmount)
						ELSE ''
					END AS PBP_B1B_COPAY_AD_AMT_INT2_98,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval2CopaymentAmount)
							ELSE ''
						END AS PBP_B1B_COPAY_AD_AMT_INT2_99,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval2BeginDay)
								ELSE ''
							END AS PBP_B1B_COINS_AD_BGND_INT_100,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval2BeginDay)
									ELSE ''
								END AS PBP_B1B_COINS_AD_BGND_INT_101,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval2BeginDay)
										ELSE ''
									END AS PBP_B1B_COINS_AD_BGND_INT_102,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInter1CoinsPerce)
											ELSE ''
										END AS PBP_B1B_COINS_AD_PCT_INT_103,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInter1CoinsPerce)
												ELSE ''
											END AS PBP_B1B_COINS_AD_PCT_INT_104
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
		MAX(PBP_B1B_COINS_AD_PCT_INT_105) AS PBP_B1B_COINS_AD_PCT_INT_105,
		MAX(PBP_B1B_AD_COST_VARY_TIER_106) AS PBP_B1B_AD_COST_VARY_TIER_106,
		MAX(PBP_B1B_COPAY_AD_BGND_INT_107) AS PBP_B1B_COPAY_AD_BGND_INT_107,
		MAX(PBP_B1B_COPAY_AD_BGND_INT_108) AS PBP_B1B_COPAY_AD_BGND_INT_108,
		MAX(PBP_B1B_COPAY_AD_BGND_INT_109) AS PBP_B1B_COPAY_AD_BGND_INT_109,
		MAX(PBP_B1B_BENDESC_LIM_AD_110) AS PBP_B1B_BENDESC_LIM_AD_110,
		MAX(PBP_B1B_AD_COST_VARY_LOW__111) AS PBP_B1B_AD_COST_VARY_LOW__111,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT_112) AS PBP_B1B_COPAY_AD_ENDD_INT_112
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInter1CoinsPerce)
					ELSE ''
				END AS PBP_B1B_COINS_AD_PCT_INT_105,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdAddDayCosSha2.bdCostSharingYesNo)
						ELSE ''
					END AS PBP_B1B_AD_COST_VARY_TIER_106,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval3BeginDay)
							ELSE ''
						END AS PBP_B1B_COPAY_AD_BGND_INT_107,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval3BeginDay)
								ELSE ''
							END AS PBP_B1B_COPAY_AD_BGND_INT_108,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval3BeginDay)
									ELSE ''
								END AS PBP_B1B_COPAY_AD_BGND_INT_109,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdUnlAddDay2.bdBenefitUnlimitedYesNo)
										ELSE ''
									END AS PBP_B1B_BENDESC_LIM_AD_110,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(AdDa1a1Com_bdAddDayCosSha2.bdLowestCostTier)
											ELSE ''
										END AS PBP_B1B_AD_COST_VARY_LOW__111,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval3EndDay)
												ELSE ''
											END AS PBP_B1B_COPAY_AD_ENDD_INT_112
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
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdAddDayCosSha2 AS AdDa1a1Com_bdAddDayCosSha2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdAddDayCosSha2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.AdDa1a1Com_bdUnlAddDay2 AS AdDa1a1Com_bdUnlAddDay2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdUnlAddDay2.ordinal_AdDa1a1Com ) main
	GROUP BY
		qid) segment_13 ON
	segment_0.qid = segment_13.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT_113) AS PBP_B1B_COPAY_AD_ENDD_INT_113,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT_114) AS PBP_B1B_COPAY_AD_ENDD_INT_114,
		MAX(PBP_B1B_COINS_AD_ENDD_INT_115) AS PBP_B1B_COINS_AD_ENDD_INT_115,
		MAX(PBP_B1B_COINS_AD_ENDD_INT_116) AS PBP_B1B_COINS_AD_ENDD_INT_116,
		MAX(PBP_B1B_COINS_AD_ENDD_INT_117) AS PBP_B1B_COINS_AD_ENDD_INT_117,
		MAX(PBP_B1B_COPAY_AD_AMT_INT_118) AS PBP_B1B_COPAY_AD_AMT_INT_118,
		MAX(PBP_B1B_COPAY_AD_AMT_INT_119) AS PBP_B1B_COPAY_AD_AMT_INT_119,
		MAX(PBP_B1B_COPAY_AD_AMT_INT_120) AS PBP_B1B_COPAY_AD_AMT_INT_120
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval3EndDay)
					ELSE ''
				END AS PBP_B1B_COPAY_AD_ENDD_INT_113,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval3EndDay)
						ELSE ''
					END AS PBP_B1B_COPAY_AD_ENDD_INT_114,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval3EndDay)
							ELSE ''
						END AS PBP_B1B_COINS_AD_ENDD_INT_115,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval3EndDay)
								ELSE ''
							END AS PBP_B1B_COINS_AD_ENDD_INT_116,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval3EndDay)
									ELSE ''
								END AS PBP_B1B_COINS_AD_ENDD_INT_117,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval3CopaymentAmount)
										ELSE ''
									END AS PBP_B1B_COPAY_AD_AMT_INT_118,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval3CopaymentAmount)
											ELSE ''
										END AS PBP_B1B_COPAY_AD_AMT_INT_119,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval3CopaymentAmount)
												ELSE ''
											END AS PBP_B1B_COPAY_AD_AMT_INT_120
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
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment ) main
	GROUP BY
		qid) segment_14 ON
	segment_0.qid = segment_14.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_AD_PCT_INT_121) AS PBP_B1B_COINS_AD_PCT_INT_121,
		MAX(PBP_B1B_COINS_AD_PCT_INT_122) AS PBP_B1B_COINS_AD_PCT_INT_122,
		MAX(PBP_B1B_COINS_AD_PCT_INT_123) AS PBP_B1B_COINS_AD_PCT_INT_123,
		MAX(PBP_B1B_COPAY_AD_BGND_INT_124) AS PBP_B1B_COPAY_AD_BGND_INT_124,
		MAX(PBP_B1B_COPAY_AD_BGND_INT_125) AS PBP_B1B_COPAY_AD_BGND_INT_125,
		MAX(PBP_B1B_COPAY_AD_BGND_INT_126) AS PBP_B1B_COPAY_AD_BGND_INT_126,
		MAX(PBP_B1B_COINS_AD_ENDD_INT_127) AS PBP_B1B_COINS_AD_ENDD_INT_127,
		MAX(PBP_B1B_COINS_AD_ENDD_INT_128) AS PBP_B1B_COINS_AD_ENDD_INT_128
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInter2CoinsPerce)
					ELSE ''
				END AS PBP_B1B_COINS_AD_PCT_INT_121,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInter2CoinsPerce)
						ELSE ''
					END AS PBP_B1B_COINS_AD_PCT_INT_122,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInter2CoinsPerce)
							ELSE ''
						END AS PBP_B1B_COINS_AD_PCT_INT_123,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval2BeginDay)
								ELSE ''
							END AS PBP_B1B_COPAY_AD_BGND_INT_124,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval2BeginDay)
									ELSE ''
								END AS PBP_B1B_COPAY_AD_BGND_INT_125,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval2BeginDay)
										ELSE ''
									END AS PBP_B1B_COPAY_AD_BGND_INT_126,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval1EndDay)
											ELSE ''
										END AS PBP_B1B_COINS_AD_ENDD_INT_127,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval1EndDay)
												ELSE ''
											END AS PBP_B1B_COINS_AD_ENDD_INT_128
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
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi3DaInSt2 AS bdTierCoins_bdCoiTi3DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi3DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi2DaInSt2 AS bdTierCoins_bdCoiTi2DaInSt2 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi2DaInSt2.ordinal_bdTierCoins
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
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
		MAX(PBP_B1B_COINS_AD_ENDD_INT_129) AS PBP_B1B_COINS_AD_ENDD_INT_129,
		MAX(PBP_B1B_COINS_AD_BGND_INT_130) AS PBP_B1B_COINS_AD_BGND_INT_130,
		MAX(PBP_B1B_COINS_AD_BGND_INT_131) AS PBP_B1B_COINS_AD_BGND_INT_131,
		MAX(PBP_B1B_COINS_AD_BGND_INT_132) AS PBP_B1B_COINS_AD_BGND_INT_132,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT_133) AS PBP_B1B_COPAY_AD_ENDD_INT_133,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT_134) AS PBP_B1B_COPAY_AD_ENDD_INT_134,
		MAX(PBP_B1B_COPAY_AD_ENDD_INT_135) AS PBP_B1B_COPAY_AD_ENDD_INT_135,
		MAX(PBP_B1B_COPAY_AD_AMT_INT_136) AS PBP_B1B_COPAY_AD_AMT_INT_136
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval1EndDay)
					ELSE ''
				END AS PBP_B1B_COINS_AD_ENDD_INT_129,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi3DaInSt2.bdDayInterval3BeginDay)
						ELSE ''
					END AS PBP_B1B_COINS_AD_BGND_INT_130,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi2DaInSt2.bdDayInterval3BeginDay)
							ELSE ''
						END AS PBP_B1B_COINS_AD_BGND_INT_131,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCoins_bdCoiTi1DaInSt5.bdDayInterval3BeginDay)
								ELSE ''
							END AS PBP_B1B_COINS_AD_BGND_INT_132,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval2EndDay)
									ELSE ''
								END AS PBP_B1B_COPAY_AD_ENDD_INT_133,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval2EndDay)
										ELSE ''
									END AS PBP_B1B_COPAY_AD_ENDD_INT_134,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval2EndDay)
											ELSE ''
										END AS PBP_B1B_COPAY_AD_ENDD_INT_135,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi3DaInSt2.bdDayInterval1CopaymentAmount)
												ELSE ''
											END AS PBP_B1B_COPAY_AD_AMT_INT_136
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
										LEFT JOIN jsonlib.bdTierCoins_bdCoiTi1DaInSt5 AS bdTierCoins_bdCoiTi1DaInSt5 ON
											AdDa1a1Com_bdTierCoins2.ordinal_bdTierCoins = bdTierCoins_bdCoiTi1DaInSt5.ordinal_bdTierCoins
										LEFT JOIN jsonlib.AdDa1a1Com_bdTierCopayment2 AS AdDa1a1Com_bdTierCopayment2 ON
											benefitDetails_AdDa1a1Com.ordinal_AdDa1a1Com = AdDa1a1Com_bdTierCopayment2.ordinal_AdDa1a1Com
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi1DaInSt5 AS bdTierCopayment_bdCopTi1DaInSt5 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi1DaInSt5.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi3DaInSt2 AS bdTierCopayment_bdCopTi3DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi3DaInSt2.ordinal_bdTierCopayment
										LEFT JOIN jsonlib.bdTierCopayment_bdCopTi2DaInSt2 AS bdTierCopayment_bdCopTi2DaInSt2 ON
											AdDa1a1Com_bdTierCopayment2.ordinal_bdTierCopayment = bdTierCopayment_bdCopTi2DaInSt2.ordinal_bdTierCopayment ) main
	GROUP BY
		qid) segment_16 ON
	segment_0.qid = segment_16.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COPAY_AD_AMT_INT_137) AS PBP_B1B_COPAY_AD_AMT_INT_137,
		MAX(PBP_B1B_COPAY_AD_AMT_INT_138) AS PBP_B1B_COPAY_AD_AMT_INT_138,
		MAX(PBP_B1B_COINS_NMCS_STRU_139) AS PBP_B1B_COINS_NMCS_STRU_139,
		MAX(PBP_B1B_COPAY_NMCS_AMT_140) AS PBP_B1B_COPAY_NMCS_AMT_140,
		MAX(PBP_B1B_COINS_PCT_NMCS_141) AS PBP_B1B_COINS_PCT_NMCS_141,
		MAX(PBP_B1B_COPAY_NMCS_BGND_NT_142) AS PBP_B1B_COPAY_NMCS_BGND_NT_142,
		MAX(PBP_B1B_COPAY_NMCS_BGND_NT_143) AS PBP_B1B_COPAY_NMCS_BGND_NT_143,
		MAX(PBP_B1B_COPAY_NMCS_BGND_NT_144) AS PBP_B1B_COPAY_NMCS_BGND_NT_144
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi2DaInSt2.bdDayInterval1CopaymentAmount)
					ELSE ''
				END AS PBP_B1B_COPAY_AD_AMT_INT_137,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b1')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(bdTierCopayment_bdCopTi1DaInSt5.bdDayInterval1CopaymentAmount)
						ELSE ''
					END AS PBP_B1B_COPAY_AD_AMT_INT_138,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCoiIntCom.bdCoinsurancePercentageYesNo)
							ELSE ''
						END AS PBP_B1B_COINS_NMCS_STRU_139,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCopIntCom.bdCopaymentAmt)
								ELSE ''
							END AS PBP_B1B_COPAY_NMCS_AMT_140,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCoiIntCom.bdCoinsAmt)
									ELSE ''
								END AS PBP_B1B_COINS_PCT_NMCS_141,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval1BeginDay)
										ELSE ''
									END AS PBP_B1B_COPAY_NMCS_BGND_NT_142,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval2BeginDay)
											ELSE ''
										END AS PBP_B1B_COPAY_NMCS_BGND_NT_143,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval3BeginDay)
												ELSE ''
											END AS PBP_B1B_COPAY_NMCS_BGND_NT_144
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
										LEFT JOIN jsonlib.benefitDetails_SinCoiIntCom AS benefitDetails_SinCoiIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCoiIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.benefitDetails_SinCopIntCom AS benefitDetails_SinCopIntCom ON
											benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_SinCopIntCom.ordinal_benefitDetails
										LEFT JOIN jsonlib.SinCopIntCom_bdCopDayIntSta2 AS SinCopIntCom_bdCopDayIntSta2 ON
											benefitDetails_SinCopIntCom.ordinal_SinCopIntCom = SinCopIntCom_bdCopDayIntSta2.ordinal_SinCopIntCom ) main
	GROUP BY
		qid) segment_17 ON
	segment_0.qid = segment_17.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COPAY_NMCS_ENDD_NT_145) AS PBP_B1B_COPAY_NMCS_ENDD_NT_145,
		MAX(PBP_B1B_COPAY_NMCS_ENDD_NT_146) AS PBP_B1B_COPAY_NMCS_ENDD_NT_146,
		MAX(PBP_B1B_COPAY_NMCS_ENDD_NT_147) AS PBP_B1B_COPAY_NMCS_ENDD_NT_147,
		MAX(PBP_B1B_COPAY_NMCS_AMT_INT_148) AS PBP_B1B_COPAY_NMCS_AMT_INT_148,
		MAX(PBP_B1B_COPAY_NMCS_AMT_INT_149) AS PBP_B1B_COPAY_NMCS_AMT_INT_149,
		MAX(PBP_B1B_COPAY_NMCS_AMT_INT_150) AS PBP_B1B_COPAY_NMCS_AMT_INT_150,
		MAX(PBP_B1B_COPAY_NMCS_STRU_151) AS PBP_B1B_COPAY_NMCS_STRU_151,
		MAX(PBP_B1B_COINS_NMCS_BGND_NT_152) AS PBP_B1B_COINS_NMCS_BGND_NT_152
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval1EndDay)
					ELSE ''
				END AS PBP_B1B_COPAY_NMCS_ENDD_NT_145,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval2EndDay)
						ELSE ''
					END AS PBP_B1B_COPAY_NMCS_ENDD_NT_146,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval3EndDay)
							ELSE ''
						END AS PBP_B1B_COPAY_NMCS_ENDD_NT_147,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval3CopaymentAmount)
								ELSE ''
							END AS PBP_B1B_COPAY_NMCS_AMT_INT_148,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval2CopaymentAmount)
									ELSE ''
								END AS PBP_B1B_COPAY_NMCS_AMT_INT_149,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCopIntCom_bdCopDayIntSta2.bdDayInterval1CopaymentAmount)
										ELSE ''
									END AS PBP_B1B_COPAY_NMCS_AMT_INT_150,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(benefitDetails_SinCopIntCom.bdCopaymentAmountYesNo)
											ELSE ''
										END AS PBP_B1B_COPAY_NMCS_STRU_151,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval2BeginDay)
												ELSE ''
											END AS PBP_B1B_COINS_NMCS_BGND_NT_152
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
		qid) segment_18 ON
	segment_0.qid = segment_18.qid
LEFT JOIN (
	SELECT
		qid,
		MAX(PBP_B1B_COINS_NMCS_BGND_NT_153) AS PBP_B1B_COINS_NMCS_BGND_NT_153,
		MAX(PBP_B1B_COINS_NMCS_BGND_NT_154) AS PBP_B1B_COINS_NMCS_BGND_NT_154,
		MAX(PBP_B1B_COINS_NMCS_PCT_INT_155) AS PBP_B1B_COINS_NMCS_PCT_INT_155,
		MAX(PBP_B1B_COINS_NMCS_PCT_INT_156) AS PBP_B1B_COINS_NMCS_PCT_INT_156,
		MAX(PBP_B1B_COINS_NMCS_PCT_INT_157) AS PBP_B1B_COINS_NMCS_PCT_INT_157,
		MAX(PBP_B1B_COINS_NMCS_ENDD_NT_158) AS PBP_B1B_COINS_NMCS_ENDD_NT_158,
		MAX(PBP_B1B_COINS_NMCS_ENDD_NT_159) AS PBP_B1B_COINS_NMCS_ENDD_NT_159,
		MAX(PBP_B1B_COINS_NMCS_ENDD_NT_160) AS PBP_B1B_COINS_NMCS_ENDD_NT_160
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
					AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval1BeginDay)
					ELSE ''
				END AS PBP_B1B_COINS_NMCS_BGND_NT_153,
				CASE
					WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
						AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval3BeginDay)
						ELSE ''
					END AS PBP_B1B_COINS_NMCS_BGND_NT_154,
					CASE
						WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
							AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInter1CoinsPerce)
							ELSE ''
						END AS PBP_B1B_COINS_NMCS_PCT_INT_155,
						CASE
							WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
								AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInter2CoinsPerce)
								ELSE ''
							END AS PBP_B1B_COINS_NMCS_PCT_INT_156,
							CASE
								WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
									AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInter3CoinsPerce)
									ELSE ''
								END AS PBP_B1B_COINS_NMCS_PCT_INT_157,
								CASE
									WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
										AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval3EndDay)
										ELSE ''
									END AS PBP_B1B_COINS_NMCS_ENDD_NT_158,
									CASE
										WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
											AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval2EndDay)
											ELSE ''
										END AS PBP_B1B_COINS_NMCS_ENDD_NT_159,
										CASE
											WHEN benefitDetails_benefDetaiInfo.categoryCode IN('1b2')
												AND benefitDetails_benefDetaiInfo.categoryTypeId = 2 THEN CATS(SinCoiIntCom_bdCoiDayIntSta2.bdDayInterval1EndDay)
												ELSE ''
											END AS PBP_B1B_COINS_NMCS_ENDD_NT_160
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
											benefitDetails_SinCoiIntCom.ordinal_SinCoiIntCom = SinCoiIntCom_bdCoiDayIntSta2.ordinal_SinCoiIntCom ) main
	GROUP BY
		qid) segment_19 ON
	segment_0.qid = segment_19.qid;
QUIT;	
RUN;

%MEND;



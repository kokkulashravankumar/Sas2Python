* PROGRAM NAME: JSON_IMPORT_PBPD_1.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPD_1();
PROC SQL;
CREATE TABLE PBPD_1_JSON AS
SELECT
	segment_0.qid,
	PBP_D_MAND_DEDUCT_16B1_N_AMT_0 AS PBP_D_MAND_DEDUCT_16B1_N_AMT,
	PBP_D_MAND_DEDUCT_16B4_N_AMT_1 AS PBP_D_MAND_DEDUCT_16B4_N_AMT,
	PBP_D_MAND_DEDUCT_16B5_N_AMT_2 AS PBP_D_MAND_DEDUCT_16B5_N_AMT,
	PBP_D_MAND_DEDUCT_16B2_N_AMT_3 AS PBP_D_MAND_DEDUCT_16B2_N_AMT,
	PBP_D_MAND_DEDUCT_16B6_N_AMT_4 AS PBP_D_MAND_DEDUCT_16B6_N_AMT,
	PBP_D_MAND_DEDUCT_16B3_N_AMT_5 AS PBP_D_MAND_DEDUCT_16B3_N_AMT,
	PBP_D_MAND_DEDUCT_16C1_N_AMT_6 AS PBP_D_MAND_DEDUCT_16C1_N_AMT,
	PBP_D_MAND_DEDUCT_16C2_N_AMT_7 AS PBP_D_MAND_DEDUCT_16C2_N_AMT,
	PBP_D_MAND_DEDUCT_16C3_N_AMT_8 AS PBP_D_MAND_DEDUCT_16C3_N_AMT,
	PBP_D_MAND_DEDUCT_16C7_N_AMT_10 AS PBP_D_MAND_DEDUCT_16C7_N_AMT,
	PBP_D_MAND_DEDUCT_16C8_N_AMT_10 AS PBP_D_MAND_DEDUCT_16C8_N_AMT,
	PBP_D_MAND_DEDUCT_16C9_N_AMT_10 AS PBP_D_MAND_DEDUCT_16C9_N_AMT
FROM
	(
	SELECT
		qid,
		MAX(PBP_D_MAND_DEDUCT_16B1_N_AMT_0) AS PBP_D_MAND_DEDUCT_16B1_N_AMT_0,
		MAX(PBP_D_MAND_DEDUCT_16B4_N_AMT_1) AS PBP_D_MAND_DEDUCT_16B4_N_AMT_1,
		MAX(PBP_D_MAND_DEDUCT_16B5_N_AMT_2) AS PBP_D_MAND_DEDUCT_16B5_N_AMT_2,
		MAX(PBP_D_MAND_DEDUCT_16B2_N_AMT_3) AS PBP_D_MAND_DEDUCT_16B2_N_AMT_3,
		MAX(PBP_D_MAND_DEDUCT_16B6_N_AMT_4) AS PBP_D_MAND_DEDUCT_16B6_N_AMT_4,
		MAX(PBP_D_MAND_DEDUCT_16B3_N_AMT_5) AS PBP_D_MAND_DEDUCT_16B3_N_AMT_5,
		MAX(PBP_D_MAND_DEDUCT_16C1_N_AMT_6) AS PBP_D_MAND_DEDUCT_16C1_N_AMT_6,
		MAX(PBP_D_MAND_DEDUCT_16C2_N_AMT_7) AS PBP_D_MAND_DEDUCT_16C2_N_AMT_7,
		MAX(PBP_D_MAND_DEDUCT_16C3_N_AMT_8) AS PBP_D_MAND_DEDUCT_16C3_N_AMT_8,
		MAX(PBP_D_MAND_DEDUCT_16C7_N_AMT_10) AS PBP_D_MAND_DEDUCT_16C7_N_AMT_10,
		MAX(PBP_D_MAND_DEDUCT_16C8_N_AMT_10) AS PBP_D_MAND_DEDUCT_16C8_N_AMT_10,
		MAX(PBP_D_MAND_DEDUCT_16C9_N_AMT_10) AS PBP_D_MAND_DEDUCT_16C9_N_AMT_10
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16b1')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16B1_N_AMT_0,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16b4')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16B4_N_AMT_1,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16b5')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16B5_N_AMT_2,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16b2')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16B2_N_AMT_3,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16b6')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16B6_N_AMT_4,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16b3')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16B3_N_AMT_5,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16c1')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16C1_N_AMT_6,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16c2')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16C2_N_AMT_7,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16c3')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16C3_N_AMT_8,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16b6')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16B6_N_AMT_9,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16c7')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16C7_N_AMT_10,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16c8')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16C8_N_AMT_10,
			CASE
				WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('16c9')
				AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN CATS(manSupBenDet_mandaEnhanBenef.deductibleAmount)
				ELSE ''
			END AS PBP_D_MAND_DEDUCT_16C9_N_AMT_10
		FROM
			jsonlib.root
		LEFT JOIN jsonlib.pbp AS pbp ON
			root.ordinal_root = pbp.ordinal_root
		LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
			pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
		LEFT JOIN jsonlib.plaLevCosSha_mandaSupplBenef AS plaLevCosSha_mandaSupplBenef ON
			pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_mandaSupplBenef.ordinal_plaLevCosSha
		LEFT JOIN jsonlib.mandaSupplBenef_manSupBenDet AS mandaSupplBenef_manSupBenDet ON
			plaLevCosSha_mandaSupplBenef.ordinal_mandaSupplBenef = mandaSupplBenef_manSupBenDet.ordinal_mandaSupplBenef
		LEFT JOIN jsonlib.manSupBenDet_mandaEnhanBenef AS manSupBenDet_mandaEnhanBenef ON
			mandaSupplBenef_manSupBenDet.ordinal_manSupBenDet = manSupBenDet_mandaEnhanBenef.ordinal_manSupBenDet ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;


* PROGRAM NAME: JSON_IMPORT_PBPB15.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPB15();
PROC SQL;
CREATE TABLE PBPB15_JSON AS
SELECT
	segment_0.qid,
	COALESCE(MRX_B_IRA_COINS_MIN_PCT_0, MRX_B_IRA_COINS_MIN_PCT_3) AS MRX_B_IRA_COINS_MIN_PCT,
	MRX_B_IRA_COPAY_MONTH_AMT_1 AS MRX_B_IRA_COPAY_MONTH_AMT,
	COALESCE(MRX_B_IRA_COPAY_AMT_MIN_2, MRX_B_IRA_COPAY_AMT_MIN_4) AS MRX_B_IRA_COPAY_AMT_MIN,
	MRX_B_MAX_OOP_AMT_5 AS MRX_B_MAX_OOP_AMT,
	MRX_B_MAX_OOP_PER_6 AS MRX_B_MAX_OOP_PER,
	MRX_B_DED_AMT_7 AS MRX_B_DED_AMT,

	CASE WHEN MRX_B_COINS_EHC_15 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN MRX_B_COINS_EHC_8 IN ('1','3') THEN '1' ELSE '0' END AS MRX_B_COINS_EHC,

	COALESCE(MRX_B_CHEMO_COINS_MIN_PCT_9, MRX_B_CHEMO_COINS_MIN_PCT_16) AS MRX_B_CHEMO_COINS_MIN_PCT,
	COALESCE(MRX_B_CHEMO_COPAY_AMT_MIN_10, MRX_B_CHEMO_COPAY_AMT_MIN_18) AS MRX_B_CHEMO_COPAY_AMT_MIN,

	CASE WHEN MRX_B_COPAY_EHC_13 IN ('1','3') THEN '1' ELSE '0' END ||
          CASE WHEN MRX_B_COPAY_EHC_11 IN ('1','3') THEN '1' ELSE '0' END AS MRX_B_COPAY_EHC,

	COALESCE(MRX_B_COINS_MIN_PCT_12, MRX_B_COINS_MIN_PCT_17) AS MRX_B_COINS_MIN_PCT,
	COALESCE(MRX_B_COPAY_MIN_AMT_14, MRX_B_COPAY_MIN_AMT_19) AS MRX_B_COPAY_MIN_AMT
FROM
	(
	SELECT
		qid,
		MAX(MRX_B_IRA_COINS_MIN_PCT_0) AS MRX_B_IRA_COINS_MIN_PCT_0,
		MAX(MRX_B_IRA_COPAY_MONTH_AMT_1) AS MRX_B_IRA_COPAY_MONTH_AMT_1,
		MAX(MRX_B_IRA_COPAY_AMT_MIN_2) AS MRX_B_IRA_COPAY_AMT_MIN_2,
		MAX(MRX_B_IRA_COINS_MIN_PCT_3) AS MRX_B_IRA_COINS_MIN_PCT_3,
		MAX(MRX_B_IRA_COPAY_AMT_MIN_4) AS MRX_B_IRA_COPAY_AMT_MIN_4,
		MAX(MRX_B_MAX_OOP_AMT_5) AS MRX_B_MAX_OOP_AMT_5,
		MAX(MRX_B_MAX_OOP_PER_6) AS MRX_B_MAX_OOP_PER_6,
		MAX(MRX_B_DED_AMT_7) AS MRX_B_DED_AMT_7,
		MAX(MRX_B_COINS_EHC_8) AS MRX_B_COINS_EHC_8,
		MAX(MRX_B_CHEMO_COINS_MIN_PCT_9) AS MRX_B_CHEMO_COINS_MIN_PCT_9,
		MAX(MRX_B_CHEMO_COPAY_AMT_MIN_10) AS MRX_B_CHEMO_COPAY_AMT_MIN_10,
		MAX(MRX_B_COPAY_EHC_11) AS MRX_B_COPAY_EHC_11,
		MAX(MRX_B_COINS_MIN_PCT_12) AS MRX_B_COINS_MIN_PCT_12,
		MAX(MRX_B_COPAY_EHC_13) AS MRX_B_COPAY_EHC_13,
		MAX(MRX_B_COPAY_MIN_AMT_14) AS MRX_B_COPAY_MIN_AMT_14,
		MAX(MRX_B_COINS_EHC_15) AS MRX_B_COINS_EHC_15,
		MAX(MRX_B_CHEMO_COINS_MIN_PCT_16) AS MRX_B_CHEMO_COINS_MIN_PCT_16,
		MAX(MRX_B_COINS_MIN_PCT_17) AS MRX_B_COINS_MIN_PCT_17,
		MAX(MRX_B_CHEMO_COPAY_AMT_MIN_18) AS MRX_B_CHEMO_COPAY_AMT_MIN_18,
		MAX(MRX_B_COPAY_MIN_AMT_19) AS MRX_B_COPAY_MIN_AMT_19
	FROM
		(
		SELECT
			put(cat(contractId,
			planId,
			put(segmentId,
			z3.)),
			$11.) AS qid,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS MRX_B_IRA_COINS_MIN_PCT_0,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdMaxPerVisitAmount)
				ELSE ''
			END AS MRX_B_IRA_COPAY_MONTH_AMT_1,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS MRX_B_IRA_COPAY_AMT_MIN_2,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS MRX_B_IRA_COINS_MIN_PCT_3,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-1')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS MRX_B_IRA_COPAY_AMT_MIN_4,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopAmount)
				ELSE ''
			END AS MRX_B_MAX_OOP_AMT_5,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_MooAmoCom.bdMoopPeriodicity)
				ELSE ''
			END AS MRX_B_MAX_OOP_PER_6,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_DedAmoCom.bdMedicareDeductibleAmt)
				ELSE ''
			END AS MRX_B_DED_AMT_7,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS MRX_B_COINS_EHC_8,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS MRX_B_CHEMO_COINS_MIN_PCT_9,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS MRX_B_CHEMO_COPAY_AMT_MIN_10,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS MRX_B_COPAY_EHC_11,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceMinAmount)
				ELSE ''
			END AS MRX_B_COINS_MIN_PCT_12,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmountYesNoMinMax)
				ELSE ''
			END AS MRX_B_COPAY_EHC_13,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentMinAmount)
				ELSE ''
			END AS MRX_B_COPAY_MIN_AMT_14,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmountYesNoMinMax)
				ELSE ''
			END AS MRX_B_COINS_EHC_15,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS MRX_B_CHEMO_COINS_MIN_PCT_16,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CoinsCompo.bdCoinsuranceAmount)
				ELSE ''
			END AS MRX_B_COINS_MIN_PCT_17,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-2')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS MRX_B_CHEMO_COPAY_AMT_MIN_18,
			CASE
				WHEN benefitDetails_benefDetaiInfo.categoryCode IN('15-3')
				AND benefitDetails_benefDetaiInfo.categoryTypeId = 1 THEN CATS(benefitDetails_CopayCompo.bdCopaymentAmount)
				ELSE ''
			END AS MRX_B_COPAY_MIN_AMT_19
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
		LEFT JOIN jsonlib.benefitDetails_CoiAdWaTiCom AS benefitDetails_CoiAdWaTiCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CoiAdWaTiCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.CoiAdWaTiCom_CoiAdWaTiComCoi AS CoiAdWaTiCom_CoiAdWaTiComCoi ON
			benefitDetails_CoiAdWaTiCom.ordinal_CoiAdWaTiCom = CoiAdWaTiCom_CoiAdWaTiComCoi.ordinal_CoiAdWaTiCom
		LEFT JOIN jsonlib.benefitDetails_CopayCompo AS benefitDetails_CopayCompo ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_CopayCompo.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_MooAmoCom AS benefitDetails_MooAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_MooAmoCom.ordinal_benefitDetails
		LEFT JOIN jsonlib.benefitDetails_DedAmoCom AS benefitDetails_DedAmoCom ON
			benefDetaiInfo_benefitDetails.ordinal_benefitDetails = benefitDetails_DedAmoCom.ordinal_benefitDetails ) main
	GROUP BY
		qid) segment_0;
QUIT;
RUN;

%MEND;



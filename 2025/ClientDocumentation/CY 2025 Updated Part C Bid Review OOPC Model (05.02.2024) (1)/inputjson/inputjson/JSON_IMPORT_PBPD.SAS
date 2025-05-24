* PROGRAM NAME: JSON_IMPORT_PBPD.SAS;
* DESCRIPTION: IMPORT JSON TABLES;

%MACRO PBPD();
* pre-process data that needs to be rolled up #1;

proc sql;
create table PBP_D_COMB_DEDUCT_INN_M_CATS_1 as
	SELECT DISTINCT
		put(cat(contractId,
		planId,
		put(segmentId,
		z3.)),
		$11.) AS qid,
		planDeducDetai_medicarService.categoryCode AS PBP_D_COMB_DEDUCT_INN_M_C_96
	FROM
		jsonlib.root
	LEFT JOIN jsonlib.pbp AS pbp ON
		root.ordinal_root = pbp.ordinal_root
	LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
		pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
	LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
		pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
	LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
		plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
	LEFT JOIN jsonlib.planDeducDetai_medicarService AS planDeducDetai_medicarService ON
		planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_medicarService.ordinal_planDeducDetai
	WHERE planDeducDetai_medicarService.combinedInNetwork = '1'
	ORDER BY 1
;
quit;

data jsonlib.PBP_D_COMB_DEDUCT_INN_M_CATS (drop=PBP_D_COMB_DEDUCT_INN_M_C_96);
set PBP_D_COMB_DEDUCT_INN_M_CATS_1;
by qid;

retain PBP_D_COMB_DEDUCT_INN_M_CATS;

length PBP_D_COMB_DEDUCT_INN_M_CATS $4000;

if first.qid then PBP_D_COMB_DEDUCT_INN_M_CATS = ' ';
if PBP_D_COMB_DEDUCT_INN_M_C_96 ^= ' ' then PBP_D_COMB_DEDUCT_INN_M_CATS = compress(PBP_D_COMB_DEDUCT_INN_M_CATS || PBP_D_COMB_DEDUCT_INN_M_C_96 || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #2;

proc sql;
create table PBP_D_COMB_DEDUCT_INN_NM_CATS_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                planDeducDetai_nonMedicServi.combinedInNetwork AS PBP_D_COMB_DEDUCT_INN_NM_C_98
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_medicarService AS planDeducDetai_medicarService ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_medicarService.ordinal_planDeducDetai
            LEFT JOIN jsonlib.planDeducDetai_nonMedicServi AS planDeducDetai_nonMedicServi ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_nonMedicServi.ordinal_planDeducDetai
            WHERE planDeducDetai_nonMedicServi.combinedInNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_COMB_DEDUCT_INN_NM_CATS (drop=PBP_D_COMB_DEDUCT_INN_NM_C_98);
set PBP_D_COMB_DEDUCT_INN_NM_CATS_1;
by qid;

retain PBP_D_COMB_DEDUCT_INN_NM_CATS;

length PBP_D_COMB_DEDUCT_INN_NM_CATS $4000;

if first.qid then PBP_D_COMB_DEDUCT_INN_NM_CATS = ' ';
if PBP_D_COMB_DEDUCT_INN_NM_C_98 ^= ' ' then PBP_D_COMB_DEDUCT_INN_NM_CATS = compress(PBP_D_COMB_DEDUCT_INN_NM_CATS || PBP_D_COMB_DEDUCT_INN_NM_C_98 || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #3-A;


proc sql;
create table PBP_D_COMB_MAX_ENR_INN_MCA_120_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                lpRpMaEnCosLiDe_mecMedSrvCtg.categoryCode AS PBP_D_COMB_MAX_ENR_INN_MCA
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiDe_mecMedSrvCtg AS lpRpMaEnCosLiDe_mecMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiDe_mecMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            WHERE lpRpMaEnCosLiDe_mecMedSrvCtg.combinedInNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_COMB_MAX_ENR_INN_MCA_120 (drop=PBP_D_COMB_MAX_ENR_INN_MCA);
set PBP_D_COMB_MAX_ENR_INN_MCA_120_1;
by qid;

retain PBP_D_COMB_MAX_ENR_INN_MCA_120;

length PBP_D_COMB_MAX_ENR_INN_MCA_120 $4000;

if first.qid then PBP_D_COMB_MAX_ENR_INN_MCA_120 = ' ';

if PBP_D_COMB_MAX_ENR_INN_MCA = '15-1' then PBP_D_COMB_MAX_ENR_INN_MCA = '15-1-I';

if PBP_D_COMB_MAX_ENR_INN_MCA ^= ' ' then PBP_D_COMB_MAX_ENR_INN_MCA_120 = compress(PBP_D_COMB_MAX_ENR_INN_MCA_120 || PBP_D_COMB_MAX_ENR_INN_MCA || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #3-B;

proc sql;
create table PBP_D_COMB_MAX_ENR_INN_MCA_135_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                maxEnrCosLimDet_medicarService.categoryCode AS PBP_D_COMB_MAX_ENR_INN_MCA
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet
            WHERE maxEnrCosLimDet_medicarService.moopCombinedInNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_COMB_MAX_ENR_INN_MCA_135 (drop=PBP_D_COMB_MAX_ENR_INN_MCA);
set PBP_D_COMB_MAX_ENR_INN_MCA_135_1;
by qid;

retain PBP_D_COMB_MAX_ENR_INN_MCA_135;

length PBP_D_COMB_MAX_ENR_INN_MCA_135 $4000;

if first.qid then PBP_D_COMB_MAX_ENR_INN_MCA_135 = ' ';

if PBP_D_COMB_MAX_ENR_INN_MCA = '15-1' then PBP_D_COMB_MAX_ENR_INN_MCA = '15-1-I';

if PBP_D_COMB_MAX_ENR_INN_MCA ^= ' ' then PBP_D_COMB_MAX_ENR_INN_MCA_135 = compress(PBP_D_COMB_MAX_ENR_INN_MCA_135 || PBP_D_COMB_MAX_ENR_INN_MCA || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #4-A;

proc sql;
create table PBP_D_COMB_MAX_ENR_INN_NMCA_22_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                lpRpMaEnCosLiD_mecNonMedSrvCtg.categoryCode AS PBP_D_COMB_MAX_ENR_INN_NMCA
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai
            LEFT JOIN jsonlib.plaLevCosSha_lppoRppoDeduc AS plaLevCosSha_lppoRppoDeduc ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lppoRppoDeduc.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lppoRppoDeduc_lppRppDedDet AS lppoRppoDeduc_lppRppDedDet ON
                plaLevCosSha_lppoRppoDeduc.ordinal_lppoRppoDeduc = lppoRppoDeduc_lppRppDedDet.ordinal_lppoRppoDeduc
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiD_mecNonMedSrvCtg AS lpRpMaEnCosLiD_mecNonMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiD_mecNonMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            WHERE lpRpMaEnCosLiD_mecNonMedSrvCtg.combinedInNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_COMB_MAX_ENR_INN_NMCA_22 (drop=PBP_D_COMB_MAX_ENR_INN_NMCA);
set PBP_D_COMB_MAX_ENR_INN_NMCA_22_1;
by qid;

retain PBP_D_COMB_MAX_ENR_INN_NMCA_22;

length PBP_D_COMB_MAX_ENR_INN_NMCA_22 $4000;

if first.qid then PBP_D_COMB_MAX_ENR_INN_NMCA_22 = ' ';

if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-1'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-1_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-2'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-2_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-3'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-3_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-4'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-4_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '7f'   then PBP_D_COMB_MAX_ENR_INN_NMCA = '7f_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '9d'   then PBP_D_COMB_MAX_ENR_INN_NMCA = '9d_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '9d-D' then PBP_D_COMB_MAX_ENR_INN_NMCA = '9d_2';

if PBP_D_COMB_MAX_ENR_INN_NMCA ^= ' ' then PBP_D_COMB_MAX_ENR_INN_NMCA_22 = compress(PBP_D_COMB_MAX_ENR_INN_NMCA_22 || PBP_D_COMB_MAX_ENR_INN_NMCA || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #4-B;

proc sql;
create table PBP_D_COMB_MAX_ENR_INN_NMCA_23_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                maxEnrCosLimDet_nonMedicServi.categoryCode AS PBP_D_COMB_MAX_ENR_INN_NMCA
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai
            LEFT JOIN jsonlib.plaLevCosSha_lppoRppoDeduc AS plaLevCosSha_lppoRppoDeduc ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lppoRppoDeduc.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lppoRppoDeduc_lppRppDedDet AS lppoRppoDeduc_lppRppDedDet ON
                plaLevCosSha_lppoRppoDeduc.ordinal_lppoRppoDeduc = lppoRppoDeduc_lppRppDedDet.ordinal_lppoRppoDeduc
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiD_mecNonMedSrvCtg AS lpRpMaEnCosLiD_mecNonMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiD_mecNonMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_nonMedicServi AS maxEnrCosLimDet_nonMedicServi ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_nonMedicServi.ordinal_maxEnrCosLimDet
            WHERE maxEnrCosLimDet_nonMedicServi.moopCombinedInNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_COMB_MAX_ENR_INN_NMCA_23 (drop=PBP_D_COMB_MAX_ENR_INN_NMCA);
set PBP_D_COMB_MAX_ENR_INN_NMCA_23_1;
by qid;

retain PBP_D_COMB_MAX_ENR_INN_NMCA_23;

length PBP_D_COMB_MAX_ENR_INN_NMCA_23 $4000;

if first.qid then PBP_D_COMB_MAX_ENR_INN_NMCA_23 = ' ';

if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-1'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-1_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-2'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-2_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-3'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-3_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '3-4'  then PBP_D_COMB_MAX_ENR_INN_NMCA = '3-4_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '7f'   then PBP_D_COMB_MAX_ENR_INN_NMCA = '7f_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '9d'   then PBP_D_COMB_MAX_ENR_INN_NMCA = '9d_2';
if PBP_D_COMB_MAX_ENR_INN_NMCA = '9d-D' then PBP_D_COMB_MAX_ENR_INN_NMCA = '9d_2';

if PBP_D_COMB_MAX_ENR_INN_NMCA ^= ' ' then PBP_D_COMB_MAX_ENR_INN_NMCA_23 = compress(PBP_D_COMB_MAX_ENR_INN_NMCA_23 || PBP_D_COMB_MAX_ENR_INN_NMCA || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #5;

proc sql;
create table PBP_D_INN_DEDUCT_M_CATS_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                planDeducDetai_medicarService.categoryCode AS PBP_D_INN_DEDUCT_M_CATS_72
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_plaLevCosShaDet AS plaLevCosSha_plaLevCosShaDet ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_plaLevCosShaDet.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_medicarService AS planDeducDetai_medicarService ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_medicarService.ordinal_planDeducDetai
            WHERE planDeducDetai_medicarService.inNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_INN_DEDUCT_M_CATS (drop=PBP_D_INN_DEDUCT_M_CATS_72);
set PBP_D_INN_DEDUCT_M_CATS_1;
by qid;

retain PBP_D_INN_DEDUCT_M_CATS;

length PBP_D_INN_DEDUCT_M_CATS $4000;

if first.qid then PBP_D_INN_DEDUCT_M_CATS = ' ';
if PBP_D_INN_DEDUCT_M_CATS_72 ^= ' ' then PBP_D_INN_DEDUCT_M_CATS = compress(PBP_D_INN_DEDUCT_M_CATS || PBP_D_INN_DEDUCT_M_CATS_72 || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #6;

proc sql;
create table PBP_D_INN_DEDUCT_NM_CATS_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                planDeducDetai_nonMedicServi.categoryCode AS PBP_D_INN_DEDUCT_NM_CATS_100
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_nonMedicServi AS planDeducDetai_nonMedicServi ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_nonMedicServi.ordinal_planDeducDetai
            WHERE planDeducDetai_nonMedicServi.inNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_INN_DEDUCT_NM_CATS (drop=PBP_D_INN_DEDUCT_NM_CATS_100);
set PBP_D_INN_DEDUCT_NM_CATS_1;
by qid;

retain PBP_D_INN_DEDUCT_NM_CATS;

length PBP_D_INN_DEDUCT_NM_CATS $4000;

if first.qid then PBP_D_INN_DEDUCT_NM_CATS = ' ';

if PBP_D_INN_DEDUCT_NM_CATS_100 = '3-1'  then PBP_D_INN_DEDUCT_NM_CATS_100 = '3-1_2';
if PBP_D_INN_DEDUCT_NM_CATS_100 = '3-2'  then PBP_D_INN_DEDUCT_NM_CATS_100 = '3-2_2';
if PBP_D_INN_DEDUCT_NM_CATS_100 = '3-3'  then PBP_D_INN_DEDUCT_NM_CATS_100 = '3-3_2';
if PBP_D_INN_DEDUCT_NM_CATS_100 = '3-4'  then PBP_D_INN_DEDUCT_NM_CATS_100 = '3-4_2';
if PBP_D_INN_DEDUCT_NM_CATS_100 = '7f'   then PBP_D_INN_DEDUCT_NM_CATS_100 = '7f_2';
if PBP_D_INN_DEDUCT_NM_CATS_100 = '9d'   then PBP_D_INN_DEDUCT_NM_CATS_100 = '9d_2';
if PBP_D_INN_DEDUCT_NM_CATS_100 = '9d-D' then PBP_D_INN_DEDUCT_NM_CATS_100 = '9d_2';

if PBP_D_INN_DEDUCT_NM_CATS_100 ^= ' ' then PBP_D_INN_DEDUCT_NM_CATS =  compress(PBP_D_INN_DEDUCT_NM_CATS || PBP_D_INN_DEDUCT_NM_CATS_100 || ';');

if last.qid then output;

run;

* pre-process data that needs to be rolled up #7-A;

proc sql;
create table PBP_D_INN_MAX_ENR_M_CAT_EX_109_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                lpRpMaEnCosLiDe_mecMedSrvCtg.categoryCode AS PBP_D_INN_MAX_ENR_M_CAT_EX
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_lppoRppoDeduc AS plaLevCosSha_lppoRppoDeduc ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lppoRppoDeduc.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lppoRppoDeduc_lppRppDedDet AS lppoRppoDeduc_lppRppDedDet ON
                plaLevCosSha_lppoRppoDeduc.ordinal_lppoRppoDeduc = lppoRppoDeduc_lppRppDedDet.ordinal_lppoRppoDeduc
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiDe_mecMedSrvCtg AS lpRpMaEnCosLiDe_mecMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiDe_mecMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            WHERE lpRpMaEnCosLiDe_mecMedSrvCtg.inNetwork = '1'
            ORDER BY 1
            ;
quit;

data jsonlib.PBP_D_INN_MAX_ENR_M_CAT_EX_109 (drop=PBP_D_INN_MAX_ENR_M_CAT_EX);
set PBP_D_INN_MAX_ENR_M_CAT_EX_109_1;
by qid;

retain PBP_D_INN_MAX_ENR_M_CAT_EX_109;

length PBP_D_INN_MAX_ENR_M_CAT_EX_109 $4000;

if first.qid then PBP_D_INN_MAX_ENR_M_CAT_EX_109 = ' ';

if PBP_D_INN_MAX_ENR_M_CAT_EX = '15-1' then PBP_D_INN_MAX_ENR_M_CAT_EX = '15-1-I';

if PBP_D_INN_MAX_ENR_M_CAT_EX ^= ' ' then PBP_D_INN_MAX_ENR_M_CAT_EX_109 = compress(PBP_D_INN_MAX_ENR_M_CAT_EX_109 || PBP_D_INN_MAX_ENR_M_CAT_EX || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #7-B;

proc sql;
create table PBP_D_INN_MAX_ENR_M_CAT_EX_126_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                maxEnrCosLimDet_medicarService.categoryCode AS PBP_D_INN_MAX_ENR_M_CAT_EX
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiDe_mecMedSrvCtg AS lpRpMaEnCosLiDe_mecMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiDe_mecMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            LEFT JOIN jsonlib.lpRpMaEnCosLiD_mecNonMedSrvCtg AS lpRpMaEnCosLiD_mecNonMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiD_mecNonMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet
            WHERE maxEnrCosLimDet_medicarService.moopInNetwork = '1'
            ORDER BY 1
            ;
quit;

data jsonlib.PBP_D_INN_MAX_ENR_M_CAT_EX_126 (drop=PBP_D_INN_MAX_ENR_M_CAT_EX);
set PBP_D_INN_MAX_ENR_M_CAT_EX_126_1;
by qid;

retain PBP_D_INN_MAX_ENR_M_CAT_EX_126;

length PBP_D_INN_MAX_ENR_M_CAT_EX_126 $4000;

if first.qid then PBP_D_INN_MAX_ENR_M_CAT_EX_126 = ' ';

if PBP_D_INN_MAX_ENR_M_CAT_EX = '15-1' then PBP_D_INN_MAX_ENR_M_CAT_EX = '15-1-I';

if PBP_D_INN_MAX_ENR_M_CAT_EX ^= ' ' then PBP_D_INN_MAX_ENR_M_CAT_EX_126 = compress(PBP_D_INN_MAX_ENR_M_CAT_EX_126 || PBP_D_INN_MAX_ENR_M_CAT_EX || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #8-A;

proc sql;
create table PBP_D_INN_MAX_ENR_NM_CA_122_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                lpRpMaEnCosLiD_mecNonMedSrvCtg.categoryCode AS PBP_D_INN_MAX_ENR_NM_CA
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiDe_mecMedSrvCtg AS lpRpMaEnCosLiDe_mecMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiDe_mecMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            LEFT JOIN jsonlib.lpRpMaEnCosLiD_mecNonMedSrvCtg AS lpRpMaEnCosLiD_mecNonMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiD_mecNonMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            WHERE lpRpMaEnCosLiD_mecNonMedSrvCtg.inNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_INN_MAX_ENR_NM_CA_122 (drop=PBP_D_INN_MAX_ENR_NM_CA);
set PBP_D_INN_MAX_ENR_NM_CA_122_1;
by qid;

retain PBP_D_INN_MAX_ENR_NM_CA_122;

length PBP_D_INN_MAX_ENR_NM_CA_122 $4000;

if first.qid then PBP_D_INN_MAX_ENR_NM_CA_122 = ' ';

if PBP_D_INN_MAX_ENR_NM_CA = '3-1'  then PBP_D_INN_MAX_ENR_NM_CA = '3-1_2';
if PBP_D_INN_MAX_ENR_NM_CA = '3-2'  then PBP_D_INN_MAX_ENR_NM_CA = '3-2_2';
if PBP_D_INN_MAX_ENR_NM_CA = '3-3'  then PBP_D_INN_MAX_ENR_NM_CA = '3-3_2';
if PBP_D_INN_MAX_ENR_NM_CA = '3-4'  then PBP_D_INN_MAX_ENR_NM_CA = '3-4_2';
if PBP_D_INN_MAX_ENR_NM_CA = '7f'   then PBP_D_INN_MAX_ENR_NM_CA = '7f_2';
if PBP_D_INN_MAX_ENR_NM_CA = '9d'   then PBP_D_INN_MAX_ENR_NM_CA = '9d_2';
if PBP_D_INN_MAX_ENR_NM_CA = '9d-D' then PBP_D_INN_MAX_ENR_NM_CA = '9d_2';

if PBP_D_INN_MAX_ENR_NM_CA ^= ' ' then PBP_D_INN_MAX_ENR_NM_CA_122 = compress(PBP_D_INN_MAX_ENR_NM_CA_122 || PBP_D_INN_MAX_ENR_NM_CA || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #8-B;

proc sql;
create table PBP_D_INN_MAX_ENR_NM_CA_147_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                maxEnrCosLimDet_nonMedicServi.categoryCode AS PBP_D_INN_MAX_ENR_NM_CA
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_nonMedicServi AS maxEnrCosLimDet_nonMedicServi ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_nonMedicServi.ordinal_maxEnrCosLimDet
            WHERE maxEnrCosLimDet_nonMedicServi.moopInNetwork = '1'
            ORDER BY 1
;
quit;

data jsonlib.PBP_D_INN_MAX_ENR_NM_CA_147 (drop=PBP_D_INN_MAX_ENR_NM_CA);
set PBP_D_INN_MAX_ENR_NM_CA_147_1;
by qid;

retain PBP_D_INN_MAX_ENR_NM_CA_147;

length PBP_D_INN_MAX_ENR_NM_CA_147 $4000;

if first.qid then PBP_D_INN_MAX_ENR_NM_CA_147 = ' ';

if PBP_D_INN_MAX_ENR_NM_CA = '3-1'  then PBP_D_INN_MAX_ENR_NM_CA = '3-1_2';
if PBP_D_INN_MAX_ENR_NM_CA = '3-2'  then PBP_D_INN_MAX_ENR_NM_CA = '3-2_2';
if PBP_D_INN_MAX_ENR_NM_CA = '3-3'  then PBP_D_INN_MAX_ENR_NM_CA = '3-3_2';
if PBP_D_INN_MAX_ENR_NM_CA = '3-4'  then PBP_D_INN_MAX_ENR_NM_CA = '3-4_2';
if PBP_D_INN_MAX_ENR_NM_CA = '7f'   then PBP_D_INN_MAX_ENR_NM_CA = '7f_2';
if PBP_D_INN_MAX_ENR_NM_CA = '9d'   then PBP_D_INN_MAX_ENR_NM_CA = '9d_2';
if PBP_D_INN_MAX_ENR_NM_CA = '9d-D' then PBP_D_INN_MAX_ENR_NM_CA = '9d_2';

if PBP_D_INN_MAX_ENR_NM_CA ^= ' ' then PBP_D_INN_MAX_ENR_NM_CA_147 = compress(PBP_D_INN_MAX_ENR_NM_CA_147 || PBP_D_INN_MAX_ENR_NM_CA || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #9;

proc sql;
create table PBP_D_MAXENR_OOPC_M_CATS_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                put(maxEnrCosLimDet_medicarService.moopNonNetwork, 8.) AS PBP_D_MAXENR_OOPC_M_CATS_144
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet
            WHERE maxEnrCosLimDet_medicarService.moopNonNetwork = 1
            ORDER BY 1
 ;
quit;

data jsonlib.PBP_D_MAXENR_OOPC_M_CATS (drop=PBP_D_MAXENR_OOPC_M_CATS_144);
set PBP_D_MAXENR_OOPC_M_CATS_1;
by qid;

retain PBP_D_MAXENR_OOPC_M_CATS;

length PBP_D_MAXENR_OOPC_M_CATS $4000;

if first.qid then PBP_D_MAXENR_OOPC_M_CATS = ' ';
if PBP_D_MAXENR_OOPC_M_CATS_144 ^= ' ' then PBP_D_MAXENR_OOPC_M_CATS = compress(PBP_D_MAXENR_OOPC_M_CATS || PBP_D_MAXENR_OOPC_M_CATS_144 || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #10;

proc sql;
create table PBP_D_MAXENR_OOPC_NM_CATS_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                put(maxEnrCosLimDet_nonMedicServi.moopNonNetwork, 8.) AS PBP_D_MAXENR_OOPC_NM_CATS_139
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet
            LEFT JOIN jsonlib.maxEnrCosLimDet_nonMedicServi AS maxEnrCosLimDet_nonMedicServi ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_nonMedicServi.ordinal_maxEnrCosLimDet
            WHERE maxEnrCosLimDet_medicarService.moopNonNetwork = 1
            ORDER BY 1
 ;
quit;

data jsonlib.PBP_D_MAXENR_OOPC_NM_CATS (drop=PBP_D_MAXENR_OOPC_NM_CATS_139);
set PBP_D_MAXENR_OOPC_NM_CATS_1;
by qid;

retain PBP_D_MAXENR_OOPC_NM_CATS;

length PBP_D_MAXENR_OOPC_NM_CATS $4000;

if first.qid then PBP_D_MAXENR_OOPC_NM_CATS = ' ';
if PBP_D_MAXENR_OOPC_NM_CATS_139 ^= ' ' then PBP_D_MAXENR_OOPC_NM_CATS = compress(PBP_D_MAXENR_OOPC_NM_CATS || PBP_D_MAXENR_OOPC_NM_CATS_139 || ';');
if last.qid then output;

run;

* pre-process data that needs to be rolled up #11;

proc sql;
create table PBP_D_NON_DEDUCT_M_CATS_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                put(planDeducDetai_medicarService.nonNetwork,8.) AS PBP_D_NON_DEDUCT_M_CATS_73
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_plaLevCosShaDet AS plaLevCosSha_plaLevCosShaDet ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_plaLevCosShaDet.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_medicarService AS planDeducDetai_medicarService ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_medicarService.ordinal_planDeducDetai
            WHERE planDeducDetai_medicarService.nonNetwork = 1
            ORDER BY 1
 ;
quit;

data jsonlib.PBP_D_NON_DEDUCT_M_CATS (drop=PBP_D_NON_DEDUCT_M_CATS_73);
set PBP_D_NON_DEDUCT_M_CATS_1;
by qid;

retain PBP_D_NON_DEDUCT_M_CATS;

length PBP_D_NON_DEDUCT_M_CATS $4000;

if first.qid then PBP_D_NON_DEDUCT_M_CATS = ' ';
if PBP_D_NON_DEDUCT_M_CATS_73 ^= ' ' then PBP_D_NON_DEDUCT_M_CATS = compress(PBP_D_NON_DEDUCT_M_CATS || PBP_D_NON_DEDUCT_M_CATS_73 || ';');
if last.qid then output;

run;
                
* pre-process data that needs to be rolled up #12;

proc sql;
create table PBP_D_NON_DEDUCT_NM_CATS_1 as
            SELECT DISTINCT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                put(planDeducDetai_nonMedicServi.nonNetwork,8.) AS PBP_D_NON_DEDUCT_NM_CATS_79
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_nonMedicServi AS planDeducDetai_nonMedicServi ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_nonMedicServi.ordinal_planDeducDetai    
            WHERE planDeducDetai_nonMedicServi.nonNetwork = 1
            ORDER BY 1
 ;
quit;

data jsonlib.PBP_D_NON_DEDUCT_NM_CATS (drop=PBP_D_NON_DEDUCT_NM_CATS_79);
set PBP_D_NON_DEDUCT_NM_CATS_1;
by qid;

retain PBP_D_NON_DEDUCT_NM_CATS;

length PBP_D_NON_DEDUCT_NM_CATS $4000;

if first.qid then PBP_D_NON_DEDUCT_NM_CATS = ' ';
if PBP_D_NON_DEDUCT_NM_CATS_79 ^= ' ' then PBP_D_NON_DEDUCT_NM_CATS = compress(PBP_D_NON_DEDUCT_NM_CATS || PBP_D_NON_DEDUCT_NM_CATS_79 || ';');
if last.qid then output;

run;

PROC SQL;
CREATE TABLE pbpd_JSON AS
SELECT
    main.*,
    PBP_D_COMB_DEDUCT_INN_M_CATS.PBP_D_COMB_DEDUCT_INN_M_CATS AS PBP_D_COMB_DEDUCT_INN_M_CATS,
    PBP_D_COMB_DEDUCT_INN_NM_CATS.PBP_D_COMB_DEDUCT_INN_NM_CATS AS PBP_D_COMB_DEDUCT_INN_NM_CATS,
    coalesce(PBP_D_COMB_MAX_ENR_INN_MCA_120.PBP_D_COMB_MAX_ENR_INN_MCA_120, PBP_D_COMB_MAX_ENR_INN_MCA_135.PBP_D_COMB_MAX_ENR_INN_MCA_135) as PBP_D_COMB_MAX_ENR_INN_MCAT_EX,
    coalesce(PBP_D_COMB_MAX_ENR_INN_NMCA_22.PBP_D_COMB_MAX_ENR_INN_NMCA_22, PBP_D_COMB_MAX_ENR_INN_NMCA_23.PBP_D_COMB_MAX_ENR_INN_NMCA_23) as PBP_D_COMB_MAX_ENR_INN_NMCATEX,
    PBP_D_INN_DEDUCT_M_CATS.PBP_D_INN_DEDUCT_M_CATS AS PBP_D_INN_DEDUCT_M_CATS,
    PBP_D_INN_DEDUCT_NM_CATS.PBP_D_INN_DEDUCT_NM_CATS AS PBP_D_INN_DEDUCT_NM_CATS,
    coalesce(PBP_D_INN_MAX_ENR_M_CAT_EX_109.PBP_D_INN_MAX_ENR_M_CAT_EX_109, PBP_D_INN_MAX_ENR_M_CAT_EX_126.PBP_D_INN_MAX_ENR_M_CAT_EX_126) as PBP_D_INN_MAX_ENR_M_CAT_EX,
    coalesce(PBP_D_INN_MAX_ENR_NM_CA_122.PBP_D_INN_MAX_ENR_NM_CA_122, PBP_D_INN_MAX_ENR_NM_CA_147.PBP_D_INN_MAX_ENR_NM_CA_147) as PBP_D_INN_MAX_ENR_NM_CAT_EX,
    PBP_D_MAXENR_OOPC_M_CATS.PBP_D_MAXENR_OOPC_M_CATS AS PBP_D_MAXENR_OOPC_M_CATS,
    PBP_D_MAXENR_OOPC_NM_CATS.PBP_D_MAXENR_OOPC_NM_CATS AS PBP_D_MAXENR_OOPC_NM_CATS,
    PBP_D_NON_DEDUCT_M_CATS.PBP_D_NON_DEDUCT_M_CATS AS PBP_D_NON_DEDUCT_M_CATS,
    PBP_D_NON_DEDUCT_NM_CATS.PBP_D_NON_DEDUCT_NM_CATS AS PBP_D_NON_DEDUCT_NM_CATS
FROM
    (SELECT
        segment_0.qid,
        PBP_D_MSA_DED_AMT_0 AS PBP_D_MSA_DED_AMT,
        PBP_D_NMC_DEDUCT_YN_1 AS PBP_D_NMC_DEDUCT_YN,
        PBP_D_DIFF_DEDUCT_YN_2 AS PBP_D_DIFF_DEDUCT_YN,
        PBP_D_DIFF_DEDUCT_3_1_AMT_3 AS PBP_D_DIFF_DEDUCT_3_1_AMT,
        PBP_D_DIFF_DEDUCT_3_3_AMT_4 AS PBP_D_DIFF_DEDUCT_3_3_AMT,
        PBP_D_DIFF_DEDUCT_3_4_AMT_5 AS PBP_D_DIFF_DEDUCT_3_4_AMT,
        PBP_D_DIFF_DEDUCT_7E1_AMT_6 AS PBP_D_DIFF_DEDUCT_7E1_AMT,
        PBP_D_DIFF_DEDUCT_7H1_AMT_7 AS PBP_D_DIFF_DEDUCT_7H1_AMT,
        PBP_D_DIFF_DEDUCT_8A1_AMT_8 AS PBP_D_DIFF_DEDUCT_8A1_AMT,
        PBP_D_DIFF_DEDUCT_8A2_AMT_9 AS PBP_D_DIFF_DEDUCT_8A2_AMT,
        PBP_D_DIFF_DEDUCT_8B1_AMT_10 AS PBP_D_DIFF_DEDUCT_8B1_AMT,
        PBP_D_DIFF_DEDUCT_8B2_AMT_11 AS PBP_D_DIFF_DEDUCT_8B2_AMT,
        PBP_D_DIFF_DEDUCT_8B3_AMT_12 AS PBP_D_DIFF_DEDUCT_8B3_AMT,
        PBP_D_DIFF_DEDUCT_15_2_AMT_13 AS PBP_D_DIFF_DEDUCT_15_2_AMT,
        PBP_D_DIFF_DEDUCT_15_3_AMT_14 AS PBP_D_DIFF_DEDUCT_15_3_AMT,
        PBP_D_DIFF_DEDUCT_16B1_N_AMT_15 AS PBP_D_DIFF_DEDUCT_16B1_N_AMT,
        PBP_D_DIFF_DEDUCT_16B4_N_AMT_16 AS PBP_D_DIFF_DEDUCT_16B4_N_AMT,
        PBP_D_DIFF_DEDUCT_16B5_N_AMT_17 AS PBP_D_DIFF_DEDUCT_16B5_N_AMT,
        PBP_D_DIFF_DEDUCT_16B2_N_AMT_18 AS PBP_D_DIFF_DEDUCT_16B2_N_AMT,

        case when 
          case when PBP_D_ANN_DEDUCT_BENS_20='1' then '1' else '0' end ||
          case when PBP_D_ANN_DEDUCT_BENS_21='1' then '1' else '0' end ||
          case when PBP_D_ANN_DEDUCT_BENS_19='1' then '1' else '0' end = '000' then ' ' else 
          case when PBP_D_ANN_DEDUCT_BENS_20='1' then '1' else '0' end ||
          case when PBP_D_ANN_DEDUCT_BENS_21='1' then '1' else '0' end ||
          case when PBP_D_ANN_DEDUCT_BENS_19='1' then '1' else '0' end
        end as PBP_D_ANN_DEDUCT_BENS,
        PBP_D_DEDUCT_IHP_AMT_25 as PBP_D_DEDUCT_IHP_AMT,
        PBP_D_DEDUCT_SNF_AMT_27 as PBP_D_DEDUCT_SNF_AMT,

        PBP_D_DIFF_DEDUCT_1A_T1_AMT_28 AS PBP_D_DIFF_DEDUCT_1A_T1_AMT,
        PBP_D_DIFF_DEDUCT_1A_T2_AMT_29 AS PBP_D_DIFF_DEDUCT_1A_T2_AMT,
        PBP_D_DIFF_DEDUCT_1A_T3_AMT_30 AS PBP_D_DIFF_DEDUCT_1A_T3_AMT,
        PBP_D_DIFF_DEDUCT_1B_T1_AMT_31 AS PBP_D_DIFF_DEDUCT_1B_T1_AMT,
        PBP_D_DIFF_DEDUCT_1B_T2_AMT_32 AS PBP_D_DIFF_DEDUCT_1B_T2_AMT,
        PBP_D_DIFF_DEDUCT_1B_T3_AMT_33 AS PBP_D_DIFF_DEDUCT_1B_T3_AMT,
        PBP_D_DIFF_DEDUCT_2_T1_AMT_34 AS PBP_D_DIFF_DEDUCT_2_T1_AMT,
        PBP_D_DIFF_DEDUCT_2_T2_AMT_35 AS PBP_D_DIFF_DEDUCT_2_T2_AMT,
        PBP_D_DIFF_DEDUCT_2_T3_AMT_36 AS PBP_D_DIFF_DEDUCT_2_T3_AMT,
        PBP_D_DIFF_DEDUCT_6_AMT_37 AS PBP_D_DIFF_DEDUCT_6_AMT,
        PBP_D_DIFF_DEDUCT_7A_AMT_38 AS PBP_D_DIFF_DEDUCT_7A_AMT,
        PBP_D_DIFF_DEDUCT_7B_AMT_39 AS PBP_D_DIFF_DEDUCT_7B_AMT,
        PBP_D_DIFF_DEDUCT_7C_AMT_40 AS PBP_D_DIFF_DEDUCT_7C_AMT,
        PBP_D_DIFF_DEDUCT_7D_AMT_41 AS PBP_D_DIFF_DEDUCT_7D_AMT,
        PBP_D_DIFF_DEDUCT_7F_AMT_42 AS PBP_D_DIFF_DEDUCT_7F_AMT,
        PBP_D_DIFF_DEDUCT_7G_AMT_43 AS PBP_D_DIFF_DEDUCT_7G_AMT,
        PBP_D_DIFF_DEDUCT_7I_AMT_44 AS PBP_D_DIFF_DEDUCT_7I_AMT,
        PBP_D_DIFF_DEDUCT_7K_AMT_45 AS PBP_D_DIFF_DEDUCT_7K_AMT,
        PBP_D_DIFF_DEDUCT_9A1_AMT_46 AS PBP_D_DIFF_DEDUCT_9A1_AMT,
        PBP_D_DIFF_DEDUCT_9A2_AMT_47 AS PBP_D_DIFF_DEDUCT_9A2_AMT,
        PBP_D_DIFF_DEDUCT_9B_AMT_48 AS PBP_D_DIFF_DEDUCT_9B_AMT,
        PBP_D_DIFF_DEDUCT_10A1_AMT_49 AS PBP_D_DIFF_DEDUCT_10A1_AMT,
        PBP_D_DIFF_DEDUCT_10A2_AMT_50 AS PBP_D_DIFF_DEDUCT_10A2_AMT,
        PBP_D_DIFF_DEDUCT_11A_AMT_51 AS PBP_D_DIFF_DEDUCT_11A_AMT,
        PBP_D_DIFF_DEDUCT_11B_AMT_52 AS PBP_D_DIFF_DEDUCT_11B_AMT,
        PBP_D_DIFF_DEDUCT_14E2_AMT_53 AS PBP_D_DIFF_DEDUCT_14E2_AMT,
        PBP_D_DIFF_DEDUCT_16A_N_AMT_56 AS PBP_D_DIFF_DEDUCT_16A_N_AMT,
        PBP_D_DIFF_DEDUCT_17A_AMT_64 AS PBP_D_DIFF_DEDUCT_17A_AMT,
        PBP_D_DIFF_DEDUCT_18A_AMT_65 AS PBP_D_DIFF_DEDUCT_18A_AMT,
        PBP_D_DEDUCT_IHA_AMT_68 as PBP_D_DEDUCT_IHA_AMT,
        PBP_D_DIFF_DEDUCT_11B1_AMT_69 AS PBP_D_DIFF_DEDUCT_11B1_AMT,
        PBP_D_DIFF_DEDUCT_11B2_AMT_70 AS PBP_D_DIFF_DEDUCT_11B2_AMT,
        PBP_D_MSA_ANNUAL_AMT_71 AS PBP_D_MSA_ANNUAL_AMT,
        PBP_D_NON_DEDUCT_YN_74 AS PBP_D_NON_DEDUCT_YN,
        PBP_D_INN_DEDUCT_AMT_75 AS PBP_D_INN_DEDUCT_AMT,
        case when 
            case when PBP_D_NON_DEDUCT_BENS_76='1' then '1' else '0' end || 
            case when PBP_D_NON_DEDUCT_BENS_91='1' then '1' else '0' end = '00' then ' ' else 
            case when PBP_D_NON_DEDUCT_BENS_76='1' then '1' else '0' end || 
            case when PBP_D_NON_DEDUCT_BENS_91='1' then '1' else '0' end 
        end as PBP_D_NON_DEDUCT_BENS,
        PBP_D_COMB_DEDUCT_INN_M_YN_77 AS PBP_D_COMB_DEDUCT_INN_M_YN,
        PBP_D_NON_DEDUCT_NM_YN_78 AS PBP_D_NON_DEDUCT_NM_YN,
        PBP_D_COMB_DEDUCT_YN_80 AS PBP_D_COMB_DEDUCT_YN,
        PBP_D_NON_DEDUCT_AMT_81 AS PBP_D_NON_DEDUCT_AMT,
        PBP_D_COMB_DEDUCT_PARTB_YN_82 AS PBP_D_COMB_DEDUCT_PARTB_YN,
        PBP_D_INN_DEDUCT_NM_YN_83 AS PBP_D_INN_DEDUCT_NM_YN,
        PBP_D_COMB_DEDUCT_INN_NM_YN_84 AS PBP_D_COMB_DEDUCT_INN_NM_YN,
        PBP_D_INN_DEDUCT_PARTB_YN_85 AS PBP_D_INN_DEDUCT_PARTB_YN,
        case when 
            case when PBP_D_COMB_DEDUCT_BENS_90='1' then '1' else '0' end || 
            case when PBP_D_COMB_DEDUCT_BENS_86='1' then '1' else '0' end || 
            case when PBP_D_COMB_DEDUCT_BENS_97='1' then '1' else '0' end || 
            case when PBP_D_COMB_DEDUCT_BENS_94='1' then '1' else '0' end  = '0000' then ' ' else
            case when PBP_D_COMB_DEDUCT_BENS_90='1' then '1' else '0' end || 
            case when PBP_D_COMB_DEDUCT_BENS_86='1' then '1' else '0' end || 
            case when PBP_D_COMB_DEDUCT_BENS_97='1' then '1' else '0' end || 
            case when PBP_D_COMB_DEDUCT_BENS_94='1' then '1' else '0' end	  
        end as PBP_D_COMB_DEDUCT_BENS,
        PBP_D_NON_DEDUCT_M_YN_87 AS PBP_D_NON_DEDUCT_M_YN,
        PBP_D_INN_DEDUCT_YN_88 AS PBP_D_INN_DEDUCT_YN,
        PBP_D_NON_DEDUCT_PARTB_YN_89 AS PBP_D_NON_DEDUCT_PARTB_YN,
        case when
            case when PBP_D_INN_DEDUCT_BENS_99='1' then '1' else '0' end || 
            case when PBP_D_INN_DEDUCT_BENS_92='1' then '1' else '0' end = '00' then ' ' else
            case when PBP_D_INN_DEDUCT_BENS_99='1' then '1' else '0' end || 
            case when PBP_D_INN_DEDUCT_BENS_92='1' then '1' else '0' end
        end as PBP_D_INN_DEDUCT_BENS,
        PBP_D_COMB_DEDUCT_AMT_93 AS PBP_D_COMB_DEDUCT_AMT,
        PBP_D_INN_DEDUCT_M_YN_95 AS PBP_D_INN_DEDUCT_M_YN,
        PBP_D_ANN_DEDUCT_INN_NMC__101 AS PBP_D_ANN_DEDUCT_INN_NMC_CATS length=4000,
        PBP_D_ANN_DEDUCT_AMT_TYPE_102 AS PBP_D_ANN_DEDUCT_AMT_TYPE,
        tranwrd(strip(PBP_D_ANN_DEDUCT_INN_MC__103), ',', ';') || case when PBP_D_ANN_DEDUCT_INN_MC__103 ^= ' ' then ';' else '' end AS PBP_D_ANN_DEDUCT_INN_MC_CATS length=4000,
        PBP_D_ANN_DEDUCT_INN_MC_YN_104 AS PBP_D_ANN_DEDUCT_INN_MC_YN,
        PBP_D_ANN_DEDUCT_YN_105 AS PBP_D_ANN_DEDUCT_YN,
        PBP_D_ANN_DEDUCT_AMT_106 AS PBP_D_ANN_DEDUCT_AMT,
        put(PBP_D_ANN_DEDUCT_COMB_TYPE_107, 1.) AS PBP_D_ANN_DEDUCT_COMB_TYPE,
        PBP_D_ANN_DEDUCT_INN_NM_108 AS PBP_D_ANN_DEDUCT_INN_NMC_YN,
        coalesce (
          case when
            case when PBP_D_COMB_MAX_ENR_OOPC__116=1 then '1' else '0' end || 
            case when PBP_D_COMB_MAX_ENR_OOPC__118=1 then '1' else '0' end || 
            case when PBP_D_COMB_MAX_ENR_OOPC__111=1 then '1' else '0' end || 
            case when PBP_D_COMB_MAX_ENR_OOPC__110=1 then '1' else '0' end = '0000' then ' ' else 
            case when PBP_D_COMB_MAX_ENR_OOPC__116=1 then '1' else '0' end || 
            case when PBP_D_COMB_MAX_ENR_OOPC__118=1 then '1' else '0' end || 
            case when PBP_D_COMB_MAX_ENR_OOPC__111=1 then '1' else '0' end || 
            case when PBP_D_COMB_MAX_ENR_OOPC__110=1 then '1' else '0' end
          end,
          case when
            case when PBP_D_COMB_MAX_ENR_OOPC__127='1' then '1' else '0' end ||
            case when PBP_D_COMB_MAX_ENR_OOPC__137='1' then '1' else '0' end ||
            case when PBP_D_COMB_MAX_ENR_OOPC__125='1' then '1' else '0' end ||
            case when PBP_D_COMB_MAX_ENR_OOPC__128='1' then '1' else '0' end = '0000' then ' ' else
            case when PBP_D_COMB_MAX_ENR_OOPC__127='1' then '1' else '0' end ||
            case when PBP_D_COMB_MAX_ENR_OOPC__137='1' then '1' else '0' end ||
            case when PBP_D_COMB_MAX_ENR_OOPC__125='1' then '1' else '0' end ||
            case when PBP_D_COMB_MAX_ENR_OOPC__128='1' then '1' else '0' end
          end
        ) as PBP_D_COMB_MAX_ENR_OOPC_BENS,
        coalesce(PBP_D_INN_MAX_ENR_M_YN_131, PBP_D_INN_MAX_ENR_M_YN_112) AS PBP_D_INN_MAX_ENR_M_YN,

        case
            when PBP_D_INN_MAX_ENR_OOPC__113 = '1' or PBP_D_INN_MAX_ENR_OOPC__123 = '1' then
                case when PBP_D_INN_MAX_ENR_OOPC__113 = '1' then '1' else '0' end || case when PBP_D_INN_MAX_ENR_OOPC__123 = '1' then '1' else '0' end 
            when PBP_D_INN_MAX_ENR_OOPC__133 = '1' or PBP_D_INN_MAX_ENR_OOPC__146 = '1' then
                case when PBP_D_INN_MAX_ENR_OOPC__133 = '1' then '1' else '0' end || case when PBP_D_INN_MAX_ENR_OOPC__146 = '1' then '1' else '0' end 
            else
                '00'
        end as PBP_D_INN_MAX_ENR_OOPC_BENS,
        coalesce(PBP_D_INN_MAX_ENR_NM_YN_114, PBP_D_INN_MAX_ENR_NM_YN_145) AS PBP_D_INN_MAX_ENR_NM_YN,
        coalesce(PBP_D_COMB_MAX_ENR_AMT_115, PBP_D_COMB_MAX_ENR_AMT_134) AS PBP_D_COMB_MAX_ENR_AMT,
        coalesce(PBP_D_COMB_MAX_ENR_INN_N_117, PBP_D_COMB_MAX_ENR_INN_N_142) AS PBP_D_COMB_MAX_ENR_INN_NM_YN,
        coalesce(PBP_D_COMB_MAX_ENR_INN__119, PBP_D_COMB_MAX_ENR_INN__132) AS PBP_D_COMB_MAX_ENR_INN_M_YN,
        coalesce(PBP_D_OUT_POCKET_AMT_121, PBP_D_OUT_POCKET_AMT_138) as PBP_D_OUT_POCKET_AMT,
        coalesce(PBP_D_OUT_POCKET_AMT_YN_124, PBP_D_OUT_POCKET_AMT_YN_129) AS PBP_D_OUT_POCKET_AMT_YN,
        PBP_D_MAXENR_OOPC_NM_CAT_130 AS PBP_D_MAXENR_OOPC_NM_CATS_YN,
        PBP_D_MAXENR_OOPC_AMT_136 AS PBP_D_MAXENR_OOPC_AMT,
        case when
            case when PBP_D_MAXENR_OOPC_BENS_141='1' then '1' else '0' end || 
            case when PBP_D_MAXENR_OOPC_BENS_140='1' then '1' else '0' end = '00' then ' ' else
            case when PBP_D_MAXENR_OOPC_BENS_141='1' then '1' else '0' end || 
            case when PBP_D_MAXENR_OOPC_BENS_140='1' then '1' else '0' end
        end as PBP_D_MAXENR_OOPC_BENS,
        PBP_D_MAXENR_OOPC_M_CAT_143 AS PBP_D_MAXENR_OOPC_M_CATS_YN,
        PBP_D_DIFF_DEDUCT_12_AMT as PBP_D_DIFF_DEDUCT_12_AMT,
        PBP_D_DIFF_DEDUCT_16C1_N_AMT_148 AS PBP_D_DIFF_DEDUCT_16C1_N_AMT,
        PBP_D_DIFF_DEDUCT_16C2_N_AMT_149 AS PBP_D_DIFF_DEDUCT_16C2_N_AMT,
        PBP_D_DIFF_DEDUCT_16C3_N_AMT_150 AS PBP_D_DIFF_DEDUCT_16C3_N_AMT,
        PBP_D_DIFF_DEDUCT_16C7_N_AMT_151 AS PBP_D_DIFF_DEDUCT_16C7_N_AMT,
        PBP_D_DIFF_DEDUCT_16C8_N_AMT_152 AS PBP_D_DIFF_DEDUCT_16C8_N_AMT,
        PBP_D_DIFF_DEDUCT_16C9_N_AMT_153 AS PBP_D_DIFF_DEDUCT_16C9_N_AMT
    FROM
        (
        SELECT
            qid,
            MAX(PBP_D_MSA_DED_AMT_0) AS PBP_D_MSA_DED_AMT_0,
            MAX(PBP_D_NMC_DEDUCT_YN_1) AS PBP_D_NMC_DEDUCT_YN_1,
            MAX(PBP_D_DIFF_DEDUCT_YN_2) AS PBP_D_DIFF_DEDUCT_YN_2,
            MAX(PBP_D_DIFF_DEDUCT_3_1_AMT_3) AS PBP_D_DIFF_DEDUCT_3_1_AMT_3,
            MAX(PBP_D_DIFF_DEDUCT_3_3_AMT_4) AS PBP_D_DIFF_DEDUCT_3_3_AMT_4,
            MAX(PBP_D_DIFF_DEDUCT_3_4_AMT_5) AS PBP_D_DIFF_DEDUCT_3_4_AMT_5,
            MAX(PBP_D_DIFF_DEDUCT_7E1_AMT_6) AS PBP_D_DIFF_DEDUCT_7E1_AMT_6,
            MAX(PBP_D_DIFF_DEDUCT_7H1_AMT_7) AS PBP_D_DIFF_DEDUCT_7H1_AMT_7
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                plaLevCosSha_plaLevCosShaDet.csbAnnualMsaDeducAmt AS PBP_D_MSA_DED_AMT_0,
                mandaSupplBenef_manSupBenDet.mandSupBenYesNo AS PBP_D_NMC_DEDUCT_YN_1,
                differeDeducti_diffeDeducDetai.diffServCatLevelYesDed AS PBP_D_DIFF_DEDUCT_YN_2,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('3-1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_3_1_AMT_3,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('3-3') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_3_3_AMT_4,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('3-4') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_3_4_AMT_5,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7e1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7E1_AMT_6,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7h1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7H1_AMT_7
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_plaLevCosShaDet AS plaLevCosSha_plaLevCosShaDet ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_plaLevCosShaDet.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.plaLevCosSha_mandaSupplBenef AS plaLevCosSha_mandaSupplBenef ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_mandaSupplBenef.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.mandaSupplBenef_manSupBenDet AS mandaSupplBenef_manSupBenDet ON
                plaLevCosSha_mandaSupplBenef.ordinal_mandaSupplBenef = mandaSupplBenef_manSupBenDet.ordinal_mandaSupplBenef
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_0
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_8A1_AMT_8) AS PBP_D_DIFF_DEDUCT_8A1_AMT_8,
            MAX(PBP_D_DIFF_DEDUCT_8A2_AMT_9) AS PBP_D_DIFF_DEDUCT_8A2_AMT_9,
            MAX(PBP_D_DIFF_DEDUCT_8B1_AMT_10) AS PBP_D_DIFF_DEDUCT_8B1_AMT_10,
            MAX(PBP_D_DIFF_DEDUCT_8B2_AMT_11) AS PBP_D_DIFF_DEDUCT_8B2_AMT_11,
            MAX(PBP_D_DIFF_DEDUCT_8B3_AMT_12) AS PBP_D_DIFF_DEDUCT_8B3_AMT_12,
            MAX(PBP_D_DIFF_DEDUCT_15_2_AMT_13) AS PBP_D_DIFF_DEDUCT_15_2_AMT_13,
            MAX(PBP_D_DIFF_DEDUCT_15_3_AMT_14) AS PBP_D_DIFF_DEDUCT_15_3_AMT_14,
            MAX(PBP_D_DIFF_DEDUCT_16B1_N_AMT_15) AS PBP_D_DIFF_DEDUCT_16B1_N_AMT_15
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('8a1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_8A1_AMT_8,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('8a2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_8A2_AMT_9,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('8b1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_8B1_AMT_10,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('8b2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_8B2_AMT_11,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('8b3') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_8B3_AMT_12,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('15-2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_15_2_AMT_13,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('15-3') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_15_3_AMT_14,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16b1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16B1_N_AMT_15
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_1 ON
        segment_0.qid = segment_1.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_16B4_N_AMT_16) AS PBP_D_DIFF_DEDUCT_16B4_N_AMT_16,
            MAX(PBP_D_DIFF_DEDUCT_16B5_N_AMT_17) AS PBP_D_DIFF_DEDUCT_16B5_N_AMT_17,
            MAX(PBP_D_DIFF_DEDUCT_16B2_N_AMT_18) AS PBP_D_DIFF_DEDUCT_16B2_N_AMT_18,
            MAX(PBP_D_ANN_DEDUCT_BENS_19) AS PBP_D_ANN_DEDUCT_BENS_19,
            MAX(PBP_D_ANN_DEDUCT_BENS_20) AS PBP_D_ANN_DEDUCT_BENS_20,
            MAX(PBP_D_ANN_DEDUCT_BENS_21) AS PBP_D_ANN_DEDUCT_BENS_21
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16b4') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16B4_N_AMT_16,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16b5') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16B5_N_AMT_17,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16b2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16B2_N_AMT_18,
                lppoRppoDeduc_lppRppDedDet.inNetworkNonMedCovBenefits AS PBP_D_ANN_DEDUCT_BENS_19,
                lppoRppoDeduc_lppRppDedDet.outNetNonMedCovBenefits AS PBP_D_ANN_DEDUCT_BENS_20,
                lppoRppoDeduc_lppRppDedDet.inNetworkMedCovBenefits AS PBP_D_ANN_DEDUCT_BENS_21
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai
            LEFT JOIN jsonlib.plaLevCosSha_lppoRppoDeduc AS plaLevCosSha_lppoRppoDeduc ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lppoRppoDeduc.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lppoRppoDeduc_lppRppDedDet AS lppoRppoDeduc_lppRppDedDet ON
                plaLevCosSha_lppoRppoDeduc.ordinal_lppoRppoDeduc = lppoRppoDeduc_lppRppDedDet.ordinal_lppoRppoDeduc
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiD_mecNonMedSrvCtg AS lpRpMaEnCosLiD_mecNonMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiD_mecNonMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet ) main
        GROUP BY
            qid) segment_2 ON
        segment_0.qid = segment_2.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DEDUCT_IHP_AMT_25) AS PBP_D_DEDUCT_IHP_AMT_25,
            MAX(PBP_D_DEDUCT_SNF_AMT_27) AS PBP_D_DEDUCT_SNF_AMT_27,
            MAX(PBP_D_DIFF_DEDUCT_1A_T1_AMT_28) AS PBP_D_DIFF_DEDUCT_1A_T1_AMT_28,
            MAX(PBP_D_DIFF_DEDUCT_1A_T2_AMT_29) AS PBP_D_DIFF_DEDUCT_1A_T2_AMT_29,
            MAX(PBP_D_DIFF_DEDUCT_1A_T3_AMT_30) AS PBP_D_DIFF_DEDUCT_1A_T3_AMT_30,
            MAX(PBP_D_DIFF_DEDUCT_1B_T1_AMT_31) AS PBP_D_DIFF_DEDUCT_1B_T1_AMT_31
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                    CASE
                        WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('1b')
                            AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN manSupBenDet_mandaEnhanBenef.deductibleAmount
                            ELSE ''
                        END AS PBP_D_DEDUCT_IHP_AMT_25,
                            CASE
                                WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('2')
                                    AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN manSupBenDet_mandaEnhanBenef.deductibleAmount
                                    ELSE ''
                                END AS PBP_D_DEDUCT_SNF_AMT_27,
                                CASE
                                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('1a') THEN diffeDeducDetai_tierDeducValue.tier1DedAmt
                                    ELSE ''
                                END AS PBP_D_DIFF_DEDUCT_1A_T1_AMT_28,
                                CASE
                                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('1a') THEN diffeDeducDetai_tierDeducValue.tier2DedAmt
                                    ELSE ''
                                END AS PBP_D_DIFF_DEDUCT_1A_T2_AMT_29,
                                CASE
                                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('1a') THEN diffeDeducDetai_tierDeducValue.tier3DedAmt
                                    ELSE ''
                                END AS PBP_D_DIFF_DEDUCT_1A_T3_AMT_30,
                                CASE
                                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('1b') THEN diffeDeducDetai_tierDeducValue.tier1DedAmt
                                    ELSE ''
                                END AS PBP_D_DIFF_DEDUCT_1B_T1_AMT_31
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
                                mandaSupplBenef_manSupBenDet.ordinal_manSupBenDet = manSupBenDet_mandaEnhanBenef.ordinal_manSupBenDet
                            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
                            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
                            LEFT JOIN jsonlib.diffeDeducDetai_tierDeducValue AS diffeDeducDetai_tierDeducValue ON
                                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDetai_tierDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_3 ON
        segment_0.qid = segment_3.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_1B_T2_AMT_32) AS PBP_D_DIFF_DEDUCT_1B_T2_AMT_32,
            MAX(PBP_D_DIFF_DEDUCT_1B_T3_AMT_33) AS PBP_D_DIFF_DEDUCT_1B_T3_AMT_33,
            MAX(PBP_D_DIFF_DEDUCT_2_T1_AMT_34) AS PBP_D_DIFF_DEDUCT_2_T1_AMT_34,
            MAX(PBP_D_DIFF_DEDUCT_2_T2_AMT_35) AS PBP_D_DIFF_DEDUCT_2_T2_AMT_35,
            MAX(PBP_D_DIFF_DEDUCT_2_T3_AMT_36) AS PBP_D_DIFF_DEDUCT_2_T3_AMT_36,
            MAX(PBP_D_DIFF_DEDUCT_6_AMT_37) AS PBP_D_DIFF_DEDUCT_6_AMT_37,
            MAX(PBP_D_DIFF_DEDUCT_7A_AMT_38) AS PBP_D_DIFF_DEDUCT_7A_AMT_38,
            MAX(PBP_D_DIFF_DEDUCT_7B_AMT_39) AS PBP_D_DIFF_DEDUCT_7B_AMT_39
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('1b') THEN diffeDeducDetai_tierDeducValue.tier2DedAmt
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_1B_T2_AMT_32,
                CASE
                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('1b') THEN diffeDeducDetai_tierDeducValue.tier3DedAmt
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_1B_T3_AMT_33,
                CASE
                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('2') THEN diffeDeducDetai_tierDeducValue.tier1DedAmt
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_2_T1_AMT_34,
                CASE
                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('2') THEN diffeDeducDetai_tierDeducValue.tier2DedAmt
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_2_T2_AMT_35,
                CASE
                    WHEN diffeDeducDetai_tierDeducValue.categoryCode IN('2') THEN diffeDeducDetai_tierDeducValue.tier3DedAmt
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_2_T3_AMT_36,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('6') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_6_AMT_37,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7a') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7A_AMT_38,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7b') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7B_AMT_39
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDetai_tierDeducValue AS diffeDeducDetai_tierDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDetai_tierDeducValue.ordinal_diffeDeducDetai
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_4 ON
        segment_0.qid = segment_4.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_7C_AMT_40) AS PBP_D_DIFF_DEDUCT_7C_AMT_40,
            MAX(PBP_D_DIFF_DEDUCT_7D_AMT_41) AS PBP_D_DIFF_DEDUCT_7D_AMT_41,
            MAX(PBP_D_DIFF_DEDUCT_7F_AMT_42) AS PBP_D_DIFF_DEDUCT_7F_AMT_42,
            MAX(PBP_D_DIFF_DEDUCT_7G_AMT_43) AS PBP_D_DIFF_DEDUCT_7G_AMT_43,
            MAX(PBP_D_DIFF_DEDUCT_7I_AMT_44) AS PBP_D_DIFF_DEDUCT_7I_AMT_44,
            MAX(PBP_D_DIFF_DEDUCT_7K_AMT_45) AS PBP_D_DIFF_DEDUCT_7K_AMT_45,
            MAX(PBP_D_DIFF_DEDUCT_9A1_AMT_46) AS PBP_D_DIFF_DEDUCT_9A1_AMT_46,
            MAX(PBP_D_DIFF_DEDUCT_9A2_AMT_47) AS PBP_D_DIFF_DEDUCT_9A2_AMT_47
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7c') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount                     
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7C_AMT_40,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7d') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7D_AMT_41,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7f') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7F_AMT_42,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7g') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7G_AMT_43,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7i') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7I_AMT_44,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('7k') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_7K_AMT_45,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('9a1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount  
                    ELSE ''     
                END AS PBP_D_DIFF_DEDUCT_9A1_AMT_46,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('9a2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_9A2_AMT_47
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_5 ON
        segment_0.qid = segment_5.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_9B_AMT_48) AS PBP_D_DIFF_DEDUCT_9B_AMT_48,
            MAX(PBP_D_DIFF_DEDUCT_10A1_AMT_49) AS PBP_D_DIFF_DEDUCT_10A1_AMT_49,
            MAX(PBP_D_DIFF_DEDUCT_10A2_AMT_50) AS PBP_D_DIFF_DEDUCT_10A2_AMT_50,
            MAX(PBP_D_DIFF_DEDUCT_11A_AMT_51) AS PBP_D_DIFF_DEDUCT_11A_AMT_51,
            MAX(PBP_D_DIFF_DEDUCT_11B_AMT_52) AS PBP_D_DIFF_DEDUCT_11B_AMT_52,
            MAX(PBP_D_DIFF_DEDUCT_14E2_AMT_53) AS PBP_D_DIFF_DEDUCT_14E2_AMT_53
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('9b') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_9B_AMT_48,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('10a1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_10A1_AMT_49,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('10a2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_10A2_AMT_50,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('11a') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_11A_AMT_51,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('11b1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_11B_AMT_52,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('14e2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_14E2_AMT_53
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_6 ON
        segment_0.qid = segment_6.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_16A_N_AMT_56) AS PBP_D_DIFF_DEDUCT_16A_N_AMT_56
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16a') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16A_N_AMT_56
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_7 ON
        segment_0.qid = segment_7.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_17A_AMT_64) AS PBP_D_DIFF_DEDUCT_17A_AMT_64,
            MAX(PBP_D_DIFF_DEDUCT_18A_AMT_65) AS PBP_D_DIFF_DEDUCT_18A_AMT_65,
            MAX(PBP_D_DEDUCT_IHA_AMT_68) AS PBP_D_DEDUCT_IHA_AMT_68,
            MAX(PBP_D_DIFF_DEDUCT_11B1_AMT_69) AS PBP_D_DIFF_DEDUCT_11B1_AMT_69,
            MAX(PBP_D_DIFF_DEDUCT_11B2_AMT_70) AS PBP_D_DIFF_DEDUCT_11B2_AMT_70,
            MAX(PBP_D_DIFF_DEDUCT_12_AMT) AS PBP_D_DIFF_DEDUCT_12_AMT
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('17a') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_17A_AMT_64,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('18a') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_18A_AMT_65,
                        CASE
                            WHEN manSupBenDet_mandaEnhanBenef.categoryCode IN('1a')
                                AND manSupBenDet_mandaEnhanBenef.categoryTypeId = 2 THEN manSupBenDet_mandaEnhanBenef.deductibleAmount
                                ELSE ''
                            END AS PBP_D_DEDUCT_IHA_AMT_68,
                            CASE
                                WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('11b1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                                ELSE ''
                            END AS PBP_D_DIFF_DEDUCT_11B1_AMT_69,
                            CASE
                                WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('11b2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                                ELSE ''
                            END AS PBP_D_DIFF_DEDUCT_11B2_AMT_70,
                            CASE
                                WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('12') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                                ELSE ''
                            END AS pbp_d_diff_deduct_12_amt
                        FROM
                            jsonlib.root
                        LEFT JOIN jsonlib.pbp AS pbp ON
                            root.ordinal_root = pbp.ordinal_root
                        LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                            pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
                        LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                            pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
                        LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                            plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
                        LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                            differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai
                        LEFT JOIN jsonlib.plaLevCosSha_mandaSupplBenef AS plaLevCosSha_mandaSupplBenef ON
                            pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_mandaSupplBenef.ordinal_plaLevCosSha
                        LEFT JOIN jsonlib.mandaSupplBenef_manSupBenDet AS mandaSupplBenef_manSupBenDet ON
                            plaLevCosSha_mandaSupplBenef.ordinal_mandaSupplBenef = mandaSupplBenef_manSupBenDet.ordinal_mandaSupplBenef
                        LEFT JOIN jsonlib.manSupBenDet_mandaEnhanBenef AS manSupBenDet_mandaEnhanBenef ON
                            mandaSupplBenef_manSupBenDet.ordinal_manSupBenDet = manSupBenDet_mandaEnhanBenef.ordinal_manSupBenDet ) main
        GROUP BY
            qid) segment_8 ON
        segment_0.qid = segment_8.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_MSA_ANNUAL_AMT_71) AS PBP_D_MSA_ANNUAL_AMT_71,
            MAX(PBP_D_NON_DEDUCT_YN_74) AS PBP_D_NON_DEDUCT_YN_74,
            MAX(PBP_D_INN_DEDUCT_AMT_75) AS PBP_D_INN_DEDUCT_AMT_75,
            MAX(PBP_D_NON_DEDUCT_BENS_76) AS PBP_D_NON_DEDUCT_BENS_76,
            MAX(PBP_D_COMB_DEDUCT_INN_M_YN_77) AS PBP_D_COMB_DEDUCT_INN_M_YN_77
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                plaLevCosSha_plaLevCosShaDet.csbAnnualCmsEnrolMsa AS PBP_D_MSA_ANNUAL_AMT_71,
                planDeductible_planDeducDetai.hasNonNetworkDed AS PBP_D_NON_DEDUCT_YN_74,
                planDeductible_planDeducDetai.inNetworkDedAmount AS PBP_D_INN_DEDUCT_AMT_75,
                planDeductible_planDeducDetai.nonNetNonMedCovBenChk AS PBP_D_NON_DEDUCT_BENS_76,
                planDeductible_planDeducDetai.combInnMedCovBenSlctAll AS PBP_D_COMB_DEDUCT_INN_M_YN_77
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_plaLevCosShaDet AS plaLevCosSha_plaLevCosShaDet ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_plaLevCosShaDet.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_medicarService AS planDeducDetai_medicarService ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_medicarService.ordinal_planDeducDetai ) main
        GROUP BY
            qid) segment_9 ON
        segment_0.qid = segment_9.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_NON_DEDUCT_NM_YN_78) AS PBP_D_NON_DEDUCT_NM_YN_78,
            MAX(PBP_D_COMB_DEDUCT_YN_80) AS PBP_D_COMB_DEDUCT_YN_80,
            MAX(PBP_D_NON_DEDUCT_AMT_81) AS PBP_D_NON_DEDUCT_AMT_81,
            MAX(PBP_D_COMB_DEDUCT_PARTB_YN_82) AS PBP_D_COMB_DEDUCT_PARTB_YN_82,
            MAX(PBP_D_INN_DEDUCT_NM_YN_83) AS PBP_D_INN_DEDUCT_NM_YN_83,
            MAX(PBP_D_COMB_DEDUCT_INN_NM_YN_84) AS PBP_D_COMB_DEDUCT_INN_NM_YN_84
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                planDeductible_planDeducDetai.nonNetNonMedCovBenSlctAll AS PBP_D_NON_DEDUCT_NM_YN_78,
                planDeductible_planDeducDetai.hasCombinedDed AS PBP_D_COMB_DEDUCT_YN_80,
                planDeductible_planDeducDetai.nonNetworkDedAmount AS PBP_D_NON_DEDUCT_AMT_81,
                planDeductible_planDeducDetai.combinedChrgPartBDed AS PBP_D_COMB_DEDUCT_PARTB_YN_82,
                planDeductible_planDeducDetai.innNonMedCovBenSlctAll AS PBP_D_INN_DEDUCT_NM_YN_83,
                planDeductible_planDeducDetai.combInnNonMedCovBenSlctAll AS PBP_D_COMB_DEDUCT_INN_NM_YN_84
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_nonMedicServi AS planDeducDetai_nonMedicServi ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_nonMedicServi.ordinal_planDeducDetai ) main
        GROUP BY
            qid) segment_10 ON
        segment_0.qid = segment_10.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_INN_DEDUCT_PARTB_YN_85) AS PBP_D_INN_DEDUCT_PARTB_YN_85,
            MAX(PBP_D_COMB_DEDUCT_BENS_86) AS PBP_D_COMB_DEDUCT_BENS_86,
            MAX(PBP_D_NON_DEDUCT_M_YN_87) AS PBP_D_NON_DEDUCT_M_YN_87,
            MAX(PBP_D_INN_DEDUCT_YN_88) AS PBP_D_INN_DEDUCT_YN_88,
            MAX(PBP_D_NON_DEDUCT_PARTB_YN_89) AS PBP_D_NON_DEDUCT_PARTB_YN_89,
            MAX(PBP_D_COMB_DEDUCT_BENS_90) AS PBP_D_COMB_DEDUCT_BENS_90,
            MAX(PBP_D_NON_DEDUCT_BENS_91) AS PBP_D_NON_DEDUCT_BENS_91
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                planDeductible_planDeducDetai.inNetworkChrgPartBDed AS PBP_D_INN_DEDUCT_PARTB_YN_85,
                planDeductible_planDeducDetai.combInnMedCovBenChk AS PBP_D_COMB_DEDUCT_BENS_86,
                planDeductible_planDeducDetai.nonNetMedCovBenSlctAll AS PBP_D_NON_DEDUCT_M_YN_87,
                planDeductible_planDeducDetai.hasInNetworkDed AS PBP_D_INN_DEDUCT_YN_88,
                planDeductible_planDeducDetai.nonNetworkChrgPartBDed AS PBP_D_NON_DEDUCT_PARTB_YN_89,
                planDeductible_planDeducDetai.combOonNonMedCovBenChk AS PBP_D_COMB_DEDUCT_BENS_90,
                planDeductible_planDeducDetai.nonNetMedCovBenChk AS PBP_D_NON_DEDUCT_BENS_91
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible ) main
        GROUP BY
            qid) segment_11 ON
        segment_0.qid = segment_11.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_INN_DEDUCT_BENS_92) AS PBP_D_INN_DEDUCT_BENS_92,
            MAX(PBP_D_COMB_DEDUCT_AMT_93) AS PBP_D_COMB_DEDUCT_AMT_93,
            MAX(PBP_D_COMB_DEDUCT_BENS_94) AS PBP_D_COMB_DEDUCT_BENS_94,
            MAX(PBP_D_INN_DEDUCT_M_YN_95) AS PBP_D_INN_DEDUCT_M_YN_95,
            MAX(PBP_D_COMB_DEDUCT_BENS_97) AS PBP_D_COMB_DEDUCT_BENS_97
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                planDeductible_planDeducDetai.innMedCovBenChk AS PBP_D_INN_DEDUCT_BENS_92,
                planDeductible_planDeducDetai.combinedDedAmount AS PBP_D_COMB_DEDUCT_AMT_93,
                planDeductible_planDeducDetai.combOonMedCovBenChk AS PBP_D_COMB_DEDUCT_BENS_94,
                planDeductible_planDeducDetai.innMedCovBenSlctAll AS PBP_D_INN_DEDUCT_M_YN_95,
                planDeductible_planDeducDetai.combInnNonMedCovBenChk AS PBP_D_COMB_DEDUCT_BENS_97
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_medicarService AS planDeducDetai_medicarService ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_medicarService.ordinal_planDeducDetai
            LEFT JOIN jsonlib.planDeducDetai_nonMedicServi AS planDeducDetai_nonMedicServi ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_nonMedicServi.ordinal_planDeducDetai ) main
        GROUP BY
            qid) segment_12 ON
        segment_0.qid = segment_12.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_INN_DEDUCT_BENS_99) AS PBP_D_INN_DEDUCT_BENS_99,
            MAX(PBP_D_ANN_DEDUCT_INN_NMC__101) AS PBP_D_ANN_DEDUCT_INN_NMC__101,
            MAX(PBP_D_ANN_DEDUCT_AMT_TYPE_102) AS PBP_D_ANN_DEDUCT_AMT_TYPE_102,
            MAX(PBP_D_ANN_DEDUCT_INN_MC__103) AS PBP_D_ANN_DEDUCT_INN_MC__103,
            MAX(PBP_D_ANN_DEDUCT_INN_MC_YN_104) AS PBP_D_ANN_DEDUCT_INN_MC_YN_104,
            MAX(PBP_D_ANN_DEDUCT_YN_105) AS PBP_D_ANN_DEDUCT_YN_105
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                planDeductible_planDeducDetai.innNonMedCovBenChk AS PBP_D_INN_DEDUCT_BENS_99,
                lppoRppoDeduc_lppRppDedDet.inNetNonMedCovSerCatSou AS PBP_D_ANN_DEDUCT_INN_NMC__101,
                lppoRppoDeduc_lppRppDedDet.annualPlnDedType AS PBP_D_ANN_DEDUCT_AMT_TYPE_102,
                lppoRppoDeduc_lppRppDedDet.inNetworkMedCovServCatSource AS PBP_D_ANN_DEDUCT_INN_MC__103,
                lppoRppoDeduc_lppRppDedDet.allInNetMedCovBenefits AS PBP_D_ANN_DEDUCT_INN_MC_YN_104,
                lppoRppoDeduc_lppRppDedDet.annualPlnYesDed AS PBP_D_ANN_DEDUCT_YN_105
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_planDeductible AS plaLevCosSha_planDeductible ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_planDeductible.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.planDeductible_planDeducDetai AS planDeductible_planDeducDetai ON
                plaLevCosSha_planDeductible.ordinal_planDeductible = planDeductible_planDeducDetai.ordinal_planDeductible
            LEFT JOIN jsonlib.planDeducDetai_nonMedicServi AS planDeducDetai_nonMedicServi ON
                planDeductible_planDeducDetai.ordinal_planDeducDetai = planDeducDetai_nonMedicServi.ordinal_planDeducDetai
            LEFT JOIN jsonlib.plaLevCosSha_lppoRppoDeduc AS plaLevCosSha_lppoRppoDeduc ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lppoRppoDeduc.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lppoRppoDeduc_lppRppDedDet AS lppoRppoDeduc_lppRppDedDet ON
                plaLevCosSha_lppoRppoDeduc.ordinal_lppoRppoDeduc = lppoRppoDeduc_lppRppDedDet.ordinal_lppoRppoDeduc ) main
        GROUP BY
            qid) segment_13 ON
        segment_0.qid = segment_13.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_ANN_DEDUCT_AMT_106) AS PBP_D_ANN_DEDUCT_AMT_106,
            MAX(PBP_D_ANN_DEDUCT_COMB_TYPE_107) AS PBP_D_ANN_DEDUCT_COMB_TYPE_107,
            MAX(PBP_D_ANN_DEDUCT_INN_NM_108) AS PBP_D_ANN_DEDUCT_INN_NM_108,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__110) AS PBP_D_COMB_MAX_ENR_OOPC__110,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__111) AS PBP_D_COMB_MAX_ENR_OOPC__111,
            MAX(PBP_D_INN_MAX_ENR_M_YN_112) AS PBP_D_INN_MAX_ENR_M_YN_112
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                lppoRppoDeduc_lppRppDedDet.otherTypeDedAmount AS PBP_D_ANN_DEDUCT_AMT_106,
                lppoRppoDeduc_lppRppDedDet.combinedMedPartABDed AS PBP_D_ANN_DEDUCT_COMB_TYPE_107,
                lppoRppoDeduc_lppRppDedDet.allInNetNonMedCovBenefits AS PBP_D_ANN_DEDUCT_INN_NM_108,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclOonCombMoopMedChk AS PBP_D_COMB_MAX_ENR_OOPC__110,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnCombMoopNonMedChk AS PBP_D_COMB_MAX_ENR_OOPC__111,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnMaxEnrlMed AS PBP_D_INN_MAX_ENR_M_YN_112
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_lppoRppoDeduc AS plaLevCosSha_lppoRppoDeduc ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lppoRppoDeduc.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lppoRppoDeduc_lppRppDedDet AS lppoRppoDeduc_lppRppDedDet ON
                plaLevCosSha_lppoRppoDeduc.ordinal_lppoRppoDeduc = lppoRppoDeduc_lppRppDedDet.ordinal_lppoRppoDeduc
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiDe_mecMedSrvCtg AS lpRpMaEnCosLiDe_mecMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiDe_mecMedSrvCtg.ordinal_lpRpMaEnCosLiDe ) main
        GROUP BY
            qid) segment_14 ON
        segment_0.qid = segment_14.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_INN_MAX_ENR_OOPC__113) AS PBP_D_INN_MAX_ENR_OOPC__113,
            MAX(PBP_D_INN_MAX_ENR_NM_YN_114) AS PBP_D_INN_MAX_ENR_NM_YN_114,
            MAX(PBP_D_COMB_MAX_ENR_AMT_115) AS PBP_D_COMB_MAX_ENR_AMT_115,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__116) AS PBP_D_COMB_MAX_ENR_OOPC__116,
            MAX(PBP_D_COMB_MAX_ENR_INN_N_117) AS PBP_D_COMB_MAX_ENR_INN_N_117,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__118) AS PBP_D_COMB_MAX_ENR_OOPC__118,
            MAX(PBP_D_COMB_MAX_ENR_INN__119) AS PBP_D_COMB_MAX_ENR_INN__119
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CATS(lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnMoopNonMedChk) AS PBP_D_INN_MAX_ENR_OOPC__113 length=4000,
                CATS(lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnMaxEnrlNonMed) AS PBP_D_INN_MAX_ENR_NM_YN_114 length=4000,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclCombMoopAmount AS PBP_D_COMB_MAX_ENR_AMT_115,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclOonCombMoopNonMedChk AS PBP_D_COMB_MAX_ENR_OOPC__116,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclCombInnMaxEnrlNonMed AS PBP_D_COMB_MAX_ENR_INN_N_117,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnCombMoopMedChk AS PBP_D_COMB_MAX_ENR_OOPC__118,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclCombInnMaxEnrlMed AS PBP_D_COMB_MAX_ENR_INN__119
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi ) main
        GROUP BY
            qid) segment_15 ON
        segment_0.qid = segment_15.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_OUT_POCKET_AMT_121) AS PBP_D_OUT_POCKET_AMT_121,
            MAX(PBP_D_INN_MAX_ENR_OOPC__123) AS PBP_D_INN_MAX_ENR_OOPC__123,
            MAX(PBP_D_OUT_POCKET_AMT_YN_124) AS PBP_D_OUT_POCKET_AMT_YN_124,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__125) AS PBP_D_COMB_MAX_ENR_OOPC__125
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnMoopAmount AS PBP_D_OUT_POCKET_AMT_121,
                CATS(lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnMoopMedChk) AS PBP_D_INN_MAX_ENR_OOPC__123 length=4000,
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.meclInnMoop AS PBP_D_OUT_POCKET_AMT_YN_124,
                maxEnrCosLim_maxEnrCosLimDet.combInnMoopNonMedCovBenChk AS PBP_D_COMB_MAX_ENR_OOPC__125
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_lpRpMaEnCosLi AS plaLevCosSha_lpRpMaEnCosLi ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_lpRpMaEnCosLi.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.lpRpMaEnCosLi_lpRpMaEnCosLiDe AS lpRpMaEnCosLi_lpRpMaEnCosLiDe ON
                plaLevCosSha_lpRpMaEnCosLi.ordinal_lpRpMaEnCosLi = lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLi
            LEFT JOIN jsonlib.lpRpMaEnCosLiDe_mecMedSrvCtg AS lpRpMaEnCosLiDe_mecMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiDe_mecMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            LEFT JOIN jsonlib.lpRpMaEnCosLiD_mecNonMedSrvCtg AS lpRpMaEnCosLiD_mecNonMedSrvCtg ON
                lpRpMaEnCosLi_lpRpMaEnCosLiDe.ordinal_lpRpMaEnCosLiDe = lpRpMaEnCosLiD_mecNonMedSrvCtg.ordinal_lpRpMaEnCosLiDe
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet ) main
        GROUP BY
            qid) segment_16 ON
        segment_0.qid = segment_16.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__127) AS PBP_D_COMB_MAX_ENR_OOPC__127,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__128) AS PBP_D_COMB_MAX_ENR_OOPC__128,
            MAX(PBP_D_OUT_POCKET_AMT_YN_129) AS PBP_D_OUT_POCKET_AMT_YN_129,
            MAX(PBP_D_MAXENR_OOPC_NM_CAT_130) AS PBP_D_MAXENR_OOPC_NM_CAT_130,
            MAX(PBP_D_INN_MAX_ENR_M_YN_131) AS PBP_D_INN_MAX_ENR_M_YN_131,
            MAX(PBP_D_COMB_MAX_ENR_INN__132) AS PBP_D_COMB_MAX_ENR_INN__132,
            MAX(PBP_D_INN_MAX_ENR_OOPC__133) AS PBP_D_INN_MAX_ENR_OOPC__133
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                maxEnrCosLim_maxEnrCosLimDet.combOonMoopNonMedCovBenChk AS PBP_D_COMB_MAX_ENR_OOPC__127,
                maxEnrCosLim_maxEnrCosLimDet.combOonMoopMedCovBenChk AS PBP_D_COMB_MAX_ENR_OOPC__128,
                maxEnrCosLim_maxEnrCosLimDet.inNWMoop AS PBP_D_OUT_POCKET_AMT_YN_129,
                CATS(maxEnrCosLim_maxEnrCosLimDet.nonNetMoopNonMedCovBenSlctAll) AS PBP_D_MAXENR_OOPC_NM_CAT_130 length=4000,
                maxEnrCosLim_maxEnrCosLimDet.innMoopMedCovBenSlctAll AS PBP_D_INN_MAX_ENR_M_YN_131,
                maxEnrCosLim_maxEnrCosLimDet.combInnMoopMedCovBenSlctAll AS PBP_D_COMB_MAX_ENR_INN__132,
                maxEnrCosLim_maxEnrCosLimDet.innMoopNonMedCovBenChk AS PBP_D_INN_MAX_ENR_OOPC__133
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim ) main
        GROUP BY
            qid) segment_17 ON
        segment_0.qid = segment_17.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_COMB_MAX_ENR_AMT_134) AS PBP_D_COMB_MAX_ENR_AMT_134,
            MAX(PBP_D_MAXENR_OOPC_AMT_136) AS PBP_D_MAXENR_OOPC_AMT_136,
            MAX(PBP_D_COMB_MAX_ENR_OOPC__137) AS PBP_D_COMB_MAX_ENR_OOPC__137,
            MAX(PBP_D_OUT_POCKET_AMT_138) AS PBP_D_OUT_POCKET_AMT_138,
            MAX(PBP_D_MAXENR_OOPC_BENS_140) AS PBP_D_MAXENR_OOPC_BENS_140
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                maxEnrCosLim_maxEnrCosLimDet.combinedMoopAmount AS PBP_D_COMB_MAX_ENR_AMT_134,
                CATS(maxEnrCosLim_maxEnrCosLimDet.nonNWMoopAmount) AS PBP_D_MAXENR_OOPC_AMT_136 length=4000,
                maxEnrCosLim_maxEnrCosLimDet.combInnMoopMedCovBenChk AS PBP_D_COMB_MAX_ENR_OOPC__137,
                maxEnrCosLim_maxEnrCosLimDet.inNWMoopAmount AS PBP_D_OUT_POCKET_AMT_138,
                maxEnrCosLim_maxEnrCosLimDet.nonNetMoopMedCovBenChk AS PBP_D_MAXENR_OOPC_BENS_140
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet
            LEFT JOIN jsonlib.maxEnrCosLimDet_nonMedicServi AS maxEnrCosLimDet_nonMedicServi ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_nonMedicServi.ordinal_maxEnrCosLimDet ) main
        GROUP BY
            qid) segment_18 ON
        segment_0.qid = segment_18.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_MAXENR_OOPC_BENS_141) AS PBP_D_MAXENR_OOPC_BENS_141,
            MAX(PBP_D_COMB_MAX_ENR_INN_N_142) AS PBP_D_COMB_MAX_ENR_INN_N_142,
            MAX(PBP_D_MAXENR_OOPC_M_CAT_143) AS PBP_D_MAXENR_OOPC_M_CAT_143,
            MAX(PBP_D_INN_MAX_ENR_NM_YN_145) AS PBP_D_INN_MAX_ENR_NM_YN_145,
            MAX(PBP_D_INN_MAX_ENR_OOPC__146) AS PBP_D_INN_MAX_ENR_OOPC__146
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                maxEnrCosLim_maxEnrCosLimDet.nonNetMoopNonMedCovBenChk AS PBP_D_MAXENR_OOPC_BENS_141,
                maxEnrCosLim_maxEnrCosLimDet.combInnMoopNonMedCovBenSlctAll AS PBP_D_COMB_MAX_ENR_INN_N_142,
                CATS(maxEnrCosLim_maxEnrCosLimDet.nonNetMoopMedCovBenSlctAll) AS PBP_D_MAXENR_OOPC_M_CAT_143 length=4000,
                maxEnrCosLim_maxEnrCosLimDet.innMoopNonMedCovBenSlctAll AS PBP_D_INN_MAX_ENR_NM_YN_145,
                maxEnrCosLim_maxEnrCosLimDet.innMoopMedCovBenChk AS PBP_D_INN_MAX_ENR_OOPC__146
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_maxEnrCosLim AS plaLevCosSha_maxEnrCosLim ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_maxEnrCosLim.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.maxEnrCosLim_maxEnrCosLimDet AS maxEnrCosLim_maxEnrCosLimDet ON
                plaLevCosSha_maxEnrCosLim.ordinal_maxEnrCosLim = maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLim
            LEFT JOIN jsonlib.maxEnrCosLimDet_medicarService AS maxEnrCosLimDet_medicarService ON
                maxEnrCosLim_maxEnrCosLimDet.ordinal_maxEnrCosLimDet = maxEnrCosLimDet_medicarService.ordinal_maxEnrCosLimDet ) main
        GROUP BY
            qid) segment_19 ON
        segment_0.qid = segment_19.qid
    LEFT JOIN (
        SELECT
            qid,
            MAX(PBP_D_DIFF_DEDUCT_16C1_N_AMT_148) AS PBP_D_DIFF_DEDUCT_16C1_N_AMT_148,
            MAX(PBP_D_DIFF_DEDUCT_16C2_N_AMT_149) AS PBP_D_DIFF_DEDUCT_16C2_N_AMT_149,
            MAX(PBP_D_DIFF_DEDUCT_16C3_N_AMT_150) AS PBP_D_DIFF_DEDUCT_16C3_N_AMT_150,
            MAX(PBP_D_DIFF_DEDUCT_16C7_N_AMT_151) AS PBP_D_DIFF_DEDUCT_16C7_N_AMT_151,
            MAX(PBP_D_DIFF_DEDUCT_16C8_N_AMT_152) AS PBP_D_DIFF_DEDUCT_16C8_N_AMT_152,
            MAX(PBP_D_DIFF_DEDUCT_16C9_N_AMT_153) AS PBP_D_DIFF_DEDUCT_16C9_N_AMT_153
        FROM
            (
            SELECT
                put(cat(contractId,
                planId,
                put(segmentId,
                z3.)),
                $11.) AS qid,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16c1') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16C1_N_AMT_148,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16c2') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16C2_N_AMT_149,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16c3') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16C3_N_AMT_150,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16c7') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16C7_N_AMT_151,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16c8') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16C8_N_AMT_152,
                CASE
                    WHEN diffeDeducDeta_diffeDeducValue.categoryCode IN('16c9') THEN diffeDeducDeta_diffeDeducValue.deductibleAmount
                    ELSE ''
                END AS PBP_D_DIFF_DEDUCT_16C9_N_AMT_153              
            FROM
                jsonlib.root
            LEFT JOIN jsonlib.pbp AS pbp ON
                root.ordinal_root = pbp.ordinal_root
            LEFT JOIN jsonlib.pbp_plaLevCosSha AS pbp_plaLevCosSha ON
                pbp.ordinal_pbp = pbp_plaLevCosSha.ordinal_pbp
            LEFT JOIN jsonlib.plaLevCosSha_differeDeducti AS plaLevCosSha_differeDeducti ON
                pbp_plaLevCosSha.ordinal_plaLevCosSha = plaLevCosSha_differeDeducti.ordinal_plaLevCosSha
            LEFT JOIN jsonlib.differeDeducti_diffeDeducDetai AS differeDeducti_diffeDeducDetai ON
                plaLevCosSha_differeDeducti.ordinal_differeDeducti = differeDeducti_diffeDeducDetai.ordinal_differeDeducti
            LEFT JOIN jsonlib.diffeDeducDeta_diffeDeducValue AS diffeDeducDeta_diffeDeducValue ON
                differeDeducti_diffeDeducDetai.ordinal_diffeDeducDetai = diffeDeducDeta_diffeDeducValue.ordinal_diffeDeducDetai ) main
        GROUP BY
            qid) segment_20 ON
        segment_0.qid = segment_20.qid
) main
LEFT JOIN
    jsonlib.PBP_D_COMB_DEDUCT_INN_M_CATS AS PBP_D_COMB_DEDUCT_INN_M_CATS ON
        main.qid = PBP_D_COMB_DEDUCT_INN_M_CATS.qid
LEFT JOIN
    jsonlib.PBP_D_COMB_DEDUCT_INN_NM_CATS AS PBP_D_COMB_DEDUCT_INN_NM_CATS ON
        main.qid = PBP_D_COMB_DEDUCT_INN_NM_CATS.qid

LEFT JOIN 
    jsonlib.PBP_D_COMB_MAX_ENR_INN_MCA_120 AS PBP_D_COMB_MAX_ENR_INN_MCA_120 ON
        main.qid = PBP_D_COMB_MAX_ENR_INN_MCA_120.qid
LEFT JOIN 
    jsonlib.PBP_D_COMB_MAX_ENR_INN_MCA_135 AS PBP_D_COMB_MAX_ENR_INN_MCA_135 ON
        main.qid = PBP_D_COMB_MAX_ENR_INN_MCA_135.qid

LEFT JOIN
    jsonlib.PBP_D_COMB_MAX_ENR_INN_NMCA_22 AS PBP_D_COMB_MAX_ENR_INN_NMCA_22 ON
        main.qid = PBP_D_COMB_MAX_ENR_INN_NMCA_22.qid
LEFT JOIN
    jsonlib.PBP_D_COMB_MAX_ENR_INN_NMCA_23 AS PBP_D_COMB_MAX_ENR_INN_NMCA_23 ON
        main.qid = PBP_D_COMB_MAX_ENR_INN_NMCA_23.qid        
        
        
LEFT JOIN
    jsonlib.PBP_D_INN_DEDUCT_M_CATS AS PBP_D_INN_DEDUCT_M_CATS ON
        main.qid = PBP_D_INN_DEDUCT_M_CATS.qid
LEFT JOIN
    jsonlib.PBP_D_INN_DEDUCT_NM_CATS AS PBP_D_INN_DEDUCT_NM_CATS ON
        main.qid = PBP_D_INN_DEDUCT_NM_CATS.qid


LEFT JOIN
    jsonlib.PBP_D_INN_MAX_ENR_M_CAT_EX_109 AS PBP_D_INN_MAX_ENR_M_CAT_EX_109 ON
        main.qid = PBP_D_INN_MAX_ENR_M_CAT_EX_109.qid
LEFT JOIN
    jsonlib.PBP_D_INN_MAX_ENR_M_CAT_EX_126 AS PBP_D_INN_MAX_ENR_M_CAT_EX_126 ON
        main.qid = PBP_D_INN_MAX_ENR_M_CAT_EX_126.qid

LEFT JOIN
    jsonlib.PBP_D_INN_MAX_ENR_NM_CA_122 AS PBP_D_INN_MAX_ENR_NM_CA_122 ON
        main.qid = PBP_D_INN_MAX_ENR_NM_CA_122.qid
LEFT JOIN
    jsonlib.PBP_D_INN_MAX_ENR_NM_CA_147 AS PBP_D_INN_MAX_ENR_NM_CA_147 ON
        main.qid = PBP_D_INN_MAX_ENR_NM_CA_147.qid
        
LEFT JOIN
    jsonlib.PBP_D_MAXENR_OOPC_M_CATS AS PBP_D_MAXENR_OOPC_M_CATS ON
        main.qid = PBP_D_MAXENR_OOPC_M_CATS.qid
LEFT JOIN
    jsonlib.PBP_D_MAXENR_OOPC_NM_CATS AS PBP_D_MAXENR_OOPC_NM_CATS ON
        main.qid = PBP_D_MAXENR_OOPC_NM_CATS.qid
LEFT JOIN
    jsonlib.PBP_D_NON_DEDUCT_M_CATS AS PBP_D_NON_DEDUCT_M_CATS ON
        main.qid = PBP_D_NON_DEDUCT_M_CATS.qid
LEFT JOIN
    jsonlib.PBP_D_NON_DEDUCT_NM_CATS AS PBP_D_NON_DEDUCT_NM_CATS ON
        main.qid = PBP_D_NON_DEDUCT_NM_CATS.qid

;

QUIT;
RUN;

%MEND;
	
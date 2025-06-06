
* PROGRAM: CONVERT_DST.SAS;
* CONVERTS APPROPRIATE CHARACTER VARIABLES TO NUMERIC LENGTH 8;
options mprint source2;

%macro c_to_n(varlist=,informat=8.);

   %local i word delm;
   %let delm = %str( );
   %let note = %str(NOTES);
   %let yn = %str(_YN);
   %let cats = %str(_CATS);
   %let bens = %str(_BENS);
   %let type = %str(_TYPE_);
   %let per = %str(_PER_);
   %let amo = %str(_AMO_);
   %let pov = %str(_POV);
   %let per_ = %str(_PER);
   %let typ_ = %str(_TYPE);
   %let ad = %str(LIM_AD);
   %let auth = %str(_AUTH);
   %let catex = %str(CATEX);
   %let cat_ex = %str(CAT_EX);
   %let charge = %str(_CHARGE);
   %let apply = %str(_APPLY);
   %let endshare = %str(_SHARE);
   %let share = %str(_SHARE_);
   %let oopt = %str(_OOPT);
   %let id = %str(_ID);
   %let struct = %str(_STRUCT);
   %let service = %str(_SERVICE);
   %let upload = %str(UPLOAD_FILENAME);
   %let name = %str(_NAME);
   %let phone = %str(_PHONE);
   %let flag = %str(_FLAG);
   %let status = %str(STATUS);
   %let p_pbp_a = %str(P_PBP_A);
   %let pbp_a = %str(PBP_A);
   %let spc_var = %str(MRX_ALT_NO_DED_TIER);
   %let ehc_var = %str(_EHC);
   %let label_var = %str(_LABEL);
   %let mrxtpstdesc = %str(MRX_TIER_POST_FORM_MODEL_DESC);
   %let mrxtfrmdesc = %str(MRX_TIER_FORM_MODEL_DESC);
   %let mrxfrmdesc = %str(MRX_FORM_MODEL_DESC);
   %let vbd = %str(_VBD_);
   %let mrxtype = %str(MRX_FORM_MODEL_TYPE);
   %let mrxpostlab = %str(MRX_TIER_POST_LABEL_LIST);
   %let partddemo = %str(PART_D_MODEL_DEMO);
   %let goaldesc = %str(MRX_D_ADHER_GOAL_DESC);
   %let mrxraidesc1=%str(MRX_RAI_FRQ_RI_DESC_1RX);
   %let mrxraidesc2=%str(MRX_RAI_FRQ_RI_DESC_2RX);
   %let mrxraidesc3=%str(MRX_RAI_FRQ_RI_DESC_3RX);
   %let mrxdeligf=%str(MRX_D_ELIG_FREQ_OTHER);
   %let mrxdeligcrit=%str(MRX_D_ELIGCRIT_OTHER);
   %let ptdenhcvrgdem=%str(PART_D_ENHNCD_CVRG_DEMO);
   %let pbpdcombomode=%str(PBP_D_COMBO_MODE);
   %let pbpasnpcondoth=%str(PBP_A_SNP_COND_OTH);
   %let pbpdricsmod=%str(PBP_D_RICS_MOD);
   %let mrxtierincl=%str(MRX_TIER_INCLUDES);
   %let mrxfirstfill=%str(MRX_FIRST_FILL);

   %let i    = 1;
   %let word = %scan(&varlist,&i,&delm);
   %let indword = %index(&word,&note) + %index(&word,&yn) + %index(&word,&cats) + %index(&word,&bens) +
        %index(&word,&type) + %index(&word,&per) + %index(&word,&amo) + %index(&word,&pov) +
        %index(&word,&per_) + %index(&word,&typ_) + %index(&word,&ad) + %index(&word,&auth) +
        %index(&word,&catex) + %index(&word,&cat_ex) + %index(&word,&charge) + %index(&word,&oopt)  +
        %index(&word,&apply) + %index(&word,&share) - %index(&word,&endshare) + %index(&word,&id) +
        %index(&word,&struct) + %index(&word,&service) + %index(&word,&name) + %index(&word,&phone) +
        %index(&word,&flag)+ %index(&word,&status) + %index(&word,&p_pbp_a) + %index(&word,&pbp_a) +
        %index(&word,&spc_var) + %index(&word,&ehc_var) + %index(&word,&label_var) + %index(&word,&mrxtpstdesc) +
        %index(&word,&mrxtfrmdesc) + %index(&word,&mrxfrmdesc) +
        %index(&word,&vbd) + %index(&word,&mrxtype) +
        %index(&word,&mrxpostlab) + %index(&word,&partddemo) + %index(&word,&goaldesc) +
        %index(&word,&mrxraidesc1) + %index(&word,&mrxraidesc2) + %index(&word,&mrxraidesc3) +
        %index(&word,&mrxdeligf) + %index(&word,&mrxdeligcrit) + %index(&word,&ptdenhcvrgdem) +
        %index(&word,&pbpdcombomode) + %index(&word,&pbpasnpcondoth) + %index(&word,&pbpdricsmod) +
        %index(&word,&mrxtierincl) + %index(&word,&mrxfirstfill);

   %do %while("&word"^="");

      %if "&word" ^= "QID" and "&word" ^= "QID_OLD" and "&word" ^= "hplan" and "&word" ^= "UPLOAD_FILENAME"
      and "%eval(&indword)" = "0" and "&word" ^= "HPLAN" and "&word" ^= "TYPE" %then %do;
         length _&i &informat;
         _&i = input(&word,&informat);
      drop &word;
      rename _&i = &word;
      %end;

      %let i = %eval(&i + 1);
      %let word = %scan(&varlist,&i,&delm);
      %let indword = %index(&word,&note) + %index(&word,&yn) + %index(&word,&cats) + %index(&word,&bens) +
        %index(&word,&type) + %index(&word,&per) + %index(&word,&amo) + %index(&word,&pov) +
        %index(&word,&per_) + %index(&word,&typ_) + %index(&word,&ad) + %index(&word,&auth) +
        %index(&word,&catex) + %index(&word,&cat_ex) + %index(&word,&charge) + %index(&word,&oopt) +
        %index(&word,&apply)  + %index(&word,&share) - %index(&word,&endshare)+ %index(&word,&id) +
        %index(&word,&struct) + %index(&word,&service) + %index(&word,&name) + %index(&word,&phone) +
        %index(&word,&flag)+ %index(&word,&status) + %index(&word,&p_pbp_a) + %index(&word,&pbp_a) +
        %index(&word,&spc_var) + %index(&word,&ehc_var) + %index(&word,&label_var) + %index(&word,&mrxtpstdesc) +
        %index(&word,&mrxtfrmdesc) + %index(&word,&mrxfrmdesc) +
        %index(&word,&vbd) + %index(&word,&mrxtype) +
        %index(&word,&mrxpostlab) + %index(&word,&partddemo) + %index(&word,&goaldesc) +
        %index(&word,&mrxraidesc1) + %index(&word,&mrxraidesc2) + %index(&word,&mrxraidesc3) +
        %index(&word,&mrxdeligf) + %index(&word,&mrxdeligcrit) + %index(&word,&ptdenhcvrgdem) + 
        %index(&word,&pbpdcombomode) + %index(&word,&pbpasnpcondoth) + %index(&word,&pbpdricsmod) +
        %index(&word,&mrxtierincl) + %index(&word,&mrxfirstfill);

      %end;

   %mend c_to_n;
run;

%macro getvars;
drop nm x varlist;
*if LAST = 1 then do;
array ch(*) _character_;
length nm $32 varlist $32767;
varlist='';
call symput('vars',varlist);
do x=1 to dim(ch);
   nm = vname(ch(x));
   if x = 1 then do;
      varlist = upcase(trim(nm));
   end;
   else do;
      varlist = trim(varlist)||' '||upcase(trim(nm));
   end;
end;
call symput('vars',varlist);
*end;
%mend;

%MACRO PBPIMPCONV(TABLE);

PROC SORT DATA= &TABLE OUT=NEW&TABLE;
WHERE PUT(SUBSTR(QID,1,11),$HPLANF.) = 'YES';
BY QID;
RUN;

DATA _NULL_;
CALL SYMPUT('OBSNUM',TRIM(LEFT(PUT(NUMOBS,4.))));
STOP;
SET NEW&TABLE NOBS=NUMOBS;
STOP;
RUN;

DATA &TABLE;
SET NEW&TABLE (RENAME=QID=QID_OLD);
LENGTH QID $11;
QID=SUBSTR(QID_OLD,1,11);
DROP QID_OLD;
%IF &OBSNUM > 0 %THEN %DO;
%GETVARS;
%END;
RUN;

DATA &TABLE;
SET &TABLE;
%IF &OBSNUM > 0 %THEN %DO;
%c_to_n(varlist = &vars);
%END;
RUN;

%MEND;
RUN;

* PROGRAM: OOPCV1P.SAS;
* DESCRIPTION: MAIN OOPC PROGRAM;

%LET INPUTDIR     = c:\oopc_d\input;
%LET PROGDIR      = c:\oopc_d\programs;
%LET FORMDIR      = c:\oopc_d\formulary;
%LET PLANFILEDIR  = c:\oopc_d\programs;
%LET JSONDIR      = c:\oopc_d\programs;
%LET OUTPUT       = c:\oopc_d\output;
%LET JSONFILES    = c:\oopc_d\pbp;
%LET JSONTEMP     = c:\oopc_d\pbptemp;
%LET INPUTJSONDIR = c:\oopc_d\input\jsonlib;

%LET INPUT=INPUTPLAN;

OPTIONS OBS = MAX NOCENTER MSGLEVEL = N SUMSIZE = 1000M BUFNO = 6
   SORTPGM = SAS  NOSORTEQUALS THREADS = YES NOTES SPOOL;
OPTIONS VALIDVARNAME=ANY;

OPTIONS NOMPRINT NOSOURCE2;
RUN;

FILENAME IN0     "&INPUTDIR.";
LIBNAME  IN1     "&INPUTDIR.";

DATA _NULL_;
RUNDATE = PUT(DATE(),YYMMDD10.);
PUT "*****BEGIN OOPC RUN******";
PUT RUNDATE=;
TIME = time();
PUT TIME = time10.2;
RUN;

*SET UP MACRO VARIABLES FOR YEARS;
%LET RUNYEAR             = 2025;
%LET DEDUCTIBLE_&RUNYEAR = 590;
%LET ICP_&RUNYEAR        = 2000;
%LET TROOP_&RUNYEAR      = 2000;
%LET DS_COINS            = 0.25;
%LET IRA_COPAY           = 35;

* DS FORMULARY DRIVER = SET T0 1 TO PRODUCE DS RUN, 0 OTHERWISE;
%LET DSFORM_FLAG = 0;

%INCLUDE IN0(CLEANUP.SAS);

LIBNAME JSONLIB "&INPUTJSONDIR.";
PROC CIMPORT LIBRARY=JSONLIB FILE="&INPUTDIR.\OOPCLIB_PARTD.CPORT" MEMTYPE=ALL;
RUN;

%MACRO PLANS(JSONFILE,PLANFILE);

  %IF "&JSONFILE" NE "" %THEN %DO;
    DATA JSONFILE;
      INFILE "&JSONFILE" TRUNCOVER;
      INPUT @1 JSONFILE_NAME $CHAR500.;
      CALL SYMPUT('JF',TRIM(LEFT(JSONFILE_NAME)));
    RUN;
  %END;
  %ELSE %DO;
    DATA JSONFILE;
      STOP;
    RUN;
  %END;

  DATA _NULL_;
    RC=JSONPP("&JSONFILES.\&JF.","&JSONTEMP.\PP_&JF.");
  RUN;

  DATA _NULL_;
    INFILE "&JSONTEMP.\PP_&JF.";

    INPUT;

    %INCLUDE "&INPUTDIR.\SHORTEN.TXT";

    FILE "&JSONTEMP.\SH_PP_&JF.";
    PUT _INFILE_;
  RUN;

  FILENAME JSONIN "&JSONTEMP.\SH_PP_&JF.";
  FILENAME JSONMP "&INPUTDIR.\PARTD_AUTOMAP.TXT";
  LIBNAME JSTEMP JSON FILEREF=JSONIN MAP=JSONMP;
  
  PROC DATASETS LIB = JSTEMP;
  RUN;
  QUIT;

  PROC SQL NOPRINT;
    SELECT COUNT(*) INTO :N
    FROM DICTIONARY.TABLES
    WHERE LIBNAME="JSTEMP" AND UPCASE(MEMNAME) ^= "ALLDATA";
  QUIT;

  PROC SQL NOPRINT;
    SELECT MEMNAME INTO :DATA1 - :DATA%LEFT(&N)
    FROM DICTIONARY.TABLES
    WHERE LIBNAME="JSTEMP" AND UPCASE(MEMNAME) ^= "ALLDATA";
  QUIT;

  %MACRO JSONTOSAS;
    %DO I=1 %TO &N.;
      DATA JSONLIB.&&DATA&I;
        SET JSTEMP.&&DATA&I;
      RUN;
    %END;
  %MEND;

  %JSONTOSAS;

  %IF "&PLANFILE" NE "" %THEN %DO;
    DATA MASTER_PLANFILE;
    INFILE "&PLANFILE";
    INPUT @1 HPLAN $CHAR11.;
    RUN;
  %END;
  %ELSE %DO;
    DATA MASTER_PLANFILE;
    STOP;
    RUN;
  %END;

%MEND;

%PLANS(&JSONDIR\JSONFILE.TXT,&PLANFILEDIR\PLANFILE.TXT);
RUN;

DATA _NULL_;
  CALL SYMPUT('NUM',TRIM(LEFT(PUT(NUMOBS,4.))));
  STOP;
  SET MASTER_PLANFILE NOBS=NUMOBS;
  STOP;
RUN;

%INCLUDE IN0(OOPCV1M.SAS)/SOURCE2;

data _null_;
  ddate = put(date(),YYMMDDN8.);
  put ddate=;
  call symput('file_date', trim(left(ddate)));
run;

data _NULL_;
  if 0 then set master_planfile nobs=n;
  call symputx('nrows',n);
  stop;
run;

%macro batchrun(iter);
  %do i=1 %to &iter.;
    data planfile;
      set master_planfile;
      iteration = input("&i.",4.);
      plancnt = input("&nrows.",4.);
      start_rec =  (iteration - 1) * ceil(plancnt/&iter.);
      end_rec =  (iteration) * ceil(plancnt/&iter.);
      if start_rec < _n_ <= end_rec;
    run;
    
    data _NULL_;
      if 0 then set planfile nobs=n;
      call symputx('nobsrows',n);
     stop;
    run;
    
    %if "&nobsrows." > "0" %then %do;
    
      %OOPCV1M(RUNYEAR          =2025,
              INP               =IN1.PERSON,
              INPUTJSONDIR      =&INPUTJSONDIR,
              FORMULARY         =&FORMDIR);
      RUN; 
    
    %end;
     
    PROC DATASETS;
    DELETE RXCUI_MASTER_FILE RICPME_SAMPLE_COHORT FREEFILL PBP_PLANS;
    RUN;
      
  %END;
%MEND;

%batchrun(1);

%INCLUDE IN0(PLAN_LEVEL.SAS)/SOURCE2;
RUN;

%INCLUDE IN0(FFS_BENEFIT_OOPC_COST_CALCULATION.SAS)/SOURCE2;
RUN;

%INCLUDE IN0(PLAN_CATNAME_NEW.SAS)/SOURCE2;
RUN;

PROC DATASETS LIBRARY=JSONLIB KILL;
RUN;
QUIT;

DATA _NULL_;
RUNDATE = PUT(DATE(),YYMMDD10.);
PUT "*****ENDED OOPC RUN******";
PUT RUNDATE=;
TIME = time();
PUT TIME = time10.2;
RUN;

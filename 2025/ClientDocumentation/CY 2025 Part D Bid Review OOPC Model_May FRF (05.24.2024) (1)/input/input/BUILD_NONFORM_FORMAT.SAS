* PROGRAM NAME: BUILD_NONFORM_FORMAT.SAS;

LIBNAME IN "&INPUTDIR";

DATA FMT;
  SET IN.BENE_RXCUI END=LAST;
  LENGTH START $17 LABEL $1 FMTNAME $8 HLO $1;
  START=LEFT(PUT(BASEID,8.)) || "_" || LEFT(PUT(RXCUI,8.));
  LABEL=RX_OUTCOME;
  FMTNAME='$NFFMT';
  HLO=' ';
  OUTPUT FMT;
  IF LAST THEN DO;
    START='OTHER';
    LABEL=' ';
    HLO='O';
    OUTPUT FMT;
  END;
  KEEP START LABEL FMTNAME HLO;
RUN;

PROC FORMAT CNTLIN=FMT LIBRARY=LIBRARY;
RUN;


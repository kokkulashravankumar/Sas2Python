
options bufno=400 ibufno=400 ubufno=20 sortsize=512M fullstimer MSGLEVEL=I;

%time(timeparm=s);

/* B-278321-Start Split logic based on Hold Codes */

/* B-278321-Read Hold Codes from input file */

data hold;
  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
  infile "&drv2/DWAC0901/holdcode.txt";
  input holdcode $;
run;

                                                                                                                                   
proc sql;                                                                                                                       
 select quote(trim(left(holdcode)))                                                                                                        
  into :holdcodelist separated by ','                                                                                                          
  from hold;                                                                                                                            
quit;                                                                                                                                   

%put &=holdcodelist;

/* B-278321-Fetch unique claimno based on hold codes from svcholds */

proc sql;
create table rsstr.svcholdsfilter as 
select distinct clamno from rsstr.svcholds where holdcd in (&holdcodelist);
quit;

/* B-278321-split datasets into svclinesa based on matched holdcodes */

proc sql;
create table rsstr.svclinesa as 
select a.* from rsstr.svclines a where a.clamno in (select clamno from rsstr.svcholdsfilter)
;
quit;

/* B-278321-split datasets into svclinesb based on nonmatched holdcodes */

proc sql;
create table rsstr.svclinesb as 
select a.* from rsstr.svclines a where a.clamno not in (select clamno from rsstr.svcholdsfilter)
;
quit;


/* B-278321 rename svclinesb to svclines */

data rsstr.svclines;
      set rsstr.svclinesb;
run;

/* B-278321 split svclinesa is maded to 100%medicaid */

data rsstr.svclinesa;
     set rsstr.svclinesa;
     mcdalwunt       =  alwunt;
     mcdtopamt       =  topamt;
     mcdnetalwamt    =  netalwamt;
     mcdalwamt       =  alwamt;
     mcdcovunt       =  covunt;
     mcdcovdys       =  covdys;
     mcdapcovdys     =  apcovdys;
     mcdutilct       =  utilct;
     mcdaputilct     =  aputilct;
     mcdaptrnamt     =  aptrnamt;
     mcralwunt       =  0;
     mcrtopamt       =  0;
     mcrnetalwamt    =  0;
     mcralwamt       =  0;
     mcrcovunt       =  0;
     mcrcovdys       =  0;
     mcrapcovdys     =  0;
     mcrutilct       =  0;
     mcraputilct     =  0;
     mcraptrnamt     =  0;
run;

/* B-278321-End Split logic based on Hold Codes */
 

%macro ccbreakout ;
/*SAS2AWS2 - added datastep to read data from RS table*/
data svclines; ;
set /*&libn1.*/rsstr.svclines; 
run;

/*SAS2AWS2 - changed library name from libnx to work */
    data /*&libnx..*/svclines;                  /* #412625 RKS */
       modify /*&libnx..*/svclines;             /* #412625 RKS */

       drop whdrate;

       cstshrnetalcoiamt = 0; 
       cstshrnetalcopamt = 0; 
       cstshrnetaldedamt = 0;
       cstshrtopaycoiamt = 0; 
       cstshrtopaycopamt = 0; 
       cstshrtopaydedamt = 0;  

       if clatyp in ('IN', 'XN') and whdamt ne 0.00 then
          do;
             if netalwamt eq 0.00 then
                 whdrate = 1.00;
             else
                 whdrate = topamt/netalwamt;
          end;
       else
           whdrate = 1.00;
 

                                     /* HS 420008 do breakout by date of service RKS */
       if svcdat le '31DEC2013'd then 
          do;
              link ccbreakout;       /* HS 420008  All other breakouts */
          end;
       else
          do;
              link ccbreakout14;     /* HS 420008  2014 breakout logic */
          end;
          
       return;                       /* HS 420008 This returns to top of data step prevents fall through RKS */



       /**************************************************************************************************************/
       /********************    Start of breakout routines for service dates older thatn 2014    *********************/
       /**************************************************************************************************************/
ccbreakout: 

       select;
                                                                               /*  100% Medicare */
         when (bencod in ('DT1', 'ERF', 'ERP', 'UCC', 'PH1', 'RTF', 'HOM', 'HPT', 'HSP', 'HSN', 'HOC', 'HHA',
                          'PSC', 'WCC', 'PSV', 'CPA', 'OCH', 'OVC', 'OOV', 'PSV', 'DAS', 'MH1', 'OTM', 'POD',
                          'POV', 'P52', 'OST', 'PTH', 'SPE', 'LAB', 'DGT', 'VEN', 'AAA', 'EKG', 'XR1', 'XRD', 
                          'CT1', 'CTP', 'MI1', 'MPR', 'MU1', 'MU2', 'PDT', 'CHE', 'RT1', 'OH1', 'AMF', 'AMP',
                          'SA1', 'BPS', 'BP2', 'DIB', 'ODB', 'DEQ', 'SID', 'DIA', 'DA1', 'DIQ', 'AST', 'IMM',
                          'VAC', 'VAN', 'ANP', 'GYN', 'PRS', '1PS', '2PS', 'COL', 'RFT', 'CLS', 'CL1', 'CL2', 
                          'CL3', 'CL4', 'CL5', 'CL6', 'BDE', 'MAM', 'MG1', 'MG2', 'MG3', 'NTH', 'KDE', 'DMT',
                          'DEN', 'EEX', 'GLA', 'HEM', 'DIA', 'DA1', 'DIQ', 'ACU', 'HIS', 'HAS', 'FER', 'FAM', 
                          'AMT', 'COS', 'COR', 'COP', 'RH1', 'VC1', 'SCC', 'RX1', 'OOC', 'MAO', 'DRP', 'EXC',
                          'IP1', 'SAP', 'MHP', 'FAS' ))
                              do;
                                 mcdalwunt       =  0;
                                 mcdtopamt       =  0;
                                 mcdnetalwamt    =  0;
                                 mcdalwamt       =  0;
                                 mcdcovunt       =  0;
                                 mcdcovdys       =  0;
                                 mcdapcovdys     =  0;
                                 mcdutilct       =  0;
                                 mcdaputilct     =  0;
                                 mcdaptrnamt     =  0;

                                 mcralwunt       =  alwunt;
                                 mcrtopamt       =  topamt;
                                 mcrnetalwamt    =  netalwamt;
                                 mcralwamt       =  alwamt;
                                 mcrcovunt       =  covunt;
                                 mcrcovdys       =  covdys;
                                 mcrapcovdys     =  apcovdys;
                                 mcrutilct       =  utilct;
                                 mcraputilct     =  aputilct;
                                 mcraptrnamt     =  aptrnamt;
                              end;

                                                                               /*  100% Medicaid */
         when (bencod in ('OT1', 'TX1', 'HDM', 'EEX', 'EYE', 'HER', 'HEA', 'HHN', 'HSS', 'HDM',
                          'ADH', 'SDC', 'HHE', 'HSK', 'PER', 'PCA', 'PDN'))
                              do;
                                 mcdalwunt       =  alwunt;
                                 mcdtopamt       =  topamt;
                                 mcdnetalwamt    =  netalwamt;
                                 mcdalwamt       =  alwamt;
                                 mcdcovunt       =  covunt;
                                 mcdcovdys       =  covdys;
                                 mcdapcovdys     =  apcovdys;
                                 mcdutilct       =  utilct;
                                 mcdaputilct     =  aputilct;
                                 mcdaptrnamt     =  aptrnamt;
                                 mcralwunt       =  0;
                                 mcrtopamt       =  0;
                                 mcrnetalwamt    =  0;
                                 mcralwamt       =  0;
                                 mcrcovunt       =  0;
                                 mcrcovdys       =  0;
                                 mcrapcovdys     =  0;
                                 mcrutilct       =  0;
                                 mcraputilct     =  0;
                                 mcraptrnamt     =  0;
                              end;

                                                                               /*Inpatient Hospital   (COST SHARING)
                                                                              IF1 = 1 - 8 days, 
                                                                                  If IF1 (ben code), $100. per day 1 - 8 is Medicaid. 
                                                                                  All other dollars for 1-8 days = Medicare.
                                                                              IF2 First 1-8 days is same as IF1. 
                                                                                  Automatically, move $800. to Medicaid 
                                                                                  All other dollars for 1-8 days = Medicare.*/
         when (bencod in ('IF1', 'IF2', 'SAF',  'OGT'))                               
                              do;
                                   if covunt le 8 then
                                      do;
                                           if abs(netalwamt) gt (covunt * 100) then
                                              do;
                                                 if netalwamt gt 0 then
                                                    do;
                                                        cstshrnetalcopamt =  (covunt * 100.00);
                                                        cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                        mcdnetalwamt      =  0.00;
                                                    end;
                                                 else
                                                    do;
                                                        cstshrnetalcopamt =  (covunt * 100.00)*-1;
                                                        cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                        mcdnetalwamt      =  0.00;
                                                    end;
                                              end;
                                           else
                                              do;
                                                 cstshrnetalcopamt =  netalwamt;
                                                 cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                 mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                              end;

                                           mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                           mcrtopamt    = mcrnetalwamt * (whdrate);
                                           mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                           if abs(aptrnamt) gt (covunt * 100) then
                                              do;
                                                 if aptrnamt gt 0 then
                                                     mcdaptrnamt  =  (covunt * 100.00) * whdrate ;       /*HS 323337 */
                                                 else
                                                     mcdaptrnamt  =  (covunt * 100.00 * -1) * whdrate;   /*HS 323337 */
                                              end;
                                           else
                                              mcdaptrnamt    =  aptrnamt;

                                           mcdalwunt       =  alwunt;
                                           mcdalwamt       =  alwamt;
                                           mcdcovunt       =  covunt;
                                           mcdcovdys       =  covdys;
                                           mcdapcovdys     =  0;
                                           mcdutilct       =  utilct;
                                           mcdaputilct     =  0;

                                           mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                           mcralwunt       =  alwunt;
                                           mcralwamt       =  alwamt;
                                           mcrcovunt       =  covunt;
                                           mcrcovdys       =  covdys;
                                           mcrapcovdys     =  apcovdys;
                                           mcrutilct       =  utilct;
                                           mcraputilct     =  aputilct;
                                      end;
                                   else
                                      do;
                                           if abs(netalwamt) gt 800.00 then
                                              do;
                                                 if netalwamt gt 0 then
                                                    do;
                                                        cstshrnetalcopamt =  800.00;
                                                        cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                        mcdnetalwamt      =  0.00;
                                                    end;
                                                 else
                                                    do;
                                                        cstshrnetalcopamt =  (800.00 * -1);
                                                        cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                        mcdnetalwamt      =  0.00;
                                                    end;
                                              end;
                                           else
                                              do;
                                                 cstshrnetalcopamt =  netalwamt;
                                                 cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                 mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                              end;

                                           mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                           mcrtopamt    = mcrnetalwamt * (whdrate);
                                           mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                           if abs(aptrnamt) ge 800 then
                                              do;
                                                 if aptrnamt gt 0 then
                                                     mcdaptrnamt  =  800.00 * whdrate;                     /*HS 323337 */
                                                 else
                                                     mcdaptrnamt  =  (800.00 * -1) * whdrate;              /*HS 323337 */
                                              end;
                                           else
                                              mcdaptrnamt  =  aptrnamt;

                                           mcdalwunt       =  alwunt;
                                           mcdalwamt       =  alwamt;
                                           mcdcovunt       =  covunt;
                                           mcdcovdys       =  covdys;
                                           mcdapcovdys     =  0;
                                           mcdutilct       =  utilct;
                                           mcdaputilct     =  0;

                                           mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                           mcralwunt       =  alwunt;
                                           mcralwamt       =  alwamt;
                                           mcrcovunt       =  covunt;
                                           mcrcovdys       =  covdys;
                                           mcrapcovdys     =  apcovdys;
                                           mcrutilct       =  utilct;
                                           mcraputilct   =  aputilct;
                                      end;

                              end;

                                                                               /*Inpatient Psychiatric Hospital  (COST SHARING)
                                                                               FP1 = 1 - 8 day 
                                                                               If FP1 (ben code), $100. per day 1 - 8 is Medicaid. 
                                                                                  All other dollars for 1-8 days = Medicare.
                                                                               FP2 (9 - 190) tagged on a claim, 
                                                                                  First 1-8 days is same as FP1. Automatically, move $800.
                                                                                  to Medicaid.  All other dollars for 1-8 days = Medicare.
                                                                               FP3 - (191 - 999):  
                                                                                  First 8 days at $100. per day = Medicaid.  
                                                                                  Days 1- 190 less copay = Medicare.  
                                                                                  Days 191 to 999 = Medicaid in full    */
         when (bencod in ('FP1', 'FP2', 'FP3', 'MR1'))                             
                              do;
                                 if covunt le 8 then
                                    do;
                                        if abs(netalwamt) gt (covunt * 100) then
                                           do;
                                              if netalwamt gt 0 then
                                                 do;
                                                     cstshrnetalcopamt =  (covunt * 100.00);
                                                     cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                     mcdnetalwamt      =  0.00;
                                                 end;
                                              else
                                                 do;
                                                     cstshrnetalcopamt =  (covunt * 100.00)*-1;
                                                     cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                     mcdnetalwamt      =  0.00;
                                                 end;
                                          end;
                                        else
                                           do;
                                              cstshrnetalcopamt =  netalwamt;
                                              cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                              mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                           end;

                                        mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                        mcrtopamt    = mcrnetalwamt * (whdrate);
                                        mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                        if abs(aptrnamt) gt (covunt * 100) then
                                           do;
                                              if aptrnamt gt 0 then
                                                  mcdaptrnamt  =  (covunt * 100.00) * whdrate;              /*HS 323337 */
                                              else
                                                  mcdaptrnamt  =  (covunt * 100.00 * -1) * whdrate;         /*HS 323337 */
                                           end;
                                        else
                                           mcdaptrnamt    =  aptrnamt;

                                        mcdalwunt       =  alwunt;
                                        mcdalwamt       =  alwamt;
                                        mcdcovunt       =  covunt;
                                        mcdcovdys       =  covdys;
                                        mcdapcovdys     =  0;
                                        mcdutilct       =  utilct;
                                        mcdaputilct     =  0;

                                        mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                        mcralwunt       =  alwunt;
                                        mcralwamt       =  alwamt;
                                        mcrcovunt       =  covunt;
                                        mcrcovdys       =  covdys;
                                        mcrapcovdys     =  apcovdys;
                                        mcrutilct       =  utilct;
                                        mcraputilct     =  aputilct;
                                    end;
                                 else
                                    if  covunt gt 8 and covunt le 190  then
                                       do;
                                           if abs(netalwamt) gt 800.00 then
                                              do;
                                                 if netalwamt gt 0 then
                                                    do;
                                                        cstshrnetalcopamt =  800.00;
                                                        cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                        mcdnetalwamt      =  0.00;
                                                    end;
                                                 else
                                                    do;
                                                        cstshrnetalcopamt =  (800 * -1);
                                                        cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                        mcdnetalwamt      =  0.00;
                                                    end;
                                             end;
                                           else
                                              do;
                                                 cstshrnetalcopamt =  netalwamt;
                                                 cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                 mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                              end;

                                           mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                           mcrtopamt    = mcrnetalwamt * (whdrate);
                                           mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                          if abs(aptrnamt) gt (covunt * 100) then
                                             do;
                                                if aptrnamt gt 0 then
                                                    mcdaptrnamt  =  800.00 * whdrate;              /*HS 323337 */
                                                else
                                                    mcdaptrnamt  = (800.00 * -1) * whdrate;        /*HS 323337 */
                                             end;
                                          else
                                             mcdaptrnamt  =  aptrnamt;

                                          mcdalwamt       =  alwamt;
                                          mcdalwunt       =  alwunt;
                                          mcdcovunt       =  covunt;
                                          mcdcovdys       =  covdys;
                                          mcdapcovdys     =  0;
                                          mcdutilct       =  utilct;
                                          mcdaputilct     =  0;

                                          mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                          mcralwamt       =  (alwamt - mcdalwamt);
                                          mcralwunt       =  alwunt;
                                          mcrcovunt       =  covunt;
                                          mcrcovdys       =  covdys;
                                          mcrapcovdys     =  apcovdys;
                                          mcrutilct       =  utilct;
                                          mcraputilct     =  aputilct;
                                       end;
                                   else 
                                       if covunt gt 190  then    
                                            do;
                                               if abs(netalwamt) ge 800.00 then
                                                  do;
                                                     if netalwamt gt 0 then
                                                        do;
                                                            cstshrnetalcopamt =  800.00;
                                                            cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                            mcdnetalwamt      =  ((covunt-190)*(netalwamt/covunt));
                                                        end;
                                                     else
                                                        do;
                                                            cstshrnetalcopamt =  (800 * -1);
                                                            cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                            mcdnetalwamt      =  ((covunt-190)*(topamt/covunt));
                                                        end;
                                                  end;
                                               else
                                                  do;
                                                     cstshrnetalcopamt =  netalwamt;
                                                     cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                     mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                                  end;

                                               mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                               mcrtopamt    = mcrnetalwamt * (whdrate);
                                               mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                               if abs(aptrnamt) gt ((covunt-190)*(aptrnamt/covunt)+ 800) then
                                                  do;
                                                     if aptrnamt gt 0 then
                                                         mcdaptrnamt    =  ((covunt-190)*(aptrnamt/covunt)+ 800) * whdrate;        /*HS 323337 */
                                                     else
                                                         mcdaptrnamt    =  (((covunt-190)*(aptrnamt/covunt)+ 800)* -1) * whdrate;  /*HS 323337 */
                                                  end;
                                               else
                                                  mcdaptrnamt    =  aptrnamt;

                                               mcdalwamt       =  alwamt;
                                               mcdalwunt       =  alwunt;
                                               mcdcovunt       =  covunt;
                                               mcdcovdys       =  covdys;
                                               mcdapcovdys     =  (apcovdys - 190);
                                               mcdutilct       =  utilct;
                                               mcdaputilct     =  aputilct;

                                               mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                               mcralwunt       =  alwunt;
                                               mcralwamt       =  alwamt;
                                               mcrcovunt       =  covunt;
                                               mcrcovdys       =  covdys;
                                               mcrapcovdys     =  (apcovdys - mcdapcovdys);
                                               mcrutilct       =  utilct;
                                               mcraputilct     =  aputilct;
                                            end;
                              end;

                                                                           /* Durable Medical Equipment and Orthodics       */
                                                                           /* Prosthetics and Medical Supplies (COST SHARING*/
                                                                           /* 100% Medicaid based on svccod                 */
                                                                           /* 100% Medicare based on svccod                 */
                                                                           /* ELSE 90% = Medicare/10% = Medicaid            */
         when (bencod in ('DME', 'ORT','MCD', 'PRO', 'MSP', 'MCP'))
               do;
                  if bencod eq 'DME' and 
                     svccod in ("A8000", "A8001") and svctyp eq 'HC' then
                       do;
                            mcdalwunt       =  alwunt;
                            mcdtopamt       =  topamt;
                            mcdnetalwamt    =  netalwamt;
                            mcdalwamt       =  alwamt;
                            mcdcovunt       =  covunt;
                            mcdcovdys       =  covdys;
                            mcdapcovdys     =  apcovdys;
                            mcdutilct       =  utilct;
                            mcdaputilct     =  aputilct;
                            mcdaptrnamt     =  aptrnamt;
                            mcralwunt       =  0;
                            mcrtopamt       =  0;
                            mcrnetalwamt    =  0;
                            mcralwamt       =  0;
                            mcrcovunt       =  0;
                            mcrcovdys       =  0;
                            mcrapcovdys     =  0;
                            mcrutilct       =  0;
                            mcraputilct     =  0;
                            mcraptrnamt     =  0;
                       end;
                  else
                       if bencod eq 'MSP' and 
                          svccod in ("A4554", "T4521", "T4522", "T4523", "T4524", "T4525", "T4526", 
                                     "T4527", "T4528", "T4529", "T4530", "T4531", "T4532", "T4533", "T4534", "T4535",
                                     "T4536", "T4537", "T4538", "T4539", "T4540", "T4541", "T4542", "T4543", "A4510",
                                     "A6531", "A6532", "A6549") and svctyp eq 'HC' then
                            do;
                                 mcdalwunt       =  alwunt;
                                 mcdtopamt       =  topamt;
                                 mcdnetalwamt    =  netalwamt;
                                 mcdalwamt       =  alwamt;
                                 mcdcovunt       =  covunt;
                                 mcdcovdys       =  covdys;
                                 mcdapcovdys     =  apcovdys;
                                 mcdutilct       =  utilct;
                                 mcdaputilct     =  aputilct;
                                 mcdaptrnamt     =  aptrnamt;
                                 mcralwunt       =  0;
                                 mcrtopamt       =  0;
                                 mcrnetalwamt    =  0;
                                 mcralwamt       =  0;
                                 mcrcovunt       =  0;
                                 mcrcovdys       =  0;
                                 mcrapcovdys     =  0;
                                 mcrutilct       =  0;
                                 mcraputilct     =  0;
                                 mcraptrnamt     =  0;
                            end;
                       else
                            if bencod eq 'ORT' and 
                               svccod in ('L3595') and svctyp eq 'HC' then
                                do;
                                      mcdalwunt       =  0;
                                      mcdtopamt       =  0;
                                      mcdnetalwamt    =  0;
                                      mcdalwamt       =  0;
                                      mcdcovunt       =  0;
                                      mcdcovdys       =  0;
                                      mcdapcovdys     =  0;
                                      mcdutilct       =  0;
                                      mcdaputilct     =  0;
                                      mcdaptrnamt     =  0;

                                      mcralwunt       =  alwunt;
                                      mcrtopamt       =  topamt;
                                      mcrnetalwamt    =  netalwamt;
                                      mcralwamt       =  alwamt;
                                      mcrcovunt       =  covunt;
                                      mcrcovdys       =  covdys;
                                      mcrapcovdys     =  apcovdys;
                                      mcrutilct       =  utilct;
                                      mcraputilct     =  aputilct;
                                      mcraptrnamt     =  aptrnamt;
                                end;
                            else
                                do;
                                    cstshrnetalcoiamt =  (.100 * netalwamt);
                                    mcdnetalwamt      =  0.00;
                                    cstshrtopaycoiamt =  (.100 * topamt);
                                    mcdtopamt         =  0.00;
                                    mcdaptrnamt       =  (.100 * aptrnamt);
                                    mcdalwunt         =  alwunt;
                                    mcdalwamt         =  alwamt;
                                    mcdcovunt         =  covunt;
                                    mcdcovdys         =  covdys;
                                    mcdapcovdys       =  0;
                                    mcdutilct         =  utilct;
                                    mcdaputilct       =  0;

                                    mcrtopamt       =  (topamt - cstshrtopaycoiamt);
                                    mcrnetalwamt    =  (netalwamt - cstshrnetalcoiamt);
                                    mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                    mcralwunt       =  alwunt;
                                    mcralwamt       =  alwamt;
                                    mcrcovunt       =  covunt;
                                    mcrcovdys       =  covdys;
                                    mcrapcovdys     =  apcovdys;
                                    mcrutilct       =  utilct;
                                    mcraputilct     =  aputilct;
                                end;
               end;
                                                                                         /*  Ambulance (COST SHARING    */
                                                                                         /*  20% of allowed = Medicaid  */
                                                                                         /*  80% of allowed = Medicare  */
         when (bencod in ('TRA'))
                              do;
                                 cstshrnetalcoiamt =  (.200 * netalwamt);
                                 mcdnetalwamt      =  0.00;
                                 cstshrtopaycoiamt =  (.200 * topamt);
                                 mcdtopamt         =  0.00;
                                 mcdaptrnamt       =  (.200 * aptrnamt);
                                 mcdalwunt         =  alwunt;
                                 mcdalwamt         =  alwamt;
                                 mcdcovunt         =  covunt;
                                 mcdcovdys         =  covdys;
                                 mcdapcovdys       =  0;
                                 mcdutilct         =  utilct;
                                 mcdaputilct       =  0;

                                 mcrtopamt       =  (topamt - cstshrtopaycoiamt);
                                 mcrnetalwamt    =  (netalwamt - cstshrnetalcoiamt);
                                 mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                 mcralwunt       =  alwunt;
                                 mcralwamt       =  alwamt;
                                 mcrcovunt       =  covunt;
                                 mcrcovdys       =  covdys;
                                 mcrapcovdys     =  apcovdys;
                                 mcrutilct       =  utilct;
                                 mcraputilct     =  aputilct;
                              end;
                                                                                         /*  Part B Rx Drugs (COST SHARING*/
                                                                                         /*  20% of allowed = Medicaid  */
                                                                                         /*  80% of allowed = Medicare  */
         when (bencod in ('OPD', 'MMD'))
                              do;
                                 cstshrnetalcoiamt =  (.200 * netalwamt);
                                 mcdnetalwamt      =  0.00;
                                 cstshrtopaycoiamt =  (.200 * topamt);
                                 mcdtopamt         =  0.00;
                                 mcdaptrnamt       =  (.200 * aptrnamt);
                                 mcdalwunt         =  alwunt;
                                 mcdalwamt         =  alwamt;
                                 mcdcovunt         =  covunt;
                                 mcdcovdys         =  covdys;
                                 mcdapcovdys       =  0;
                                 mcdutilct         =  utilct;
                                 mcdaputilct       =  0;

                                 mcrtopamt       =  (topamt - cstshrtopaycoiamt);
                                 mcrnetalwamt    =  (netalwamt - cstshrnetalcoiamt);
                                 mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                 mcralwunt       =  alwunt;
                                 mcralwamt       =  alwamt;
                                 mcrcovunt       =  covunt;
                                 mcrcovdys       =  covdys;
                                 mcrapcovdys     =  apcovdys;
                                 mcrutilct       =  utilct;
                                 mcraputilct     =  aputilct;
                              end;

                                                                               /*  DEFAULT TO: 100% Medicare  */
         otherwise do;
                      mcdalwunt       =  0;
                      mcdtopamt       =  0;
                      mcdnetalwamt    =  0;
                      mcdalwamt       =  0;
                      mcdcovunt       =  0;
                      mcdcovdys       =  0;
                      mcdapcovdys     =  0;
                      mcdutilct       =  0;
                      mcdaputilct     =  0;
                      mcdaptrnamt     =  0;

                      mcralwunt       =  alwunt;
                      mcrtopamt       =  topamt;
                      mcrnetalwamt    =  netalwamt;
                      mcralwamt       =  alwamt;
                      mcrcovunt       =  covunt;
                      mcrcovdys       =  covdys;
                      mcrapcovdys     =  apcovdys;
                      mcrutilct       =  utilct;
                      mcraputilct     =  aputilct;
                      mcraptrnamt     =  aptrnamt;
                   end;
       end;                                                                  /*end select*/

       return;                       /* HS 420008 This returns to 2013 or older link RKS */



       /**************************************************************************************************************/
       /***************************    Start of breakout routines for service dates 2014    **************************/
       /**************************************************************************************************************/
ccbreakout14: 

       select;
                                                                               /*  100% Medicare */
                                                                               /* HS 420008 Removed RH1 and added IF2 FP2 FP3  RKS  */
         when (bencod in ('DT1', 'ERF', 'ERP', 'UCC', 'PH1', 'RTF', 'HOM', 'HPT', 'HSP', 'HSN', 'HOC', 'HHA',
                          'PSC', 'WCC', 'PSV', 'CPA', 'OCH', 'OVC', 'OOV', 'PSV', 'DAS', 'MH1', 'OTM', 'POD',
                          'POV', 'P52', 'OST', 'PTH', 'SPE', 'LAB', 'DGT', 'VEN', 'AAA', 'EKG', 'XR1', 'XRD', 
                          'CT1', 'CTP', 'MI1', 'MPR', 'MU1', 'MU2', 'PDT', 'CHE', 'RT1', 'OH1', 'AMF', 'AMP',
                          'SA1', 'BPS', 'BP2', 'DIB', 'ODB', 'DEQ', 'SID', 'DIA', 'DA1', 'DIQ', 'AST', 'IMM',
                          'VAC', 'VAN', 'ANP', 'GYN', 'PRS', '1PS', '2PS', 'COL', 'RFT', 'CLS', 'CL1', 'CL2', 
                          'CL3', 'CL4', 'CL5', 'CL6', 'BDE', 'MAM', 'MG1', 'MG2', 'MG3', 'NTH', 'KDE', 'DMT',
                          'DEN', 'EEX', 'GLA', 'HEM', 'DIA', 'DA1', 'DIQ', 'ACU', 'HIS', 'HAS', 'FER', 'FAM', 
                          'AMT', 'COS', 'COR', 'COP', 'VC1', 'SCC', 'RX1', 'OOC', 'MAO', 'DRP', 'EXC', 
                          'FAS', 'SAP', 'IP1', 'IF2', 'FP2', 'FP3', 'MHP' ))
                          do;
                             mcdalwunt       =  0;
                             mcdtopamt       =  0;
                             mcdnetalwamt    =  0;
                             mcdalwamt       =  0;
                             mcdcovunt       =  0;
                             mcdcovdys       =  0;
                             mcdapcovdys     =  0;
                             mcdutilct       =  0;
                             mcdaputilct     =  0;
                             mcdaptrnamt     =  0;

                             mcralwunt       =  alwunt;
                             mcrtopamt       =  topamt;
                             mcrnetalwamt    =  netalwamt;
                             mcralwamt       =  alwamt;
                             mcrcovunt       =  covunt;
                             mcrcovdys       =  covdys;
                             mcrapcovdys     =  apcovdys;
                             mcrutilct       =  utilct;
                             mcraputilct     =  aputilct;
                             mcraptrnamt     =  aptrnamt;
                          end;

                                                                               /*  100% Medicaid */
         when (bencod in ('OT1', 'TX1', 'HDM', 'EEX', 'EYE', 'HER', 'HEA', 'HHN', 'HSS', 'HDM',
                          'ADH', 'SDC', 'HHE', 'HSK', 'PER', 'PCA', 'PDN'))
                          do;
                             mcdalwunt       =  alwunt;
                             mcdtopamt       =  topamt;
                             mcdnetalwamt    =  netalwamt;
                             mcdalwamt       =  alwamt;
                             mcdcovunt       =  covunt;
                             mcdcovdys       =  covdys;
                             mcdapcovdys     =  apcovdys;
                             mcdutilct       =  utilct;
                             mcdaputilct     =  aputilct;
                             mcdaptrnamt     =  aptrnamt;
                             mcralwunt       =  0;
                             mcrtopamt       =  0;
                             mcrnetalwamt    =  0;
                             mcralwamt       =  0;
                             mcrcovunt       =  0;
                             mcrcovdys       =  0;
                             mcrapcovdys     =  0;
                             mcrutilct       =  0;
                             mcraputilct     =  0;
                             mcraptrnamt     =  0;
                          end;

                                                                               /*Inpatient Hospital 
                                                                                 $175 per day for days 1-6 should be reported
                                                                                  as Medicaid costs,  all remaining dollars go to Medicare */

         when (bencod in ('IF1', 'OGT', 'SAF', 'MR1', 'RH1' ))                 /* HS 420008 Logic Change for 2014  RKS  */                           
                      do;
                           if covunt le 6 then
                              do;
                                   if abs(netalwamt) gt (covunt * 175) then
                                      do;
                                         if netalwamt gt 0 then
                                            do;
                                                cstshrnetalcopamt =  (covunt * 175.00);
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                         else
                                            do;
                                                cstshrnetalcopamt =  (covunt * 175.00)*-1;
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                      end;
                                   else
                                      do;
                                         cstshrnetalcopamt =  netalwamt;
                                         cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                         mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                      end;

                                   mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                   mcrtopamt    = mcrnetalwamt * (whdrate);
                                   mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                   if abs(aptrnamt) gt (covunt * 175) then
                                      do;
                                         if aptrnamt gt 0 then
                                             mcdaptrnamt  =  (covunt * 175.00) * whdrate ;     
                                         else
                                             mcdaptrnamt  =  (covunt * 175.00 * -1) * whdrate;   
                                      end;
                                   else
                                      mcdaptrnamt    =  aptrnamt;

                                   mcdalwunt       =  alwunt;
                                   mcdalwamt       =  alwamt;
                                   mcdcovunt       =  covunt;
                                   mcdcovdys       =  covdys;
                                   mcdapcovdys     =  0;
                                   mcdutilct       =  utilct;
                                   mcdaputilct     =  0;

                                   mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                   mcralwunt       =  alwunt;
                                   mcralwamt       =  alwamt;
                                   mcrcovunt       =  covunt;
                                   mcrcovdys       =  covdys;
                                   mcrapcovdys     =  apcovdys;
                                   mcrutilct       =  utilct;
                                   mcraputilct     =  aputilct;
                              end;
                           else
                              do;
                                   if abs(netalwamt) gt 1050.00 then
                                      do;
                                         if netalwamt gt 0 then
                                            do;
                                                cstshrnetalcopamt =  1050.00;
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                         else
                                            do;
                                                cstshrnetalcopamt =  (1050.00 * -1);
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                      end;
                                   else
                                      do;
                                         cstshrnetalcopamt =  netalwamt;
                                         cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                         mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                      end;

                                   mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                   mcrtopamt    = mcrnetalwamt * (whdrate);
                                   mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                   if abs(aptrnamt) ge 1050 then
                                      do;
                                         if aptrnamt gt 0 then
                                             mcdaptrnamt  =  1050.00 * whdrate;                     /*HS 323337 */
                                         else
                                             mcdaptrnamt  =  (1050.00 * -1) * whdrate;              /*HS 323337 */
                                      end;
                                   else
                                      mcdaptrnamt  =  aptrnamt;

                                   mcdalwunt       =  alwunt;
                                   mcdalwamt       =  alwamt;
                                   mcdcovunt       =  covunt;
                                   mcdcovdys       =  covdys;
                                   mcdapcovdys     =  0;
                                   mcdutilct       =  utilct;
                                   mcdaputilct     =  0;

                                   mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                   mcralwunt       =  alwunt;
                                   mcralwamt       =  alwamt;
                                   mcrcovunt       =  covunt;
                                   mcrcovdys       =  covdys;
                                   mcrapcovdys     =  apcovdys;
                                   mcrutilct       =  utilct;
                                   mcraputilct   =  aputilct;
                              end;

                      end;

                                                                               /*Inpatient Psychiatric Hospital  (COST SHARING)
                                                                               FP1 = 1 - 8 day 
                                                                               If FP1 (ben code), $100. per day 1 - 8 is Medicaid. 
                                                                                  All other dollars for 1-8 days = Medicare.
                                                                               FP2 (9 - 190) tagged on a claim, 
                                                                                  First 1-8 days is same as FP1. Automatically, move $800.
                                                                                  to Medicaid.  All other dollars for 1-8 days = Medicare.
                                                                               FP3 - (191 - 999):  
                                                                                  First 8 days at $100. per day = Medicaid.  
                                                                                  Days 1- 190 less copay = Medicare.  
                                                                                  Days 191 to 999 = Medicaid in full    */

         when (bencod in ('FP1'))                                              /* HS 420008 Logic Change for 2014  RKS  */                         
                      do;
                           if covunt le 6 then
                              do;
                                   if abs(netalwamt) gt (covunt * 175) then
                                      do;
                                         if netalwamt gt 0 then
                                            do;
                                                cstshrnetalcopamt =  (covunt * 175.00);
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                         else
                                            do;
                                                cstshrnetalcopamt =  (covunt * 175.00)*-1;
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                      end;
                                   else
                                      do;
                                         cstshrnetalcopamt =  netalwamt;
                                         cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                         mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                      end;

                                   mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                   mcrtopamt    = mcrnetalwamt * (whdrate);
                                   mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                   if abs(aptrnamt) gt (covunt * 175) then
                                      do;
                                         if aptrnamt gt 0 then
                                             mcdaptrnamt  =  (covunt * 175.00) * whdrate ;     
                                         else
                                             mcdaptrnamt  =  (covunt * 175.00 * -1) * whdrate;   
                                      end;
                                   else
                                      mcdaptrnamt    =  aptrnamt;

                                   mcdalwunt       =  alwunt;
                                   mcdalwamt       =  alwamt;
                                   mcdcovunt       =  covunt;
                                   mcdcovdys       =  covdys;
                                   mcdapcovdys     =  0;
                                   mcdutilct       =  utilct;
                                   mcdaputilct     =  0;

                                   mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                   mcralwunt       =  alwunt;
                                   mcralwamt       =  alwamt;
                                   mcrcovunt       =  covunt;
                                   mcrcovdys       =  covdys;
                                   mcrapcovdys     =  apcovdys;
                                   mcrutilct       =  utilct;
                                   mcraputilct     =  aputilct;
                              end;
                           else
                              do;
                                   if abs(netalwamt) gt 1050.00 then
                                      do;
                                         if netalwamt gt 0 then
                                            do;
                                                cstshrnetalcopamt =  1050.00;
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                         else
                                            do;
                                                cstshrnetalcopamt =  (1050.00 * -1);
                                                cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      =  0.00;
                                            end;
                                      end;
                                   else
                                      do;
                                         cstshrnetalcopamt =  netalwamt;
                                         cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                         mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                      end;

                                   mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                   mcrtopamt    = mcrnetalwamt * (whdrate);
                                   mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                   if abs(aptrnamt) ge 1050 then
                                      do;
                                         if aptrnamt gt 0 then
                                             mcdaptrnamt  =  1050.00 * whdrate;                     /*HS 323337 */
                                         else
                                             mcdaptrnamt  =  (1050.00 * -1) * whdrate;              /*HS 323337 */
                                      end;
                                   else
                                      mcdaptrnamt  =  aptrnamt;

                                   mcdalwunt       =  alwunt;
                                   mcdalwamt       =  alwamt;
                                   mcdcovunt       =  covunt;
                                   mcdcovdys       =  covdys;
                                   mcdapcovdys     =  0;
                                   mcdutilct       =  utilct;
                                   mcdaputilct     =  0;

                                   mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                   mcralwunt       =  alwunt;
                                   mcralwamt       =  alwamt;
                                   mcrcovunt       =  covunt;
                                   mcrcovdys       =  covdys;
                                   mcrapcovdys     =  apcovdys;
                                   mcrutilct       =  utilct;
                                   mcraputilct   =  aputilct;
                              end;

                      end;

                                                                           /* Durable Medical Equipment and Orthodics       */
                                                                           /* Prosthetics and Medical Supplies (COST SHARING*/
                                                                           /* 100% Medicaid based on svccod                 */
                                                                           /* 100% Medicare based on svccod                 */
                                                                           /* ELSE 90% = Medicare/10% = Medicaid            */
         when (bencod in ('DME', 'ORT','MCD', 'PRO', 'MSP', 'MCP'))
               do;
                  if bencod eq 'DME' and 
                     svccod in ("A8000", "A8001") and svctyp eq 'HC' then
                       do;
                            mcdalwunt       =  alwunt;
                            mcdtopamt       =  topamt;
                            mcdnetalwamt    =  netalwamt;
                            mcdalwamt       =  alwamt;
                            mcdcovunt       =  covunt;
                            mcdcovdys       =  covdys;
                            mcdapcovdys     =  apcovdys;
                            mcdutilct       =  utilct;
                            mcdaputilct     =  aputilct;
                            mcdaptrnamt     =  aptrnamt;
                            mcralwunt       =  0;
                            mcrtopamt       =  0;
                            mcrnetalwamt    =  0;
                            mcralwamt       =  0;
                            mcrcovunt       =  0;
                            mcrcovdys       =  0;
                            mcrapcovdys     =  0;
                            mcrutilct       =  0;
                            mcraputilct     =  0;
                            mcraptrnamt     =  0;
                       end;
                  else
                       if bencod eq 'MSP' and 
                          svccod in ("A4554", "T4521", "T4522", "T4523", "T4524", "T4525", "T4526", 
                                     "T4527", "T4528", "T4529", "T4530", "T4531", "T4532", "T4533", "T4534", "T4535",
                                     "T4536", "T4537", "T4538", "T4539", "T4540", "T4541", "T4542", "T4543", "A4510",
                                     "A6531", "A6532", "A6549") and svctyp eq 'HC' then
                            do;
                                 mcdalwunt       =  alwunt;
                                 mcdtopamt       =  topamt;
                                 mcdnetalwamt    =  netalwamt;
                                 mcdalwamt       =  alwamt;
                                 mcdcovunt       =  covunt;
                                 mcdcovdys       =  covdys;
                                 mcdapcovdys     =  apcovdys;
                                 mcdutilct       =  utilct;
                                 mcdaputilct     =  aputilct;
                                 mcdaptrnamt     =  aptrnamt;
                                 mcralwunt       =  0;
                                 mcrtopamt       =  0;
                                 mcrnetalwamt    =  0;
                                 mcralwamt       =  0;
                                 mcrcovunt       =  0;
                                 mcrcovdys       =  0;
                                 mcrapcovdys     =  0;
                                 mcrutilct       =  0;
                                 mcraputilct     =  0;
                                 mcraptrnamt     =  0;
                            end;
                       else
                            if bencod eq 'ORT' and 
                               svccod in ('L3595') and svctyp eq 'HC' then
                                do;
                                      mcdalwunt       =  0;
                                      mcdtopamt       =  0;
                                      mcdnetalwamt    =  0;
                                      mcdalwamt       =  0;
                                      mcdcovunt       =  0;
                                      mcdcovdys       =  0;
                                      mcdapcovdys     =  0;
                                      mcdutilct       =  0;
                                      mcdaputilct     =  0;
                                      mcdaptrnamt     =  0;

                                      mcralwunt       =  alwunt;
                                      mcrtopamt       =  topamt;
                                      mcrnetalwamt    =  netalwamt;
                                      mcralwamt       =  alwamt;
                                      mcrcovunt       =  covunt;
                                      mcrcovdys       =  covdys;
                                      mcrapcovdys     =  apcovdys;
                                      mcrutilct       =  utilct;
                                      mcraputilct     =  aputilct;
                                      mcraptrnamt     =  aptrnamt;
                                end;
                            else
                                do;
                                    cstshrnetalcoiamt =  (.100 * netalwamt);
                                    mcdnetalwamt      =  0.00;
                                    cstshrtopaycoiamt =  (.100 * topamt);
                                    mcdtopamt         =  0.00;
                                    mcdaptrnamt       =  (.100 * aptrnamt);
                                    mcdalwunt         =  alwunt;
                                    mcdalwamt         =  alwamt;
                                    mcdcovunt         =  covunt;
                                    mcdcovdys         =  covdys;
                                    mcdapcovdys       =  0;
                                    mcdutilct         =  utilct;
                                    mcdaputilct       =  0;

                                    mcrtopamt       =  (topamt - cstshrtopaycoiamt);
                                    mcrnetalwamt    =  (netalwamt - cstshrnetalcoiamt);
                                    mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                    mcralwunt       =  alwunt;
                                    mcralwamt       =  alwamt;
                                    mcrcovunt       =  covunt;
                                    mcrcovdys       =  covdys;
                                    mcrapcovdys     =  apcovdys;
                                    mcrutilct       =  utilct;
                                    mcraputilct     =  aputilct;
                                end;
               end;
                                                                                         /*  Ambulance (COST SHARING    */
                                                                                         /*  20% of allowed = Medicaid  */
                                                                                         /*  80% of allowed = Medicare  */
         when (bencod in ('TRA'))
                              do;
                                 cstshrnetalcoiamt =  (.200 * netalwamt);
                                 mcdnetalwamt      =  0.00;
                                 cstshrtopaycoiamt =  (.200 * topamt);
                                 mcdtopamt         =  0.00;
                                 mcdaptrnamt       =  (.200 * aptrnamt);
                                 mcdalwunt         =  alwunt;
                                 mcdalwamt         =  alwamt;
                                 mcdcovunt         =  covunt;
                                 mcdcovdys         =  covdys;
                                 mcdapcovdys       =  0;
                                 mcdutilct         =  utilct;
                                 mcdaputilct       =  0;

                                 mcrtopamt       =  (topamt - cstshrtopaycoiamt);
                                 mcrnetalwamt    =  (netalwamt - cstshrnetalcoiamt);
                                 mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                 mcralwunt       =  alwunt;
                                 mcralwamt       =  alwamt;
                                 mcrcovunt       =  covunt;
                                 mcrcovdys       =  covdys;
                                 mcrapcovdys     =  apcovdys;
                                 mcrutilct       =  utilct;
                                 mcraputilct     =  aputilct;
                              end;
                                                                                         /*  Part B Rx Drugs (COST SHARING*/
                                                                                         /*  20% of allowed = Medicaid  */
                                                                                         /*  80% of allowed = Medicare  */
         when (bencod in ('OPD', 'MMD'))
                              do;
                                 cstshrnetalcoiamt =  (.200 * netalwamt);
                                 mcdnetalwamt      =  0.00;
                                 cstshrtopaycoiamt =  (.200 * topamt);
                                 mcdtopamt         =  0.00;
                                 mcdaptrnamt       =  (.200 * aptrnamt);
                                 mcdalwunt         =  alwunt;
                                 mcdalwamt         =  alwamt;
                                 mcdcovunt         =  covunt;
                                 mcdcovdys         =  covdys;
                                 mcdapcovdys       =  0;
                                 mcdutilct         =  utilct;
                                 mcdaputilct       =  0;

                                 mcrtopamt       =  (topamt - cstshrtopaycoiamt);
                                 mcrnetalwamt    =  (netalwamt - cstshrnetalcoiamt);
                                 mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                 mcralwunt       =  alwunt;
                                 mcralwamt       =  alwamt;
                                 mcrcovunt       =  covunt;
                                 mcrcovdys       =  covdys;
                                 mcrapcovdys     =  apcovdys;
                                 mcrutilct       =  utilct;
                                 mcraputilct     =  aputilct;
                              end;

                                                                               /*  DEFAULT TO: 100% Medicare  */
         otherwise do;
                      mcdalwunt       =  0;
                      mcdtopamt       =  0;
                      mcdnetalwamt    =  0;
                      mcdalwamt       =  0;
                      mcdcovunt       =  0;
                      mcdcovdys       =  0;
                      mcdapcovdys     =  0;
                      mcdutilct       =  0;
                      mcdaputilct     =  0;
                      mcdaptrnamt     =  0;

                      mcralwunt       =  alwunt;
                      mcrtopamt       =  topamt;
                      mcrnetalwamt    =  netalwamt;
                      mcralwamt       =  alwamt;
                      mcrcovunt       =  covunt;
                      mcrcovdys       =  covdys;
                      mcrapcovdys     =  apcovdys;
                      mcrutilct       =  utilct;
                      mcraputilct     =  aputilct;
                      mcraptrnamt     =  aptrnamt;
                   end;
       end;                                                                  /*end select*/

       return;                       /* HS 420008 This returns to 2014 link RKS */
    run;

/* SAS2AWS2: added sas2rs_dataload_bul macro to load data into RS table */
/* %sas2rs_dataload_bul(loadtech=delete, srclib=work, srctbl=svclines, trgtlib=&libn1., trgttbl=svclines); */
data rsstr.svclines;
set svclines;
run;
%mend ccbreakout;
                                                                               /* Silled Nursing  (COST SHARING) */
%macro ccsnfbreakout ; 
     data extract (keep=  membno provno mosdat svcdat enddat aptrndat clamno lineno
                          bencod svcstat unitct clatyp whdamt
                          alwunt     mcdalwunt     mcralwunt   
                          topamt     mcdtopamt     mcrtopamt   
                          netalwamt  mcdnetalwamt  mcrnetalwamt
                          alwamt     mcdalwamt     mcralwamt   
                          covunt     mcdcovunt     mcrcovunt   
                          covdys     mcdcovdys     mcrcovdys   
                          apcovdys   mcdapcovdys   mcrapcovdys 
                          utilct     mcdutilct     mcrutilct   
                          aputilct   mcdaputilct   mcraputilct 
                          aptrnamt   mcdaptrnamt   mcraptrnamt );
          set /*&libn1.*/rsstr.svclines;                                   /* #412625 RKS */
          where bencod in ('SN1', 'SN2', 'SN3')
           ;
     run;

     proc sort data=extract (where=(svcstat ne 'PA')) out=allothersnf ;
          by membno provno mosdat svcdat enddat clamno aptrndat ;
     run;

     proc sort data=extract (where=(svcstat eq 'PA'));
          by membno provno mosdat svcdat enddat clamno aptrndat ;
     run;

     data snfdups notdups1;
          set extract;
          by membno provno mosdat svcdat enddat ;

          If first.enddat and last.enddat then output notdups1;
          else output snfdups;
     run;

     data snfdups notdups2;
          set snfdups;
          by membno provno mosdat svcdat enddat clamno ;

          If first.clamno and last.clamno then output snfdups;
          else output notdups2;
     run;

     proc sort data=snfdups;
          by membno provno mosdat svcdat enddat aptrndat ;
     run;

     data truedups notdups3;
          set snfdups;
          by membno provno mosdat svcdat enddat ;

          if first.enddat and not last.enddat then 
               output notdups3;
          else 
              do;
                   mcdalwunt         =  0;
                   mcdtopamt         =  0;
                   mcdnetalwamt      =  0;
                   mcdalwamt         =  0;
                   mcdcovunt         =  0;
                   mcdcovdys         =  0;
                   mcdapcovdys       =  0;
                   mcdutilct         =  0;
                   mcdaputilct       =  0;
                   mcdaptrnamt       =  0;
                   cstshrnetalcoiamt =  0; 
                   cstshrnetalcopamt =  0; 
                   cstshrnetaldedamt =  0;
                   cstshrtopaycoiamt =  0; 
                   cstshrtopaycopamt =  0; 
                   cstshrtopaydedamt =  0;  

                   mcralwunt       =  alwunt;
                   mcrtopamt       =  topamt;
                   mcrnetalwamt    =  netalwamt;
                   mcralwamt       =  alwamt;
                   mcrcovunt       =  covunt;
                   mcrcovdys       =  covdys;
                   mcrapcovdys     =  apcovdys;
                   mcrutilct       =  utilct;
                   mcraputilct     =  aputilct;
                   mcraptrnamt     =  aptrnamt;

                   output truedups;
              end;
     run;

     data notdups;
          set notdups1
              notdups2
              notdups3
              allothersnf ;
          format calc_enddat mmddyy10. ;

          if svcdat = enddat then 
               calc_enddat = enddat + (abs(unitct)-1);
          else 
               calc_enddat = enddat;
     run;

     proc sort data=notdups;
          by membno svcdat calc_enddat aptrndat clamno lineno;
     run;

     data groups (drop= save_svcdat save_enddat  save_calc_enddat);
          set notdups;
          by membno;

          retain save_svcdat
                 save_enddat
                 save_calc_enddat
                 rollup_ind;
               ;

          if first.membno then
              do;
                 rollup_ind       = 1;
                 save_svcdat      = svcdat;
                 save_enddat      = enddat;
                 save_calc_enddat = calc_enddat;
                 output;
              end;
          else
              do;
                  if   save_svcdat eq svcdat 
                   and save_calc_enddat eq calc_enddat then 
                      do;
                         save_calc_enddat = calc_enddat;
                         output;
                      end;
                  else
                      if (svcdat - save_calc_enddat) ge 0 and
                         (svcdat - save_calc_enddat) le 1 then
                         do;
                             save_svcdat = svcdat;
                             save_enddat = enddat;
                             save_calc_enddat = calc_enddat;
                             output;
                         end;
                      else
                         do;
                             rollup_ind = rollup_ind + 1 ;
                             save_svcdat = svcdat;
                             save_enddat = enddat;
                             save_calc_enddat = calc_enddat;
                             output;
                         end; 
              end;
     run;

     proc sort data=groups;
          by membno rollup_ind svcdat calc_enddat aptrndat clamno lineno;
     run;

            ******************************************************************************************************;
            *******************************    BRAKEOUT LOGIC STARTS HERE    *************************************;
            ******************************************************************************************************;

     data groups; 
        set groups;
        by membno rollup_ind;

        retain saved_svcdat;
        retain days_used 0;
        retain days_used_apcovdys 0;
        retain cstshrnet_used 0;
                
        cstshrnetalcoiamt = 0; 
        cstshrnetalcopamt = 0; 
        cstshrnetaldedamt = 0;
        cstshrtopaycoiamt = 0; 
        cstshrtopaycopamt = 0; 
        cstshrtopaydedamt = 0;  

        if clatyp in ('IN', 'XN') and whdamt ne 0.00 then
            do;
                if netalwamt eq 0.00 then
                   whdrate = 1.00;
                else
                   whdrate = topamt/netalwamt;
            end;
        else
            whdrate = 1.00;

        retain days_used  0
               saved_svcdat;                              /* HS 420008 RKS Breakout is based on initial date of service retain that date */ 

				                                          /* HS 420008 RKS Breakout is based on initial date of service for the group */ 
		if first.rollup_ind then
		   do;
		       saved_svcdat = svcdat;
		   end;

        if saved_svcdat ge '01JAN2012'd and saved_svcdat le '31DEC2013'd then 
           do;
               link ccsnfbreakout13;
           end;
        else 
           do;
               link ccsnfbreakout14;
           end;
 
        return;                                           /* HS 420008 RKS This returns to top of data step prevents fall through */

            ******************************************************************************************************;
            *************************   HS 420008 RKS  SNF breakout logic for 2013   *****************************;
            ******************************************************************************************************;
ccsnfbreakout13:
                                                                     *  SINGLE ROll UP  *;
            if first.rollup_ind and last.rollup_ind then
                do;
                    if svcstat not in ('PD', 'DE') then
                       do;
                             if covunt le 30 then
                                  do;
                                     if abs(netalwamt) gt (covunt * 50.00) then
                                        do;
                                           if netalwamt gt 0 then
                                              do;
                                                  cstshrnetalcopamt = (covunt * 50.00);
                                                  cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                  mcdnetalwamt      = 0.00;
                                              end;
                                           else
                                              do;
                                                  cstshrnetalcopamt = (covunt * 50.00 * -1);
                                                  cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                  mcdnetalwamt      = 0.00;
                                              end;
                                        end;
                                     else
                                        do;
                                            cstshrnetalcopamt = netalwamt;
                                            cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                            mcdnetalwamt =  0.00;
                                        end;

                                     mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                     mcrtopamt    = mcrnetalwamt * whdrate;
                                     mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                     if abs(aptrnamt) gt (covunt * 50) then
                                        do;
                                           if aptrnamt gt 0 then
                                                mcdaptrnamt = (covunt * 50.00) * whdrate;          /*HS 323337 */
                                           else mcdaptrnamt = (covunt * 50.00 * -1) * whdrate;     /*HS 323337 */
                                        end;
                                     else
                                        mcdaptrnamt =  aptrnamt;

                                     mcdalwunt       =  alwunt;
                                     mcdalwamt       =  alwamt;
                                     mcdcovunt       =  covunt;
                                     mcdcovdys       =  covdys;
                                     mcdapcovdys     =  0;
                                     mcdutilct       =  utilct;
                                     mcdaputilct     =  0;

                                     mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                     mcralwunt       =  alwunt;
                                     mcralwamt       =  alwamt;
                                     mcrcovunt       =  covunt;
                                     mcrcovdys       =  covdys;
                                     mcrapcovdys     =  apcovdys;
                                     mcrutilct       =  utilct;
                                     mcraputilct     =  aputilct;
                                  end;                                                       * end of le 30 days *;
                             else
                                  if covunt gt 30 and covunt le 100 then
                                       do;                                                   * 30 days x $50.00 = 1500.00 *;
                                          if abs(netalwamt) gt 1500.00 then
                                             do;
                                                if netalwamt gt 0 then
                                                   do;
                                                       cstshrnetalcopamt = 1500.00; 
                                                       cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                       mcdnetalwamt      = 0.00;
                                                   end;
                                                else
                                                   do;
                                                       cstshrnetalcopamt = (1500.00 * -1);
                                                       cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                       mcdnetalwamt      = 0.00;
                                                   end;
                                             end;
                                          else
                                             do;
                                                 cstshrnetalcopamt = netalwamt;
                                                 cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                 mcdnetalwamt =  0.00;
                                             end;

                                          mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                          mcrtopamt    = mcrnetalwamt * whdrate;
                                          mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                          if abs(aptrnamt) gt 1500.00 then
                                             do;
                                                if aptrnamt gt 0 then
                                                     mcdaptrnamt = 1500.00 * whdrate;              /*HS 323337 */
                                                else mcdaptrnamt = (1500.00 * -1) * whdrate;       /*HS 323337 */
                                             end;
                                          else
                                             mcdaptrnamt =  aptrnamt;

                                          mcdalwunt       =  alwunt;
                                          mcdalwamt       =  alwamt;
                                          mcdcovunt       =  covunt;
                                          mcdcovdys       =  covdys;
                                          mcdapcovdys     =  0;
                                          mcdutilct       =  utilct;
                                          mcdaputilct     =  0;

                                          mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                          mcralwunt       =  alwunt;
                                          mcralwamt       =  alwamt;
                                          mcrcovunt       =  covunt;
                                          mcrcovdys       =  covdys;
                                          mcrapcovdys     =  apcovdys;
                                          mcrutilct       =  utilct;
                                          mcraputilct     =  aputilct;
                                       end; 
                                 else
                                      do;                                                        * single over 100 days sn3 *;
                                          if abs(netalwamt) gt ( ((netalwamt/covunt)*(covunt-100)) + 1500.00) then
                                             do;
                                                if netalwamt gt 0 then
                                                   do;
                                                       cstshrnetalcopamt = 1500.00;
                                                       cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                       mcdnetalwamt      = (netalwamt/covunt)*(covunt-100);
                                                   end;
                                                else 
                                                   do;
                                                       cstshrnetalcopamt = (1500.00 * -1);
                                                       cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                       mcdnetalwamt = ((netalwamt/covunt)*(covunt-100) * -1);
                                                   end;
                                             end;
                                          else
                                             do;
                                                 cstshrnetalcopamt = netalwamt;
                                                 cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                 mcdnetalwamt      =  netalwamt - cstshrnetalcopamt;
                                             end;

                                          mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                          mcrtopamt    = mcrnetalwamt * whdrate;
                                          mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                          if abs(aptrnamt) gt ( ((aptrnamt/covunt)*(covunt-100)) + 1500.00) then
                                             do;
                                                if aptrnamt gt 0 then
                                                   do;
                                                       mcdaptrnamt = 1500.00 * whdrate;               /*HS 323337 */
                                                   end;
                                                else 
                                                   do;
                                                       mcdaptrnamt = (1500.00 * -1) * whdrate;        /*HS 323337 */
                                                   end;
                                             end;
                                          else
                                             do;
                                                 mcdaptrnamt = aptrnamt;
                                             end;
          
                                          mcdalwunt       =  alwunt;
                                          mcdalwamt       =  alwamt;
                                          mcdcovunt       =  covunt;
                                          mcdcovdys       =  covdys;
                                          mcdapcovdys     =  (apcovdys - 100);
                                          mcdutilct       =  utilct;
                                          mcdaputilct     =  aputilct;

                                          mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                          mcralwunt       =  alwunt;
                                          mcralwamt       =  alwamt;
                                          mcrcovunt       =  covunt;
                                          mcrcovdys       =  covdys;
                                          mcrapcovdys     =  (apcovdys - mcdapcovdys);
                                          mcrutilct       =  utilct;
                                          mcraputilct     =  aputilct;
                                      end;
                       end;                                                              * end of not PD logic *;
                    else
                       do;                                                               *  To handle Denies *;
                             mcdtopamt       =  0;
                             mcdnetalwamt    =  0;
                             mcdalwamt       =  0;
                             mcdaptrnamt     =  0;
                             mcdalwunt       =  0;
                             mcdcovunt       =  0;
                             mcdcovdys       =  0;
                             mcdapcovdys     =  0;
                             mcdutilct       =  0;
                             mcdaputilct     =  0;

                             mcrtopamt       =  topamt;
                             mcrnetalwamt    =  netalwamt;
                             mcralwamt       =  alwamt;
                             mcraptrnamt     =  aptrnamt;
                             mcralwunt       =  alwunt;
                             mcrcovunt       =  covunt;
                             mcrcovdys       =  covdys;
                             mcrapcovdys     =  apcovdys;
                             mcrutilct       =  utilct;
                             mcraputilct     =  aputilct;
                       end;

                end;                                                                  * end of single roll up *;

            ************************    consecutive billings logic starts here  **********************************;
            else 
                 do;
                   if first.rollup_ind then 
                      do;
                          days_used          = 0;
                          cstshrnet_used     = 0;
                          cstshrtop_used     = 0;
                          days_used_apcovdys = 0;
                      end;
            
                   if svcstat not in ('PD', 'DE') then
                     do;                             
                                **************************************************************** less then 30 days *;
                        if (days_used + covunt) lt 30 then 
                            do;
                                if abs(netalwamt) gt (covunt * 50.00) then
                                   do;
                                      if netalwamt gt 0 then
                                         do;
                                             cstshrnetalcopamt = (covunt * 50.00);
                                             cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                             mcdnetalwamt      = 0.00;
                                         end;
                                      else
                                         do;
                                             cstshrnetalcopamt = (covunt * 50.00 * -1);
                                             cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                             mcdnetalwamt      = 0.00;
                                         end;
                                   end;
                                else
                                   do;
                                       cstshrnetalcopamt = netalwamt;
                                       cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                       mcdnetalwamt      = 0.00;
                                   end;                  

                                mcrnetalwamt    =  netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                mcrtopamt       =  mcrnetalwamt * whdrate;
                                mcdtopamt       =  topamt - mcrtopamt - cstshrtopaycopamt;
                                mcdaptrnamt     =  mcdtopamt;
                                mcdalwunt       =  alwunt;
                                mcdalwamt       =  alwamt;
                                mcdcovunt       =  covunt;
                                mcdcovdys       =  covdys;
                                mcdapcovdys     =  0;
                                mcdutilct       =  utilct;
                                mcdaputilct     =  0;

                                mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                mcralwunt       =  alwunt;
                                mcralwamt       =  alwamt;
                                mcrcovunt       =  covunt;
                                mcrcovdys       =  covdys;
                                mcrapcovdys     =  apcovdys;
                                mcrutilct       =  utilct;
                                mcraputilct     =  aputilct;

                                days_used          = days_used + covunt;
                                cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                days_used_apcovdys = days_used_apcovdys + apcovdys;
                            end;                                                        
                                ************************************************************* end of le 30 days *;

                                ************************************************************* transition from 30 to le 100 *;
                        else
                            if days_used lt 30 and 
                              (days_used + covunt) gt 30  and 
                              (days_used + covunt) le 100 then 
                                do;   
                                     if abs(netalwamt) gt ((30 - days_used) * 50.00) then
                                        do;
                                           if netalwamt gt 0 then
                                              do;
                                                  cstshrnetalcopamt = ((30 - days_used) * 50.00);
                                                  cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                  mcdnetalwamt      = 0.00;
                                              end;
                                           else
                                              do;
                                                  cstshrnetalcopamt = ((days_used - 30) * 50.00 * -1);
                                                  cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                  mcdnetalwamt      = 0.00;
                                              end;
                                        end;
                                     else
                                        do;
                                            cstshrnetalcopamt = netalwamt;
                                            cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                            mcdnetalwamt      = 0.00;
                                        end;

                                     mcrnetalwamt    =  netalwamt - mcdnetalwamt - cstshrtopaycopamt;
                                     mcrtopamt       =  mcrnetalwamt * whdrate;
                                     mcdtopamt       =  topamt - mcrtopamt - cstshrtopaycopamt;
                                     mcdaptrnamt     =  mcdtopamt;
                                     mcdalwunt       =  alwunt;
                                     mcdalwamt       =  alwamt;
                                     mcdcovunt       =  covunt;
                                     mcdcovdys       =  covdys;
                                     mcdapcovdys     =  0;
                                     mcdutilct       =  utilct;
                                     mcdaputilct     =  0;

                                     mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                     mcralwunt       =  alwunt;
                                     mcralwamt       =  alwamt;
                                     mcrcovunt       =  covunt;
                                     mcrcovdys       =  covdys;
                                     mcrapcovdys     =  apcovdys;
                                     mcrutilct       =  utilct;
                                     mcraputilct     =  aputilct;

                                     days_used          = days_used + covunt;
                                     cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                     days_used_apcovdys = days_used_apcovdys + apcovdys;
                                end;
                                *********************************************************** end of transition to 30+ days *;

                                ************************************************************* 31 - 100 days *;
                            else
                                if days_used gt 30 and 
                                  (days_used + covunt) le 100 then 
                                  do;  
                                       mcdnetalwamt    =  0.00;
                                       mcrnetalwamt    =  netalwamt - mcdnetalwamt;
                                       mcrtopamt       =  mcrnetalwamt * whdrate;
                                       mcdtopamt       =  topamt - mcrtopamt;
                                       mcdaptrnamt     =  mcdtopamt;
                                       mcdalwunt       =  alwunt;
                                       mcdalwamt       =  alwamt;
                                       mcdcovunt       =  covunt;
                                       mcdcovdys       =  covdys;
                                       mcdapcovdys     =  0;
                                       mcdutilct       =  utilct;
                                       mcdaputilct     =  0;

                                       mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                       mcralwunt       =  alwunt;
                                       mcralwamt       =  alwamt;
                                       mcrcovunt       =  covunt;
                                       mcrcovdys       =  covdys;
                                       mcrapcovdys     =  apcovdys;
                                       mcrutilct       =  utilct;
                                       mcraputilct     =  aputilct;

                                       days_used          = days_used + covunt;
                                       cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                       days_used_apcovdys = days_used_apcovdys + apcovdys;
                                  end;
                                 ************************************************************* end of 30-100 days *;

                                 **************************************************** transition from le 100 to over 100 days *;
                                else
                                    if days_used le 100 and 
                                      (days_used + covunt) gt 100 then 
                                      do;
                                         if abs(netalwamt) gt (1500.00 - cstshrnet_used) then
                                            do;
                                              if netalwamt gt 0 then
                                                 do;
                                                    cstshrnetalcopamt = (1500.00 - cstshrnet_used);
                                                    cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                    mcdnetalwamt = (((days_used + covunt - 100)/covunt) * netalwamt) - cstshrnetalcopamt;
                                                    mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                                 end;
                                              else
                                                 do;
                                                    cstshrnetalcopamt = ((1500.00 - cstshrnet_used) * -1);
                                                    cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                    mcrnetalwamt = (((days_used + covunt - 100)/covunt) * netalwamt) - cstshrnetalcopamt;
                                                    mcdnetalwamt = netalwamt - mcrnetalwamt  - cstshrnetalcopamt;
                                                 end;
                                            end;
                                         else
                                            do;
                                                cstshrnetalcopamt = (1500.00 - cstshrnet_used);
                                                cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                mcrnetalwamt = (((days_used + covunt - 100)/covunt) * netalwamt) - cstshrnetalcopamt;
                                                mcdnetalwamt = netalwamt - mcrnetalwamt  - cstshrnetalcopamt;
                                            end;

                                         mcdtopamt       =  mcdnetalwamt * whdrate;
                                         mcrtopamt       =  mcrnetalwamt * whdrate;
                                         mcdaptrnamt     =  mcdtopamt;
                                         mcdalwunt       =  alwunt;
                                         mcdalwamt       =  alwamt;
                                         mcdcovunt       =  covunt;
                                         mcdcovdys       =  covdys;
                                         mcdapcovdys     =  ((days_used_apcovdys + apcovdys)- 100);
                                         mcdutilct       =  utilct;
                                         mcdaputilct     =  aputilct;

                                         mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                         mcralwunt       =  alwunt;
                                         mcralwamt       =  alwamt;
                                         mcrcovunt       =  covunt;
                                         mcrcovdys       =  covdys;
                                         mcrapcovdys     =  apcovdys - mcdapcovdys;
                                         mcrutilct       =  utilct;
                                         mcraputilct     =  aputilct;

                                         days_used          = days_used + covunt;
                                         cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                         days_used_apcovdys = days_used_apcovdys + apcovdys;
                                   end;
                                 ******************************************** end of transition from le 100 to over 100 days *;


                                 ************************************************************ over 100 days *;
                                    else
                                       if days_used gt 100 then 
                                         do;
                                             cstshrnetalcopamt = 0.00;
                                             cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                             mcdnetalwamt      = netalwamt;
                                             mcrnetalwamt      =  netalwamt - mcdnetalwamt;
                                             mcrtopamt         =  mcrnetalwamt * whdrate;
                                             mcdtopamt         =  topamt - mcrtopamt;
                                             mcdaptrnamt       =  mcdtopamt;
                                             mcdalwunt         =  alwunt;
                                             mcdalwamt         =  alwamt;
                                             mcdcovunt         =  covunt;
                                             mcdcovdys         =  covdys;
                                             mcdapcovdys       =  apcovdys;
                                             mcdutilct         =  utilct;
                                             mcdaputilct       =  aputilct;

                                             mcraptrnamt       =  (aptrnamt - mcdaptrnamt);
                                             mcralwunt         =  alwunt;
                                             mcralwamt         =  alwamt;
                                             mcrcovunt         =  covunt;
                                             mcrcovdys         =  covdys;
                                             mcrapcovdys       =  0;
                                             mcrutilct         =  utilct;
                                             mcraputilct       =  0;
     
                                             days_used          = days_used + covunt;
                                             cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                             days_used_apcovdys = days_used_apcovdys + apcovdys;
                                   end;    **************************************************** end of over 100 days;

                     end;        ************************************************************** end of consecutive bill split *;
     
                   else
                      do;                                                   *  To handle Denies  for consecutive bill logic  *;
                           mcdtopamt       =  0;
                           mcdnetalwamt    =  0;
                           mcdalwamt       =  0;
                           mcdaptrnamt     =  0;
                           mcdalwunt       =  0;
                           mcdcovunt       =  0;
                           mcdcovdys       =  0;
                           mcdapcovdys     =  0;
                           mcdutilct       =  0;
                           mcdaputilct     =  0;

                           mcrtopamt       =  topamt;
                           mcrnetalwamt    =  netalwamt;
                           mcralwamt       =  alwamt;
                           mcraptrnamt     =  aptrnamt;
                           mcralwunt       =  alwunt;
                           mcrcovunt       =  covunt;
                           mcrcovdys       =  covdys;
                           mcrapcovdys     =  apcovdys;
                           mcrutilct       =  utilct;
                           mcraputilct     =  aputilct;
                      end;
                 end;                                                     /* end of otherwise consecutive group */;
     return;                                                          /* returns back to snf link 2013 */


            ******************************************************************************************************;
            *************************   HS 420008 RKS  SNF breakout logic for 2014   *****************************;
            ******************************************************************************************************;
ccsnfbreakout14:
                                                                  /************************  SINGLE ROll UP  ****************/
            if first.rollup_ind and last.rollup_ind then
                do;
                    if svcstat not in ('PD', 'DE') then
                       do;
                           if covunt le 20 then
                              do;
                                 if abs(netalwamt) gt (covunt * 25.00) then
                                    do;
                                       if netalwamt gt 0 then
                                          do;
                                              cstshrnetalcopamt = (covunt * 25.00);
                                              cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                              mcdnetalwamt      = 0.00;
                                          end;
                                       else
                                          do;
                                              cstshrnetalcopamt = (covunt * 25.00 * -1);
                                              cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                              mcdnetalwamt      = 0.00;
                                          end;
                                    end;
                                 else
                                    do;
                                        cstshrnetalcopamt = netalwamt;
                                        cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                        mcdnetalwamt =  0.00;
                                    end;

                                 mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                 mcrtopamt    = mcrnetalwamt * whdrate;
                                 mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                 if abs(aptrnamt) gt (covunt * 25) then
                                    do;
                                       if aptrnamt gt 0 then
                                            mcdaptrnamt = (covunt * 25.00) * whdrate;         
                                       else mcdaptrnamt = (covunt * 25.00 * -1) * whdrate;  
                                    end;
                                 else
                                    mcdaptrnamt =  aptrnamt;

                                 mcdalwunt       =  alwunt;
                                 mcdalwamt       =  alwamt;
                                 mcdcovunt       =  covunt;
                                 mcdcovdys       =  covdys;
                                 mcdapcovdys     =  0;
                                 mcdutilct       =  utilct;
                                 mcdaputilct     =  0;

                                 mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                 mcralwunt       =  alwunt;
                                 mcralwamt       =  alwamt;
                                 mcrcovunt       =  covunt;
                                 mcrcovdys       =  covdys;
                                 mcrapcovdys     =  apcovdys;
                                 mcrutilct       =  utilct;
                                 mcraputilct     =  aputilct;
                              end;                                                       /*************** end of le 20 days */
                           else
                              if covunt gt 20 and covunt le 40 then
                                   do;                                                   /* 20 days x $25.00 = 500.00  days 21-30 @ $152 */
                                      if abs(netalwamt) ge (500.00 + ((covunt - 20)*152)) then
                                         do;
                                            if netalwamt gt 0 then
                                               do;
                                                   cstshrnetalcopamt = (500.00 + ((covunt - 20)*152)); 
                                                   cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                   mcdnetalwamt      = 0.00;
                                               end;
                                            else
                                               do;
                                                   cstshrnetalcopamt = ((500.00 + ((covunt - 20)*152)) * -1);
                                                   cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                                   mcdnetalwamt      = 0.00;
                                               end;
                                         end;
                                      else
                                         do;
                                             cstshrnetalcopamt = netalwamt;
                                             cstshrtopaycopamt =  cstshrnetalcopamt * whdrate;
                                             mcdnetalwamt =  0.00;
                                         end;

                                      mcrnetalwamt = netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                      mcrtopamt    = mcrnetalwamt * whdrate;
                                      mcdtopamt    = topamt - mcrtopamt - cstshrtopaycopamt;

                                      if abs(aptrnamt) gt (500.00 + ((covunt - 20)*152)) then
                                         do;
                                            if aptrnamt gt 0 then
                                                 mcdaptrnamt = (500.00 + ((covunt - 20)*152)) * whdrate;            
                                            else mcdaptrnamt = ((500.00 + ((covunt - 20)*152)) * -1) * whdrate; 
                                         end;
                                      else
                                         mcdaptrnamt =  aptrnamt;

                                      mcdalwunt       =  alwunt;
                                      mcdalwamt       =  alwamt;
                                      mcdcovunt       =  covunt;
                                      mcdcovdys       =  covdys;
                                      mcdapcovdys     =  0;
                                      mcdutilct       =  utilct;
                                      mcdaputilct     =  0;

                                      mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                      mcralwunt       =  alwunt;
                                      mcralwamt       =  alwamt;
                                      mcrcovunt       =  covunt;
                                      mcrcovdys       =  covdys;
                                      mcrapcovdys     =  apcovdys;
                                      mcrutilct       =  utilct;
                                      mcraputilct     =  aputilct;
                                   end; 
                              else
                                   do;                                                        /***** single over 40 days sn3 */
                                      mcdtopamt       =  0;
                                      mcdnetalwamt    =  0;
                                      mcdalwamt       =  0;
                                      mcdaptrnamt     =  0;
                                      mcdalwunt       =  0;
                                      mcdcovunt       =  0;
                                      mcdcovdys       =  0;
                                      mcdapcovdys     =  0;
                                      mcdutilct       =  0;
                                      mcdaputilct     =  0;

                                      mcrtopamt       =  topamt;
                                      mcrnetalwamt    =  netalwamt;
                                      mcralwamt       =  alwamt;
                                      mcraptrnamt     =  aptrnamt;
                                      mcralwunt       =  alwunt;
                                      mcrcovunt       =  covunt;
                                      mcrcovdys       =  covdys;
                                      mcrapcovdys     =  apcovdys;
                                      mcrutilct       =  utilct;
                                      mcraputilct     =  aputilct;
                                   end;
                       end;                                                                     /******* end of not PD logic */
                    else
                       do;                                                                      /*******  To handle Denies */
                             mcdtopamt       =  0;
                             mcdnetalwamt    =  0;
                             mcdalwamt       =  0;
                             mcdaptrnamt     =  0;
                             mcdalwunt       =  0;
                             mcdcovunt       =  0;
                             mcdcovdys       =  0;
                             mcdapcovdys     =  0;
                             mcdutilct       =  0;
                             mcdaputilct     =  0;

                             mcrtopamt       =  topamt;
                             mcrnetalwamt    =  netalwamt;
                             mcralwamt       =  alwamt;
                             mcraptrnamt     =  aptrnamt;
                             mcralwunt       =  alwunt;
                             mcrcovunt       =  covunt;
                             mcrcovdys       =  covdys;
                             mcrapcovdys     =  apcovdys;
                             mcrutilct       =  utilct;
                             mcraputilct     =  aputilct;
                       end;
                end;                                            /**************************   end of single roll up   *******************/
            else    
                do;                                             /************   2014 CONSECUTIVE billings logic starts here   ***********/
                   if first.rollup_ind then 
                      do;
                          days_used          = 0;
                          cstshrnet_used     = 0;
                          cstshrtop_used     = 0;
                          days_used_apcovdys = 0;
                      end;
            
                   if svcstat not in ('PD', 'DE') then
                     do;                             
                                                                                             /************************ less then 20 days */
                        if (days_used + covunt) lt 20 then 
                            do;
                                if abs(netalwamt) gt (covunt * 25.00) then
                                   do;
                                      if netalwamt gt 0 then
                                         do;
                                             cstshrnetalcopamt = (covunt * 25.00);
                                             cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                             mcdnetalwamt      = 0.00;
                                         end;
                                      else
                                         do;
                                             cstshrnetalcopamt = (covunt * 25.00 * -1);
                                             cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                             mcdnetalwamt      = 0.00;
                                         end;
                                   end;
                                else
                                   do;
                                       cstshrnetalcopamt = netalwamt;
                                       cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                       mcdnetalwamt      = 0.00;
                                   end;                  

                                mcrnetalwamt    =  netalwamt - mcdnetalwamt - cstshrnetalcopamt;
                                mcrtopamt       =  mcrnetalwamt * whdrate;
                                mcdtopamt       =  topamt - mcrtopamt - cstshrtopaycopamt;
                                mcdaptrnamt     =  mcdtopamt;
                                mcdalwunt       =  alwunt;
                                mcdalwamt       =  alwamt;
                                mcdcovunt       =  covunt;
                                mcdcovdys       =  covdys;
                                mcdapcovdys     =  0;
                                mcdutilct       =  utilct;
                                mcdaputilct     =  0;

                                mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                mcralwunt       =  alwunt;
                                mcralwamt       =  alwamt;
                                mcrcovunt       =  covunt;
                                mcrcovdys       =  covdys;
                                mcrapcovdys     =  apcovdys;
                                mcrutilct       =  utilct;
                                mcraputilct     =  aputilct;

                                days_used          = days_used + covunt;
                                cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                days_used_apcovdys = days_used_apcovdys + apcovdys;
                            end;                                                        
                                                                                                     /************************** end of le 20 days */

                                                                                          /*************************** transition from 20 to le 40 */
                        else
                            if days_used lt 20 and 
                              (days_used + covunt) gt 20  and 
                              (days_used + covunt) le 40 then 
                              do;   
                                     if abs(netalwamt) gt ((20 - days_used) * 25.00) + 
                                                          ((covunt - (20 - days_used)) * 152.00) then
                                        do;
                                           if netalwamt gt 0 then
                                              do;
                                                  cstshrnetalcopamt = ((20 - days_used)  * 25.00) + 
                                                         ((covunt - (20 - days_used)) * 152.00);

                                                  cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                  
                                                  mcdnetalwamt = 0;
                                              end;
                                           else
                                              do;
                                                  cstshrnetalcopamt = ((20 - days_used) * 25.00) + 
                                                     ((covunt - (20 - days_used) * 152.00) * -1);

                                                  cstshrtopaycopamt = cstshrnetalcopamt  * whdrate;

                                                  mcdnetalwamt = (netalwamt - cstshrnetalcopamt) * -1;
                                              end;
                                        end;
                                     else
                                        do;
                                            cstshrnetalcopamt = netalwamt;
                                            cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                            mcdnetalwamt      = 0.00;
                                        end;

                                     mcrnetalwamt    =  netalwamt - mcdnetalwamt - cstshrtopaycopamt;
                                     mcrtopamt       =  mcrnetalwamt * whdrate;
                                     mcdtopamt       =  topamt - mcrtopamt - cstshrtopaycopamt;
                                     mcdaptrnamt     =  mcdtopamt;
                                     mcdalwunt       =  alwunt;
                                     mcdalwamt       =  alwamt;
                                     mcdcovunt       =  covunt;
                                     mcdcovdys       =  covdys;
                                     mcdapcovdys     =  0;
                                     mcdutilct       =  utilct;
                                     mcdaputilct     =  0;

                                     mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                     mcralwunt       =  alwunt;
                                     mcralwamt       =  alwamt;
                                     mcrcovunt       =  covunt;
                                     mcrcovdys       =  covdys;
                                     mcrapcovdys     =  apcovdys;
                                     mcrutilct       =  utilct;
                                     mcraputilct     =  aputilct;

                                     days_used          = days_used + covunt;
                                     cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                     days_used_apcovdys = days_used_apcovdys + apcovdys;
                              end;
                                                                                              /************************ end of transition to 20+ days */

                                                                                              /********************************** 21 - 40 days ********/
                            else
                              if days_used gt 20 and 
                                (days_used + covunt) le 40 then 
                                do;  
                                   if abs(netalwamt) gt (covunt * 152.00) then
                                      do;
                                         if netalwamt gt 0 then
                                            do;
                                                cstshrnetalcopamt = (covunt * 152.00);
                                                cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      = 0.00;
                                            end;
                                         else
                                            do;
                                                cstshrnetalcopamt = (covunt * 152.00 * -1);
                                                cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      = 0.00;
                                            end;
                                      end;
                                   else
                                      do;
                                          cstshrnetalcopamt = netalwamt;
                                          cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                          mcdnetalwamt      = 0.00;
                                      end;

                                   mcrnetalwamt    =  netalwamt - mcdnetalwamt - cstshrtopaycopamt;
                                   mcrtopamt       =  mcrnetalwamt * whdrate;
                                   mcdtopamt       =  topamt - mcrtopamt - cstshrtopaycopamt;
                                   mcdaptrnamt     =  mcdtopamt;
                                   mcdalwunt       =  alwunt;
                                   mcdalwamt       =  alwamt;
                                   mcdcovunt       =  covunt;
                                   mcdcovdys       =  covdys;
                                   mcdapcovdys     =  0;
                                   mcdutilct       =  utilct;
                                   mcdaputilct     =  0;

                                   mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                   mcralwunt       =  alwunt;
                                   mcralwamt       =  alwamt;
                                   mcrcovunt       =  covunt;
                                   mcrcovdys       =  covdys;
                                   mcrapcovdys     =  apcovdys;
                                   mcrutilct       =  utilct;
                                   mcraputilct     =  aputilct;

                                   days_used          = days_used + covunt;
                                   cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                   days_used_apcovdys = days_used_apcovdys + apcovdys;
                                end;
                                                                                       /******************* end of 21-40 days *********************/

                                                                                       /******************* transition from le 40 to over 40 days */
                              else
                                  if days_used lt 40 and 
                                    (days_used + covunt) gt 40 then
                                    do;   
                                         if abs(netalwamt) gt ((40 - days_used) * 152.00) then
                                            do;
                                               if netalwamt gt 0 then
                                                  do;
                                                      cstshrnetalcopamt = ((40 - days_used)  * 152.00);
                                                      cstshrtopaycopamt = cstshrnetalcopamt  * whdrate;
                                                      mcdnetalwamt      = 0.00;
                                                  end;
                                               else
                                                  do;
                                                      cstshrnetalcopamt = (((40 - days_used)  * 152.00) * -1);
                                                      cstshrtopaycopamt = cstshrnetalcopamt  * whdrate;
                                                      mcdnetalwamt      = 0.00;
                                                  end;
                                            end;
                                         else
                                            do;
                                                cstshrnetalcopamt = netalwamt;
                                                cstshrtopaycopamt = cstshrnetalcopamt * whdrate;
                                                mcdnetalwamt      = 0.00;
                                            end;

                                         mcrnetalwamt    =  netalwamt - mcdnetalwamt - cstshrtopaycopamt;
                                         mcrtopamt       =  mcrnetalwamt * whdrate;
                                         mcdtopamt       =  topamt - mcrtopamt - cstshrtopaycopamt;
                                         mcdaptrnamt     =  mcdtopamt;
                                         mcdalwunt       =  alwunt;
                                         mcdalwamt       =  alwamt;
                                         mcdcovunt       =  covunt;
                                         mcdcovdys       =  covdys;
                                         mcdapcovdys     =  0;
                                         mcdutilct       =  utilct;
                                         mcdaputilct     =  0;

                                         mcraptrnamt     =  (aptrnamt - mcdaptrnamt);
                                         mcralwunt       =  alwunt;
                                         mcralwamt       =  alwamt;
                                         mcrcovunt       =  covunt;
                                         mcrcovdys       =  covdys;
                                         mcrapcovdys     =  apcovdys;
                                         mcrutilct       =  utilct;
                                         mcraputilct     =  aputilct;

                                         days_used          = days_used + covunt;
                                         cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                         days_used_apcovdys = days_used_apcovdys + apcovdys;
                                    end;                                                    /********** end of transition from le 40 to over 40 days */
     
                                  else                                                      /********** over 40 days ***********/
                                      if days_used gt 40 then 
                                         do;
                                             cstshrnetalcopamt =  0;
                                             cstshrtopaycopamt =  0;
                                             mcdtopamt         =  0;
                                             mcdnetalwamt      =  0;
                                             mcdalwamt         =  0;
                                             mcdaptrnamt       =  0;
                                             mcdalwunt         =  0;
                                             mcdcovunt         =  0;
                                             mcdcovdys         =  0;
                                             mcdapcovdys       =  0;
                                             mcdutilct         =  0;
                                             mcdaputilct       =  0;

                                             mcrtopamt       =  topamt;
                                             mcrnetalwamt    =  netalwamt;
                                             mcralwamt       =  alwamt;
                                             mcraptrnamt     =  aptrnamt;
                                             mcralwunt       =  alwunt;
                                             mcrcovunt       =  covunt;
                                             mcrcovdys       =  covdys;
                                             mcrapcovdys     =  apcovdys;
                                             mcrutilct       =  utilct;
                                             mcraputilct     =  aputilct;
     
                                             days_used          = days_used + covunt;
                                             cstshrnet_used     = cstshrnet_used + cstshrnetalcopamt;
                                             days_used_apcovdys = days_used_apcovdys + apcovdys;
                                         end;            /**************************************************** end of over 40 days  */

                     end;                                /************************************** end of consecutive bill split 2014 */
                   else
                     do;                                                          /*  To handle Denies  for consecutive bill logic  */
                         mcdtopamt       =  0;
                         mcdnetalwamt    =  0;
                         mcdalwamt       =  0;
                         mcdaptrnamt     =  0;
                         mcdalwunt       =  0;
                         mcdcovunt       =  0;
                         mcdcovdys       =  0;
                         mcdapcovdys     =  0;
                         mcdutilct       =  0;
                         mcdaputilct     =  0;

                         mcrtopamt       =  topamt;
                         mcrnetalwamt    =  netalwamt;
                         mcralwamt       =  alwamt;
                         mcraptrnamt     =  aptrnamt;
                         mcralwunt       =  alwunt;
                         mcrcovunt       =  covunt;
                         mcrcovdys       =  covdys;
                         mcrapcovdys     =  apcovdys;
                         mcrutilct       =  utilct;
                         mcraputilct     =  aputilct;
                     end;                                             /* end of denies logic            */
                end;                                                  /* end of consecutive group logic */
     return;                                                          /* returns back to snf link 2014  */

/*************************************************************************************************************/
/**********************************       end of linked routines      ****************************************/
/*************************************************************************************************************/

     data finalsnf (keep=  clamno  lineno 
                           mcdalwunt          mcralwunt   
                           mcdtopamt          mcrtopamt   
                           mcdnetalwamt       mcrnetalwamt
                           mcdalwamt          mcralwamt   
                           mcdcovunt          mcrcovunt   
                           mcdcovdys          mcrcovdys   
                           mcdapcovdys        mcrapcovdys 
                           mcdutilct          mcrutilct   
                           mcdaputilct        mcraputilct 
                           mcdaptrnamt        mcraptrnamt 
                           cstshrnetalcoiamt  cstshrnetalcopamt
                           cstshrnetaldedamt  cstshrtopaycoiamt 
                           cstshrtopaycopamt  cstshrtopaydedamt);
          set truedups
              groups;
     run;
/* SAS2AWS2: added step to read data from redshift table */
data svclines;
set /*&libn1.*/rsstr.svclines;
run;
proc sort data=svclines;
by clamno lineno;
run;
proc sort data=finalsnf;
by clamno lineno;
run;
/* SAS2AWS2: changed library name from libn1 to work */
     data /*&libnx..*/svclines;                       /* #412625 RKS */
    /* SAS2AWS2: commented libnx and also changed modify statement as update*/
         update /*&libnx..*/svclines finalsnf;        /* #412625 RKS */
         by clamno lineno;
     run;

/* SAS2AWS2: added sas2rs_dataload_bul macro to load data into RS table */
/* %sas2rs_dataload_bul(loadtech=delete, srclib=work, srctbl=svclines, trgtlib=&libn1., trgttbl=svclines); */
data rsstr.svclines;
set svclines;
run;
%mend ccsnfbreakout;
%global fida_switch;
%macro switch;/*HS 510822 - SM, Macro to read fida.csv file and assign value for fidaswitch */
data inp;
  /* SAS2AWS2: ReplacedSlash-ChangedFolderName */
  infile "&drv2/DWAC0901/fida.csv" delimiter=',';
  input fida $ comp;
  com=put(comp,2.);
  /* SAS2AWS2: ReplacedFunctionTrim */
  compn=ktrim(com);
 run;
%nobs(inp);
%let recs=&nobs.;
proc sql noprint;
select fida,compn into :fida_1-:fida_&recs., :compn1-:compn&recs. from inp;
quit;
%do i=1 %to &recs.;
%let compn=&&compn&i;
%let fid=&&fida_&i ;
 data _null_;
   %if "&compn."="&compno." %then %do;/*HS 510822 - SM, To check compno from csv file matches the compno from sasjob */
   %let fida_switch=&fid;
   %end;
run;
%end;
%mend switch;
%switch;
%macro dwac0901 ;

/* SAS2AWS2: CommentedMacroLock */
/*%lock(dataset = &libnx..svclines);                *//* #412625 RKS */

%ccbreakout ;

options MSGLEVEL=N ;

%ccsnfbreakout ;

options MSGLEVEL=I ;

/*HS 445559 - MV, for FIDA (CC data) MCD, MCR & Cost Sharing fields are assigned as 0 */
/*HS 510822 - SM, for FIDA (MSO data) MCD, MCR & Cost Sharing fields are assigned as 0 */
/*Macro variable FIDA switch provided by wkly34_CDC and Wkly64_CDC job*/
/* SAS2AWS2: ReplacedFunctionUpcase */
%if %kupcase(&Fida_switch.) = YES %then %do;
 	/* SAS2AWS2: added step to read data from redshift table */
	data svclines;
	set /*&libn1.*/rsstr.svclines;
	run;
	/* SAS2AWS2: changed library name from libn1 to work */
	 data /*&libnx..*/svclines;
         modify /*&libnx..*/svclines ;
		 /* SAS2AWS2: ReplacedFunctionUpcase */
		 if kupcase(lobcod)='FDA' or kupcase(lobcod)='NSF' then do;/*HS 510822 - SM, Added FIDA value for MSO "NSF" in if condition */
         mcralwunt = 0;
         mcrtopamt = 0;
         mcrnetalwamt = 0;
         mcralwamt = 0;
         mcrcovunt = 0;
         mcrcovdys = 0;
         mcrapcovdys = 0;
         mcrutilct = 0;
         mcraputilct = 0;
         mcraptrnamt = 0;
         mcdalwunt = 0;
         mcdtopamt = 0;
         mcdnetalwamt = 0;
         mcdalwamt = 0;
         mcdcovunt = 0;
         mcdcovdys = 0;
         mcdapcovdys = 0;
         mcdutilct = 0;
         mcdaputilct = 0;
         mcdaptrnamt = 0;
         cstshrnetalcoiamt = 0;
         cstshrnetalcopamt = 0;
         cstshrnetaldedamt = 0;
         cstshrtopaycoiamt = 0;
         cstshrtopaycopamt = 0;
         cstshrtopaydedamt = 0;
		 end;
     run;

/* SAS2AWS2: added sas2rs_dataload_bul macro to load data into RS table */
/* %sas2rs_dataload_bul(loadtech=delete, srclib=work, srctbl=svclines, trgtlib=&libn1., trgttbl=svclines); */
data rsstr.svclines;
set svclines;
run;

%end;


/* SAS2AWS2: CommentedMacroUnlock */
/*%unlock(dataset = &libnx..svclines);              *//* #412625 RKS */


%mend dwac0901;

%dwac0901;

proc contents data=&libn1..svclines;              /* #412625 RKS */
     title "Descriptor for &libn1..SVCLINES ";    /* #412625 RKS */
run;

%time(timeparm=e); 
%timestamp;                                                                               

%initvar;
quit;




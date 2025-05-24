if maximum_troop < &&TROOP_&RUNYEAR. then do;

  if mrx_alt_ded_type_yn = '2' & (deductible < plan_deductible) then do;
    select(tier_level_value);
      when(1) do;
        if substr(mrx_alt_no_ded_tier,2,1) = '1' then deductible_exemption = '1';
      end;
      when(2) do;
        if substr(mrx_alt_no_ded_tier,3,1) = '1' then deductible_exemption = '1';
      end;
      when(3) do;
        if substr(mrx_alt_no_ded_tier,4,1) = '1' then deductible_exemption = '1';
      end;
      when(4) do;
        if substr(mrx_alt_no_ded_tier,5,1) = '1' then deductible_exemption = '1';
      end;
      when(5) do;
        if substr(mrx_alt_no_ded_tier,6,1) = '1' then deductible_exemption = '1';
      end;
      when(6) do;
        if substr(mrx_alt_no_ded_tier,7,1) = '1' then deductible_exemption = '1';
      end;
      when(7) do;
        if substr(mrx_alt_no_ded_tier,1,1) = '1' then deductible_exemption = '1';
      end;
      otherwise;
    end;
  end;

  if put(rxcui,insfmt.)='1' then do;

    if plan_ira_copay=. then do;
      if plan_copay^=. then plan_ira_copay=min(&IRA_COPAY.,plan_copay);
      else plan_ira_copay=&IRA_COPAY.;
    end;
    else if plan_ira_copay^=. & plan_copay^=. then 
      plan_ira_copay=min(plan_ira_copay,plan_copay,&IRA_COPAY.);
    else if plan_ira_copay^=. & plan_copay=. then
      plan_ira_copay=min(plan_ira_copay,&IRA_COPAY.);

    remaining_troop = sum(plan_icp,-troop);
    remaining_maximum_troop = max(sum(&&TROOP_&RUNYEAR.,-maximum_troop),0);

    if mrx_first_fill='1' & put(cprx,$fffmt.)='yes' & rx_script_order=1 then do;
      remaining_avg_cost=0;
    end;

    else if maximum_troop < plan_icp then do;
      select(mrx_alt_pre_icl_cost_share);
        when('1') do;
          remaining_avg_cost = 0;
        end;
        when('2') do;
          copay_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
          ea_script_copay_oopc = min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
          total_covered_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
          troop + min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
        end;
        when('3') do;
          copay_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
          ea_script_copay_oopc = min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
          total_covered_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
          troop + min(avg_price,plan_ira_copay*script_cnt30,remaining_maximum_troop);
        end;
        otherwise;
      end;
    end;

    if ea_script_copay_oopc = . then ea_script_copay_oopc = 0;
    ea_script_cost = ea_script_copay_oopc;
    
    maximum_troop + ea_script_cost;

    if troop >= plan_icp then do;
      total_covered_oopc + 0;
    end;

  end;

  else do;

    remaining_troop = sum(plan_icp,-troop);
    ds_remaining_deductible = max(sum(&&DEDUCTIBLE_&RUNYEAR.,-maximum_troop),0);
    ea_remaining_deductible = max(sum(plan_deductible,-deductible),0);
    remaining_deductible = min(ds_remaining_deductible,ea_remaining_deductible);
    remaining_maximum_troop = max(sum(&&TROOP_&RUNYEAR.,-maximum_troop),0);

    if maximum_troop < &&DEDUCTIBLE_&RUNYEAR. then do;
      ds_script_deductible = min(avg_price,sum(&&DEDUCTIBLE_&RUNYEAR,-maximum_troop),remaining_maximum_troop);
      ds_remaining_avg_cost = max(0,sum(avg_price,-ds_script_deductible));
    end;
    else ds_remaining_avg_cost = avg_price;

    if ds_remaining_avg_cost > 0 and maximum_troop < plan_icp then do;
      ds_script_coins=min(ds_remaining_avg_cost * &DS_COINS.,remaining_maximum_troop);
    end;

    ds_script_cost = min(sum(ds_script_deductible,ds_script_coins),remaining_maximum_troop);

    if mrx_first_fill='1' & put(cprx,$fffmt.)='yes' & rx_script_order=1 then do;
      remaining_avg_cost=0;
    end;
    else do;
      if remaining_deductible > 0 then do;
        if ((mrx_alt_ded_type_yn = '1') or
          (mrx_alt_ded_type_yn = '2' and deductible_exemption = ' ')) then do;
          deductible + min(remaining_deductible,avg_price,remaining_maximum_troop);
          ea_script_deductible = min(remaining_deductible,avg_price,remaining_maximum_troop);
          total_covered_oopc + min(remaining_deductible,avg_price,remaining_maximum_troop);
          troop + min(remaining_deductible,avg_price,remaining_maximum_troop);
          remaining_troop = sum(plan_icp,-troop);
          remaining_avg_cost = max(0,sum(avg_price,-remaining_deductible));
        end;
        else if (mrx_alt_ded_type_yn = '2' and deductible_exemption = '1') then do;
          select(mrx_alt_gen_cstshr_struct);
            when('1') do;
              coins_oopc + min(avg_price * plan_coins_pre,remaining_maximum_troop);
              ea_script_coins_oopc = min(avg_price * plan_coins_pre,remaining_maximum_troop);
              total_covered_oopc + min(avg_price * plan_coins_pre,remaining_maximum_troop);
              troop + min(avg_price * plan_coins_pre,remaining_maximum_troop);
            end;
            when('2') do;
              copay_oopc + min(avg_price,plan_copay_pre*script_cnt30,remaining_maximum_troop);
              ea_script_copay_oopc = min(avg_price,plan_copay_pre*script_cnt30,remaining_maximum_troop);
              total_covered_oopc + min(avg_price,plan_copay_pre*script_cnt30,remaining_maximum_troop);
              troop + min(avg_price,plan_copay_pre*script_cnt30,remaining_maximum_troop);
            end;
            when('3') do;
              ea_script_coins_oopc = min(avg_price * plan_coins_pre,remaining_maximum_troop);
              ea_script_copay_oopc = min(avg_price,plan_copay_pre*script_cnt30,remaining_maximum_troop);
              if ea_script_coins_oopc > ea_script_copay_oopc then do;
                coins_oopc + ea_script_coins_oopc;
                total_covered_oopc + ea_script_coins_oopc;
                troop + ea_script_coins_oopc;
              end;
              else do;
                copay_oopc + ea_script_copay_oopc;
                total_covered_oopc + ea_script_copay_oopc;
                troop + ea_script_copay_oopc;
              end;
            end;
            when('4') do;
              ea_script_coins_oopc = min(avg_price * plan_coins_pre,remaining_maximum_troop);
              ea_script_copay_oopc = min(avg_price,plan_copay_pre*script_cnt30,remaining_maximum_troop);
              if ea_script_coins_oopc < ea_script_copay_oopc then do;
                coins_oopc + ea_script_coins_oopc;
                total_covered_oopc + ea_script_coins_oopc;
                troop + ea_script_coins_oopc;
              end;
              else do;
                copay_oopc + ea_script_copay_oopc;
                total_covered_oopc + ea_script_copay_oopc;
                troop + ea_script_copay_oopc;
              end;
            end;
            otherwise;
          end;
        end;
      end;
      else remaining_avg_cost = avg_price;
    end;

    if remaining_avg_cost > 0 and troop < plan_icp then do;
      select(mrx_alt_pre_icl_cost_share);
        when('1') do;
          remaining_avg_cost = 0;
        end;
        when('2') do;
          coins_oopc + min(remaining_avg_cost * &DS_COINS.,remaining_maximum_troop);
          ea_script_coins_oopc = min(remaining_avg_cost * &DS_COINS.,remaining_maximum_troop);
          total_covered_oopc + min(remaining_avg_cost * &DS_COINS.,remaining_maximum_troop);
          troop + min(remaining_avg_cost * &DS_COINS.,remaining_maximum_troop);
          remaining_avg_cost = 0;
        end;
        when('3') do;
          select(substr(mrx_tier_cstshr_struct_type,1,1));
            when('1') do;
              coins_oopc + min(remaining_avg_cost * plan_coins,remaining_maximum_troop);
              ea_script_coins_oopc = min(remaining_avg_cost * plan_coins,remaining_maximum_troop);
              total_covered_oopc + min(remaining_avg_cost * plan_coins,remaining_maximum_troop);
              troop + min(remaining_avg_cost * plan_coins,remaining_maximum_troop);
            end;
            when('2') do;
              copay_oopc + min(remaining_avg_cost,plan_copay*script_cnt30,remaining_maximum_troop);
              ea_script_copay_oopc = min(remaining_avg_cost,plan_copay*script_cnt30,remaining_maximum_troop);
              total_covered_oopc + min(remaining_avg_cost,plan_copay*script_cnt30,remaining_maximum_troop);
              troop + min(remaining_avg_cost,plan_copay*script_cnt30,remaining_maximum_troop);
            end;
            when('3') do;
              ea_script_coins_oopc = min(remaining_avg_cost * plan_coins,remaining_maximum_troop);
              ea_script_copay_oopc = min(remaining_avg_cost,plan_copay*script_cnt30,remaining_maximum_troop);
              if ea_script_coins_oopc > ea_script_copay_oopc then do;
                coins_oopc + ea_script_coins_oopc;
                total_covered_oopc + ea_script_coins_oopc;
                troop + ea_script_coins_oopc;
              end;
              else do;
                copay_oopc + ea_script_copay_oopc;
                total_covered_oopc + ea_script_copay_oopc;
                troop + ea_script_copay_oopc;
              end;
            end;
            when('4') do;
              ea_script_coins_oopc = min(remaining_avg_cost * plan_coins,remaining_maximum_troop);
              ea_script_copay_oopc = min(remaining_avg_cost,plan_copay*script_cnt30,remaining_maximum_troop);
              if ea_script_coins_oopc < ea_script_copay_oopc then do;
                coins_oopc + ea_script_coins_oopc;
                total_covered_oopc + ea_script_coins_oopc;
                troop + ea_script_coins_oopc;
              end;
              else do;
                copay_oopc + ea_script_copay_oopc;
                total_covered_oopc + ea_script_copay_oopc;
                troop + ea_script_copay_oopc;
              end;
            end;
            otherwise;
          end;
        end;
        otherwise;
      end;
      remaining_avg_cost = 0;
    end;

    ea_script_cost = sum(ea_script_copay_oopc,ea_script_coins_oopc,ea_script_deductible);
    
    maximum_troop + max(ea_script_cost,ds_script_cost);
    
    if remaining_avg_cost > 0 and troop >= plan_icp then do;
      total_covered_oopc + 0;
    end;
  end;
end;

else do;
  total_covered_oopc + 0;
end;

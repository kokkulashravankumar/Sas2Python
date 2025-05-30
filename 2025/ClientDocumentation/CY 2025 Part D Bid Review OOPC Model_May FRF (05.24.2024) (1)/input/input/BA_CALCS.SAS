if troop < plan_icp then do;

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

    if mrx_first_fill='1' & put(cprx,$fffmt.)='yes' & rx_script_order=1 then do;
      remaining_avg_cost=0;
    end;

    else if troop < plan_icp then do;
      select(mrx_alt_pre_icl_cost_share);
        when('1') do;
          remaining_avg_cost = 0;
        end;
        when('2') do;
          copay_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
          script_copay_oopc = min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
          total_covered_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
          troop + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
        end;
        when('3') do;
          copay_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
          script_copay_oopc = min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
          total_covered_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
          troop + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
        end;
        otherwise;
      end;
    end;

    if troop >= plan_icp then do;
      total_covered_oopc + 0;
    end;

  end;

  else do;

    remaining_troop = sum(plan_icp,-troop);
    ds_remaining_deductible = max(sum(&&DEDUCTIBLE_&RUNYEAR.,-troop),0);
    ba_remaining_deductible = max(sum(plan_deductible,-deductible),0);
    remaining_deductible = min(ds_remaining_deductible,ba_remaining_deductible);

    if mrx_first_fill='1' & put(cprx,$fffmt.)='yes' & rx_script_order=1 then do;
      remaining_avg_cost=0;
    end;
    else do;
      if remaining_deductible > 0 then do;
        if ((mrx_alt_ded_type_yn = '1') or
          (mrx_alt_ded_type_yn = '2' and deductible_exemption = ' ')) then do;
          deductible + min(remaining_deductible,avg_price,remaining_troop);
          script_deductible = min(remaining_deductible,avg_price,remaining_troop);
          total_covered_oopc + min(remaining_deductible,avg_price,remaining_troop);
          troop + min(remaining_deductible,avg_price,remaining_troop);
          remaining_troop = sum(plan_icp,-troop);
          remaining_avg_cost = max(0,sum(avg_price,-remaining_deductible));
        end;
        else if (mrx_alt_ded_type_yn = '2' and deductible_exemption = '1') then do;
          select(mrx_alt_gen_cstshr_struct);
            when('1') do;
              coins_oopc + min(avg_price * plan_coins_pre,remaining_troop);
              script_coins_oopc = min(avg_price * plan_coins_pre,remaining_troop);
              total_covered_oopc + min(avg_price * plan_coins_pre,remaining_troop);
              troop + min(avg_price * plan_coins_pre,remaining_troop);
            end;
            when('2') do;
              copay_oopc + min(avg_price,plan_copay_pre*script_cnt30,remaining_troop);
              script_copay_oopc = min(avg_price,plan_copay_pre*script_cnt30,remaining_troop);
              total_covered_oopc + min(avg_price,plan_copay_pre*script_cnt30,remaining_troop);
              troop + min(avg_price,plan_copay_pre*script_cnt30,remaining_troop);
            end;
            when('3') do;
              script_coins_oopc = min(avg_price * plan_coins_pre,remaining_troop);
              script_copay_oopc = min(avg_price,plan_copay_pre*script_cnt30,remaining_troop);
              if script_coins_oopc > script_copay_oopc then do;
                coins_oopc + script_coins_oopc;
                total_covered_oopc + script_coins_oopc;
                troop + script_coins_oopc;
              end;
              else do;
                copay_oopc + script_copay_oopc;
                total_covered_oopc + script_copay_oopc;
                troop + script_copay_oopc;
              end;
            end;
            when('4') do;
              script_coins_oopc = min(avg_price * plan_coins_pre,remaining_troop);
              script_copay_oopc = min(avg_price,plan_copay_pre*script_cnt30,remaining_troop);
              if script_coins_oopc < script_copay_oopc then do;
                coins_oopc + script_coins_oopc;
                total_covered_oopc + script_coins_oopc;
                troop + script_coins_oopc;
              end;
              else do;
                copay_oopc + script_copay_oopc;
                total_covered_oopc + script_copay_oopc;
                troop + script_copay_oopc;
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
          coins_oopc + min(remaining_avg_cost * &DS_COINS.,remaining_troop);
          script_coins_oopc = min(remaining_avg_cost * &DS_COINS.,remaining_troop);
          total_covered_oopc + min(remaining_avg_cost * &DS_COINS.,remaining_troop);
          troop + min(remaining_avg_cost * &DS_COINS.,remaining_troop);
          remaining_avg_cost = 0;
        end;
        when('3') do;
          select(substr(mrx_tier_cstshr_struct_type,1,1));
            when('1') do;
              coins_oopc + min(remaining_avg_cost * plan_coins,remaining_troop);
              script_coins_oopc = min(remaining_avg_cost * plan_coins,remaining_troop);
              total_covered_oopc + min(remaining_avg_cost * plan_coins,remaining_troop);
              troop + min(remaining_avg_cost * plan_coins,remaining_troop);
            end;
            when('2') do;
              copay_oopc + min(remaining_avg_cost,plan_copay*script_cnt30,remaining_troop);
              script_copay_oopc = min(remaining_avg_cost,plan_copay*script_cnt30,remaining_troop);
              total_covered_oopc + min(remaining_avg_cost,plan_copay*script_cnt30,remaining_troop);
              troop + min(remaining_avg_cost,plan_copay*script_cnt30,remaining_troop);
            end;
            when('3') do;
              script_coins_oopc = min(remaining_avg_cost * plan_coins,remaining_troop);
              script_copay_oopc = min(remaining_avg_cost,plan_copay*script_cnt30,remaining_troop);
              if script_coins_oopc > script_copay_oopc then do;
                coins_oopc + script_coins_oopc;
                total_covered_oopc + script_coins_oopc;
                troop + script_coins_oopc;
              end;
              else do;
                copay_oopc + script_copay_oopc;
                total_covered_oopc + script_copay_oopc;
                troop + script_copay_oopc;
              end;
            end;
            when('4') do;
              script_coins_oopc = min(remaining_avg_cost * plan_coins,remaining_troop);
              script_copay_oopc = min(remaining_avg_cost,plan_copay*script_cnt30,remaining_troop);
              if script_coins_oopc < script_copay_oopc then do;
                coins_oopc + script_coins_oopc;
                total_covered_oopc + script_coins_oopc;
                troop + script_coins_oopc;
              end;
              else do;
                copay_oopc + script_copay_oopc;
                total_covered_oopc + script_copay_oopc;
                troop + script_copay_oopc;
              end;
            end;
            otherwise;
          end;
        end;
        otherwise;
      end;
      remaining_avg_cost = 0;
    end;

    if remaining_avg_cost > 0 and troop >= plan_icp then do;
      total_covered_oopc + 0;
    end;
  end;
end;

else do;
  total_covered_oopc + 0;
end;

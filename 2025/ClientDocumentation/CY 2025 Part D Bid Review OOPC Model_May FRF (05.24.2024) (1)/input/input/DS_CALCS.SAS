if troop < plan_icp then do;

  if put(rxcui,insfmt.)='1' then do;
    
    if plan_ira_copay=. then plan_ira_copay=&IRA_COPAY.;

    remaining_troop = sum(plan_icp,-troop);
    
    if troop < plan_icp then do;
      copay_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
      script_copay_oopc = min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
      total_covered_oopc + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
      troop + min(avg_price,plan_ira_copay*script_cnt30,remaining_troop);
    end;

    if troop >= plan_icp then do;
      total_covered_oopc + 0;
    end;
    
  end;

  else do;

    remaining_troop = sum(plan_icp,-troop);

    if max(deductible,troop) < plan_deductible then do;
      remaining_deductible = sum(plan_deductible,-max(deductible,troop));
      deductible + min(remaining_deductible,avg_price,remaining_troop);
      script_deductible = min(remaining_deductible,avg_price,remaining_troop);
      total_covered_oopc + min(remaining_deductible,avg_price,remaining_troop);
      troop + min(remaining_deductible,avg_price,remaining_troop);
      remaining_troop = sum(plan_icp,-troop);
      remaining_avg_cost = max(0,sum(avg_price,-remaining_deductible));
    end;
    else remaining_avg_cost = avg_price;

    if remaining_avg_cost > 0 and troop < plan_icp then do;
      coins_oopc + min(remaining_avg_cost * &DS_COINS.,remaining_troop);
      script_coins_oopc = min(remaining_avg_cost * &DS_COINS.,remaining_troop);  
      total_covered_oopc + min(remaining_avg_cost * &DS_COINS.,remaining_troop);
      troop + min(remaining_avg_cost * &DS_COINS.,remaining_troop);  
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

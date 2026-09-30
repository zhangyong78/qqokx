"""Post-selection robustness only. Does not change locked models or scores."""
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from research.btc_volatility_validation_20260930 import OUT,save,metrics,loss,block_ci,ridge_predict,predict_candidate,source_data


def main():
    f=pd.DataFrame(json.loads((OUT/'feature_snapshot.json').read_text(encoding='utf-8')))
    output={}
    cleaned,clean_meta=source_data()
    snapshot=json.loads((OUT/'source_daily_snapshot.json').read_text(encoding='utf-8'))
    pd.testing.assert_frame_equal(cleaned,pd.DataFrame(snapshot['rows']),check_dtype=False)
    output['cache_cleanup']={'valid_model_input_unchanged':True,'remaining_anomalous_rows':len(clean_meta['anomalous_timestamp_rows_excluded']),'backup':'D:/qqokx/backups/dvol_epoch_cleanup/deribit_volatility_cache_before_20260930_112630_227331.json','cache_sha256_after_cleanup':clean_meta['volatility_sha256'],'cache_sha256_at_original_fit':snapshot['metadata']['volatility_sha256']}
    one=pd.DataFrame(json.loads((OUT/'rv_1d_predictions.json').read_text(encoding='utf-8')))
    refit_dates=(one.origin<'2025-01-01')&(one.target_end<'2025-01-01')
    one=one.merge(f[['date','ewma_var','next_weekend','atr14']],left_on='origin',right_on='date',validate='one_to_one')
    ewma_factors={};atr_factors={}
    for group in [0,1]:
        m=refit_dates.to_numpy() & (one.next_weekend.to_numpy()==group)
        ewma_factors[group]=float(np.mean(one.actual.to_numpy()[m]/one.ewma_var.to_numpy()[m]))
        atr_factors[group]=float(np.mean(one.actual.to_numpy()[m]/one.atr14.to_numpy()[m]**2))
    output['frozen_calendar_factors']={'ewma':ewma_factors,'atr14':atr_factors,'estimated_on':'resolved origins and labels through 2024-12-31 only'}
    for h in [1,7,30]:
        result=json.loads((OUT/f'rv_{h}d_results.json').read_text(encoding='utf-8'))
        rows=pd.DataFrame(json.loads((OUT/f'rv_{h}d_predictions.json').read_text(encoding='utf-8')))
        rows=rows.merge(f[['date','ewma_var','atr14','var1',f'weekend_fraction{h}']],left_on='origin',right_on='date',validate='one_to_one')
        m=(rows.phase=='test').to_numpy();y=rows.actual.to_numpy();p=rows.predicted.to_numpy();weekends=rows[f'weekend_fraction{h}'].to_numpy()
        bases={'calendar_ewma':rows.ewma_var.to_numpy()*(ewma_factors[0]*(1-weekends)+ewma_factors[1]*weekends),'calendar_atr':rows.atr14.to_numpy()**2*(atr_factors[0]*(1-weekends)+atr_factors[1]*weekends)}
        compare={}
        for name,base in bases.items():
            compare[name]={'metrics':metrics('rv',y,base,100*np.sqrt(rows.var1.to_numpy()),m),'incremental_gain_ci':block_ci((loss('rv',y,base)-loss('rv',y,p))[m],max(h,14))}
        fixed=np.flatnonzero(m)[::h]
        base=rows.baseline.to_numpy()
        record={'additional_baselines':compare,'nonoverlapping':{'n':len(fixed),'model_qlike':float(loss('rv',y[fixed],p[fixed]).mean()),'locked_baseline_qlike':float(loss('rv',y[fixed],base[fixed]).mean())},'bootstrap_block_sensitivity':{str(block):block_ci((loss('rv',y,base)-loss('rv',y,p))[m],block) for block in sorted({14,30,60,h})}}
        # Test-year residuals are not used for fitting or calibrating predictions.
        for year in ['2025','2026']:
            keep=m&rows.origin.str.startswith(year).to_numpy()
            record[year+'_calendar_ewma_gain']=float((loss('rv',y,bases['calendar_ewma'])-loss('rv',y,p))[keep].mean())
        output[f'rv_{h}d']=record
    # Verify saved coefficients reproduce every test prediction; express the coefficients
    # in original feature units for independent implementation.
    output['coefficient_reproduction']={};output['unstandardized_models']={}
    for key in ['dvol_1d','dvol_7d','rv_1d','rv_7d','rv_30d']:
        r=json.loads((OUT/f'{key}_results.json').read_text(encoding='utf-8'))
        model=r['selection']['frozen_test_model']
        for field in ['coef','mean','std']: model[field]=np.array(model[field])
        p=predict_candidate(f,model)
        rows=pd.DataFrame(json.loads((OUT/f'{key}_predictions.json').read_text(encoding='utf-8')))
        ids=rows.origin.map(dict(zip(f.date,range(len(f))))).to_numpy()
        test=rows.phase=='test'
        np.testing.assert_allclose(p[ids[test]],rows.loc[test,'predicted'],rtol=1e-12,atol=1e-12)
        slopes=model['coef'][1:]/model['std'];intercept=model['coef'][0]-np.sum(slopes*model['mean'])
        output['coefficient_reproduction'][key]=True
        output['unstandardized_models'][key]={'intercept':float(intercept),'features':{name:float(weight) for name,weight in zip(model['features'],slopes)},'smearing':model.get('smearing'),'transform':model['transform']}
    save(OUT/'supplementary_checks.json',output)
    print(json.dumps(output,ensure_ascii=False),flush=True)


if __name__=='__main__': main()

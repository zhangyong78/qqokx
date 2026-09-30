"""Robustness checks and frozen candidate specification; no production changes."""
from __future__ import annotations
import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from research.btc_daily_rules_20260929 import OUT,QUANTILES,TARGETS,load,features,direction_models,shape_models,shape_metrics,block_ci,write_json


def loss_rows(y,p):
    error=y[:,:,None]-p
    return np.maximum(error*QUANTILES,error*(QUANTILES-1)).mean(axis=(1,2))


def main():
    p1=json.loads((OUT/'primary_predictions.json').read_text(encoding='utf-8'))
    p2=json.loads((OUT/'primary_phase2_predictions.json').read_text(encoding='utf-8'))
    f=pd.DataFrame(p1['features']);target=np.array(p1['targets']);y=np.array(p1['actual_direction']);dates=np.array(p1['dates']);n=len(f)
    candidate=np.array(p2['shape']['atr14_daytype_all'])
    reference=np.array(p1['shape']['rolling365'])
    audit=(dates>='2025-01-01')
    gain=loss_rows(target,reference)-loss_rows(target,candidate)
    result={'blocks':{str(b):block_ci(gain[audit],block=b) for b in [7,14,28,60]}}
    outlier=np.quantile(target[audit,3],.99)
    ordinary=audit & (target[:,3]<=outlier)
    result['remove_top_1pct_ranges']={'removed':int(audit.sum()-ordinary.sum()),'candidate':shape_metrics(target,candidate,ordinary),'rolling365':shape_metrics(target,reference,ordinary),'mean_gain':float(gain[ordinary].mean())}
    result['yearly_candidate']={year:shape_metrics(target,candidate,np.char.startswith(dates.astype(str),year)) for year in ['2023','2024','2025','2026']}
    # Explicit prefix checks, including labels at/after the prediction origin.
    index=int(np.flatnonzero(dates>='2025-07-01')[0])
    df,meta=load()
    prefix=features(df.iloc[:index+61])
    full_features=features(df)
    check_columns=['atr14','atr50','ema_distance','ma_distance','ema_slope','ma_slope','body','range_norm','vol_ratio','volume_ratio','signed_streak','ret3','ret10']
    np.testing.assert_allclose(prefix[check_columns].iloc[-1],full_features[check_columns].iloc[index+60],rtol=1e-12,atol=1e-12)
    prefix_direction=direction_models(f.iloc[:index+1],y[:index+1])
    for name,value in prefix_direction.items():
        expected=np.array(p1['direction'][name])[:index+1]
        np.testing.assert_allclose(np.nan_to_num(value,nan=-999),expected,rtol=1e-12,atol=1e-12)
    prefix_shape=shape_models(f.iloc[:index+1],target[:index+1])
    for name,value in prefix_shape.items():
        np.testing.assert_allclose(np.nan_to_num(value,nan=-999),np.array(p1['shape'][name])[:index+1],rtol=1e-12,atol=1e-12)
    # Rebuild conditional predictions independently for several origins, excluding current target.
    normalized=target/f.atr14.to_numpy()[:,None]
    weekend=(pd.to_datetime(f.target_date).dt.dayofweek.to_numpy()>=5).astype(int)
    keys=np.digitize(f.range_norm.to_numpy(),[.7,1.3])*2+weekend
    for i in [365,index,n-1]:
        use=weekend[:i]==weekend[i]
        if use.sum()<40:
            use=np.ones(i,dtype=bool)
        independent=np.quantile(normalized[:i][use],QUANTILES,axis=0).T*f.atr14.iloc[i]
        np.testing.assert_allclose(independent,candidate[i],rtol=1e-12,atol=1e-12)
    result['causality_checks']={'feature_prefix':True,'all_26_direction_prefix':True,'all_10_shape_prefix':True,'conditional_quantiles_independent_check':True,'checkpoint_target':str(dates[index])}
    # Compare to the literal original streak matching, using raw history from index 12.
    raw_f=features(df)
    raw_sign=raw_f.sign.to_numpy();raw_streak=raw_f.streak.to_numpy()
    raw_targets=np.column_stack([df.close.shift(-1)/df.close-1,df.high.shift(-1)/df.close-1,df.low.shift(-1)/df.close-1,(df.high.shift(-1)-df.low.shift(-1))/df.close])
    original=np.full_like(candidate,np.nan)
    for j in np.flatnonzero(dates>='2023-01-01'):
        i=j+60;ids=np.arange(12,i)
        if raw_sign[i]==0:
            ids=ids[raw_sign[ids]!=0]
        else:
            exact=ids[(raw_sign[ids]==raw_sign[i])&(raw_streak[ids]==raw_streak[i])]
            same=ids[raw_sign[ids]==raw_sign[i]]
            ids=exact if len(exact)>=8 else same if len(same)>=8 else ids
        original[j]=np.quantile(raw_targets[ids],QUANTILES,axis=0).T
    result['original_streak_audit']=shape_metrics(target,original,audit)
    result['candidate_audit']=shape_metrics(target,candidate,audit)
    result['original_gain_ci']=block_ci((loss_rows(target,original)-loss_rows(target,candidate))[audit])
    # Same common dates for correlated auxiliary checks; do not claim independent assets.
    payloads={s:(json.loads((OUT/f'{s}_predictions.json').read_text(encoding='utf-8')),json.loads((OUT/f'{s}_phase2_predictions.json').read_text(encoding='utf-8'))) for s in ['primary','utc','spot']}
    common=set(payloads['primary'][0]['dates'])
    for a,b in payloads.values():
        common &= set(a['dates'])
    common={d for d in common if d>='2025-01-01'}
    result['matched_auxiliary']={}
    for suffix,(a,b) in payloads.items():
        m=np.array([d in common for d in a['dates']]);t=np.array(a['targets'])
        cand=np.array(b['shape']['atr14_daytype_all']);ref=np.array(a['shape']['rolling365'])
        result['matched_auxiliary'][suffix]={'candidate':shape_metrics(t,cand,m),'baseline':shape_metrics(t,ref,m),'gain_ci':block_ci((loss_rows(t,ref)-loss_rows(t,cand))[m])}
    result['optional_range_group_increment']={}
    for suffix,(a,b) in payloads.items():
        m=np.array(a['dates'])>='2025-01-01';t=np.array(a['targets'])
        simple=np.array(b['shape']['atr14_daytype_all']);complex_model=np.array(b['shape']['atr14_range_daytype_all'])
        result['optional_range_group_increment'][suffix]=block_ci((loss_rows(t,simple)-loss_rows(t,complex_model))[m])
    # Freeze shape distribution at 2024 year-end while allowing observed ATR to change.
    frozen=np.full_like(candidate,np.nan)
    pre_audit=dates<'2025-01-01'
    for group in [0,1]:
        distribution=np.quantile(normalized[pre_audit&(weekend==group)],QUANTILES,axis=0).T
        selected=audit&(weekend==group)
        frozen[selected]=distribution[None,:,:]*f.atr14.to_numpy()[selected,None,None]
    result['frozen_2024_distribution_audit']=shape_metrics(target,frozen,audit)
    result['frozen_gain_ci']=block_ci((loss_rows(target,reference)-loss_rows(target,frozen))[audit])
    # Chronological rule state table for interpretable condition multipliers.
    result['state_table']=[]
    for phase,m in [('validation',(dates>='2023-01-01')&(dates<'2025-01-01')),('audit',audit)]:
        for k in range(6):
            use=m&(keys==k)
            result['state_table'].append({'phase':phase,'state':k,'today_range_bucket':['quiet','normal','large'][k//2],'target_weekend':k%2,'n':int(use.sum()),'next_range_over_atr_median':float(np.median(normalized[use,3])),'next_range_pct_median':float(np.median(target[use,3])*100)})
    # Latest complete daily candle -> full-day distribution estimate for the next daily session.
    last=raw_f.iloc[-1];next_date=(pd.Timestamp(last.date)+pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    next_weekend=int(pd.Timestamp(next_date).dayofweek>=5)
    lastkey=int(np.digitize(last.range_norm,[.7,1.3])*2+next_weekend)
    eligible=weekend==next_weekend
    selected=normalized[eligible] if eligible.sum()>=40 else normalized
    forecast=np.quantile(selected,QUANTILES,axis=0).T*last.atr14
    p_up=float((y[eligible]==1).mean())
    latest={'origin_date':str(last.date),'target_date':next_date,'confirmed_close':float(last.close),'atr14_price':float(last.atr14*last.close),'atr14_pct':float(last.atr14*100),'today_range_over_atr':float(last.range_norm),'state':lastkey,'target_weekend':next_weekend,'sample_count':int(len(selected)),'direction_status':'NO_VALIDATED_DIRECTION_EDGE','historical_bull_frequency_descriptive_only':p_up,'forecast_percent':{name:{str(q):float(value*100) for q,value in zip(QUANTILES,forecast[j])} for j,name in enumerate(TARGETS)},'forecast_prices':{name:{str(q):float(last.close*(1+value)) for q,value in zip(QUANTILES,forecast[j])} for j,name in enumerate(TARGETS[:3])},'note':'Target may already be in progress at report time. Only completed origin daily candles used. This is not a recorded pre-session live prediction.'}
    latest['state']=next_weekend
    spec={'version':'btc-daily-shape-research-v1','status':'research_candidate_not_production','chosen_model':'atr14_daytype_all','optional_refinement':'atr14_range_daytype_all; smaller incremental effect does not transfer reliably','instrument':'BTC-USDT-SWAP','bar':'1D','timezone':'Asia/Shanghai','atr_method':'TR=max(high-low,abs(high-prev_close),abs(low-prev_close)); EWM alpha=1/14, adjust=False, initialize first TR','warmup_bars':60,'normalized_shape':'for origin j, next OHLC moves vs close_j divided by ATR14_j / close_j; next range=(high_next-low_next)/ATR14_j','state':'next-day Sat/Sun vs weekday, two groups only','minimum_group':40,'fallback':'all complete normalized observations','quantiles':QUANTILES,'history':'expanding, only resolved next-day outcomes','direction':'abstain; no validated direction improvement','selection_caveat':'phase2 exploratory after primary audit; conservative final simplification after auxiliary audit; needs prospectively frozen forward validation','source':meta,'latest_snapshot':latest}
    write_json(OUT/'robustness_results.json',result)
    write_json(OUT/'candidate_specification.json',spec)
    write_json(OUT/'primary_source_snapshot.json',{'metadata':meta,'candles':df.to_dict(orient='records')})
    print('CAUSALITY',result['causality_checks'])
    print('BLOCKS',result['blocks'])
    print('ORIGINAL',result['original_streak_audit'])
    print('CANDIDATE',result['candidate_audit'])
    print('STATES',result['state_table'])
    print('LATEST',latest)


if __name__=='__main__':
    main()

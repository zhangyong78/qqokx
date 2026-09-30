"""Local BTC DVOL / realized volatility fitting, chronological validation and audit.

See btc_volatility_validation_20260930_protocol.md. No exchange requests or writes.
"""
from __future__ import annotations
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from okx_quant.candle_store import candle_store_db_path
from okx_quant.persistence import deribit_volatility_cache_file_path

OUT=ROOT/'reports'/'btc_volatility_validation_20260930'
EPS=1e-8


def save(path,value,compact=False):
    def convert(x):
        if isinstance(x,np.ndarray): return x.tolist()
        if isinstance(x,np.generic): return x.item()
        raise TypeError(type(x).__name__)
    path.write_text(json.dumps(value,ensure_ascii=False,default=convert,allow_nan=False,indent=None if compact else 2),encoding='utf-8')


def source_data():
    vol_path=deribit_volatility_cache_file_path()
    raw=vol_path.read_bytes();payload=json.loads(raw)
    v=pd.DataFrame(payload['BTC|hourly_base']['volatility_hourly'])
    v['dt']=pd.to_datetime(v.ts,unit='ms',utc=True).dt.tz_convert('Asia/Shanghai')
    abnormal=v.dt<pd.Timestamp('2021-01-01',tz='Asia/Shanghai')
    anomalous=v.loc[abnormal,['ts','open','high','low','close']].to_dict('records')
    v=v[~abnormal].copy().sort_values('ts')
    assert v.ts.is_unique
    for col in ['open','high','low','close']: v[col]=v[col].astype(float)
    assert ((v.low>0)&(v.low<=v[['open','close']].min(axis=1))&(v.high>=v[['open','close']].max(axis=1))).all()
    v['date']=v.dt.dt.strftime('%Y-%m-%d')
    daily=v.groupby('date',sort=True).agg(iv_open=('open','first'),iv_high=('high','max'),iv_low=('low','min'),iv=('close','last'),hours=('ts','count'),first=('ts','min'),last=('ts','max'))
    complete=(daily.hours==24)&(daily['last']-daily['first']==23*3600000)&(daily.index<'2026-09-30')
    excluded=daily.loc[~complete,['hours']].reset_index().to_dict('records')
    daily=daily[complete].copy()
    with sqlite3.connect(candle_store_db_path().as_uri()+'?mode=ro',uri=True) as db:
        rows=db.execute('SELECT ts,open,high,low,close,volume FROM candles WHERE inst_id=? AND bar=? AND confirmed=1 ORDER BY ts',('BTC-USDT','1H')).fetchall()
    h=pd.DataFrame(rows,columns=['ts','open','high','low','close','volume'])
    h=h.sort_values('ts');assert h.ts.is_unique
    for col in ['open','high','low','close','volume']: h[col]=h[col].astype(float)
    h['dt']=pd.to_datetime(h.ts,unit='ms',utc=True).dt.tz_convert('Asia/Shanghai');h['date']=h.dt.dt.strftime('%Y-%m-%d')
    h['log_return']=np.log(h.close/h.close.shift())
    h.loc[h.ts.diff()!=3600000,'log_return']=np.nan
    h['squared_return']=h.log_return**2
    btc=h.groupby('date',sort=True).agg(open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),volume=('volume','sum'),hours=('ts','count'),return_hours=('log_return','count'),rvvar=('squared_return','sum'))
    btc=btc[(btc.hours==24)&(btc.return_hours==24)&(btc.index<'2026-09-30')].copy()
    btc['rvvar']*=365
    d=daily.drop(columns=['hours','first','last']).join(btc.drop(columns=['hours','return_hours']),how='inner').reset_index()
    assert np.all(pd.to_datetime(d.date).diff().dropna()==pd.Timedelta(days=1))
    assert np.isfinite(d.select_dtypes('number')).all().all()
    meta={'volatility_file':str(vol_path),'volatility_sha256':hashlib.sha256(raw).hexdigest(),'spot_hourly_sha256':hashlib.sha256(json.dumps(rows).encode()).hexdigest(),'anomalous_timestamp_rows_excluded':anomalous,'incomplete_days_excluded':excluded,'daily_start':d.date.iloc[0],'daily_end':d.date.iloc[-1],'daily_count':len(d),'daily_timezone':'Asia/Shanghai','realized_source':'BTC-USDT spot 1H, sum of 24 hourly close-to-close log-return squares, annualize 365','dvol_definition':'30-day annualized implied volatility index, in percentage points','official_definition':'https://insights.deribit.com/exchange-updates/deribit-launches-volatility-index/'}
    return d,meta


def feature_frame(data):
    f=data.copy();v=f.rvvar.clip(lower=EPS);iv=f.iv
    f['log_iv']=np.log(iv)
    for w in [5,7,22,30]:
        f[f'iv_mean{w}']=iv.rolling(w).mean()
        f[f'log_iv_mean{w}']=np.log(f[f'iv_mean{w}'])
    for w in [1,7,30]:
        f[f'var{w}']=v.rolling(w).mean()
        f[f'log_var{w}']=np.log(f[f'var{w}'].clip(lower=EPS))
    f['ewma_var']=v.ewm(alpha=.06,adjust=False).mean()
    f['iv_var']=(iv/100)**2
    f['log_iv_var']=np.log(f.iv_var)
    f['iv_change1']=iv.diff();f['iv_change5']=iv.diff(5)
    f['iv_range']=f.iv_high-f.iv_low
    f['iv_rv_gap']=iv-100*np.sqrt(f.var30)
    for w in [1,7]: f[f'btc_ret{w}']=np.log(f.close/f.close.shift(w))
    tr=pd.concat([f.high-f.low,(f.high-f.close.shift()).abs(),(f.low-f.close.shift()).abs()],axis=1).max(axis=1)
    f['atr14']=tr.ewm(alpha=1/14,adjust=False).mean()/f.close
    f['log_atr']=np.log(f.atr14)
    f['range_norm']=(f.high-f.low)/(f.atr14*f.close)
    f['next_weekend']=((pd.to_datetime(f.date)+pd.Timedelta(days=1)).dt.dayofweek>=5).astype(int)
    for h in [1,7,30]:
        f[f'weekend_fraction{h}']=sum(((pd.to_datetime(f.date)+pd.Timedelta(days=k)).dt.dayofweek>=5).astype(int) for k in range(1,h+1))/h
    return f


def ridge_fit(x,y,penalty):
    mu=x.mean(axis=0);sd=x.std(axis=0);sd[sd<1e-10]=1
    xx=np.column_stack([np.ones(len(x)),(x-mu)/sd])
    reg=np.eye(xx.shape[1])*penalty;reg[0,0]=0
    beta=np.linalg.solve(xx.T@xx+reg+np.eye(xx.shape[1])*1e-10,xx.T@y)
    return {'mean':mu,'std':sd,'coef':beta}


def ridge_predict(model,x):
    return np.column_stack([np.ones(len(x)),(x-model['mean'])/model['std']])@model['coef']


def masks_for(f,h,y):
    dates=f.date.to_numpy();end=(pd.to_datetime(f.date)+pd.Timedelta(days=h)).dt.strftime('%Y-%m-%d').to_numpy()
    valid=np.isfinite(y)
    masks={'fit':valid&(dates<'2024-01-01')&(end<'2024-01-01'), 'validation':valid&(dates>='2024-01-01')&(dates<'2025-01-01')&(end<'2025-01-01'),'test':valid&(dates>='2025-01-01')}
    masks['refit']=valid&(dates<'2025-01-01')&(end<'2025-01-01')
    for year in ['2025','2026']: masks[year]=masks['test'] & np.char.startswith(dates.astype(str),year)
    return masks,end


def loss(kind,y,p):
    if kind=='dvol': return (y-p)**2
    ratio=y/np.maximum(p,EPS)
    return ratio-np.log(np.maximum(ratio,EPS))-1


def metrics(kind,y,p,current,mask):
    keep=mask&np.isfinite(y)&np.isfinite(p)
    yy=y[keep];pp=p[keep]
    if len(yy)==0: return {'n':0}
    if kind=='rv': yy_level=100*np.sqrt(yy);pp_level=100*np.sqrt(pp)
    else: yy_level=yy;pp_level=pp
    direction=np.sign(pp_level-current[keep]);actual=np.sign(yy_level-current[keep]);active=direction!=0
    return {'n':len(yy),'mae_vol_points':float(np.mean(abs(yy_level-pp_level))),'rmse_vol_points':float(np.sqrt(np.mean((yy_level-pp_level)**2))),'main_loss':float(loss(kind,yy,pp).mean()),'change_direction_accuracy':float((direction[active]==actual[active]).mean()) if active.any() else None,'direction_coverage':float(active.mean())}


def block_ci(x,block=30,reps=3000):
    x=np.asarray(x);rng=np.random.default_rng(159);values=[]
    for _ in range(reps):
        starts=rng.integers(0,len(x),size=(len(x)+block-1)//block)
        ids=((starts[:,None]+np.arange(block))%len(x)).ravel()[:len(x)]
        values.append(x[ids].mean())
    return {'mean':float(x.mean()),'ci95':np.quantile(values,[.025,.975]).tolist(),'block_days':block,'reps':reps}


def model_specs(kind,h):
    if kind=='dvol':
        families={'ar1':['iv'],'har':['iv','iv_mean5','iv_mean22'],'joint':['iv','iv_mean5','iv_mean22','iv_change1','iv_range','iv_rv_gap','log_var1','log_var7','log_var30','btc_ret1','btc_ret7','log_atr','next_weekend']}
        # Predict level change; persistence is the explicit baseline.
        return {f'{name}_ridge{alpha}':{'features':cols,'alpha':alpha,'transform':'iv_change'} for name,cols in families.items() for alpha in [1,10,100]}
    families={'har':['log_var1','log_var7','log_var30'], 'har_iv':['log_var1','log_var7','log_var30','log_iv_var'], 'integrated':['log_var1','log_var7','log_var30','log_iv_var','iv_change1','btc_ret1','btc_ret7','log_atr','range_norm',f'weekend_fraction{h}']}
    return {f'{name}_ridge{alpha}':{'features':cols,'alpha':alpha,'transform':'log_variance'} for name,cols in families.items() for alpha in [1,10,100]}


def fit_candidate(f,y,spec,mask):
    x=f[spec['features']].to_numpy();target=np.log(y[mask]) if spec['transform']=='log_variance' else y[mask]-f.iv.to_numpy()[mask]
    model=ridge_fit(x[mask],target,spec['alpha'])
    model['features']=spec['features'];model['transform']=spec['transform']
    if spec['transform']=='log_variance':
        model['smearing']=float(np.mean(np.exp(target-ridge_predict(model,x[mask]))))
    return model


def predict_candidate(f,model):
    estimate=ridge_predict(model,f[model['features']].to_numpy())
    if model['transform']=='log_variance': return np.maximum(np.exp(np.clip(estimate,-20,10))*model['smearing'],EPS)
    return np.maximum(f.iv.to_numpy()+estimate,.01)


def index_strategy(f,y,p,masks,threshold=None):
    prediction_change=p-f.iv.to_numpy()
    # Decision after current day closes; entry at next-day index open, exit next-day close.
    outcome=y-f.iv_open.shift(-1).to_numpy()
    choice_rows=[]
    for cut in [0,.5,1,2]:
        position=np.where(abs(prediction_change)>cut,np.sign(prediction_change),0)
        signal=position!=0;m=masks['validation']
        score=position*outcome-.25*signal
        choice_rows.append({'threshold':cut,'validation_active':int(signal[m].sum()),'validation_mean_proxy_points':float(score[m].mean())})
    cut=max(choice_rows,key=lambda row:row['validation_mean_proxy_points'])['threshold'] if threshold is None else threshold
    return cut,choice_rows


def variance_strategy(f,y,p,masks,threshold=None):
    gap=f.iv.to_numpy()-100*np.sqrt(p)
    outcome=f.iv_var.to_numpy()-y
    rows=[]
    for cut in [0,5,10,15]:
        signal=gap>cut;m=masks['validation']
        rows.append({'threshold_vol_points':cut,'validation_signals':int(signal[m].sum()),'validation_mean_theoretical_variance':float(np.where(signal,outcome,0)[m].mean())})
    cut=max(rows,key=lambda row:row['validation_mean_theoretical_variance'])['threshold_vol_points'] if threshold is None else threshold
    return cut,rows


def task_fit(f,kind,h):
    if kind=='dvol':
        y=f.iv.shift(-h).to_numpy();current=f.iv.to_numpy()
        baselines={'persistence':f.iv.to_numpy(),'mean5':f.iv_mean5.to_numpy()}
    else:
        y=pd.concat([f.rvvar.shift(-i) for i in range(1,h+1)],axis=1).mean(axis=1,skipna=False).to_numpy()
        current=100*np.sqrt(f.var1.to_numpy())
        baselines={f'rv{k}':f[f'var{k}'].to_numpy() for k in [1,7,30]}
        baselines['ewma']=f.ewma_var.to_numpy()
        if h==30: baselines['market_iv']=f.iv_var.to_numpy()
    masks,end=masks_for(f,h,y);specs=model_specs(kind,h)
    predictions={};models={};validation_table={}
    for name,spec in specs.items():
        model=fit_candidate(f,y,spec,masks['fit']);p=predict_candidate(f,model)
        models[name]=model;predictions[name]=p
        validation_table[name]={'fit':metrics(kind,y,p,current,masks['fit']),'validation':metrics(kind,y,p,current,masks['validation'])}
    baseline_table={name:metrics(kind,y,p,current,masks['validation']) for name,p in baselines.items()}
    chosen=min(predictions,key=lambda k:validation_table[k]['validation']['main_loss'])
    base_name=min(baselines,key=lambda k:baseline_table[k]['main_loss'])
    period={}
    for phase in ['fit','validation','test','refit']:
        idx=np.flatnonzero(masks[phase]);period[phase]={'n':len(idx),'origin_start':f.date.iloc[idx[0]],'origin_end':f.date.iloc[idx[-1]],'last_label_end':end[idx[-1]]}
    choice={'kind':kind,'horizon_days':h,'model':chosen,'baseline':base_name,'periods':period,'validation_candidates':validation_table,'validation_baselines':baseline_table,'fit_model':models[chosen]}
    if kind=='dvol' and h==1: choice['strategy_threshold'],choice['strategy_validation']=index_strategy(f,y,predictions[chosen],masks)
    if kind=='rv' and h==30: choice['strategy_threshold'],choice['strategy_validation']=variance_strategy(f,y,predictions[chosen],masks)
    final_model=fit_candidate(f,y,specs[chosen],masks['refit'])
    choice['frozen_test_model']=final_model
    # Persist locked model, baseline and strategy choices before calculating final test scores.
    key=f'{kind}_{h}d';save(OUT/f'{key}_locked_selection.json',choice)
    final=predict_candidate(f,final_model);base=baselines[base_name]
    results={'selection':choice,'final_metrics':{phase:metrics(kind,y,final,current,masks[phase]) for phase in ['test','2025','2026']},'baseline_metrics':{phase:metrics(kind,y,base,current,masks[phase]) for phase in ['test','2025','2026']},'all_baselines_test':{name:metrics(kind,y,p,current,masks['test']) for name,p in baselines.items()}}
    m=masks['test'];improvement=loss(kind,y[m],base[m])-loss(kind,y[m],final[m])
    ci=block_ci(improvement,block=max(h,14));skill=1-results['final_metrics']['test']['main_loss']/results['baseline_metrics']['test']['main_loss']
    byyear=all(results['final_metrics'][year]['main_loss']<results['baseline_metrics'][year]['main_loss'] for year in ['2025','2026'])
    results['score']={'relative_skill':float(skill),'score_0_100':float(np.clip(50+50*skill,0,100)),'ci':ci,'both_test_years_improve':byyear,'statistical_gate_pass':bool(skill>0 and byyear and ci['ci95'][0]>0)}
    records=[]
    for i in range(len(f)):
        if not np.isfinite(y[i]): continue
        phase='fit' if masks['fit'][i] else 'validation' if masks['validation'][i] else 'test' if m[i] else 'purged'
        pred=final[i] if phase=='test' else predictions[chosen][i]
        records.append({'origin':f.date.iloc[i],'target_end':end[i],'phase':phase,'actual':float(y[i]),'predicted':float(pred),'baseline':float(base[i]),'dvol':float(f.iv.iloc[i])})
    save(OUT/f'{key}_predictions.json',records,compact=True)
    latest={'origin':f.date.iloc[-1],'target_end':end[-1],'prediction':float(final[-1]),'current_dvol':float(f.iv.iloc[-1]),'forecast_vol_points':float(final[-1] if kind=='dvol' else 100*np.sqrt(final[-1])),'model_frozen_since':'2025-01-01','uses_final_model_without_refit_on_test':True}
    results['latest_snapshot']=latest
    if kind=='dvol' and h==1:
        cut=choice['strategy_threshold'];change=final-f.iv.to_numpy();position=np.where(abs(change)>cut,np.sign(change),0);active=position!=0
        next_open=f.iv_open.shift(-1).to_numpy();outcome=y-next_open
        strategy={'threshold_vol_points':cut,'kind':'nontradable_index_signal_proxy_not_futures_pnl','test_days':int(m.sum()),'active_days':int((m&active).sum()),'long_days':int((m&(position>0)).sum()),'short_days':int((m&(position<0)).sum())}
        if (m&active).any(): strategy['active_direction_win_rate']=float((position[m&active]*outcome[m&active]>0).mean())
        strategy['cost_sensitivity']={}
        for cost in [0,.25,.5]:
            proxy=(position*outcome-cost*active)[m]
            always_short=(-outcome-cost)[m]
            curve=np.cumsum(proxy);drawdown=np.maximum.accumulate(np.r_[0,curve])[1:]-curve
            strategy['cost_sensitivity'][str(cost)]={'sum_index_points':float(proxy.sum()),'mean_index_points_per_day':float(proxy.mean()),'max_drawdown_index_points':float(drawdown.max()),'always_short_sum_points':float(always_short.sum()),'mean_vs_always_short_ci':block_ci(proxy-always_short,14)}
        results['strategy']=strategy
    if kind=='rv' and h==30:
        cut=choice['strategy_threshold'];gap=f.iv.to_numpy()-100*np.sqrt(final);signal=gap>cut;outcome=f.iv_var.to_numpy()-y
        strategy={'kind':'theoretical_variance_difference_not_tradable_returns','threshold_vol_points':cut,'overlapping_test_signals':int((m&signal).sum()),'overlapping_test_days':int(m.sum()),'fixed_step_days':30}
        fixed=np.flatnonzero(m)[::30];active=fixed[signal[fixed]]
        pay=np.where(signal[fixed],outcome[fixed],0)
        strategy.update(nonoverlapping_opportunities=len(fixed),nonoverlapping_selected=len(active),mean_difference_per_opportunity=float(pay.mean()),always_short_mean_difference=float(outcome[fixed].mean()),selected_positive_spread_rate=float((outcome[active]>0).mean()) if len(active) else None,selected_average_difference=float(outcome[active].mean()) if len(active) else None,worst_selected_difference=float(outcome[active].min()) if len(active) else None)
        strategy['nonoverlapping_rows']=[{'origin':f.date.iloc[i],'end':end[i],'signal':bool(signal[i]),'dvol':float(f.iv.iloc[i]),'predicted_rv':float(100*np.sqrt(final[i])),'actual_rv':float(100*np.sqrt(y[i])),'variance_difference':float(outcome[i])} for i in fixed]
        # Report whether selecting observations beats the same always-short reference.
        strategy['paired_gain_ci']=block_ci(pay-outcome[fixed],block=2)
        results['strategy']=strategy
    save(OUT/f'{key}_results.json',results)
    print('FINAL',key,'model',chosen,'baseline',base_name,'validation',validation_table[chosen]['validation'],'test',results['final_metrics']['test'],'score',results['score'],flush=True)
    if 'strategy' in results: print('STRATEGY',key,json.dumps({k:v for k,v in results['strategy'].items() if k!='nonoverlapping_rows'},ensure_ascii=False),flush=True)
    return results


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    raw,meta=source_data();f=feature_frame(raw).iloc[60:].reset_index(drop=True)
    assert np.isfinite(f.select_dtypes('number').to_numpy()).all()
    save(OUT/'source_daily_snapshot.json',{'metadata':meta,'rows':raw.to_dict(orient='records')},compact=True)
    save(OUT/'feature_snapshot.json',f.to_dict(orient='records'),compact=True)
    print('DATA',json.dumps(meta,ensure_ascii=False),flush=True)
    results={}
    for kind,h in [('dvol',1),('dvol',7),('rv',1),('rv',7),('rv',30)]: results[f'{kind}_{h}d']=task_fit(f,kind,h)
    summary={'metadata':meta,'results':{key:{'model':r['selection']['model'],'baseline':r['selection']['baseline'],'periods':r['selection']['periods'],'validation':r['selection']['validation_candidates'][r['selection']['model']]['validation'],'test':r['final_metrics']['test'],'baseline_test':r['baseline_metrics']['test'],'score':r['score'],'latest':r['latest_snapshot']} for key,r in results.items()}}
    save(OUT/'summary.json',summary)
    # Causality: historical features do not change if later data are removed.
    checkpoint=raw.index[raw.date=='2025-06-30'][0]
    prefix=feature_frame(raw.iloc[:checkpoint+1]);full=feature_frame(raw)
    cols=list(full.select_dtypes('number').columns)
    np.testing.assert_allclose(prefix[cols].iloc[-1],full[cols].iloc[checkpoint],atol=1e-12,rtol=1e-12)
    for key,r in results.items():
        p=r['selection']['periods'];assert p['fit']['last_label_end']<'2024-01-01';assert p['validation']['last_label_end']<'2025-01-01';assert p['refit']['last_label_end']<'2025-01-01'
    save(OUT/'validation_checks.json',{'feature_prefix_invariant':True,'purged_label_boundaries_pass':True,'model_and_threshold_saved_before_test_scoring':True,'daily_sequence_contiguous':True,'note':'Tests establish chronology, not market profitability. DVOL caches have no per-hour confirmed field, so only complete dates before today are used.'})


if __name__=='__main__': main()

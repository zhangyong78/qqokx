"""Exploratory follow-up: calendar and intraday data known at the daily close.

Primary audit results were already inspected before defining this phase.
This is supplementary research, not a new untouched holdout experiment.
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
from research.btc_daily_rules_20260929 import OUT,QUANTILES,phases,direction_metrics,shape_metrics,block_ci,logistic_fit,write_json


def hourly_features(inst='BTC-USDT-SWAP',offset=8):
    with sqlite3.connect(candle_store_db_path().as_uri()+'?mode=ro',uri=True) as db:
        raw=db.execute('SELECT ts,open,high,low,close,volume FROM candles WHERE inst_id=? AND bar=? AND confirmed=1 ORDER BY ts',(inst,'1H')).fetchall()
    digest=hashlib.sha256(json.dumps(raw).encode()).hexdigest()
    h=pd.DataFrame(raw,columns=['ts','open','high','low','close','volume'])
    for col in h.columns:
        h[col]=h[col].astype(float)
    shifted=pd.to_datetime(h.ts,unit='ms',utc=True)+pd.Timedelta(hours=offset)
    h['date']=shifted.dt.strftime('%Y-%m-%d');h['hour']=shifted.dt.hour
    rows=[]
    for day,g in h.groupby('date',sort=True):
        if len(g)!=24 or not np.array_equal(g.hour.to_numpy(),np.arange(24)):
            continue
        close=g.close.iloc[-1];opening=g.open.iloc[0]
        rv=np.sqrt(np.sum(np.log(g.close.to_numpy()/g.open.to_numpy())**2))
        volume=g.volume.to_numpy();prices=(g.high.to_numpy()+g.low.to_numpy()+g.close.to_numpy())/3
        vwap=float(np.sum(volume*prices)/np.sum(volume)) if volume.sum()>0 else close
        r={'date':day,'hourly_open':opening,'hourly_close':close,'hourly_high':g.high.max(),'hourly_low':g.low.min(),'rv':rv,'late_volume':volume[-4:].sum()/max(volume.sum(),1),'vwap_gap':close/vwap-1,'low_hour':float(np.argmin(g.low.to_numpy()))/23,'high_hour':float(np.argmax(g.high.to_numpy()))/23}
        for k in [1,4,8]:
            r[f'last{k}']=close/g.open.iloc[-k]-1
        r['first8']=g.close.iloc[7]/opening-1
        rows.append(r)
    return pd.DataFrame(rows),{'hourly_rows':len(h),'complete_days':len(rows),'sha256':digest}


def evaluate(suffix='primary',inst='BTC-USDT-SWAP',offset=8,frozen=None):
    prior=json.loads((OUT/f'{suffix}_predictions.json').read_text(encoding='utf-8'))
    f=pd.DataFrame(prior['features'])
    hf,metadata=hourly_features(inst,offset)
    f=f.merge(hf,on='date',how='left',validate='one_to_one')
    complete=f.rv.notna().to_numpy()
    mismatch={}
    for col in ['open','close','high','low']:
        diff=(f['hourly_'+col]/f[col]-1).abs()
        mismatch[col]={'count_gt_0_1pct':int((diff>.001).sum()),'max_pct':float(diff.max()*100)}
    metadata['daily_hourly_mismatch']=mismatch
    # OHLC reconciliation is reported; hourly direction fits use only complete sessions.
    for col in ['last1','last4','last8','first8','vwap_gap']:
        f[col]=f[col]/f.atr14
    f['rv_ratio']=f.rv/f.atr14
    f['rv14']=f.rv.ewm(span=14,adjust=False).mean()
    f['rv30']=f.rv.ewm(span=30,adjust=False).mean()
    y=np.array(prior['actual_direction']);targets=np.array(prior['targets']);n=len(f)
    masks=phases(f)
    base=np.array(prior['direction']['base_all']);base[base<0]=np.nan
    directions={}
    groups={
        'intraday':['last1','last4','last8','first8','vwap_gap','late_volume','low_hour','high_hour','rv_ratio','target_weekend'],
        'daily_intraday':['ema_distance','ma_distance','ema_slope','ma_slope','signed_streak','ret1','ret3','ret10','body_ratio','close_location','range_norm','vol_ratio','volume_ratio','weekday_sin','weekday_cos','last1','last4','last8','first8','vwap_gap','late_volume','low_hour','high_hour','rv_ratio','target_weekend'],
    }
    months=f.date.str[:7].to_numpy()
    for group,columns in groups.items():
        x=f[columns].to_numpy()
        valid=np.isfinite(x).all(axis=1)
        for window,penalty in [(0,10),(0,100),(730,100)]:
            p=np.full(n,np.nan);fit=None
            for i in range(300,n):
                if not valid[i]:
                    continue
                if fit is None or months[i]!=months[i-1]:
                    start=max(0,i-window) if window else 0
                    train=valid[start:i]&(y[start:i]>=0)
                    if train.sum()<200:
                        continue
                    fit=logistic_fit(x[start:i][train],y[start:i][train],penalty)
                mu,sd,beta=fit
                p[i]=1/(1+np.exp(-np.clip(np.r_[1,np.clip((x[i]-mu)/sd,-8,8)]@beta,-25,25)))
            directions[f'{group}_{window or "all"}_l2_{penalty}']=p
    shapes={}
    next_weekday=pd.to_datetime(f.target_date).dt.dayofweek.to_numpy()
    types={'daytype':(next_weekday>=5).astype(int),'weekday':next_weekday,'range_daytype':np.digitize(f.range_norm.to_numpy(),[.7,1.3])*2+(next_weekday>=5)}
    specs=[('atr14_daytype_all','atr14','daytype',0),('atr14_daytype_365','atr14','daytype',365),('atr14_weekday_all','atr14','weekday',0),('atr14_range_daytype_all','atr14','range_daytype',0),('rv14_daytype_all','rv14','daytype',0),('rv30_daytype_all','rv30','daytype',0)]
    for name,scale_name,keyname,window in specs:
        scale=f[scale_name].to_numpy();keys=types[keyname]
        normalized=targets/scale[:,None]
        p=np.full((n,4,5),np.nan)
        for i in range(300,n):
            if not np.isfinite(scale[i]) or scale[i]<=0:
                continue
            start=max(0,i-window) if window else 0
            valid=np.isfinite(normalized[start:i]).all(axis=1)
            if valid.sum()<40:
                continue
            same=(keys[start:i]==keys[i]) & valid
            use=same if same.sum()>=40 else valid
            p[i]=np.quantile(normalized[start:i][use],QUANTILES,axis=0).T*scale[i]
        shapes[name]=p
    dmetrics={name:{phase:direction_metrics(y,p,base,mask) for phase,mask in masks.items()} for name,p in directions.items()}
    smetrics={name:{phase:shape_metrics(targets,p,mask) for phase,mask in masks.items()} for name,p in shapes.items()}
    chosen_d=min(directions,key=lambda k:dmetrics[k]['validation'].get('brier',1))
    chosen_s=min(shapes,key=lambda k:smetrics[k]['validation'].get('mean_pinball',1))
    selection={'direction':chosen_d,'shape':chosen_s,'exploratory_after_primary_audit':True}
    if frozen:
        chosen_d=selection['direction']=frozen['direction'];chosen_s=selection['shape']=frozen['shape']
        selection['frozen_transfer']=True
    write_json(OUT/f'{suffix}_phase2_selection.json',selection)
    m=masks['audit']&np.isfinite(directions[chosen_d])
    yy=(y[m]==1).astype(float)
    dci=block_ci((base[m]-yy)**2-(directions[chosen_d][m]-yy)**2)
    m=masks['audit']&np.isfinite(shapes[chosen_s][:,0,0])
    def loss(p):
        e=targets[m,:,None]-p[m]
        return np.maximum(e*QUANTILES,e*(QUANTILES-1)).mean(axis=(1,2))
    sci={name:block_ci(loss(np.array(prior['shape'][name]))-loss(shapes[chosen_s])) for name in ['all','rolling365','atr14_all']}
    relation=[]
    for phase in ['validation','audit','2025','2026']:
        for value in [0,1]:
            m=masks[phase]&(f.target_weekend.to_numpy()==value)
            relation.append({'phase':phase,'weekend':value,'n':int(m.sum()),'median_range_pct':float(np.median(targets[m,3])*100),'median_range_over_atr':float(np.median(targets[m,3]/f.atr14.to_numpy()[m]))})
    result={'metadata':metadata,'selection':selection,'directions':dmetrics,'shapes':smetrics,'direction_ci':dci,'shape_ci':sci,'calendar_relation':relation}
    write_json(OUT/f'{suffix}_phase2_results.json',result)
    write_json(OUT/f'{suffix}_phase2_predictions.json',{'dates':f.target_date.tolist(),'direction':{k:np.nan_to_num(v,nan=-999) for k,v in directions.items()},'shape':{k:np.nan_to_num(v,nan=-999) for k,v in shapes.items()},'features':f.astype(object).where(pd.notna(f),None).to_dict(orient='records')})
    print(suffix,'selection',selection,'hourly_quality',metadata,flush=True)
    print('DIRECTION',dmetrics[chosen_d]['validation'],dmetrics[chosen_d]['audit'],dci,flush=True)
    print('SHAPE',smetrics[chosen_s]['validation'],smetrics[chosen_s]['audit'],sci,flush=True)
    print('CALENDAR',relation,flush=True)


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--suffix',default='primary')
    parser.add_argument('--inst',default='BTC-USDT-SWAP')
    parser.add_argument('--offset',type=int,default=8)
    parser.add_argument('--frozen-selection',type=Path)
    a=parser.parse_args()
    frozen=json.loads(a.frozen_selection.read_text(encoding='utf-8')) if a.frozen_selection else None
    evaluate(a.suffix,a.inst,a.offset,frozen)

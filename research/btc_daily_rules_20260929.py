"""Reproducible, local-only walk-forward BTC daily research. See sibling protocol."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from okx_quant.candle_store import candle_store_db_path

OUT = ROOT / 'reports' / 'btc_daily_rules_20260929'
QUANTILES = np.array([.1, .25, .5, .75, .9])
TARGETS = ['close', 'high', 'low', 'range']


def write_json(path, value):
    def encode(x):
        if isinstance(x, np.ndarray):
            return x.tolist()
        if isinstance(x, np.generic):
            return x.item()
        raise TypeError(type(x).__name__)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=encode, allow_nan=False), encoding='utf-8')


def load(inst='BTC-USDT-SWAP', bar='1D'):
    path = candle_store_db_path()
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as db:
        raw = db.execute('SELECT ts,open,high,low,close,volume FROM candles WHERE inst_id=? AND bar=? AND confirmed=1 ORDER BY ts', (inst, bar)).fetchall()
    digest = hashlib.sha256(json.dumps(raw).encode()).hexdigest()
    df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
    df['date'] = pd.to_datetime(df.ts, unit='ms', utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')
    df = df[df.date < '2026-09-29'].copy()
    for c in ['open','high','low','close','volume']:
        df[c] = df[c].astype(float)
    assert df.ts.is_unique
    gaps = int((df.ts.diff().dropna() != 86400000).sum())
    assert ((df.low > 0) & (df.low <= df[['open','close']].min(axis=1)) & (df.high >= df[['open','close']].max(axis=1))).all()
    return df.reset_index(drop=True), dict(inst=inst,bar=bar,n=len(df),start=df.date.iloc[0],end=df.date.iloc[-1],gaps=gaps,sha256=digest)


def features(df):
    f = df.copy()
    o,h,l,c,v = (f[k] for k in ['open','high','low','close','volume'])
    prev = c.shift()
    tr = pd.concat([h-l,(h-prev).abs(),(l-prev).abs()],axis=1).max(axis=1)
    f['atr14'] = tr.ewm(alpha=1/14,adjust=False).mean()/c
    f['atr50'] = tr.ewm(alpha=1/50,adjust=False).mean()/c
    ret = c.pct_change()
    f['ewma10'] = ret.pow(2).ewm(span=10,adjust=False).mean().pow(.5)
    f['ewma30'] = ret.pow(2).ewm(span=30,adjust=False).mean().pow(.5)
    f['range20'] = ((h-l)/c).rolling(20).median()
    atr = f.atr14*c
    ema = c.ewm(span=15,adjust=False).mean()
    ma = c.rolling(50).mean()
    f['ema_distance'] = (c-ema)/atr
    f['ma_distance'] = (c-ma)/atr
    f['ema_slope'] = (ema-ema.shift(5))/atr
    f['ma_slope'] = (ma-ma.shift(5))/atr
    f['trend'] = (c>ema).astype(int) + 2*(c>ma).astype(int)
    f['body'] = (c-o)/atr
    f['range_norm'] = (h-l)/atr
    f['body_ratio'] = (c-o)/(h-l).replace(0,np.nan)
    f['close_location'] = (c-l)/(h-l).replace(0,np.nan)
    f['upper_wick'] = (h-pd.concat([o,c],axis=1).max(axis=1))/(h-l).replace(0,np.nan)
    f['lower_wick'] = (pd.concat([o,c],axis=1).min(axis=1)-l)/(h-l).replace(0,np.nan)
    f['vol_ratio'] = f.atr14/f.atr50
    f['volume_ratio'] = (v/v.rolling(20).mean()).clip(0,10)
    for k in [1,3,5,10,20]:
        f[f'ret{k}'] = c.pct_change(k)/f.atr14
    sign=np.sign(c-o).to_numpy(dtype=int)
    streak=np.zeros(len(f),dtype=int)
    for i in range(len(f)):
        streak[i] = 0 if sign[i]==0 else (streak[i-1]+1 if i and sign[i]==sign[i-1] else 1)
    f['sign'] = sign
    f['streak'] = np.minimum(streak,4)
    f['signed_streak'] = sign*np.minimum(streak,4)
    weekday = (pd.to_datetime(f.date)+pd.Timedelta(days=1)).dt.dayofweek
    f['weekday_sin'] = np.sin(2*np.pi*weekday/7)
    f['weekday_cos'] = np.cos(2*np.pi*weekday/7)
    f['target_weekend'] = (weekday>=5).astype(int)
    f['break_high'] = (c>h.shift().rolling(20).max()).astype(int)
    f['break_low'] = (c<l.shift().rolling(20).min()).astype(int)
    return f


def phases(f):
    date = f.target_date.to_numpy()
    return {'train':date<'2023-01-01','validation':(date>='2023-01-01') & (date<'2025-01-01'),'audit':date>='2025-01-01', **{str(y):np.char.startswith(date.astype(str),str(y)) for y in range(2021,2027)}}


def direction_metrics(y,p,base,mask):
    keep=mask & np.isfinite(p) & np.isfinite(base)
    if not keep.any():
        return {'n':0}
    yy=y[keep]; pp=p[keep]; bb=base[keep]
    correct=np.where(pp>=.5,yy==1,yy==0)
    base_correct=np.where(bb>=.5,yy==1,yy==0)
    binary=(yy==1).astype(float)
    return dict(n=int(keep.sum()),accuracy=float(correct.mean()),baseline_accuracy=float(base_correct.mean()),brier=float(((pp-binary)**2).mean()),baseline_brier=float(((bb-binary)**2).mean()),mean_p=float(pp.mean()),bull_rate=float(binary.mean()),mean_confidence=float(np.maximum(pp,1-pp).mean()))


def logistic_fit(x,y,penalty):
    mu=x.mean(axis=0);sd=x.std(axis=0);sd[sd<1e-8]=1
    xx=np.column_stack([np.ones(len(x)),np.clip((x-mu)/sd,-8,8)])
    beta=np.zeros(xx.shape[1]);beta[0]=math.log((y.sum()+1)/(len(y)-y.sum()+1))
    ridge=np.full(len(beta),penalty);ridge[0]=0
    for _ in range(25):
        prob=1/(1+np.exp(-np.clip(xx@beta,-25,25)))
        grad=xx.T@(prob-y)+ridge*beta
        hess=(xx.T*np.maximum(prob*(1-prob),1e-7))@xx+np.diag(ridge+1e-6)
        step=np.linalg.solve(hess,grad)
        beta-=step
        if np.max(np.abs(step))<1e-6:
            break
    return mu,sd,beta


def direction_models(f,y):
    n=len(f)
    predictions={}
    base=np.full(n,np.nan); recent=np.full(n,np.nan)
    for i in range(300,n):
        prior=y[:i]; recent_y=y[max(0,i-365):i]
        base[i]=(np.sum(prior==1)+1)/(np.sum(prior>=0)+2)
        recent[i]=(np.sum(recent_y==1)+1)/(np.sum(recent_y>=0)+2)
    predictions['base_all']=base
    predictions['base_365']=recent
    sign=f.sign.to_numpy(int);streak=f.streak.to_numpy(int);trend=f.trend.to_numpy(int)
    keys={
        'streak':(sign+1)*5+streak,
        'trend':trend,
        'streak_trend':((sign+1)*5+streak)*4+trend,
        'sign_trend_vol':((sign+1)*4+trend)*2+(f.vol_ratio.to_numpy()>1),
    }
    for name,key in keys.items():
        for window,alpha in [(0,20),(0,100),(365,20)]:
            p=np.full(n,np.nan)
            for i in range(300,n):
                start=max(0,i-window) if window else 0
                matches=(key[start:i]==key[i]) & (y[start:i]>=0)
                outcomes=y[start:i][matches]
                prior=recent[i] if window else base[i]
                p[i]=(outcomes.sum()+alpha*prior)/(len(outcomes)+alpha)
            predictions[f'bucket_{name}_{window or "all"}_a{alpha}']=p
    # Unshrunk streak variant on the feature-ready history after the 60-bar warm-up.
    # The exact original rule (raw history from index 12) is evaluated separately.
    p=np.full(n,np.nan)
    for i in range(300,n):
        matches=(keys['streak'][:i]==keys['streak'][i])
        if matches.sum()<8:
            matches=sign[:i]==sign[i]
        if matches.sum()<8:
            matches=np.ones(i,dtype=bool)
        p[i]=np.mean(y[:i][matches]==1)
    predictions['streak_unshrunk']=p
    groups={
        'trend':['ema_distance','ma_distance','ema_slope','ma_slope','signed_streak','ret3','ret10'],
        'shape':['body','range_norm','body_ratio','close_location','upper_wick','lower_wick','signed_streak'],
        'full':['ema_distance','ma_distance','ema_slope','ma_slope','signed_streak','ret1','ret3','ret10','body_ratio','close_location','upper_wick','lower_wick','range_norm','vol_ratio','volume_ratio','weekday_sin','weekday_cos'],
    }
    months=f.date.str[:7].to_numpy()
    for name,columns in groups.items():
        x=f[columns].to_numpy(float)
        for window,penalty in [(0,10),(0,100),(730,100)]:
            p=np.full(n,np.nan);fit=None
            for i in range(300,n):
                if fit is None or months[i]!=months[i-1]:
                    start=max(0,i-window) if window else 0
                    valid=y[start:i]>=0
                    fit=logistic_fit(x[start:i][valid],y[start:i][valid],penalty)
                mu,sd,beta=fit
                p[i]=1/(1+np.exp(-np.clip(np.r_[1,np.clip((x[i]-mu)/sd,-8,8)]@beta,-25,25)))
            predictions[f'logistic_{name}_{window or "all"}_l2_{penalty}']=p
    x=f[groups['full']].to_numpy(float)
    for k in [50,150]:
        p=np.full(n,np.nan)
        for i in range(300,n):
            prior_x=x[:i];sd=prior_x.std(axis=0);sd[sd<1e-8]=1
            distance=np.mean(np.minimum(((prior_x-x[i])/sd)**2,25),axis=1)
            distance[y[:i]<0]=np.inf
            ids=np.argpartition(distance,k)[:k]
            p[i]=(np.sum(y[ids]==1)+20*base[i])/(k+20)
        predictions[f'knn_{k}']=p
    return predictions


def shape_models(f,targets):
    n=len(f);predictions={}
    specs=[('all',None,0),('rolling180',None,180),('rolling365',None,365)]
    specs += [(f'{scale}_{window or "all"}',scale,window) for scale,window in [('atr14',0),('atr14',180),('atr14',365),('atr50',365),('ewma10',365),('ewma30',365),('range20',365)]]
    for name,scale_name,window in specs:
        scale=np.ones(n) if scale_name is None else f[scale_name].to_numpy()
        normalized=targets/scale[:,None]
        p=np.full((n,4,len(QUANTILES)),np.nan)
        for i in range(300,n):
            start=max(0,i-window) if window else 0
            p[i]=np.quantile(normalized[start:i],QUANTILES,axis=0).T*scale[i]
        predictions[name]=p
    return predictions


def shape_metrics(target,p,mask):
    mask=mask & np.isfinite(p[:,0,0])
    if not mask.any():
        return {'n':0}
    yy=target[mask];pp=p[mask];error=yy[:,:,None]-pp
    pinball=np.maximum(QUANTILES*error,(QUANTILES-1)*error)
    result={'n':int(mask.sum()),'mean_pinball':float(pinball.mean())}
    for j,name in enumerate(TARGETS):
        lower,upper=pp[:,j,0],pp[:,j,-1]
        width=upper-lower
        score=width+10*np.maximum(lower-yy[:,j],0)+10*np.maximum(yy[:,j]-upper,0)
        result[name]={'mae':float(np.abs(yy[:,j]-pp[:,j,2]).mean()),'pinball':float(pinball[:,j].mean()),'coverage80':float(((yy[:,j]>=lower)&(yy[:,j]<=upper)).mean()),'width80':float(width.mean()),'interval_score80':float(score.mean())}
    return result


def block_ci(values,block=14,repetitions=3000):
    values=np.asarray(values);rng=np.random.default_rng(159)
    means=np.empty(repetitions)
    for j in range(repetitions):
        starts=rng.integers(0,len(values),size=(len(values)+block-1)//block)
        ids=((starts[:,None]+np.arange(block))%len(values)).ravel()[:len(values)]
        means[j]=values[ids].mean()
    return {'mean':float(values.mean()),'ci95':np.quantile(means,[.025,.975]).tolist(),'block':block,'repetitions':repetitions}


def condition_rules(f,y,masks):
    predicates={}
    s=f.sign.to_numpy();streak=f.streak.to_numpy()
    for sign,label in [(1,'bull'),(-1,'bear')]:
        predicates[label]=s==sign
        for k in [2,3,4]:
            predicates[f'{label}_streak{k}']=(s==sign)&(streak==k)
    for name in ['ema_distance','ma_distance','ema_slope','ma_slope']:
        x=f[name].to_numpy()
        predicates[name+'_positive']=x>0
        predicates[name+'_negative']=x<0
    for name,low,high in [('body',-.75,.75),('ret3',-1,1),('ema_distance',-1.5,1.5),('close_location',.2,.8),('vol_ratio',.8,1.2),('range_norm',.6,1.5),('volume_ratio',.7,1.5),('upper_wick',.15,.5),('lower_wick',.15,.5)]:
        x=f[name].to_numpy()
        predicates[name+'_low']=x<low
        predicates[name+'_high']=x>high
    predicates['break20_high']=f.break_high.to_numpy()==1
    predicates['break20_low']=f.break_low.to_numpy()==1
    predicates['next_weekend']=f.target_weekend.to_numpy()==1
    predicates['next_weekday']=f.target_weekend.to_numpy()==0
    candidates=list(predicates.items());names=list(predicates)
    for j,a in enumerate(names):
        for b in names[j+1:]:
            candidates.append((a+' & '+b,predicates[a]&predicates[b]))
    found=[];seen=set();valid_count=0
    for name,condition in candidates:
        fingerprint=condition.tobytes()
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        train=condition&masks['train']&(y>=0)
        validation=condition&masks['validation']&(y>=0)
        if train.sum()<80 or validation.sum()<60:
            continue
        valid_count+=1
        direction=1 if y[train].mean()>=.5 else 0
        tr=float((y[train]==direction).mean());val=float((y[validation]==direction).mean())
        baseline=float(y[validation].mean())
        yearly=[]
        for year in ['2023','2024']:
            part=condition&masks[year]&(y>=0)
            yearly.append((int(part.sum()),float((y[part]==direction).mean()) if part.any() else 0))
        if tr<.55 or val<.55 or val-baseline<.02 or any(n<20 or acc<.52 for n,acc in yearly):
            continue
        found.append(dict(name=name,direction=direction,train_n=int(train.sum()),train_accuracy=tr,validation_n=int(validation.sum()),validation_accuracy=val,validation_baseline=baseline,years_validation=yearly,rank=(val-.5)*math.sqrt(validation.sum())))
    found.sort(key=lambda x:x['rank'],reverse=True)
    return predicates,found[:10],dict(raw_candidates=len(candidates),unique_candidates=len(seen),eligible=valid_count,passed_selection=len(found))


def condition_mask(name,predicates):
    parts=name.split(' & ')
    return np.logical_and.reduce([predicates[p] for p in parts])


def run(inst='BTC-USDT-SWAP',bar='1D',suffix='primary',frozen_selection=None):
    OUT.mkdir(parents=True,exist_ok=True)
    df,metadata=load(inst,bar)
    assert metadata['gaps']==0, metadata
    f=features(df)
    f['target_date']=df.date.shift(-1)
    sign=np.sign(df.close.shift(-1)-df.open.shift(-1))
    f['y']=np.where(sign>0,1,np.where(sign<0,0,-1))
    for name,value in {
        'target_close':df.close.shift(-1)/df.close-1,
        'target_high':df.high.shift(-1)/df.close-1,
        'target_low':df.low.shift(-1)/df.close-1,
        'target_range':(df.high.shift(-1)-df.low.shift(-1))/df.close,
    }.items():
        f[name]=value
    # Fixed 60-bar indicator warm-up. No feature depends on the discarded future target.
    f=f.iloc[60:-1].reset_index(drop=True)
    assert np.isfinite(f.select_dtypes('number').to_numpy()).all()
    y=f.y.to_numpy();target=f[['target_'+k for k in TARGETS]].to_numpy()
    masks=phases(f)
    directions=direction_models(f,y)
    shapes=shape_models(f,target)
    base=directions['base_all']
    dmetrics={name:{phase:direction_metrics(y,p,base,mask) for phase,mask in masks.items()} for name,p in directions.items()}
    smetrics={name:{phase:shape_metrics(target,p,mask) for phase,mask in masks.items()} for name,p in shapes.items()}
    # Freeze choices using validation only. Audit results cannot affect this selection.
    direction_order=sorted(directions,key=lambda k:dmetrics[k]['validation'].get('brier',1))
    shape_order=sorted(shapes,key=lambda k:smetrics[k]['validation'].get('mean_pinball',1))
    predicates,selected_rules,rule_counts=condition_rules(f,y,masks)
    selection={'direction':direction_order[0],'shape':shape_order[0],'direction_order':direction_order,'shape_order':shape_order,'rules':selected_rules,'rule_counts':rule_counts,'metadata':metadata}
    if frozen_selection is not None:
        selection['direction']=frozen_selection['direction']
        selection['shape']=frozen_selection['shape']
        selection['rules']=[]
        selected_rules=[]
        selection['transfer_from_primary']=True
    write_json(OUT/f'{suffix}_selection.json',selection)
    print('SELECTION',suffix,json.dumps(selection,ensure_ascii=True),flush=True)
    audits=[]
    for rule in selected_rules:
        condition=condition_mask(rule['name'],predicates)
        stats={}
        for phase in ['audit','2025','2026']:
            m=condition&masks[phase]&(y>=0)
            stats[phase]={'n':int(m.sum()),'accuracy':float((y[m]==rule['direction']).mean()) if m.any() else None,'baseline':float(y[m].mean()) if m.any() else None}
        audits.append({**rule,'audit_results':stats})
    chosen=selection['direction'];m=masks['audit'] & np.isfinite(directions[chosen]);yy=(y[m]==1).astype(float)
    dci=block_ci((base[m]-yy)**2-(directions[chosen][m]-yy)**2)
    shape_choice=selection['shape'];m=masks['audit'] & np.isfinite(shapes[shape_choice][:,0,0])
    def losses(p):
        e=target[m,:,None]-p[m]
        return np.maximum(e*QUANTILES,e*(QUANTILES-1)).mean(axis=(1,2))
    sci={name:block_ci(losses(shapes[name])-losses(shapes[shape_choice])) for name in ['all','rolling180','rolling365']}
    result={'metadata':metadata,'selection':selection,'directions':dmetrics,'shapes':smetrics,'audit_rules':audits,'direction_brier_improvement_ci':dci,'shape_pinball_improvement_ci':sci}
    write_json(OUT/f'{suffix}_results.json',result)
    write_json(OUT/f'{suffix}_predictions.json',{'dates':f.target_date.tolist(),'actual_direction':y,'targets':target,'direction':{name:np.nan_to_num(p,nan=-999).tolist() for name,p in directions.items()},'shape':{name:np.nan_to_num(p,nan=-999).tolist() for name,p in shapes.items()},'feature_columns':list(f.columns),'features':f.to_dict(orient='records')})
    print('AUDIT_DIRECTION',suffix,chosen,json.dumps(dmetrics[chosen]['audit']),dci,flush=True)
    print('AUDIT_SHAPE',suffix,shape_choice,json.dumps(smetrics[shape_choice]['audit']),sci,flush=True)
    print('FILES',str(OUT),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--inst',default='BTC-USDT-SWAP')
    parser.add_argument('--bar',default='1D')
    parser.add_argument('--suffix',default='primary')
    parser.add_argument('--frozen-selection',type=Path)
    args=parser.parse_args()
    frozen=json.loads(args.frozen_selection.read_text(encoding='utf-8')) if args.frozen_selection else None
    run(args.inst,args.bar,args.suffix,frozen)

#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install pyEX   


# In[ ]:


import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from sklearn.cluster import DBSCAN
import numpy as np
import pandas as pd
import math

from itertools import chain   
from scipy import stats
from sklearn.preprocessing import LabelEncoder
import pyarrow.parquet as pq
from sklearn import preprocessing

import pyEX as p
isSandbox = False
if isSandbox:
    token ='Tpk_02dcd2036e7641b880dd4cbb01fa9c67'
    iex_ver = 'sandbox'
else:
    token ='pk_2e94555e43da4135a6032433c6b18fa5'
    iex_ver = 'stable'
pyEX_cl = p.Client(api_token=token)
#https://www.analyticsvidhya.com/blog/2021/06/style-your-pandas-dataframe-and-make-it-stunning/


# In[ ]:


sufx="bbo"
#s3://maystreetdata/analysis/joined_df_stats_by_symbol_mt_oddlot_mt_roundlot_bbo.parquet/
res_file_name=f'joined_df_stats_by_symbol_mt_oddlot_mt_roundlot_{sufx}.parquet/'  
res_dir=f's3://maystreetdata/analysis/'
res_file=f'{res_dir}{res_file_name}/'
df = pq.read_table(source=res_file).to_pandas()
df['hour_est']=df['hour_est'].astype(float)
cols_to_num=['diff_exchange_timestamp_sum','bidask_timeweight_mt_oddlot_sum'
             ,f'bidask_timeweight_mt_roundlot_{sufx}_sum','percent_oddlot_is_top',f'bid_ask_tw_mt_oddlot',f'bid_ask_tw_mt_roundlot_{sufx}']
for one_col in cols_to_num:
    df[one_col]=df[one_col].astype(float)
#agg_pd = pd.read_parquet(res_file, engine='pyarrow')


# In[ ]:


df


# In[ ]:


test_df=df[[f'bid_ask_tw_mt_oddlot',f'bid_ask_tw_mt_roundlot_{sufx}']].dropna()
stats.ttest_ind(test_df['bid_ask_tw_mt_oddlot'], test_df[f'bid_ask_tw_mt_roundlot_{sufx}'])


# In[ ]:


df[['is_trading_hours','price_bucket_mt_oddlot',f'bid_ask_tw_mt_oddlot',f'bid_ask_tw_mt_roundlot_{sufx}','percent_oddlot_is_top']].dropna().groupby(['is_trading_hours']).mean()


# In[ ]:


ol_rl_stats = df.groupby(['hour_est','is_trading_hours','price_bucket_mt_oddlot']).agg({'bid_ask_tw_mt_oddlot':['mean','count']
                                                               ,f'bid_ask_tw_mt_roundlot_{sufx}':['mean','count']
                                                               ,'percent_oddlot_is_top':['mean','count']
                                                              })
ol_rl_stats.columns=['_'.join(col) for col in ol_rl_stats.columns.values]
#print(ol_rl_stats)
disp_df=pd.pivot_table(ol_rl_stats.query(f"is_trading_hours == is_trading_hours"), values=['bid_ask_tw_mt_oddlot_mean',f'bid_ask_tw_mt_roundlot_{sufx}_mean'],
                       index=['hour_est','is_trading_hours','price_bucket_mt_oddlot'],
                    columns=[], aggfunc=np.mean)/100


fname=f's3://maystreetdata/analysis/blog_graphs/oddlot_roundlot(bbo)_spread_by_HOUR_PRICEBUCKET_V2.csv'
disp_df.to_csv(fname)
disp_df.query("hour_est>0").style.format({
    'bid_ask_tw_mt_oddlot_mean': '{:,.4%}'.format,
    f'bid_ask_tw_mt_roundlot_{sufx}_mean': '{:,.4%}'.format,
}).bar()


# In[ ]:


disp_df.bid_ask_tw_mt_oddlot_mean


# In[ ]:


#res_file='s3://maystreetdata/analysis/ARKW_trades.parquet/'
#arkw_pd = pq.read_table(source=res_file).to_pandas().set_index('ExchangeTimestamp').sort_index()


# In[ ]:





# In[ ]:


if True:
    res_file='s3://maystreetdata/analysis/trade_stats_by_mp.parquet/'
    trade_stats_pd = pq.read_table(source=res_file).to_pandas().set_index('Product').sort_index().reset_index()
    trade_stats_pd['hour_est']=trade_stats_pd['hour_est'].astype(int)
    trade_stats_pd=trade_stats_pd.set_index('hour_est','is_trading_hours')
    cols_to_num=['trade_volume_daily_mean','trade_count_daily_mean','day_count']
    for one_col in cols_to_num:
        trade_stats_pd[one_col]=trade_stats_pd[one_col].astype(float)
    


# In[ ]:


trade_stats_all = trade_stats_pd.query("(Feed=='CTS' or Feed=='UTDF') and Printable=='Printable'")
trade_stats_dark_pool_only = trade_stats_pd.query("(Feed=='CTS' or Feed=='UTDF') and Printable=='Printable' and MarketParticipant=='FINRA' ")
trade_stats_dark_pool_EXC = trade_stats_pd.query("(Feed=='CTS' or Feed=='UTDF') and Printable=='Printable' and MarketParticipant!='FINRA' ")


# In[ ]:





# In[ ]:


def make_trade_agg(df_l):
    df_l_agg=df_l.groupby(['hour_est','is_trading_hours']).agg({'trade_volume_daily_mean':['sum']})
    df_l_agg.columns=['_'.join(col) for col in df_l_agg.columns.values]
    return(df_l_agg)

trade_stats_all_agg_all=make_trade_agg(trade_stats_all)
trade_stats_all_agg_dark_pool_only=make_trade_agg(trade_stats_dark_pool_only)
trade_stats_all_agg_dark_pool_EXC=make_trade_agg(trade_stats_dark_pool_EXC)

if True:
    spread_volume_pd = trade_stats_all_agg_all.join(disp_df).dropna().sort_index().reset_index()
    spread_volume_pd['hour_est'] = spread_volume_pd['hour_est'].astype(int)
    spread_volume_pd=spread_volume_pd.query('hour_est>0')
    spread_volume_pd=spread_volume_pd.set_index(['hour_est','is_trading_hours'])
    spread_volume_pd['ol_rl_pct_diff']=spread_volume_pd['bid_ask_tw_mt_oddlot_mean']-spread_volume_pd[f'bid_ask_tw_mt_roundlot_{sufx}_mean']
    spread_volume_pd['ol_rl_pct_diff_X_volume']=spread_volume_pd['ol_rl_pct_diff']*spread_volume_pd['trade_volume_daily_mean_sum']

    spread_volume_pd['ol_rl_pct_diff_X_volume'].sum()/spread_volume_pd['trade_volume_daily_mean_sum'].sum()*100
trade_stats_all_agg_all.style.format({
        'trade_volume_daily_mean_sum': '{:,.0f}'.format,
    })


# In[ ]:


spread_volume_pd.query('hour_est>8').style.format({
    'trade_volume_sum': '{:,.0f}'.format,
    'bid_ask_tw_mt_oddlot_mean': '{:,.2%}'.format,
    f'bid_ask_tw_mt_roundlot_{sufx}_mean': '{:,.2%}'.format,
    'ol_rl_pct_diff': '{:,.2%}'.format,
}).bar()


# In[ ]:


print(trade_stats_all.trade_count_daily_mean.sum()
,'\n',trade_stats_dark_pool_only.trade_count_daily_mean.sum()
,'\n',trade_stats_dark_pool_EXC.trade_count_daily_mean.sum()
     )
spread_volume_pd


# In[ ]:


def get_OL_RL_stats(df_l,col_name,col_func,fname=None):
    ts_comb=df_l.query('is_odd_lot==True').groupby(['hour_est']).agg({col_name:[col_func]}).join(
    df_l.query('is_odd_lot==False').groupby(['hour_est']).agg({col_name:[col_func]}),lsuffix='_ol', rsuffix='_rl')
    if fname==None:
        pass
    else:
        ts_comb.to_csv(fname)
    return(ts_comb)
get_OL_RL_stats(trade_stats_all,'trade_volume_daily_mean','sum',fname=f's3://maystreetdata/analysis/blog_graphs/trade_volume_all.csv')\
.plot.bar(title='Trade Volume (All)')
get_OL_RL_stats(trade_stats_dark_pool_only,'trade_volume_daily_mean','sum',fname=f's3://maystreetdata/analysis/blog_graphs/trade_volume_dark_pool_only.csv')\
.plot.bar(title='Trade Volume (Dark Pool Only)')
get_OL_RL_stats(trade_stats_dark_pool_EXC,'trade_volume_daily_mean','sum',fname=f's3://maystreetdata/analysis/blog_graphs/trade_volume_dark_pool_exclude.csv')\
.plot.bar(title='Trade Volume (Exclude Dark Pool)')


get_OL_RL_stats(trade_stats_all,'trade_count_daily_mean','sum',fname=f's3://maystreetdata/analysis/blog_graphs/trade_count_all.csv')\
.plot.bar(title='Trade Count (All)')
get_OL_RL_stats(trade_stats_dark_pool_only,'trade_count_daily_mean','sum',fname=f's3://maystreetdata/analysis/blog_graphs/trade_count_dark_pool_only.csv')\
.plot.bar(title='Trade Count (Dark Pool Only)')
get_OL_RL_stats(trade_stats_dark_pool_EXC,'trade_count_daily_mean','sum',fname=f's3://maystreetdata/analysis/blog_graphs/trade_count_dark_pool_exclude.csv')\
.plot.bar(title='Trade Count (Exclude Dark Pool)')


# total trade couint - 58024178
# 
# odd l;ot trade count - 33045020
# 
# Total FINRA trade counts - 16002382
# 
# FINRA odd lot trade counts-  8426264
# 
# 
# trade_stats_all_agg_all=make_trade_agg(trade_stats_all)
# 
# trade_stats_all_agg_dark_pool_only=make_trade_agg(trade_stats_dark_pool_only)
# 
# trade_stats_all_agg_dark_pool_EXC=make_trade_agg(trade_stats_dark_pool_EXC)

# In[ ]:





# In[ ]:


(spread_volume_pd).query('hour_est>0').groupby(['hour_est','price_bucket_mt_oddlot']).agg({'ol_rl_pct_diff':['mean']}).plot.bar()
fname=f's3://maystreetdata/analysis/blog_graphs/oddlot_roundlot(bbo)_spread_by_HOUR_PRICEBUCKET.csv'
(spread_volume_pd).query('hour_est>0').groupby(['hour_est','price_bucket_mt_oddlot']).agg({'ol_rl_pct_diff':['mean']}).to_csv(fname)


# In[ ]:


def pre_post_split(df_l,fname=None):
    df_l_agg=df_l.query("Product=='AMZN'").groupby(['before_after_split','is_odd_lot']).agg({'trade_volume_daily_mean':['mean']})
    df_l_agg_before_split=df_l_agg.query('before_after_split=="pre_split"')
    df_l_agg_after_split=df_l_agg.query('before_after_split=="post_split"')
    if fname==None:
        pass
    else:
        df_l_agg_before_split.to_csv(f'{fname}_before_split.csv')
        df_l_agg_after_split.to_csv(f'{fname}_after_split.csv')
    return((df_l_agg_after_split,df_l_agg_before_split))

trade_stats_all_B_A = pre_post_split(trade_stats_all,fname=f's3://maystreetdata/analysis/blog_graphs/all')
trade_stats_all_B_A[0].plot.pie(subplots=True,title='Trading Volume (all - post split)')#y='trade_volume')
trade_stats_all_B_A[1].plot.pie(subplots=True,title='Trading Volume (all - before split)')#y='trade_volume')

trade_stats_all_B_A = pre_post_split(trade_stats_dark_pool_only,fname=f's3://maystreetdata/analysis/blog_graphs/dark_pool_only')
trade_stats_all_B_A[0].plot.pie(subplots=True,title='Trading Volume (dark_pool_only - post split)')#y='trade_volume')
trade_stats_all_B_A[1].plot.pie(subplots=True,title='Trading Volume (dark_pool_only - before split)')#y='trade_volume')

trade_stats_all_B_A = pre_post_split(trade_stats_dark_pool_EXC,fname=f's3://maystreetdata/analysis/blog_graphs/dark_pool_EXC')
trade_stats_all_B_A[0].plot.pie(subplots=True,title='Trading Volume (dark_pool_EXC - post split)')#y='trade_volume')
trade_stats_all_B_A[1].plot.pie(subplots=True,title='Trading Volume (dark_pool_EXC - before split)')#y='trade_volume')


# In[ ]:


msg_count_dict={}


# In[ ]:


sufx="roundlot_bbo"
#s3://maystreetdata/analysis/joined_df_stats_by_symbol_mt_oddlot_mt_roundlot_bbo.parquet/
res_file_name=f'mt_{sufx}_message_count.parquet'  
res_dir=f's3://maystreetdata/analysis/'
res_file=f'{res_dir}{res_file_name}/'
print(res_file)

msg_count_dict[sufx]= pq.read_table(source=res_file).to_pandas()
#df['hour_est']=df['hour_est'].astype(float)
#cols_to_num=['diff_exchange_timestamp_sum','bidask_timeweight_mt_oddlot_sum'
#             ,f'bidask_timeweight_mt_roundlot_{sufx}_sum','percent_oddlot_is_top',f'bid_ask_tw_mt_oddlot',f'bid_ask_tw_mt_roundlot_{sufx}']
#for one_col in cols_to_num:
#    df[one_col]=df[one_col].astype(float)
#agg_pd = pd.read_parquet(res_file, engine='pyarrow')


# In[ ]:


msg_count_dict.get('roundlot_bbo')#.to_csv(f's3://maystreetdata/analysis/blog_graphs/roundlot_bbo_msg_count.csv')


# In[ ]:


msg_count_dict.get('oddlot')#.to_csv(f's3://maystreetdata/analysis/blog_graphs/oddlot_msg_count.csv')


# In[ ]:


ol_rl_pd=msg_count_dict.get('oddlot').set_index('MarketParticipant').join(msg_count_dict.get('roundlot_bbo').set_index('MarketParticipant'),how='outer')/1
ol_rl_pd['msg_icrease_pct']=ol_rl_pd['oddlot_message_count']/ol_rl_pd['message_count']
fname=f's3://maystreetdata/analysis/blog_graphs/msg_count.csv'
ol_rl_pd.to_csv(fname)
ol_rl_pd


# In[ ]:


stats_pd=pd.concat(res_pd_array)
for one_col in ['feed_oddlot','f_oddlot','is_trading_hours']:
    label_encoder = LabelEncoder()
    label_encoder.fit(stats_pd[one_col])
    stats_pd[f'{one_col}_encoded']=label_encoder.transform(stats_pd[one_col])
    
#print(stats_pd.groupby(['is_trading_hours']).sum())
stats_disp=stats_pd.sort_values(['is_trading_hours','feed_oddlot','f_oddlot','update_count_pctrank']).drop(columns=['feed_oddlot_encoded','f_oddlot_encoded','is_trading_hours_encoded']).reset_index().drop(columns=['index'])
def above_zero(val):
  try:  
    color = 'green' if val > 0 else 'red'
  except:
    color = 'grey'
  return 'color: %s' % color


# In[ ]:


one_df


# In[ ]:


stats_disp.query('update_count_pctrank>=0.6').dropna().style.applymap(above_zero).set_precision(2)


# In[ ]:


stats_disp.query('update_count_pctrank==0').dropna().style.applymap(above_zero).set_precision(2)


# In[ ]:


stats_disp.query('update_count_pctrank>=0.3 and update_count_pctrank<=0.5').dropna().style.applymap(above_zero).set_precision(2)


# In[ ]:


stats_pd.groupby(['is_trading_hours']).sum()


# In[ ]:


spread_stats_pd=pd.pivot_table(stats_pd.round(2),values=['statistic_odd_vs_roundBBO','statistic_odd_vs_roundNBBO'],columns=['is_trading_hours','hour_est']
               ,index=['feed_oddlot','f_oddlot','update_count_pctrank'])
spread_stats_pd.to_csv(f'{res_dir}spread_stats_pivot_{comp_type}.csv')
spread_stats_pd.style.applymap(above_zero).set_precision(2)


# In[ ]:


cols =['is_trading_hours','is_trading_hours_encoded','statistic_odd_vs_roundBBO','statistic_odd_vs_roundNBBO']
pd.plotting.parallel_coordinates(
    stats_pd[cols]*1, 'is_trading_hours', color=('#556270', '#4ECDC4', '#C7F464')
)


# In[ ]:


import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from sklearn.cluster import DBSCAN
import numpy as np
import pandas as pd

from itertools import chain   
from scipy import stats
from sklearn.preprocessing import LabelEncoder


# In[ ]:


spy_full_pd = pd.read_csv('s3://maystreetdata/analysis/spy_full.csv')
spy_full_pd=spy_full_pd.set_index('exchange_ts').sort_index()
spy_full_pd


# In[ ]:


spy_full_pd['Bid']=spy_full_pd['BidPrice_1'].fillna(method='ffill')
spy_full_pd['Ask']=spy_full_pd['AskPrice_1'].fillna(method='ffill')
spy_full_pd=spy_full_pd#.fillna(method='ffill')
spy_full_pd['is_bid_trade']=(spy_full_pd['Price'] <= spy_full_pd['Bid'])*-1
spy_full_pd['is_ask_trade']=(spy_full_pd['Price'] >= spy_full_pd['Ask'])*1
spy_full_pd['is_invert']=((spy_full_pd['is_bid_trade']+spy_full_pd['is_ask_trade'])!=0)*1

spy_full_pd.query(f"((is_bid_trade==-1) or (is_ask_trade==1)) and is_invert==1").groupby(['is_bid_trade','is_ask_trade','is_invert']).count()


# In[ ]:


spy_full_pd['side_w_quantity']=spy_full_pd['Quantity']*spy_full_pd['is_bid_trade']+spy_full_pd['Quantity']*spy_full_pd['is_ask_trade']
spy_full_pd['vw_price']=spy_full_pd['Quantity']*spy_full_pd['Price']*(spy_full_pd['is_bid_trade']+spy_full_pd['is_ask_trade'])
spy_full_pd['vwap']=spy_full_pd['vw_price'].rolling(10000).sum()/spy_full_pd['Quantity'].rolling(10000).sum()


# In[ ]:


spy_full_pd['vwap'].cumsum().reset_index()['vwap'].plot()


# In[ ]:


spy_full_pd.groupby(['SaleCondition','SaleCondition2','SaleCondition3','SaleCondition4']).agg({'side_w_quantity':['count','sum']})


# In[ ]:


res_file='s3://maystreetdata/analysis/Watsco.parquet/'
df = pq.read_table(source=res_file).to_pandas()
pairs_pd=df.set_index(['exchange_timestamp_mt_roundlot_bbo']).sort_index().fillna(method='ffill')
cols=['WSO_price_mean','WSO_B_price_mean']
for one_col in cols:
    pairs_pd[one_col]=pairs_pd[one_col].astype(float)


# In[ ]:


plot_pd=pairs_pd.query('WSO_B_price_mean>500')


# In[ ]:


(plot_pd['WSO_price_mean'].pct_change().cumsum()-plot_pd['WSO_B_price_mean'].pct_change().cumsum()).reset_index().plot()


# In[ ]:


pairs_pd.query('WSO_B_price_mean<500')[['WSO_price_mean','WSO_B_price_mean']].plot()


# In[ ]:


res_file='s3://maystreetdata/analysis/spy_OF_price.parquet/'
df = pq.read_table(source=res_file).to_pandas().set_index(['date_est','timestamp_ts_est']).sort_index().fillna(method='ffill')
for one_col in ['trade_volume_signed_directional_sum','traded_price_mean']:
    df[one_col]=df[one_col].astype(float)


# In[ ]:


df_study=df.query("(hour_est>=9)").query("hour_est<16")
df_study


# In[ ]:


def norm_col(df_l,col_l):
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(df_l[[col_l]].values)
    return(x_scaled)
def get_corr(df_l):
    corr_pd = pd.DataFrame(df_l.groupby(['hour_est']).apply(lambda x: np.corrcoef(x['sd_cumsum'],x['traded_price_mean'])[0][1]),columns=['corr'])
    corr_pd['date_est']=one_day
    return(corr_pd)
corr_arr=[]
for one_day in df_study.reset_index()['date_est'].drop_duplicates():
    df_study_one_day=df_study.query(f'date_est=="{one_day}"').copy(deep=True)
    df_study_one_day['sd_cumsum']=df_study_one_day['trade_volume_signed_directional_sum'].cumsum().copy(deep=True)#.rolling(5000, min_periods=1).sum().copy(deep=True)
    df_study_one_day['sd_cumsum_norm']=norm_col(df_study_one_day,'sd_cumsum')
    df_study_one_day['traded_price_mean_norm']=norm_col(df_study_one_day,'traded_price_mean')
    corr_arr=corr_arr+[get_corr(df_study_one_day)]
    df_study_one_day.reset_index().drop(columns=['date_est']).set_index('timestamp_ts_est').sort_index()[['sd_cumsum_norm','traded_price_mean_norm']].plot()
corr_pd = pd.concat(corr_arr)


# In[ ]:


corr_pd.reset_index()['corr'].hist(by=corr_pd.reset_index()['hour_est'])


# In[ ]:


corr_dict


# In[ ]:


ord_imb_file=f"s3://maystreetdata/analysis/spy_ord_imbalance_sample.parquet"
df = pq.read_table(source=ord_imb_file).to_pandas()#.set_index(['date_est','timestamp_ts_est']).sort_index().fillna(method='ffill')
#for one_col in ['trade_volume_signed_directional_sum','traded_price_mean']:
#    df[one_col]=df[one_col].astype(float)
#df.to_csv(ord_imb_file.replace('.parquet','.csv'))
df.groupby(['Product','AuctionTimestamp','AuctionType','dt']).max()['']


# In[ ]:


ord_imb_sample_file =f"s3://maystreetdata/analysis/spy_ord_imbalance_sample_last.parquet"
ord_imb_pd = pq.read_table(source=ord_imb_sample_file).to_pandas().dropna()
for one_col in ['TotalImbalance_signed','TotalImbalance_signed_pct']:
    ord_imb_pd[one_col]=ord_imb_pd[one_col].astype(float)


# In[ ]:


imbalance_stats  = pd.pivot_table(ord_imb_pd, values=['TotalImbalance_signed','TotalImbalance_signed_pct'],
                       index=['dt'],
                    columns=['AuctionType'], aggfunc=np.mean)
imbalance_stats


# In[ ]:


sym='SPY'
timeframe='ytd'
spy_ohlc = pyEX_cl.chartDF(symbol=sym, timeframe=timeframe)[['close', 'open','high','low']]


# In[ ]:


spy_ohlc


# In[ ]:


imb_price_pd = imbalance_stats.join(spy_ohlc)


# In[ ]:


imb_price_pd.to_csv('s3://maystreetdata/analysis/imb_price.csv')


# In[ ]:


of_sample= 's3://maystreetdata/analysis/spy_odd_lot_sample_OF_SAMPLE.parquet'
ord_imb_file=f"s3://maystreetdata/analysis/spy_ord_imbalance_sample.parquet"
of_sample_pd = pq.read_table(source=of_sample).to_pandas()#.set_index(['date_est','timestamp_ts_est']).sort_index().fillna(method='ffill')
of_sample_pd.to_csv('s3://maystreetdata/analysis/spy_odd_lot_sample_OF_SAMPLE.csv')


# In[ ]:


of_sample_pd['MidPrice']=((of_sample_pd['BidPrice_1']+of_sample_pd['AskPrice_1'])/2).astype(float)


# In[ ]:


#BidQuantity_1_change
col_range=[i+1 for i in range(10)]
weight_cols =[]
bid_change_w_cols=[]
ask_change_w_cols=[]
for one_col in col_range:
    weight_c =f"{one_col}_weight"
    weight_cols=weight_cols+[weight_c]
    bid_col = f"BidQuantity_{one_col}_change"
    ask_col = f"AskQuantity_{one_col}_change"
    bid_change_w_cols=bid_change_w_cols+[f"{bid_col}_weight"]
    ask_change_w_cols=ask_change_w_cols+[f"{ask_col}_weight"]
    
    of_sample_pd[weight_c]=math.sqrt(1/one_col)
    of_sample_pd[f"{bid_col}_weight"]=of_sample_pd[weight_c]*of_sample_pd[f"{bid_col}"]
    of_sample_pd[f"{ask_col}_weight"]=of_sample_pd[weight_c]*of_sample_pd[f"{ask_col}"]*-1
of_sample_pd['sum_of_weights']=of_sample_pd[weight_cols].sum(axis=1)
of_sample_pd['sum_of_bid_w']=of_sample_pd[bid_change_w_cols].sum(axis=1)
of_sample_pd['sum_of_ask_w']=of_sample_pd[ask_change_w_cols].sum(axis=1)


# In[ ]:


of_sample_pd_by_second = of_sample_pd.groupby(['date_est','time_est']).agg({'sum_of_weights':'sum','sum_of_bid_w':'sum','sum_of_ask_w':'sum','MidPrice':['median','min','max']})
of_sample_pd_by_second['bid_ask_imb']=(of_sample_pd_by_second['sum_of_ask_w']+of_sample_pd_by_second['sum_of_bid_w'])/(of_sample_pd_by_second['sum_of_ask_w'].abs()+of_sample_pd_by_second['sum_of_bid_w'].abs())
of_sample_pd_by_second


# In[ ]:


of_sample= 's3://maystreetdata/analysis/spy_odd_lot_sample_OF_by_second.parquet'
of_sample_pd_by_second = pq.read_table(source=of_sample).to_pandas().set_index(['dt','time_est']).sort_index().dropna().drop(columns=['Product']).astype(float)#.astype(float)
#of_sample_pd.to_csv('s3://maystreetdata/analysis/spy_odd_lot_sample_OF_by_second.csv')
of_sample_pd_by_second


# In[ ]:





# In[ ]:


of_sample_pd_by_second[['bid_ask_imb_mean']].quantile([0.1,0.25,0.4,0.6,0.75,0.9,0.95])


# In[ ]:


of_sample_pd_by_second[['bid_ask_imb_sum']].hist()


# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_cell_magic('configure', '-f', '{ "conf":{\n        "spark.pyspark.python": "python3"\n    ,"spark.pyspark.virtualenv.enabled": "true"\n    ,"spark.pyspark.virtualenv.type":"native"\n    ,"spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv"\n    ,"spark.sql.files.ignoreCorruptFiles":"true"\n    ,"spark.dynamicAllocation.executorIdleTimeout":"18000"\n    ,"spark.driver.memory":"12g","spark.executor.memory":"12g"\n    ,"spark.driver.cores":"3"\n    ,"spark.driver.maxResultSize":"10g"\n    ,"spark.yarn.executor.Overhead":"10g"\n    ,"livy.server.session.timeout":"10h"\n         }\n}  \n')


# In[1]:


get_ipython().run_cell_magic('configure', '-f', '{ "conf":{\n        "spark.pyspark.python": "python3"\n    ,"spark.pyspark.virtualenv.enabled": "true"\n    ,"spark.pyspark.virtualenv.type":"native"\n    ,"spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv"\n    ,"spark.sql.files.ignoreCorruptFiles":"true"\n    ,"spark.dynamicAllocation.executorIdleTimeout":"18000"\n    ,"spark.driver.memory":"8g","spark.executor.memory":"8g"\n    ,"spark.driver.cores":"3"\n    ,"spark.driver.maxResultSize":"6g"\n    ,"spark.yarn.executor.Overhead":"6g"\n    ,"livy.server.session.timeout":"10h"\n         }\n}  \n')


# https://towardsdatascience.com/basics-of-apache-spark-configuration-settings-ca4faff40d45
# https://luminousmen.com/post/spark-tips-partition-tuning
# https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-partitionby/

# In[3]:


try:
    sc.install_pypi_package("aiohttp==3.8.1")
except: 
    print(f'aiohttp is installed')
try:
    import pandas as pd
except: 
    sc.install_pypi_package("pandas==1.1.5")
    import pandas as pd
try:
    import pyarrow
except: 
    sc.install_pypi_package("pyarrow==0.14.1")
    import pyarrow 
try:
    import s3fs
except: 
    sc.install_pypi_package("s3fs")
    import s3fs 
try:
    import fsspec
except: 
    sc.install_pypi_package("fsspec")
    import fsspec 
if False:
    try:
        import matplotlib
        import matplotlib.pyplot as plt
    except: 
        sc.install_pypi_package("matplotlib")
        import matplotlib
        import matplotlib.pyplot as plt
import pyspark.sql.functions as py_f
from pyspark.sql.window import Window


# In[4]:


print(spark.version,"\n\n")
configurations = spark.sparkContext.getConf().getAll()
for conf in configurations:
    print(conf)


# We analyzed the data published over the SIP feeds (CTA : CQS / CTS and UTP : UQDF / UTDF) and the depth of the book feeds of the 16 lit venues under the Reg. NMS system to see the impact of these proposed reforms on the quality of the market. 
# 
# Definitions 
# Quoted spread = (bid - ask) / midpoint 
# Spread -  bid - ask 
# 
# Results - 
# 1)	Number of current odd - lot trades within each bucket. & number of trades in each bucket. 
#     a)	Graphic concentration 
#     b)	– look for rationale & see if it is – 
# 2)	Average round-lot and odd-lot quoted spreads across each bucket 
#     a)	matrix 
#     b)	Heat map 
#     c)	Hour of the day ? 
# 3)	Effect on market data-  
#     a)	Anticipated increase in MD volumes - counts, etc
#     b)	Number of direct feed updates where top of the book is an odd - lot
# 4)	Case study around AMZN stock split - 
#     a)	Round lot spreads for AMZN per exchange - when high priced before the split
#     b)	Odd-lot spreads for AMZN per exchange  - after the split. 
# 

# In[5]:


class MtRoundLot():
    def __init__(self,part_experiment_id):
        self.s3_dir_root="s3://maystreetdata/feeds_norm/mstnorm_parquet_0_5_0"
        self.s3_dir_root_prepped="s3://maystreetdata/feeds_norm/partition_scheme_experiments_7/mstnorm_parquet_0_5_0/"
        self.s3_dir_partition_experiments =f"s3://maystreetdata/feeds_norm/partition_scheme_experiments_{part_experiment_id}/mstnorm_parquet_0_5_0"
        self.s3_dir_root_results =f"s3://maystreetdata/analysis/"
        self.tables={"mt_roundlot_bbo":{"tables":[f"{self.s3_dir_root}/mt=bbo_quote/"]},
                     "mt_roundlot_nbbo":{"tables":[f"{self.s3_dir_root}/mt=nbbo_quote/"]},
                     "mt_oddlot":{"tables":[f"{self.s3_dir_root}/mt=aggregated_price_update/"]}
                    }
        self.tables_prepped={"mt_roundlot_bbo":{"tables":[f"{self.s3_dir_root_prepped}/mt_roundlot_bbo.parquet"]},
                     "mt_roundlot_nbbo":{"tables":[f"{self.s3_dir_root_prepped}/mt_roundlot_nbbo.parquet"]},
                     "mt_oddlot":{"tables":[f"{self.s3_dir_root_prepped}/mt_oddlot.parquet"]}
                    }
        self.raw_df ={}
        self.raw_df_prepped ={}
        self.stats_df ={}
        self.data_validation ={}
        self.column_map={}
        self.joined_df={}
        self.column_map['mt_oddlot']={
            'ask':'AskPrice_1'
            ,'bid':'BidPrice_1'
            ,'timestamp':'LastExchangeTimestamp'
            ,'seq_number':'LastSequenceNumber'
            ,'partition_by':['Product','FeedType',"Feed","dt",'f','MarketParticipant','is_trading_hours','hour_est']
        }
        self.column_map['mt_roundlot_bbo']={
            'ask':'AskPrice'
            ,'bid':'BidPrice'
            ,'timestamp':'ExchangeTimestamp'
            ,'seq_number':'SequenceNumber'
            ,'partition_by':['Product','FeedType',"Feed","dt",'f','is_trading_hours','hour_est']
        }
        self.column_map['mt_roundlot_nbbo']={
            'ask':'AskPrice'
            ,'bid':'BidPrice'
            ,'timestamp':'ExchangeTimestamp'
            ,'seq_number':'SequenceNumber'
            ,'partition_by':['Product','FeedType',"Feed","dt",'f','is_trading_hours','hour_est']
        }
        self.round_factor=0.333333333333
        print(self.s3_dir_partition_experiments)
    def set_data_prepped(self,data_label):
        col_map=self.column_map.get(data_label)
        data_files = self.tables_prepped.get(data_label).get('tables')
        data_df=None
        for one_file in data_files:
            one_data_df=spark.read.parquet(one_file)
            if data_df is None:
                data_df=one_data_df
            else:
                data_df=data_df.union(one_data_df)
        self.raw_df_prepped[f"{data_label}"]=data_df
    def set_data(self,data_label, is_raw=False):
        col_map=self.column_map.get(data_label)
        data_files = self.tables.get(data_label).get('tables')
        data_df=None
        for one_file in data_files:
            feed_filters = self.tables.get(data_label).get('feeds',None)
            path_parts= one_file.split("/")
            feed_type=path_parts[len(path_parts)-2:len(path_parts)-1][0]
            if feed_filters is not None:
                filter_string='"'+'","'.join(feed_filters)+'"'
                one_data_df = spark.read.parquet(one_file).filter(f'Feed in ({filter_string})') 
            else:
                one_data_df = spark.read.parquet(one_file)
            if 'f' not in one_data_df.columns:
                one_data_df = one_data_df.withColumn('f', py_f.col("Feed"))
            if data_df is None:
                data_df=one_data_df
            else:
                data_df=data_df.union(one_data_df)
            data_df = data_df.withColumn('FeedType', py_f.lit(feed_type))\
            .select('FeedType','Feed','f','Product',col_map['bid'],col_map['ask'],col_map['timestamp'])\
            .groupBy('FeedType','Feed','f','Product',col_map['timestamp']).agg(
                 py_f.round(py_f.max(col_map['bid']),3).alias(f'best_bid_{data_label}')
                ,py_f.round(py_f.min(col_map['ask']),3).alias(f'best_ask_{data_label}')
            ).withColumnRenamed('FeedType', f'FeedType_{data_label}')\
            .withColumnRenamed('Feed', f'Feed_{data_label}')\
            .withColumnRenamed(col_map['timestamp'], f'exchange_timestamp_{data_label}')\
            .withColumnRenamed('f', f'f_{data_label}')\
            .withColumn(f"mid_{data_label}",(py_f.col(f'best_ask_{data_label}')-py_f.col(f'best_bid_{data_label}'))/py_f.lit(2))\
            .withColumn(f"bid_ask_{data_label}",(py_f.col(f'best_ask_{data_label}')-py_f.col(f'best_bid_{data_label}'))/py_f.col(f"mid_{data_label}")) \
            .withColumn(f'timestamp_ts_utc_{data_label}',py_f.from_unixtime(py_f.col(f'exchange_timestamp_{data_label}')/1000/1000/1000))\
            .withColumn(f'timestamp_ts_est_{data_label}',py_f.from_utc_timestamp((py_f.from_unixtime(py_f.col(f'exchange_timestamp_{data_label}')/1000/1000/1000)),'America/New_York'))
        part_by = [f'FeedType_{data_label}',"Product"]
        self.raw_df[f"{data_label}"]=data_df.repartition(*part_by)
    def dv_universe(self):
        dv_key='universe_check'
        self.data_validation[dv_key]={}
        for one_key in self.raw_df.keys():
            one_df = self.raw_df.get(one_key)
            col_name = f"{one_key}_ticker_count"
            curr_count = one_df.agg(py_f.countDistinct("Product").alias(col_name)).collect()
            curr_count =    [i.__getitem__(col_name) for i in curr_count][0]
            self.data_validation[dv_key][one_key]=curr_count
    
    def dv_ts_unique(self):
        dv_key='ts_unique_check'
        self.data_validation[dv_key]={}
        for one_key in self.raw_df.keys():
            one_df = self.raw_df.get(one_key)
            col_map=self.column_map.get(one_key)
            ts_field=col_map.get('timestamp')
            seq_field=col_map.get('seq_number')
            count_alias,countDistinct_alias = f'count_{ts_field}',f'countDistinct_{ts_field}'
            uniq_ts_check = one_df.groupBy(col_map.get('partition_by')).agg(
                py_f.count(ts_field).alias(count_alias),py_f.countDistinct(ts_field,seq_field).alias(countDistinct_alias)
            ).where(f'{count_alias}>{countDistinct_alias}').count()
            self.data_validation[dv_key][one_key]=uniq_ts_check
    def set_volume_ptile(self):
        by_prod_feed=self.raw_df_prepped["mt_roundlot_nbbo"].groupBy('Product').count().orderBy('Product')
        by_prod_feed=by_prod_feed.select("Product",'count', 
            py_f.round((py_f.floor(py_f.percent_rank().over( Window.partitionBy().orderBy(by_prod_feed['count']))/py_f.lit(self.round_factor))*py_f.lit(self.round_factor)),1).alias("update_count_pctrank"))
        by_prod_feed=by_prod_feed.withColumn('volume_level',py_f.when(py_f.col('update_count_pctrank')==0.0,'low')\
                                                                                .otherwise(py_f.when(py_f.col('update_count_pctrank')==0.3,'moderate').otherwise('high'))).cache()
        #by_prod_feed.groupBy('update_count_pctrank').count()
        self.volume_rank_df = by_prod_feed

    def set_common_universe(self):
        bbo_nbbo_cols = ['Product','Feed','dt','f','bidask_spread_timew_avg','data_count','is_trading_hours','hour_est']
        df1=self.stats_df['mt_roundlot_bbo_stats_agg'].select(bbo_nbbo_cols)\
        .withColumnRenamed('bidask_spread_timew_avg',f'bidask_spread_timew_avg_bbo_roundlot').withColumnRenamed('data_count',f'data_count_bbo_roundlot')
        df2=self.stats_df['mt_roundlot_nbbo_stats_agg'].select(bbo_nbbo_cols)\
        .withColumnRenamed('bidask_spread_timew_avg',f'bidask_spread_timew_avg_nbbo_roundlot').withColumnRenamed('data_count',f'data_count_nbbo_roundlot')
        temp_df = df1.join(df2
                 ,(df1.Product==df2.Product)
                 & (df1.Feed==df2.Feed)
                 & (df1.dt==df2.dt)
                 & (df1.f==df2.f)
                 & (df1.is_trading_hours==df2.is_trading_hours)
                 & (df1.hour_est==df2.hour_est)
                ).drop(df2.Product).drop(df2.Feed).drop(df2.dt).drop(df2.f).drop(df2.is_trading_hours).drop(df2.hour_est)

        odd_lot_cols = ['Product','dt','bidask_spread_timew_avg','FeedType','Feed','f','data_count','is_trading_hours','hour_est']
        df3 = self.stats_df['mt_oddlot_stats_agg'].select(odd_lot_cols)\
                .withColumnRenamed('bidask_spread_timew_avg',f'bidask_spread_timew_avg_oddlot')\
                .withColumnRenamed('data_count',f'data_count_oddlot')\
                .withColumnRenamed('Feed',f'Feed_oddlot')\
                .withColumnRenamed('f',f'f_oddlot')
        final_df=temp_df.join(df3
                 ,(temp_df.Product==df3.Product)
                 & (temp_df.dt==df3.dt)
                 & (temp_df.is_trading_hours==df3.is_trading_hours)
                 & (temp_df.hour_est==df3.hour_est)
                             ).drop(df3.Product).drop(df3.dt).drop(temp_df.is_trading_hours).drop(temp_df.hour_est)
        volume_rank_df = self.volume_rank_df.select('Product','update_count_pctrank')
        final_df=final_df.join(volume_rank_df
                              ,(volume_rank_df.Product==final_df.Product)).drop(volume_rank_df.Product)
        self.stats_df['all_by_symbol_feed_date']=final_df.cache()
        
    def join_dfs(self,dl1,dl2):
        df1=self.raw_df_prepped[dl1]
        df2=self.raw_df_prepped[dl2]
        df1_df2=df1.join(df2, (df1.Product==df2.Product) &  (df1[f'exchange_timestamp_{dl1}']==df2[f'exchange_timestamp_{dl2}']),'inner').drop(df2.Product)
        df1_df2=df1_df2.withColumn('exchange_timestamp',py_f.when(py_f.col(f'exchange_timestamp_{dl1}').isNull(), py_f.col(f'exchange_timestamp_{dl2}')).otherwise(py_f.col(f'exchange_timestamp_{dl1}')))
        self.joined_df[f"{dl1}_{dl2}"]=df1_df2.cache()
    [
    'FeedType_mt_oddlot', 'Feed_mt_oddlot', 'f_mt_oddlot', 'Product'
    , 'exchange_timestamp_mt_oddlot', 'best_bid_mt_oddlot', 'best_ask_mt_oddlot', 'bid_ask_mt_oddlot'
    , 'timestamp_ts_utc_mt_oddlot', 'timestamp_ts_est_mt_oddlot'
    , 'FeedType_mt_roundlot_bbo', 'Feed_mt_roundlot_bbo', 'f_mt_roundlot_bbo'
    , 'exchange_timestamp_mt_roundlot_bbo', 'best_bid_mt_roundlot_bbo', 'best_ask_mt_roundlot_bbo', 'bid_ask_mt_roundlot_bbo'
    , 'timestamp_ts_utc_mt_roundlot_bbo', 'timestamp_ts_est_mt_roundlot_bbo', 'exchange_timestamp'
    ]
    def calc_timew_spread_paired(self,dl1,dl2):
        part_cols = [f'FeedType_{dl1}', f'Feed_{dl1}', f'f_{dl1}', 'Product', f'FeedType_{dl2}', f'Feed_{dl2}', f'f_{dl2}']
        joined_df=self.joined_df[f"{dl1}_{dl2}"]
        joined_df=joined_df.withColumn('time_est', py_f.date_format(f'timestamp_ts_est_{dl1}', 'HH:mm:ss'))\
                   .withColumn('hour_est', py_f.date_format(f'timestamp_ts_est_{dl1}', 'HH'))\
                    .withColumn('is_trading_hours', ((py_f.col('time_est')>=py_f.lit('09:30:00')) & (py_f.col('time_est')<=py_f.lit('15:59:00'))))
        prev_window = Window.partitionBy(*[part_cols]).orderBy(py_f.col('exchange_timestamp'))
        joined_df = joined_df.withColumn("prev_exchange_timestamp", py_f.lag(py_f.col('exchange_timestamp')).over(prev_window))
        joined_df = joined_df.withColumn("diff_exchange_timestamp",joined_df.exchange_timestamp-joined_df.prev_exchange_timestamp)
        joined_df = joined_df.withColumn(f"bidask_timeweight_{dl1}",joined_df[f'bid_ask_{dl1}']*joined_df.diff_exchange_timestamp)    
        joined_df = joined_df.withColumn(f"bidask_timeweight_{dl2}",joined_df[f'bid_ask_{dl2}']*joined_df.diff_exchange_timestamp)     
        #df_vol_rank=self.volume_rank_df.drop('update_count_pctrank').drop('count')
        #joined_df = df_vol_rank.join(py_f.broadcast(joined_df),(df_vol_rank.Product==joined_df.Product)).drop(df_vol_rank.Product)
        self.stats_df[f'joined_df_{dl1}_{dl2}']=joined_df
        joined_df_stats_by_symbol=joined_df.groupBy('Product',f'Feed_{dl2}',f'f_{dl2}','is_trading_hours','hour_est').agg(
             py_f.sum(py_f.col('diff_exchange_timestamp')).alias('diff_exchange_timestamp_sum')
            ,py_f.sum(py_f.col(f'bidask_timeweight_{dl1}')).alias(f'bidask_timeweight_{dl1}_sum')
            ,py_f.sum(py_f.col(f'bidask_timeweight_{dl2}')).alias(f'bidask_timeweight_{dl2}_sum')
        ).withColumn(f'bid_ask_tw_{dl1}',py_f.col(f'bidask_timeweight_{dl1}_sum')/py_f.col('diff_exchange_timestamp_sum'))\
        .withColumn(f'bid_ask_tw_{dl2}',py_f.col(f'bidask_timeweight_{dl2}_sum')/py_f.col('diff_exchange_timestamp_sum'))\
        .orderBy('Product',f'Feed_{dl2}',f'f_{dl2}','is_trading_hours').cache()
        self.stats_df[f'joined_df_stats_by_symbol_{dl1}_{dl2}']=joined_df_stats_by_symbol
        
        joined_df_stats_by_trading_hour=joined_df_stats_by_symbol.groupBy(f'Feed_{dl2}',f'f_{dl2}','is_trading_hours','hour_est')\
        .agg(
             py_f.mean(py_f.col(f'bid_ask_tw_{dl1}'))
            ,py_f.mean(py_f.col(f'bid_ask_tw_{dl2}'))
            ,py_f.count(py_f.col(f'bid_ask_tw_{dl1}'))
            ,py_f.count(py_f.col(f'bid_ask_tw_{dl2}'))
        )
        self.stats_df[f'joined_df_stats_by_trading_hour_{dl1}_{dl2}']=joined_df_stats_by_trading_hour
        
    def calc_timew_spread(self,data_label):
        col_map=self.column_map.get(data_label)
        l_df =  self.raw_df.get(data_label)
        l_df = l_df.withColumn('timestamp_ts_utc',py_f.from_unixtime(py_f.col(col_map.get('timestamp'))/1000/1000/1000))\
                   .withColumn('timestamp_ts_est',py_f.from_utc_timestamp((py_f.from_unixtime(py_f.col(col_map.get('timestamp'))/1000/1000/1000)),'America/New_York'))\
                   .withColumn('time_est', py_f.date_format('timestamp_ts_est', 'HH:mm:ss'))\
                   .withColumn('hour_est', py_f.date_format('timestamp_ts_est', 'HH'))\
                   .withColumn('is_trading_hours', ((py_f.col('time_est')>=py_f.lit('09:30:00'))&(py_f.col('time_est')<=py_f.lit('15:59:00'))))
        l_df = l_df.withColumn("bid_ask",(py_f.col(col_map.get('ask'))-py_f.col(col_map.get('bid')))/py_f.col(col_map.get('bid')) )
        prev_window = Window.partitionBy(*col_map.get('partition_by')).\
                        orderBy(py_f.col(col_map.get('timestamp')),py_f.col(col_map.get('seq_number')),l_df.bid_ask.desc())
        l_df = l_df.withColumn("next_LastReceiptTimestamp", py_f.lead(py_f.col(col_map.get('timestamp'))).over(prev_window))
        l_df = l_df.withColumn("diff_LastReceiptTimestamp",py_f.col(col_map.get('timestamp'))-l_df.next_LastReceiptTimestamp)
        l_df = l_df.withColumn("bidask_timeweight",l_df.bid_ask*l_df.diff_LastReceiptTimestamp)
        bid_ask_agg= l_df.where('diff_LastReceiptTimestamp is not null and bid_ask<100').groupby(*col_map.get('partition_by')).\
                agg(py_f.sum('diff_LastReceiptTimestamp').alias('time_sum'),
                    py_f.sum('bidask_timeweight').alias('bidask_timeweight_sum'),
                    py_f.count(py_f.lit(1)).alias('data_count'))
        bid_ask_agg=bid_ask_agg.withColumn("bidask_spread_timew_avg",bid_ask_agg.bidask_timeweight_sum/bid_ask_agg.time_sum)  
        self.stats_df[f"{data_label}_stats_intermediate"]=l_df
        self.stats_df[f"{data_label}_stats_agg"]=bid_ask_agg
        self.stats_df[f"{data_label}_stats_agg_final"]=bid_ask_agg.agg(py_f.mean(bid_ask_agg.bidask_spread_timew_avg).alias('bidask_mean_timew'),
                                                                       py_f.expr('percentile(bidask_spread_timew_avg, array(0.5))').alias('bidask_median_timew'),
                                                                        py_f.sum(bid_ask_agg.data_count).alias('data_count'))
            
mt_roundlot=MtRoundLot(7) 
print('mt_oddlot')
mt_roundlot.set_data("mt_oddlot")
print('mt_roundlot_bbo')
mt_roundlot.set_data("mt_roundlot_bbo")
print('mt_roundlot_nbbo')
mt_roundlot.set_data("mt_roundlot_nbbo")
#
if True:
    print('mt_oddlot_prepped')
    mt_roundlot.set_data_prepped("mt_oddlot")
    print('mt_roundlot_bbo_prepped')
    mt_roundlot.set_data_prepped("mt_roundlot_bbo")
    print('mt_roundlot_nbbo_prepped')
    mt_roundlot.set_data_prepped("mt_roundlot_nbbo")

mt_roundlot.set_volume_ptile()
mt_roundlot.join_dfs('mt_oddlot','mt_roundlot_bbo')
mt_roundlot.join_dfs('mt_oddlot','mt_roundlot_nbbo')

#if False:
#    mt_roundlot.dv_universe()
#    mt_roundlot.dv_ts_unique()

mt_roundlot.calc_timew_spread_paired("mt_oddlot","mt_roundlot_bbo")
mt_roundlot.calc_timew_spread_paired("mt_oddlot","mt_roundlot_nbbo")
#mt_roundlot.calc_timew_spread("mt_roundlot_bbo")
#mt_roundlot.calc_timew_spread("mt_roundlot_nbbo")
#mt_roundlot.set_common_universe()


# In[29]:


if False:
    for one_result in [ 'joined_df_stats_by_symbol_mt_oddlot_mt_roundlot_nbbo'
                       ,'joined_df_stats_by_symbol_mt_oddlot_mt_roundlot_bbo'
                       ,'joined_df_stats_by_trading_hour_mt_oddlot_mt_roundlot_bbo'
                       ,'joined_df_stats_by_trading_hour_mt_oddlot_mt_roundlot_bbo']:
        mt_roundlot.stats_df[one_result].write.option("header",True).mode("overwrite").parquet(f"{mt_roundlot.s3_dir_root_results}/{one_result}.parquet")
else:
    pass


# In[8]:


mt_roundlot.raw_df['mt_roundlot_bbo'].show()


# In[ ]:


mt_roundlot.joined_df['mt_oddlot_mt_roundlot_bbo'].show()


# -----------------------------------------
# -----------------------------------------
# -----------------------------------------
# the cell below:
# 1. the first If statement writes repartitioned parquet file if activated. experiment 7 is what currently used as input data
# 2. the second If, when activated, produces .csv files that feed tables and charts in the blog:
# a)ol_rl_nbbo<_pivot>.csv - odd lots vs round lots on NBBO
# b)ol_rl_bbo<_pivot>.csv - odd lots vs round lots on BBO
# 3. the rest of the data is picked up and produced by mt_roundlot_study_post_analysis.ipynb. both notebook need to be combined into singhle clean and visual notebook that runs end-to-end
# 4. graph data is available: s3://maystreetdata/analysis/blog_graphs/
# -----------------------------------------
# -----------------------------------------
# -----------------------------------------

# In[30]:


if False:
    for one_feed_type in ['mt_oddlot','mt_roundlot_bbo','mt_roundlot_nbbo']:
        print(one_feed_type)
        #part_by = [f"FeedType_{one_feed_type}","Product",f"Feed_{one_feed_type}",f'f_{one_feed_type}']
        part_by = [f'FeedType_{one_feed_type}',"Product"]
        mt_roundlot.raw_df[one_feed_type]\
                    .write\
                    .option("header",True) \
                    .partitionBy(*part_by) \
                    .mode("overwrite") \
                    .parquet(f"{mt_roundlot.s3_dir_partition_experiments}/{one_feed_type}.parquet")

if False:
    sort_val=['Product','Feed_mt_roundlot_nbbo','f_mt_roundlot_nbbo','hour_est']
    ol_nbbo_pd=mt_roundlot.stats_df[f'joined_df_stats_by_symbol_mt_oddlot_mt_roundlot_nbbo'].toPandas()\
    .drop(columns=['diff_exchange_timestamp_sum','bidask_timeweight_mt_oddlot_sum','bidask_timeweight_mt_roundlot_nbbo_sum'])
    ol_nbbo_pd['ol_rl_comp']= (ol_nbbo_pd['bid_ask_tw_mt_oddlot']-ol_nbbo_pd['bid_ask_tw_mt_roundlot_nbbo']).astype(float)
    ol_nbbo_pd['is_trading_hours']=ol_nbbo_pd['is_trading_hours']*1
    ol_nbbo_pd.sort_values(['Product','Feed_mt_roundlot_nbbo','f_mt_roundlot_nbbo','hour_est']).to_csv(f"s3://maystreetdata/analysis/{f'ol_rl_nbbo.csv'}")
    pd.pivot_table(ol_nbbo_pd,values=['ol_rl_comp'],index=['volume_level','Product','Feed_mt_roundlot_nbbo','f_mt_roundlot_nbbo'],columns=['hour_est','is_trading_hours'])\
    .to_csv(f"s3://maystreetdata/analysis/{f'ol_rl_nbbo_pivot.csv'}")

    ol_bbo_pd=mt_roundlot.stats_df[f'joined_df_stats_by_symbol_mt_oddlot_mt_roundlot_bbo'].toPandas()\
    .drop(columns=['diff_exchange_timestamp_sum','bidask_timeweight_mt_oddlot_sum','bidask_timeweight_mt_roundlot_bbo_sum'])
    ol_bbo_pd['ol_rl_comp']= (ol_bbo_pd['bid_ask_tw_mt_oddlot']-ol_bbo_pd['bid_ask_tw_mt_roundlot_bbo']).astype(float)
    ol_bbo_pd['is_trading_hours']=ol_bbo_pd['is_trading_hours']*1
    ol_bbo_pd.sort_values(['Product','Feed_mt_roundlot_bbo','f_mt_roundlot_bbo','hour_est']).to_csv(f"s3://maystreetdata/analysis/{f'ol_rl_bbo.csv'}")
    pd.pivot_table(ol_bbo_pd,values=['ol_rl_comp'],index=['volume_level','Product','Feed_mt_roundlot_bbo','f_mt_roundlot_bbo'],columns=['hour_est','is_trading_hours'])\
    .to_csv(f"s3://maystreetdata/analysis/{f'ol_rl_bbo_pivot.csv'}")


# In[31]:


if False:
    for one_lbl in ['oddlot','roundlot_bbo','roundlot_nbbo']:
        odd_lot_sample = mt_roundlot.stats_df[f"mt_{one_lbl}_stats_intermediate"].\
        where(f"is_trading_hours==True  and bid_ask>0.00")
        odd_lot_sample_t=odd_lot_sample.join(mt_roundlot.volume_rank_df,
                           (odd_lot_sample.Product==mt_roundlot.volume_rank_df.Product)).drop(mt_roundlot.volume_rank_df.Product).drop(py_f.col('count'))
        odd_lot_sample_t.where(f"update_count_pctrank >0.5 ").groupBy('hour_est')\
        .agg(py_f.mean(odd_lot_sample_t.bid_ask).alias('bidask_mean'),py_f.count(odd_lot_sample_t.update_count_pctrank).alias('data_count'))\
        .orderBy('hour_est').show(5000)


# In[32]:


if False:
    oddlot_df=mt_roundlot.raw_df["mt_oddlot"].\
    where(f"Product=='A'")\
    .select('Feed','f','Product','BidPrice_1','AskPrice_1','LastExchangeTimestamp','LastSequenceNumber')\
    .groupBy('Feed','f','Product','LastExchangeTimestamp').agg(py_f.round(py_f.max('BidPrice_1'),2).alias('best_bid_ol'),py_f.round(py_f.min('AskPrice_1'),2).alias('best_ask_ol'))\
    .withColumn('timestamp_ts_utc',py_f.from_unixtime(py_f.col('LastExchangeTimestamp')/1000/1000/1000))\
    .withColumn('timestamp_ts_est',py_f.from_utc_timestamp((py_f.from_unixtime(py_f.col('LastExchangeTimestamp')/1000/1000/1000)),'America/New_York'))\
    .where("timestamp_ts_est >= '2022-08-22 09:30:00'").orderBy('Product','LastExchangeTimestamp')
    oddlot_df.show()


# In[33]:


mt_roundlot.raw_df["mt_roundlot_bbo"].columns


# In[34]:


if False:
    roundlot_bbo_df=mt_roundlot.raw_df["mt_roundlot_bbo"].\
    where(f"Product=='A'")\
    .select('Feed','Product','BidPrice','AskPrice','ExchangeTimestamp','SequenceNumber')\
    .groupBy('Feed','Product','ExchangeTimestamp').agg(py_f.round(py_f.max('BidPrice'),2).alias('best_bid_rl'),py_f.round(py_f.min('AskPrice'),2).alias('best_ask_rl'))\
    .withColumn('timestamp_ts_utc',py_f.from_unixtime(py_f.col('ExchangeTimestamp')/1000/1000/1000))\
    .withColumn('timestamp_ts_est',py_f.from_utc_timestamp((py_f.from_unixtime(py_f.col('ExchangeTimestamp')/1000/1000/1000)),'America/New_York'))\
    .where("timestamp_ts_est >= '2022-08-22 09:30:00'")

    roundlot_bbo_df.orderBy('Product','ExchangeTimestamp').show(50)


# In[35]:


if False:
    df_ol=mt_roundlot.raw_df["mt_oddlot"].where("Product=='A'")
    df_ol.show()
    df_rl_bbo=mt_roundlot.raw_df["mt_roundlot_bbo"].where("Product=='A'")
    df_rl_bbo.show()

    round_odd_lot_df=df_ol.join(df_rl_bbo,
                        (df_ol.Product==df_rl_bbo.Product) 
                         & (df_ol.exchange_timestamp_mt_oddlot==df_rl_bbo.exchange_timestamp_mt_roundlot_bbo) ,'outer')\
    .withColumn('exchange_timestamp',py_f.when(py_f.col('exchange_timestamp_mt_oddlot').isNull(), py_f.col('exchange_timestamp_mt_roundlot_bbo')).otherwise(py_f.col('exchange_timestamp_mt_oddlot')))

    round_odd_lot_df.where("exchange_timestamp_mt_oddlot is not null and exchange_timestamp_mt_roundlot_bbo is not null ").orderBy('exchange_timestamp').agg(
        py_f.count('exchange_timestamp_mt_oddlot')
        ,py_f.count('exchange_timestamp_mt_roundlot_bbo')
        ,py_f.count('exchange_timestamp')
    )
    round_odd_lot_df\
    .where("exchange_timestamp_mt_oddlot is not null and exchange_timestamp_mt_roundlot_bbo is not null")\
    .agg(py_f.count('exchange_timestamp_mt_oddlot'),py_f.count('exchange_timestamp_mt_roundlot_bbo'),py_f.count('exchange_timestamp')).show()


# In[36]:


if False:
    lm=0.00
    l_df=mt_roundlot.stats_df['all_by_symbol_feed_date']
    l_df.where(f"Product=='ABC' and  bidask_spread_timew_avg_bbo_roundlot > {lm} ").orderBy('Product', 'dt', 'Feed', 'f','data_count_oddlot').show(500)
    l_df.select('data_count_oddlot').rdd.flatMap(lambda x: x).histogram(100) ,


# In[ ]:





# In[37]:


if False:
    agg_1=mt_roundlot.stats_df['all_by_symbol_feed_date'].groupBy('Product','is_trading_hours','Feed_oddlot','f_oddlot','update_count_pctrank','hour_est').agg(
     py_f.expr('percentile(bidask_spread_timew_avg_bbo_roundlot, 0.5)').alias('bid_ask_round_lot_bbo_median')
    ,py_f.expr('percentile(bidask_spread_timew_avg_nbbo_roundlot, 0.5)').alias('bid_ask_round_lot_nbbo_median')
    ,py_f.expr('percentile(bidask_spread_timew_avg_oddlot, 0.5)').alias('bid_ask_oddlot_median')


    ,py_f.mean('bidask_spread_timew_avg_bbo_roundlot').alias('bid_ask_round_lot_bbo_mean')
    ,py_f.mean('bidask_spread_timew_avg_nbbo_roundlot').alias('bid_ask_round_lot_nbbo_mean')
    ,py_f.mean('bidask_spread_timew_avg_oddlot').alias('bid_ask_oddlot_mean')
    ,py_f.mean('data_count_oddlot').alias('oddlot_data_count_mean')
    ).orderBy('Product','is_trading_hours')
    agg_1=agg_1\
    .withColumn("oddlot_bbo_mean",py_f.col('bid_ask_oddlot_mean')>=py_f.col('bid_ask_round_lot_bbo_mean'))\
    .withColumn("oddlot_nbbo_mean",py_f.col('bid_ask_oddlot_mean')>=py_f.col('bid_ask_round_lot_nbbo_mean'))\
    .withColumn("oddlot_bbo_median",py_f.col('bid_ask_oddlot_median')>=py_f.col('bid_ask_round_lot_bbo_median'))\
    .withColumn("oddlot_nbbo_median",py_f.col('bid_ask_oddlot_median')>=py_f.col('bid_ask_round_lot_nbbo_median'))
    #agg_1.agg(py_f.mean('bid_ask_round_lot_bbo'),py_f.mean('bid_ask_round_lot_nbbo'),py_f.mean('bid_ask_oddlot')).show()
    agg_pd=agg_1.orderBy('Product','is_trading_hours','hour_est').toPandas()
    agg_pd
    agg_pd.to_csv(f"s3://maystreetdata/analysis/{f'agg_pd.csv'}")


# In[38]:


#from scipy import stats 


# In[39]:


if False:
    for one_key in ['mt_oddlot_stats_intermediate', 'mt_oddlot_stats_agg', 'mt_oddlot_stats_agg_final'
                    , 'mt_roundlot_bbo_stats_intermediate', 'mt_roundlot_bbo_stats_agg'
                    , 'mt_roundlot_bbo_stats_agg_final', 'mt_roundlot_nbbo_stats_intermediate'
                    , 'mt_roundlot_nbbo_stats_agg', 'mt_roundlot_nbbo_stats_agg_final']:
    #for one_key in ['mt_oddlot_stats_intermediate']:
        if '_stats_intermediate' in one_key:
            mt_roundlot.stats_df.get(one_key).where(f"Product like 'AAP%' ")\
                    .repartition(1)\
                    .write\
                    .option("header",True) \
                    .partitionBy("FeedType","Product","Feed",'f',"dt")\
                    .mode("overwrite") \
                    .parquet(f"{mt_roundlot.s3_dir_root_results}/{one_key}.parquet")
        else:
            mt_roundlot.stats_df.get(one_key).repartition(1).write\
                    .option("header",True) \
                    .mode("overwrite") \
                    .parquet(f"{mt_roundlot.s3_dir_root_results}/{one_key}.parquet")

#mt_oddlot_stats_pd=mt_roundlot.stats_df.get('mt_oddlot_stats_agg').toPandas()
#mt_roundlot.stats_df.get('mt_oddlot_stats').show()


# In[40]:


if False:
    key='oddlot'
    mt_roundlot.stats_df.get(f'mt_{key}_stats_intermediate').where(f"Product=='ABC' and Feed=='XDPV2' and dt=='2022-08-22' and f=='xdp_nyse_integrated' ")\
    .toPandas().sort_values(['Product','FeedType',"Feed","dt",'f','MarketParticipant','LastExchangeTimestamp','LastSequenceNumber']).to_csv(f"s3://maystreetdata/analysis/{f'{key}_sample_pd.csv'}")


# In[41]:


if False:
    mt_roundlot.stats_df.get(f'mt_{key}_stats_intermediate').where(f"is_trading_hours=='false'")\
           .select('timestamp_ts_utc','timestamp_ts_est','time_est','is_trading_hours').show(truncate=False)


# In[42]:


if False:
    mt_roundlot.stats_df.get(f'mt_{key}_stats_intermediate').where(f"Product=='ABC' and Feed=='XDPV2' and dt=='2022-08-22' and f=='xdp_nyse_integrated' ").show()


# In[ ]:





# In[43]:


if False:
    for one_key in mt_roundlot.raw_df.keys():
    one_df = mt_roundlot.raw_df.get(one_key)
    break
    #'FirstReceiptTimestamp', 'LastReceiptTimestamp', 'FirstExchangeTimestamp', 'LastExchangeTimestamp', 'FirstExchangeSendTimestamp', 'LastExchangeSendTimestamp',
    dup_check = one_df.groupBy('Product','FeedType',"Feed","dt",'f','MarketParticipant').agg(
        py_f.count('FirstReceiptTimestamp').alias('FirstReceiptTimestamp'),py_f.countDistinct('FirstReceiptTimestamp','LastSequenceNumber').alias(f"{'FirstReceiptTimestamp'}_dist")
        ,py_f.count('LastReceiptTimestamp').alias('LastReceiptTimestamp'),py_f.countDistinct('LastReceiptTimestamp','LastSequenceNumber').alias(f"{'LastReceiptTimestamp'}_dist")
        ,py_f.count('FirstExchangeTimestamp').alias('FirstExchangeTimestamp'),py_f.countDistinct('FirstExchangeTimestamp','LastSequenceNumber').alias(f"{'FirstExchangeTimestamp'}_dist")
        ,py_f.count('LastExchangeTimestamp').alias('LastExchangeTimestamp'),py_f.countDistinct('LastExchangeTimestamp','LastSequenceNumber').alias(f"{'LastExchangeTimestamp'}_dist")
        ,py_f.count('FirstExchangeSendTimestamp').alias('FirstExchangeSendTimestamp'),py_f.countDistinct('FirstExchangeSendTimestamp','LastSequenceNumber').alias(f"{'FirstExchangeSendTimestamp'}_dist")
        ,py_f.count('LastExchangeSendTimestamp').alias('LastExchangeSendTimestamp'),py_f.countDistinct('LastExchangeSendTimestamp','LastSequenceNumber').alias(f"{'LastExchangeSendTimestamp'}_dist")
    )
    dup_check
    dup_check.where(f"LastExchangeTimestamp>LastExchangeTimestamp_dist or LastExchangeSendTimestamp>LastExchangeSendTimestamp_dist").sort('Product','FeedType',"Feed","dt",'f','MarketParticipant').show()


# In[44]:


def save_slice_to_parquet(df_l,l_fname):
    df_l.repartition(1).write\
            .option("header",True) \
            .partitionBy("FeedType","Product","Feed",'f',"dt") \
            .mode("overwrite") \
            .parquet(f"{l_fname}")
if False:

    if False:    
        for one_feed in feeds:
            one_feed_dets = list(one_feed)
            feed_type,feed,f = one_feed_dets[0],one_feed_dets[1],one_feed_dets[2]
            full_file_name = f"{mt_roundlot.s3_dir_partition_experiments}/{feed_type}.parquet"
            write_df = mt_roundlot.raw_df.get('mt_oddlot').where(f"FeedType=='{feed_type}' and Feed=='{feed}' and f=='{f}'")
            #print(f"f:{feed},count:{write_df.count()}")
            print(feed_type,feed,f,"\n\n")    
            save_slice_to_parquet(write_df,full_file_name)
    else:
        feed_type,feed,f ='f=bats_edgx', 'BatsPitch', 'BatsPitch'
        full_file_name = f"{mt_roundlot.s3_dir_partition_experiments}/{feed_type}.parquet"
        write_df = mt_roundlot.raw_df.get('mt_oddlot').where(f"FeedType=='{feed_type}' and Feed=='{feed}' and f=='{f}' and Product like 'AA%' ")
        #save_slice_to_parquet(write_df,full_file_name)
        read_df = spark.read.parquet(full_file_name)


# In[45]:


print('\n',mt_roundlot.raw_df.get('mt_roundlot_bbo').columns)
print('\n',mt_roundlot.raw_df.get('mt_roundlot_nbbo').columns)
print('\n',mt_roundlot.raw_df.get('mt_oddlot').columns)


# In[46]:


print(
    mt_roundlot.stats_df.get('mt_oddlot_stats_agg_final').show()
    ,mt_roundlot.stats_df.get('mt_roundlot_nbbo_stats_agg_final').show()
    ,mt_roundlot.stats_df.get('mt_roundlot_bbo_stats_agg_final').show()
)


# In[ ]:





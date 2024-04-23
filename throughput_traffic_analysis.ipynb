import pandas as pd
import numpy as np
from datatable import dt, fread
import matplotlib.pyplot as plt
import plotly.express as px
import zipfile

data1 = fread('D:/1. CEM/1. MPT/7. Data Request/20240422_MPT_throughputRegion/user_throughput_export_20240423-19836.csv' , fill = True, skip_to_line = 0)
df1 = dt.Frame.to_pandas(data1)
data2 = fread('D:/1. CEM/1. MPT/7. Data Request/20240422_MPT_throughputRegion/mpt.througput_region_user_19_LTE.csv' , fill = True, skip_to_line = 0)
df2 = dt.Frame.to_pandas(data2)

df = pd.concat([df1, df2], ignore_index = True)
df.replace('NULL', np.nan, inplace = True)

columns_to_float = ['dl_throughput_kbps', 'ul_throughput_kbps']
columns_to_string = ['imsi', 'msisdn','tac']
df[columns_to_float] = df[columns_to_float].astype(float)
df[columns_to_string] = df[columns_to_string].astype(str)
df['date'] = pd.to_datetime(df['date'])

bins = np.arange(0,5300,100)
bins = np.append(bins, float('inf'))
df['dl_bin'] =pd.cut(df['dl_throughput_kbps'], bins=bins)
# Group by the bins and count the number of 'imsi' for each bin
hist_data_dl = df.groupby(['date','layer2name', 'dl_bin']).agg({'traffic_MB': 'sum', 'msisdn' :'nunique'}).reset_index()

hist_data_dl.to_excel('D:/1. CEM/1. MPT/7. Data Request/20240422_MPT_throughputRegion/hist_data_dl.csv')
# %%
import pandas as pd

# %%
needed_data = pd.read_json('proj5_params.json',typ='series')

data = pd.read_csv('proj5_timeseries.csv')



# %%
data.columns = data.columns.str.lower().str.replace(r"[^a-z]", "_", regex=True)
data['date'] =pd.to_datetime(data['date'],format='mixed')
data.set_index('date',inplace = True)
data = data.asfreq(needed_data['original_frequency'])





# %%

print(data)

# %%
import pickle
with open('proj5_ex01.pkl','wb') as f:
    pickle.dump(data,f)


# %%


data2 = pd.read_pickle('proj5_ex01.pkl')

data2 = data2.asfreq(needed_data['target_frequency'])


# %%
data2

# %%
with open('proj5_ex02.pkl','wb') as f:
    pickle.dump(data2,f)

# %%
data3 =pd.read_pickle('proj5_ex01.pkl')

resampled_df = data3.resample(str(needed_data['downsample_periods']) + needed_data['downsample_units']).sum(min_count=needed_data['downsample_periods'])

resampled_df

# %%
with open('proj5_ex03.pkl','wb') as f:
    pickle.dump(resampled_df,f)

# %%
upsample_periods = 2
upsample_units = 'h'
data4 = pd.read_pickle('proj5_ex01.pkl')

upsampled_df = data4.resample(f'{upsample_periods}{upsample_units}').asfreq()
interpolated_df = upsampled_df.interpolate(method=needed_data['interpolation'], order=needed_data['interpolation_order'])
original_freq = pd.Timedelta(data4.index.freq)
upsampled_freq = pd.Timedelta(upsampled_df.index.freq)

scaling_ratio = upsampled_freq / original_freq
interpolated_df *= scaling_ratio

# %%
interpolated_df

# %%
with open('proj5_ex04.pkl','wb') as f:
    pickle.dump(interpolated_df,f)

# %%
df = pd.read_pickle("proj5_sensors.pkl")
df = df.pivot(columns='device_id', values='value')
new_index = pd.date_range(start=df.index.round("1min").min(), end=df.index.round("1min").max(), freq=str(needed_data['sensors_periods']) + str(needed_data['sensors_units']))
df = df.reindex(new_index.union(df.index))
interpolated_df = df.interpolate(method = 'linear')
result_df = interpolated_df.dropna()

# %%
print(result_df)

# %%
with open('proj5_ex05.pkl','wb') as f:
    pickle.dump(result_df,f)



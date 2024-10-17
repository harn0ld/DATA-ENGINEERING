# %%
import pandas as pd
import numpy as np


# %%
#zad1
df1 = pd.read_json('proj3_data1.json')
df2 = pd.read_json('proj3_data2.json')
df3 = pd.read_json('proj3_data3.json')
df = pd.concat([df1,df2,df3])
df.reset_index(inplace=True,drop=True)
df.duplicated(keep=False)
df.to_json('proj3_ex01_all_data.json')
df_copy = df.copy()

print(df)


# %%
#zad2
for column in df.columns:
    missing = df[column].isna().sum()
    if missing >0:
        new_column_name = f"{column},{missing}"
        df.rename(columns={column: new_column_name}, inplace=True)
print(df)
df.to_csv('proj3_ex02_no_nulls.csv')

# %%
#zad3
df_copy['description'] = None
df_copy
concat_columns = ['make', 'model', 'engine']
df_copy['description'] = df_copy[concat_columns].apply(lambda x: ' '.join(x), axis=1)
df_copy.to_json('proj3_ex03_descriptions.json')

        

# %%
#zad4
zad4 = pd.read_json('proj3_more_data.json')
join_column = 'engine'
merged_df = df_copy.merge(zad4, on=join_column, how='left')
print(merged_df)
merged_df_copy = merged_df.copy()
merged_df.to_json('proj3_ex04_joined.json')

# %%
#zad5
def convert(description):
    return description.lower().replace(' ', '_')
for index,row in merged_df.iterrows():
    des = convert(row['description'])
    row_without_des = row.drop('description')
    int_columns= [
        "doors",
        "displacement",
        "horsepower",
        "cylinders"
    ]
    for column in int_columns:
        if(pd.isna(row_without_des[column])):
            continue
        
        row_without_des[column] = int(row_without_des[column])
    row_without_des = row_without_des.where(pd.notnull(row_without_des), None)
    print(row_without_des)
    row_without_des.to_json(f'proj3_ex05_{des}.json')

# %%
#zad6
import json
aggregations = [
    ('displacement', 'min'),
    ('displacement', 'max'),
    ('fuel_consumption', 'mean')
]
aggregated_dict = {}
for column, func in aggregations:
    if func == 'min':
        aggregated_dict[f"{func}_{column}"] = merged_df_copy[column].min()
    elif func == 'max':
        aggregated_dict[f"{func}_{column}"] = merged_df_copy[column].max()
    elif func == 'mean':
        aggregated_dict[f"{func}_{column}"] = merged_df_copy[column].mean()
print(aggregated_dict)
with open("proj3_ex06_aggregations.json", 'w') as json_file:
    json.dump(aggregated_dict, json_file)

# %%
#zad7
grouping_column = 'make'
grouped = merged_df_copy.groupby(grouping_column)

# Filter out groups containing only one record and calculate the mean values for all numerical columns within each group
#mean_values = grouped.filter(lambda x: len(x) > 1).groupby(grouping_column).mean()

# Reset index to include grouping_column as a regular column
#mean_values.reset_index(inplace=True)

#print(mean_values)

# %%
pivot_index= "make"
pivot_columns= "fuel_type"
pivot_values= "fuel_consumption"

pivot_df = merged_df.pivot_table(index='make', columns='fuel_type', values='fuel_consumption', aggfunc='max')
print(pivot_df.columns)

pivot_df.to_pickle('proj3_ex08_pivot.pkl')


print(pivot_df)

# %%
id_vars = ['make', 'model']

# Melt the DataFrame to convert it from wide to long format
melted_df = df.melt(id_vars=id_vars, var_name='variable', value_name='value')
print(melted_df)

# Save the result to proj3_ex08_melt.csv
melted_df.to_csv("proj3_ex08_melt.csv", index=False)

# %%
df_ = pd.read_csv("proj3_statistics.csv")


melted_df = df_.melt(id_vars='Country', var_name='variable', value_name='value')


melted_df[['make', 'year']] = melted_df['variable'].str.split('_', expand=True)

pivot_df = melted_df.pivot_table(index=['Country', 'year'], columns='make', values='value')

pivot_df.reset_index(inplace=True)


pivot_df.index = list(zip(pivot_df['Country'], pivot_df['year']))

pivot_df.index = list(zip(pivot_df['Country'].apply(lambda x: f"'{x}'"), pivot_df['year']))
pivot_df.drop(['Country', 'year'], axis=1, inplace=True)


pivot_df.to_pickle('proj3_ex08_stats.pkl')
print(pivot_df)



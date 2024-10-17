# %%
import pandas as pd
import numpy as np
import pickle

# %%

possible_separators = [';', ',','|']
possible_decimal = [".",',']


for separator in possible_separators:
    for decimal in possible_decimal:
        try:
            print(separator,decimal)
            if decimal == separator:
                continue
            df = pd.read_csv("proj2_data.csv", sep=separator,decimal=decimal)
            if df.select_dtypes(include='float').shape[1] > 0:
                print(f"Successfully read the file with separator: {separator}")
                break
        except Exception as e:
            print(f"Failed to read the file with separator {separator}: {e}")
    if df.select_dtypes(include='float').shape[1] > 0:
        print(f"Successfully read the file with separator: {separator}")
        break
#df = pd.read_csv("proj2_data.csv", sep='|',decimal=',')

# %%
df.to_pickle('dane.pkl')

# %%
df['full_name']

# %%
with open('proj2_ex01.pkl','wb') as f:
    pickle.dump(df,f)

# %%
with open("proj2_scale.txt", "r") as file:
    scale_values = [line.strip() for line in file.readlines()]

# %%


df2 = pd.read_pickle('dane.pkl')

# %%
df2

# %%
changed_columns =[]
for column in df2.columns:

    if set(df2[column]).issubset(scale_values):

        df2[column] = df2[column].map({value: i+1 for i, value in enumerate(scale_values)})
        changed_columns.append(column)

# %%
df2

# %%
with open('proj2_ex02.pkl','wb') as f:
    pickle.dump(df2,f)

# %%
df4 = pd.read_pickle('dane.pkl')

# %%
for column in changed_columns:
    df4[column] = pd.Categorical(df4[column], categories=scale_values, ordered=False)

# %%
df4.dtypes

# %%
with open('proj2_ex03.pkl','wb') as f:
    pickle.dump(df4,f)

# %%
df

# %%
i=0
df5 = pd.DataFrame()
for column in df.select_dtypes(exclude=['number']):
    x = df[column].str.extract(r'([-+]?\d*[\.,]?\d+)')

    if not x[0].isna().all():
        x = x.iloc[:, 0]
        df5[column] = x
        i=i+1



# %%
df5

# %%
with open('proj2_ex04.pkl','wb') as f:
    pickle.dump(df5,f)

# %%
df6 = pd.read_pickle('dane.pkl')
df2
df6


# %%
for column in df6.select_dtypes(include=['object']): 
    uniq_val = df6[column].nunique()
    print(uniq_val)

# %%
i=1
for column in df6.select_dtypes(include=['object']): 
    uniq_val = df6[column].nunique()
    if uniq_val <= 10:

        if df6[column].str.match(r'^[a-z]+$').all() and not set(df6[column]).issubset(scale_values):

            encoded_df = pd.get_dummies(df6[column])
            print(encoded_df)

            output_file = f'proj2_ex05_{i}.pkl'
            with open(output_file,'wb') as f:
                pickle.dump(encoded_df,f)
            i =i +1



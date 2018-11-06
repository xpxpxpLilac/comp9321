import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#dataset1 -> summer
#dataset2 -> winter


def question_1_helper():
    dataset1 = pd.read_csv('Olympics_dataset1.csv', skiprows=1)
    dataset1.rename(columns={'Unnamed: 0':'Country'}, inplace = True)
    dataset2 = pd.read_csv('Olympics_dataset2.csv', skiprows=1)
    dataset2.rename(columns={'Unnamed: 0':'Country'}, inplace = True)
    full_db = pd.merge(dataset1, dataset2, how="left", left_on="Country", right_on="Country")

    return full_db

def question_1():
    pd.set_option('display.max_columns',500)
    df = question_1_helper()
    print(df.head())
    

def question_2_helper():
    df = question_1_helper()
    df = df.set_index('Country')
    
    return df

def question_2():
    df = question_2_helper()
    print(df.head(1))

def question_3_helper():
    df = question_2_helper()
    df = df.drop(columns=['Rubish'])

    return df

def question_3():
    df = question_3_helper()
    print(df.head())

def question_4_helper():
    df = question_3_helper()
    df = df.dropna()
    return df

def question_4():
    df = question_4_helper()
    print(df.tail(10))

def question_5():
    dataset1 = pd.read_csv('Olympics_dataset1.csv', skiprows=1)
    df = dataset1.set_index('Unnamed: 0')
    df = df.dropna()
    df = df.drop(df.index[-1])
    df = df.apply(pd.to_numeric, errors='ignore')
    df["Gold"] = df["Gold"].str.replace(",","").astype(int)
    print(df['Gold'].idxmax())

def get_clean_dataset1():
    dataset1 = pd.read_csv('Olympics_dataset1.csv', skiprows=1)
    dataset1.rename(columns={'Unnamed: 0':'Country'}, inplace = True)
    df = dataset1.set_index('Country')
    df = df.dropna()
    df = df.drop(df.index[-1])
    df = df.apply(pd.to_numeric, errors='ignore')
    df["Gold"] = df["Gold"].str.replace(",","").astype(int)
    df["Total"] = df["Total"].str.replace(",","").astype(int)

    return df

def get_clean_dataset2():
    dataset2 = pd.read_csv('Olympics_dataset2.csv', skiprows=1)
    dataset2.rename(columns={'Unnamed: 0':'Country'}, inplace = True)
    df = dataset2.set_index('Country')
    df = df.dropna()
    df = df.drop(df.index[-1])
    df = df.apply(pd.to_numeric, errors='ignore')
    df["Gold.1"] = df["Gold.1"].str.replace(",","").astype(int)
    df["Total.1"] = df["Total.1"].str.replace(",","").astype(int)

    return df

def question_6():
    df = get_clean_dataset2()
    df['Diff'] = df['Total.1'] - 2 * df['Total']
    df['Diff'] = df['Diff'].abs()
    print(df['Diff'].idxmax())

def question_7():
    df = get_clean_dataset2()
    df = df.sort_values(by='Total.1', ascending=False)
    frame = df[['Total.1']]
    frame.rename(columns={'Total.1':'Total'}, inplace = True)
    print("===============  head 5 rows =================\n")
    print(frame.head())
    print("===============  tail 5 rows =================\n")
    print(frame.tail())

def question_8():
    df1 = get_clean_dataset1()
    df2 = get_clean_dataset2()
    df = pd.merge(df1, df2, how="left", left_on="Country", right_on="Country")
    df = df.sort_values(by='Total.1', ascending=False)
    df = df[['Total_x','Total_y']]
    df = df.head(10)
    df.rename(columns={'Total_x':'Summer'}, inplace = True)
    df.rename(columns={'Total_y':'Winter'}, inplace = True)
    df.plot.barh(stacked=True)
    plt.show()

def question_9():
    df = get_clean_dataset2()
    df.index = df.index.to_series().astype(str).str.replace(r' \(.*','')
    df.index = df.index.to_series().astype(str).str.replace(r'^ *','')
    df = df.loc[['United States', 'Australia', 'Great Britain', 'Japan', 'New Zealand']]
    df = df[['Gold', 'Silver', 'Bronze']]
    df = df.plot.bar(rot=0)
    plt.show()

if __name__ == "__main__":
    question_1()
    question_2()
    question_3()
    question_4()
    question_5()
    question_6()
    question_7()
    question_8()
    question_9()

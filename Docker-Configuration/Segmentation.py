import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import pyodbc
import datetime as dt
import lifetimes
######################################################################################


# # Define your connection string
# conn_str = 'DRIVER={SQL Server};' \
#            'SERVER= 172.17.208.1;' \
#            'DATABASE=ecommerce_dw;' \
#            'UID=sa;' \
#            'PWD=CJM2023@'

# # Establish a connection to the database
# conn = pyodbc.connect(conn_str)
###########################################################################
print(pyodbc.drivers())
cnxn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}', host='sql-server', database='ecommerce_dw', user='sa', password='CJM2023@')
############################################################################## 
# # Defining the connection string
# conn = pyodbc.connect('''DRIVER={SQL Server}; Server=127.0.0.1; 
#                         UID=sa; PWD=CJM2023@; DataBase=PROD_AZ''')
 
# # Fetching the data from the selected table using SQL query
df= pd.read_sql_query('''select timeINFO.[full_date] as event_time ,trans.event_type,trans.user_session,trans.revenue AS price,
cust.[customer_id] AS user_id,cust.start_date, 
camp.campion_1,camp.campion_2,camp.campion_3,camp.campion_4,camp.campion_5,camp.used_discount
from [dbo].[transaction_fact] AS trans
left join [dbo].Campion_fact AS camp on trans.transaction_id = camp.transaction_id
left join [dbo].[customer_info_dimension] AS cust on trans.[customer_id] = cust.[customer_id]
left join [dbo].[time_dimension] AS timeINFO on trans.time_id = timeINFO.time_id''', cnxn)
##############################################################################

# df = pd.read_csv('data_with_cmp1.csv',usecols=['event_time','event_type','price','user_id','user_session','campion_1','campion_2','campion_3',
# 'campion_4','campion_5','start_date','used_discount']) #,sep='\t'


############################################################################## 
### Data Prepration 

df['Dt_Customer'] = pd.to_datetime(df['start_date'])
data=df.loc[df.event_type == 'purchase']

# %%time
data['event_time']=pd.to_datetime(data['event_time'])
############################################################################## 


grouped_data=data.groupby(by='user_session').agg(Date_order=('event_time','max'),
                                        user_id=('user_id','unique'),
                                        Quantity=('user_session','count'),
                                        money_spent=('price','sum'),
                                        campion_1=('campion_1','max'),
                                        campion_2=('campion_2','max'),
                                        campion_3=('campion_3','max'),
                                        campion_4=('campion_4','max'),
                                        campion_5=('campion_5','max'),
                                        Dt_Customer=('Dt_Customer','min'),
                                        used_discount=('used_discount','sum')).reset_index(drop=True)
grouped_data['user_id'] = grouped_data['user_id'].astype('int')

grouped_data['Date_order'].max()

study_date = dt.datetime(2019,11,1)
grouped_data=pd.DataFrame(grouped_data)
grouped_data['last_purchase']=study_date - grouped_data['Date_order']
grouped_data['last_purchase'].dt.days
grouped_data['last_purchase']=grouped_data['last_purchase'] / np.timedelta64(1, 'D')

df_RFM= grouped_data.groupby('user_id').agg(Recency=('last_purchase','min'),
                                 Frequency=('user_id','count'),
                                 Monetary=('money_spent','sum'))
############################################################################## 

# RFM Customer Segmentation 
quintiles = df_RFM[['Recency', 'Frequency', 'Monetary']].quantile([.2, .4, .6, .8]).to_dict()

def r_score(x):
    if x <= quintiles['Recency'][.2]:
        return 5
    elif x <= quintiles['Recency'][.4]:
        return 4
    elif x <= quintiles['Recency'][.6]:
        return 3
    elif x <= quintiles['Recency'][.8]:
        return 2
    else:
        return 1

def fm_score(x, c):
    if x <= quintiles[c][.2]:
        return 1
    elif x <= quintiles[c][.4]:
        return 2
    elif x <= quintiles[c][.6]:
        return 3
    elif x <= quintiles[c][.8]:
        return 4
    else:
        return 5  
    
df_RFM['R'] = df_RFM['Recency'].apply(lambda x: r_score(x))
df_RFM['F'] = df_RFM['Frequency'].apply(lambda x: fm_score(x, 'Frequency'))
df_RFM['M'] = df_RFM['Monetary'].apply(lambda x: fm_score(x, 'Monetary'))

df_RFM['RFM Score'] = df_RFM['R'].map(str) + df_RFM['F'].map(str) + df_RFM['M'].map(str)

segt_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at risk',
    r'[1-2]5': 'can\'t loose',
    r'3[1-2]': 'about to sleep',
    r'33': 'need attention',
    r'[3-4][4-5]': 'loyal customers',
    r'41': 'promising',
    r'51': 'new customers',
    r'[4-5][2-3]': 'potential loyalists',
    r'5[4-5]': 'champions'
}

df_RFM['Segment'] = df_RFM['R'].map(str) + df_RFM['F'].map(str)
df_RFM['Segment'] = df_RFM['Segment'].replace(segt_map, regex=True)
############################################################################## 

df_RFM_score = grouped_data.merge(df_RFM,how='left',on='user_id')
df_RFM_score['NumDealsPurchases'] =  df_RFM_score['used_discount'].mask(df_RFM_score['used_discount']>5,5)



############################################################################## 

# Estimating Customer lifetime Value using Gamma-Gamma Model 
df_RFM_score['T'] = ( df_RFM_score['Dt_Customer'].max()- df_RFM_score['Dt_Customer']).dt.days +90


def CustomerLifeTimeValue(df,penalizer_coef=0.01,months_to_predict=3, discount_rate=0.01):
    '''
    args = df must have Frequency(count), Recency(days), Monetary($) and T (Transation period in days)
    output = [['ID','predicted_clv','manual_predicted_clv']]
    '''

# Discount rate converts future cash flows (that is revenue/profits) into today’s money for the firm
# discount_rate=0.01 ----> monthly discount rate ~ 12.7% annually  


    #       Filter out customer those who have never visited again 
    df = df[df['Frequency']>=1]
    bgf = lifetimes.BetaGeoFitter(penalizer_coef=penalizer_coef)
    bgf.fit(df['Frequency'], df['Recency'], df['T'])

    # Compute the customer alive probability
    df['probability_alive'] = bgf.conditional_probability_alive(df['Frequency'], df['Recency'], df['T'])

    # Predict future transaction for the next 90 (months_to_predict*30) days based on historical data
    transaction_date = months_to_predict*30
    df['pred_num_txn'] = round(bgf.conditional_expected_number_of_purchases_up_to_time(transaction_date, 
                                                                                       df['Frequency'],
                                                                                       df['Recency'],
                                                                                       df['T']),2)


    df_repeated_customers = df.copy()
    # Modeling the monetary value using Gamma-Gamma Model from Lifetimes python library 
    ggf = lifetimes.GammaGammaFitter(penalizer_coef=penalizer_coef)
    ggf.fit(df_repeated_customers['Frequency'],
     df_repeated_customers['Monetary'])

    df_repeated_customers['exp_avg_sales'] = ggf.conditional_expected_average_profit(df_repeated_customers['Frequency'],
                                     df_repeated_customers['Monetary'])

    # predicted_clv --> predicted_annual_lifetime_value
    # Predicting Customer Lifetime Value for the next 3 months
    df_repeated_customers['predicted_clv'] = ggf.customer_lifetime_value(bgf,
                                     df_repeated_customers['Frequency'],
                                     df_repeated_customers['Recency'],
                                     df_repeated_customers['T'],
                                     df_repeated_customers['Monetary'],
                                     time=months_to_predict,     # lifetime in months
                                     freq='D',   # frequency in which the data is present(transaction_date)      
                                     discount_rate=discount_rate) # discount rate

    # Manual predict clv = Predicted no. of transactions * Expected avg sales 
    df_repeated_customers['manual_predicted_clv'] = (df_repeated_customers['pred_num_txn'] *
                                                     df_repeated_customers['exp_avg_sales'])

#     if the clv is nan impute with mean
#     df_repeated_customers['predicted_clv'].fillna(df_repeated_customers['predicted_clv'].mean(), inplace=True)
#     df_repeated_customers['manual_predicted_clv'].fillna(df_repeated_customers['manual_predicted_clv'].mean(), inplace=True)
    df_repeated_customers = df_repeated_customers.round(2)

    return df_repeated_customers[['user_id','predicted_clv','manual_predicted_clv']]


clv = CustomerLifeTimeValue(df_RFM_score,months_to_predict=3)

clv['clv_label'] = clv['predicted_clv']>0
clv['clv_label'] = clv['clv_label'].map({True: 'profitable', False: 'Non-profitable'})

# LeftMerge CLV on main df
df_RFM_score = df_RFM_score.merge(clv,how='left', on = 'user_id')


################################################################################

# Assuming your DataFrame is called 'df'



# Create a cursor from the connection
cursor = cnxn.cursor()

# Iterate over the rows of the DataFrame and insert them into the database
for index, row in df_RFM_score.iterrows():
    cursor.execute('''
        INSERT INTO [dbo].SegmebtedData (Date_order, [customer_id], Quantity, money_spent, campion_1, campion_2, campion_3, campion_4, campion_5, 
                                     Dt_Customer, used_discount, last_purchase, Recency, Frequency, Monetary, R, F, M, RFM_Score, Segment, 
                                     NumDealsPurchases, T, predicted_clv, manual_predicted_clv, clv_label)
        VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        row['Date_order'], row['user_id'], row['Quantity'], row['money_spent'], row['campion_1'], row['campion_2'], row['campion_3'], row['campion_4'],
        row['campion_5'], row['Dt_Customer'], row['used_discount'], row['last_purchase'], row['Recency'], row['Frequency'], row['Monetary'],
        row['R'], row['F'], row['M'], row['RFM Score'], row['Segment'], row['NumDealsPurchases'], row['T'], row['predicted_clv'],
        row['manual_predicted_clv'], row['clv_label']
    )

# Commit the changes and close the connection
cnxn.commit()
cnxn.close()

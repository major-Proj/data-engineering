import pandas as pd

def model(dbt, session):
    df = dbt.ref("timesheets").to_pandas()

    total_value_df = df.drop(columns=['TOTAL'])
    total_value_df.set_index('EMAIL', inplace=True)
    unique_emails = total_value_df.index.get_level_values('EMAIL').unique()

    dfs = []
    for email in unique_emails:
        total_value_df_email = total_value_df.loc[email]
        new_row_data = {'EMAIL': [email]}
        for index, row in total_value_df_email.iterrows():
            original_date = pd.to_datetime(row['START_DATE'])
            new_dates = [str(original_date + pd.DateOffset(days=i)) for i in range(7)]  # Convert timestamp to string
            day = 0
            row.drop(['START_DATE', 'END_DATE', 'NAME'], inplace=True)
            for idx, val in row.items():
                new_row_data[new_dates[day]] = [val]
                day += 1
            
        df_total = pd.DataFrame(new_row_data, index=[0])
        dfs.append(df_total)

    master_df = pd.concat(dfs, ignore_index=True)
    
    return master_df

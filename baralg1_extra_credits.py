import pandas as pd
import numpy as np
import requests as req

def gather_data1(): 
    link2 = "https://data.cityofnewyork.us/api/views/rc75-m7u3/rows.csv?accessType=DOWNLOAD" #first file
    response2 = req.get(link2)
    if response2.status_code == 200:
        file_path = "COVID-19_Daily_Counts_of_Cases__Hospitalizations__and_Deaths.csv"
        with open(file_path, 'wb') as file:
            file.write(response2.content) #saving the first data file as a csv file

def gather_data2(): 
    link = "https://data.cdc.gov/api/views/hk9y-quqm/rows.csv?accessType=DOWNLOAD" #second file
    response = req.get(link)
    if response.status_code == 200:
        file_path = "Conditions_Contributing_to_COVID-19_Deaths__by_State_and_Age__Provisional_2020-2023.csv"
        with open(file_path, 'wb') as file:
            file.write(response.content) #saving the second data file as a csv file

def gather_data3():
    link = "https://healthdata.gov/api/views/g62h-syeh/rows.csv?accessType=DOWNLOAD"#third file
    response = req.get(link)
    if response.status_code == 200:
        file_path = "COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_State_Timeseries__RAW_.csv"
        with open(file_path, 'wb') as file:
            file.write(response.content)#saving the third data file as a csv file
    

def gather_data4(): 
    link3 = "https://data.cdc.gov/api/views/tpcp-uiv5/rows.csv?accessType=DOWNLOAD" #fourth file
    response3 = req.get(link3)
    if response3.status_code == 200:
        file_path = "Provisional_COVID-19_Deaths_by_HHS_Region__Race__and_Age.csv"
        with open(file_path, 'wb') as file:
            file.write(response3.content)#saving the fourth data file as a csv file

def gather_data5(): 
    link3 = "https://data.cdc.gov/api/views/9dzk-mvmi/rows.csv?accessType=DOWNLOAD" #fifth file
    response3 = req.get(link3)
    if response3.status_code == 200:
        file_path = "Monthly Provisional Counts of Deaths by Select Causes, 2020-2023.csv"
        with open(file_path, 'wb') as file:
            file.write(response3.content)#saving the fifth data file as a csv file
            
    

def first_file():  # Function to download and process COVID-19 data.
    url = "COVID-19_Daily_Counts_of_Cases__Hospitalizations__and_Deaths.csv"  # Data source: data.gov
    df = pd.read_csv(url)  # Read CSV file into a pandas DataFrame
    df['date_of_interest'] = pd.to_datetime(df['date_of_interest'])  # Convert 'date_of_interest' to datetime format
    df.sort_values(by='date_of_interest', inplace=True)  # Sort DataFrame by date
    df['year_month'] = df['date_of_interest'].dt.to_period('M')  # Extract year and month, store in a new column
    df.drop(columns=['date_of_interest'], inplace=True)  # Drop the original date column
    df_monthly = df.groupby('year_month').sum().reset_index()  # Group by month and sum the values
    df_monthly['year_month'] = df_monthly['year_month'].astype(str)  # Convert object to string
    df_monthly[['year', 'month']] = df_monthly['year_month'].str.split('-', expand=True)  # Split year and month
    df_monthly.drop(columns=['year_month', 'INCOMPLETE'], inplace=True)  # Drop unnecessary columns
    df_monthly = df_monthly.drop(0)  # Drop the row with index 0
    df_monthly = df_monthly.reset_index(drop=True)  # Reset the index
    df_monthly['year'] = df_monthly['year'].astype(int)  # Convert 'year' to integer
    df_monthly['month'] = df_monthly['month'].astype(int)  # Convert 'month' to integer
    df_monthly.rename(columns={'year': 'Year', 'month': 'Month'}, inplace=True)  # Rename columns
    return df_monthly  # Return the processed DataFrame with monthly data  # (39 values)


def second_file():  # Function to download and process COVID-19 mortality data.
    df2 = pd.read_csv("Conditions_Contributing_to_COVID-19_Deaths__by_State_and_Age__Provisional_2020-2023.csv")  # Data source: data.gov
    filtered_df = df2[(df2["Group"] == "By Month") & (df2["Age Group"] == "All Ages") & (df2["State"] == "United States") & (df2["Condition"] == "COVID-19")].copy()  # Filter relevant data
    filtered_df.loc[:, 'End Date'] = pd.to_datetime(filtered_df['End Date'])  # Convert 'End Date' to datetime
    filtered_df.drop(columns=['Data As Of', 'Flag', 'Start Date', "End Date", "Group", "Condition Group", "Condition", "State", "Number of Mentions"], inplace=True)  # Drop unnecessary columns
    filtered_df = filtered_df.reset_index(drop=True)  # Reset the index
    filtered_df = filtered_df.drop([0, 1])  # Drop rows with index 0 and 1
    filtered_df = filtered_df.reset_index(drop=True)  # Reset the index again
    filtered_df['Year'] = filtered_df['Year'].astype(int)  # Convert 'Year' to integer
    filtered_df['Month'] = filtered_df['Month'].astype(int)  # Convert 'Month' to integer
    filtered_df = filtered_df[(filtered_df['Year'] < 2023) | ((filtered_df['Year'] == 2023) & (filtered_df['Month'] <= 5))]  # Filter data for the specified time range
    return filtered_df  # Return the processed DataFrame with mortality data  # (39 values)


def third_file():  # Function to download and process COVID-19 hospital data.
    df = pd.read_csv("COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_State_Timeseries__RAW_.csv")  # Data source: healthdata.gov
    df['date'] = pd.to_datetime(df['date'])  # Convert 'date' to datetime format
    df['Month'] = df['date'].dt.month  # Extract month and store in a new column
    df['Year'] = df['date'].dt.year  # Extract year and store in a new column
    grouped_df = df.groupby(['Year', 'Month'])  # Group by year and month
    total_coverage_by_month = grouped_df[['inpatient_beds_used_covid', 'total_staffed_pediatric_icu_beds', 'all_pediatric_inpatient_beds', 'all_pediatric_inpatient_bed_occupied']].sum().reset_index()  # Sum relevant columns
    total_coverage_by_month = total_coverage_by_month.drop([0, 1])  # Drop rows with index 0 and 1
    total_coverage_by_month = total_coverage_by_month.reset_index()  # Reset the index
    total_coverage_by_month = total_coverage_by_month[(total_coverage_by_month['Year'] < 2023) | ((total_coverage_by_month['Year'] == 2023) & (total_coverage_by_month['Month'] <= 5))]  # Filter data for the specified time range
    total_coverage_by_month.drop(columns=['index'], inplace=True)  # Drop the 'index' column
    return total_coverage_by_month  # Return the processed DataFrame with hospital data  # (39 values)


def fourth_file():  # Function to download and process provisional COVID-19 deaths data.
    url = "Provisional_COVID-19_Deaths_by_HHS_Region__Race__and_Age.csv"  # Data source: data.gov
    df = pd.read_csv(url, low_memory=False)  # Read CSV file into a pandas DataFrame
    df.drop(columns=['Data As Of', "Age Group", "Footnote"], inplace=True)  # Drop unnecessary columns
    df = df[(df["Group"] == "By Month")]  # Filter data for 'By Month' group
    df['Start Date'] = pd.to_datetime(df['Start Date'])  # Convert 'Start Date' to datetime format
    df['End Date'] = pd.to_datetime(df['End Date'])  # Convert 'End Date' to datetime format
    df['Year'] = df['Start Date'].dt.year  # Extract year from 'Start Date'
    df['Month'] = df['Start Date'].dt.month  # Extract month from 'Start Date'
    columns = ['Year', 'Month', 'Race and Hispanic Origin Group', "COVID-19 Deaths"]
    grouped_df = df[columns]  # Select relevant columns
    grouped_df = grouped_df.dropna(subset=['COVID-19 Deaths'])  # Drop rows with NaN values in 'COVID-19 Deaths'
    grouped_df = grouped_df.dropna()  # Drop any remaining rows with NaN values
    grouped_df = grouped_df[grouped_df['COVID-19 Deaths'] > 0].reset_index()  # Drop rows with 'COVID-19 Deaths' less than or equal to 0
    grouped_sum = df.groupby(['Year', 'Month', 'Race and Hispanic Origin Group'])['COVID-19 Deaths'].sum().reset_index()  # Group by year, month, and race, summing COVID-19 deaths
    grouped_pivot = grouped_sum.pivot_table(index=['Year', 'Month'], columns='Race and Hispanic Origin Group', values='COVID-19 Deaths', fill_value=0).reset_index()  # Pivot the table
    grouped_pivot = grouped_pivot.drop([0, 1])  # Drop rows with index 0 and 1
    filtered_df = grouped_pivot[(grouped_pivot['Year'] < 2023) | ((grouped_pivot['Year'] == 2023) & (grouped_pivot['Month'] <= 5))]  # Filter data for the specified time range
    filtered_df = filtered_df.reset_index()  # Reset the index
    filtered_df.drop(columns=['index'], inplace=True)  # Drop the 'index' column
    return filtered_df  # Return the processed DataFrame with deaths data  # (39 values)

def fifth_file():  # Function to download and process monthly provisional counts of deaths data.
    df = pd.read_csv("Monthly Provisional Counts of Deaths by Select Causes, 2020-2023.csv")  # Read CSV file into a pandas DataFrame
    df.drop(columns=['Jurisdiction of Occurrence', 'Data As Of', "flag_accid", "flag_mva", "flag_suic", "flag_homic", "flag_drugod", "Start Date", "End Date"], inplace=True)  # Drop unnecessary columns
    df = df.drop([0, 1])  # Drop rows with index 0 and 1
    df = df[(df['Year'] < 2023) | ((df['Year'] == 2023) & (df['Month'] <= 5))]  # Filter data for the specified time range
    df = df.reset_index()  # Reset the index
    return df  # Return the processed DataFrame with monthly deaths data


#data collection
print("Execution Started")
print("Downloading first data; Contains, covid case count, death count, hospitalized count")
gather_data1()
print("First Data downloaded")
print("-------------------------------")
print("Downloading second data; contains the ICD10_codes, age group and total death count")
gather_data2()
print("Second Data downloaded")
print("-------------------------------")
print("Downloading third data; contains the information on total bed capacity for each month aong with beds used by covid ")
gather_data3()
print("Third Data downloaded")
print("-------------------------------")
print("Downloading fourth data; contains the information of race of the infected individuals by covid ")
gather_data4()
print("Fourth Data downloaded")
print("-------------------------------")
print("Downloading fifth data; contains the total death caused by different diseases along with covid, also contains total death in USA per month ")
gather_data5()
print("Fifth Data downloaded")
print("-------------------------------")
print("All Data DOWNLOAD COMPLETE")

#data_cleaning
print("Cleaning first Data File")
first_file_data = first_file()
print("Cleaning Second Data File")
second_file_data = second_file()
print("Cleaning Third Data File")
third_file_data = third_file()
print("Cleaning Fourth Data File")
fourth_file_data = fourth_file()
print("Cleaning Fifth Data File")
fifth_file_data = fifth_file()
print("All data cleaned")
# Merge DataFrames on 'Year' and 'Month'
print("Merging all dataframes")
merged_df = pd.merge(first_file_data, second_file_data, on=['Year', 'Month'])
merged_df = pd.merge(merged_df, third_file_data, on=['Year', 'Month'])
merged_df = pd.merge(merged_df, fourth_file_data, on=['Year', 'Month'])
merged_df = pd.merge(merged_df, fifth_file_data, on=['Year', 'Month'])
print("Merging complete")
print("Saving as a CSV file")
#clean merged_df:
merged_df_cleaned = merged_df.dropna().drop_duplicates()
merged_df_cleaned.to_csv("baralg1_extra_credits.csv", index=False)
print("Cleaned DataFrame saved to 'baralg1_extra_credits.csv'")


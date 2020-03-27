"""function for copd 19 data analysis
- read_daily_reports
- summarize_by_country
- summarize_by_county

## usage: S:\Learn\python\covid_analysis\covid_analysis>python covid_utils.py ..\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\ *.csv
"""

# libraries
import sys
import os
import pandas as pd
import glob

## read in arguments
dir_input = sys.argv[1]
file_pattern = sys.argv[2]

print(f'{sys.argv}')


def read_daily_reports(dir_in, file_pattern):
    list_file = glob.glob(dir_in + "/" + file_pattern)
    print(f'Total daily reports: {len(list_file)}')

    # read files in loop
    list_df = list()
    for file in list_file:
        temp_df = pd.read_csv(file)
        list_df.append(temp_df)

    # concatinate the data
    combine_df = pd.concat(list_df, axis=0, ignore_index=True, sort=True)
    return(combine_df)


def summarize_by_county(input_df, list_fields):
    summary_df = input_df.groupby(list_fields).sum()
    return(summary_df)


def main():
    # read dialy reports

    combine_df = read_daily_reports(dir_input, file_pattern)
    print(f'Dim of combine df: {combine_df.shape}')
    print(combine_df.head())

    list_country_summ = ['Country/Region', 'Province/State']
    country_summ = summarize_by_county(combine_df, list_country_summ)
    print(country_summ.head())


if __name__ == "__main__":
    main()

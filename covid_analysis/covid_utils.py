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
import logging

## read in arguments
dir_input = sys.argv[1]
file_pattern = sys.argv[2]

print(f'{sys.argv}')

# Add logging feature to capture daily changes and appends 'a' to the log file
logging.basicConfig(filename="covid_compile.log",
                    format='%(asctime)s %(message)s',
                    filemode='a', level=logging.DEBUG)

# Creating an object
logger = logging.getLogger()


def rename_df_col(input_df, rename_col):
    """function to rename columns

    Arguments:
        input_df {dataframe} -- input pandas dataframe
        rename_col {dict} -- dictionary of column name to be renamed
    """
    input_df_new = input_df.rename(columns=rename_col)
    return(input_df_new)


def read_daily_reports(dir_in, file_pattern, rename_col):
    """function to read dialy reports and compile

    Arguments:
        dir_in {str} -- path to input file location
        file_pattern {str} -- pattern of input file names
        rename_col {dict} -- dictionary of column names to be renamed
    """
    list_file = glob.glob(dir_in + "/" + file_pattern)
    print(f'Total daily reports: {len(list_file)}')

    # read files in loop
    list_df = list()
    for file in list_file:
        temp_df = pd.read_csv(file)
        temp_df = rename_df_col(temp_df, rename_col)
        list_df.append(temp_df)

    # concatinate the data
    combine_df = pd.concat(list_df, axis=0, ignore_index=True, sort=True)
    return(combine_df)


def summarize_by_county(input_df, list_groups, list_display):
    """function to summarize

    Arguments:
        input_df {dataframe} -- combined input dataframe
        list_groups {list} -- list of columns for groupby
        list_display {list} -- columns to display after groupby
    """
    list_select = list_groups + list_display
    summary_df = input_df[list_select].groupby(list_groups).sum()
    return(summary_df)


def main():
    # read dialy reports
    column_rename = {'Country/Region': 'Country_Region', 'Province/State': 'Province_State',
                     'Last Update': 'Last_Update', 'Latitude': 'Lat', 'Longitude': 'Long_'}

    combine_df = read_daily_reports(dir_input, file_pattern, column_rename)
    logger.info(f'Dim of combine df: {str(combine_df.shape)}')
    print(combine_df.head())

    list_country_summ = ['Country_Region', 'Province_State']
    list_display = []
    country_summ = summarize_by_county(
        combine_df, list_country_summ, list_display=list_display)
    print(country_summ.head())


if __name__ == "__main__":
    main()

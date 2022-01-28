import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import json as js
import math
import numpy as np


def open_excel_file(connection_string):

    data_frame = pd.read_excel(connection_string)

    return(data_frame)

def write_excel_file(output_file_path, output_data_frame):
    Excelwriter = pd.ExcelWriter(output_file_path, engine="xlsxwriter")
    output_data_frame.to_excel(Excelwriter, sheet_name="Output" ,index=True)
    Excelwriter.close()
    return

def open_csv_file(connection_string):
    
    data_frame = pd.read_csv( connection_string)

    return data_frame

def get_sql_table(connection_string, query):

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=ATHPRODBIDB01;'
                          'Database=AHLDW;'
                          'Trusted_Connection=yes;')

    #data_frame = pd.read_sql_query(str(query), pyodbc.connect(connection_string))
    data_frame = pd.read_sql_query(str(query), conn)

    return data_frame
    

def get_bda_ail(valuation_date, type, cedent, ae_type, connection_string, columntype):

    ail_type = type
    block = cedent
  
    if columntype == 'static':
        default_columnlist = "ClientShortName, ValuationDate, Entity, IssueAge, Gender, IssueYear, IssueMonth, FFileKey, ProductType, ProductName, PolicyNumber, ModelPlan, \
                          PlanCode, OrigIssueYear, OrigIssueMonth, PolicyCount, LineOfBusiness"
    elif columntype == 'variable':
        default_columnlist = "ClientShortName, NewOrSurviving, ValuationDate, Entity, ProductType, ProductName, PolicyNumber, ModelPlan, PlanCode, \
                          PolicyCount, AccountValueTotal, AccountValueFixed, AccountValueIndexTotal, CSVValue, GCSVValue, IAV, CreditRate, Idx1FV, Idx2FV, Idx3FV, Idx4FV, Idx5FV, Idx6FV"
    else:
        default_columnlist = "ClientShortName, NewOrSurviving, ValuationDate, Entity, IssueAge, Gender, Class, Char1, IssueYear, IssueMonth, FFileKey, ProductType, ProductName, PolicyNumber, ModelPlan, PlanCode, \
                          PolicyCount, AccountValueTotal, AccountValueFixed, AccountValueIndexTotal, CSVValue, GCSVValue, IAV, CreditRate, \
                          Idx1FV, Idx2FV, Idx3FV, Idx4FV, Idx5FV, Idx6FV"

    if ail_type == 'New':
        ailtype = " and NewOrSurviving = 'N\'"
    elif ail_type == 'Surviving':
        ailtype = " and NewOrSurviving = 'S\'"
    else:
        ailtype = " "

    if block == 'AEL':
        client = "\'AEL\'"
    elif block == 'EGL':
        client = "\'EGL\'"
    elif block == 'MNL':
        client = "\'MNL\'"
    else:
        client = "\'AEL\', \'EGL\', \'MNL\'"

    if ae_type == 'A':
       actorest = " and ActualOrEstimate = 'A\'"
    elif ae_type == 'E':
        actorest = " and ActualOrEstimate = 'E\'"
    else:
        print('No Actual or Estimate Value Provide')

    querystring = "select " + default_columnlist + " from AHLDW.rpt.AILPlus where ClientShortName in (" + client + ") and ValuationDate=\'" + valuation_date + "\'" + ailtype + actorest

     ## import AIL data   ProductType (Fixed or Indexed)   ProductName (FIA)
    ail_data = get_sql_table(connection_string, querystring)

    return(ail_data)


#    Currently not used.  The intention for this was to use todays date and return a dataframe with current and prior valuation dates
def get_valuation_dates():

    current_date = datetime.now()
    current_year = current_date.year
    current_quarter = math.floor((current_date.month - 1) / 3)
 
    if current_quarter == 0:
        current_quarter =4
        current_year = current_year - 1
 
    if current_quarter == 1:
        prior_quarter = 4
        prior_year = current_year - 1
    else:
        prior_quarter = current_quarter - 1
        prior_year = current_year
    
    if current_quarter == 4:
        Adj = 1
    else:
        Adj = 0
 
    first_date = datetime(current_year + Adj, 3 * current_quarter + 1 - (12*Adj), 1) + timedelta(days=-1)
    last_date = datetime(prior_year, 3 * prior_quarter + 1, 1) + timedelta(days=-1)

    data = { 'Current':[first_date],
             'Prior':[last_date] }
 
    # Create DataFrame
    valuationdate_df = pd.DataFrame(data)

    return(valuationdate_df)

def create_report(valuation_date, filename, outputdf, output_dir):

    vdate = datetime.strptime(valuation_date, '%Y/%m/%d').date()

    fulldate = vdate.strftime("%m%d%y")

    exceloutputfile = output_dir + "\\" + filename + "_" + fulldate + ".xlsx"

    write_excel_file(exceloutputfile, outputdf)

    return

def output_to_file(string):

  f = open('./output/Compare_Log.txt', 'a+')

  newstring = string + '\n'
  f.write(newstring) 
  f.close()

  return

def compare_dataframes(df1, df2):

    difflist = pd.DataFrame()

    for i in df1.head():
      
        difference = compare_series(df1[i], df2[i])
        difflen = len(difference.index)

        if difflen > 0:
            difflist = difflist.append(difference)
            difflen = 0

    return difflist

def compare_series(s1, s2):

    first = s1
    second = s2

    difference = s1.values[1] != s2.values[1]
    
    difference['Column'] = difference.apply(get_col_name, axis=1)

    return difference


def compare_static_ail_columns(prior, current):



    return
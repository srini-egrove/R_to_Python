import pandas as pd
import requests
import logging
import gspread
import time
import json
from datetime import datetime
from datetime import timedelta
from oauth2client.service_account import ServiceAccountCredentials
logging.basicConfig(filename='R_to_Python.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Script started")

# Keys
my_key = "c8ob3uaad3iddfsar4r0"

# Number of times to query an endpoint
query_max = 5

# Errors
errors_dt = pd.DataFrame()

temp_error =""

#----- First script read and update 4 csv's ------------
try:
    logging.info("Read and update 4 csv's script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

    # Scrape Index ETF Constituents

    # Get Index Constituents From iShares Disclosures
    exchange_list = [
        "NASDAQ",
        "New York Stock Exchange Inc.",
        "Nyse Mkt Llc",
        "Cboe BZX formerly known as BATS"
    ]

    # Russell 1000 Holdings
    r1000_index = pd.read_csv("https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund", skiprows=9, skipfooter=1, engine='python')
    r1000_index = r1000_index[r1000_index['Asset Class'] == "Equity"]
    r1000_index = r1000_index[r1000_index['Exchange'].isin(exchange_list)]
    r1000_index.to_csv("datasets/r1000.csv", index=False)

    # Russell 2000 Holdings
    r2000_index = pd.read_csv("https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund", skiprows=9, skipfooter=1, engine='python')
    r2000_index = r2000_index[r2000_index['Asset Class'] == "Equity"]
    r2000_index = r2000_index[r2000_index['Exchange'].isin(exchange_list)]
    r2000_index.to_csv("datasets/r2000.csv", index=False,)

    # Russell 3000 Holdings
    r3000_index = pd.read_csv("https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund", skiprows=9, skipfooter=1, engine='python')
    r3000_index = r3000_index[r3000_index['Asset Class'] == "Equity"]
    r3000_index = r3000_index[r3000_index['Exchange'].isin(exchange_list)]
    r3000_index.to_csv("datasets/r3000.csv", index=False)

    # S&P 500 Holdings
    sp500_index = pd.read_csv("https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund", skiprows=9, skipfooter=1, engine='python')
    sp500_index = sp500_index[sp500_index['Asset Class'] == "Equity"]
    sp500_index = sp500_index[sp500_index['Exchange'].isin(exchange_list)]
    sp500_index.to_csv("datasets/sp500.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Read and update 4 csv's script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

#-------Second Script Generate Universe csv  -------------------

try:
    logging.info("Universe script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    # # Get Universe Names
    universe_url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={my_key}"
    universe_data = requests.get(universe_url).json()
    universe_dt = pd.DataFrame(universe_data)

    # Get Index Data
    sp500_dt = pd.read_csv("datasets/sp500.csv")
    r1000_dt = pd.read_csv("datasets/r1000.csv")
    r2000_dt = pd.read_csv("datasets/r2000.csv")
    r3000_dt = pd.read_csv("datasets/r3000.csv")

    # Replace with your JSON key file and spreadsheet ID
    json_keyfile = '/home/egrove/Downloads/airy-task-279409-eb5866af4131.json'
    spreadsheet_id = '1njs2QjtkCYF5z6QxUahRbcJcovTkkLCl_DZGJXusJow'

    # Authenticate using the JSON key file
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
    client = gspread.authorize(creds)

    # Open the Google Sheets spreadsheet
    sheet = client.open_by_key(spreadsheet_id)
    worksheet = sheet.get_worksheet(0)
    data = worksheet.get_all_records()

    # Create a pandas DataFrame from the list of dictionaries
    coverage_inputs_raw = pd.DataFrame(data)

    universe_names = list(set(
      sp500_dt['Ticker'].tolist() +
      r1000_dt['Ticker'].tolist() +
      r2000_dt['Ticker'].tolist() +
      r3000_dt['Ticker'].tolist() +
      coverage_inputs_raw[coverage_inputs_raw['ID'] != 'Cash']['ID'].tolist()
    ))

    universe_dt = universe_dt[universe_dt['symbol'].isin(universe_names)]
    # Get Universe Profile Data
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                profile_url = f"https://finnhub.io/api/v1/stock/profile?symbol={i}&token={my_key}"
                profile_data = requests.get(profile_url).json()
                temp_dt = pd.DataFrame([profile_data])

                if len(temp_dt) != 0:
                    universe_dt.loc[universe_dt['symbol'] == i, 'market_cap'] = temp_dt['marketCapitalization'].iloc[0]
                    universe_dt.loc[universe_dt['symbol'] == i, 'shares_out'] = temp_dt['shareOutstanding'].iloc[0]
                    universe_dt.loc[universe_dt['symbol'] == i, 'model_group'] = temp_dt['finnhubIndustry'].iloc[0]
                    universe_dt.loc[universe_dt['symbol'] == i, 'sector'] = temp_dt['gsector'].iloc[0]
                    universe_dt.loc[universe_dt['symbol'] == i, 'industry_group'] = temp_dt['ggroup'].iloc[0]
                    universe_dt.loc[universe_dt['symbol'] == i, 'industry'] = temp_dt['gind'].iloc[0]
                    universe_dt.loc[universe_dt['symbol'] == i, 'subindustry'] = temp_dt['gsubind'].iloc[0]
                    
                    if temp_dt['ticker'].iloc[0] != i:
                        temp_error = pd.DataFrame({
                            'ticker': [i],
                            'endpoint': ['Profile 1 Data'],
                            'type': ['neither'],
                            'description': [f"Acquired data is for {temp_dt['ticker'].iloc[0]}"]
                        })
                        errors_dt = pd.concat([errors_dt, temp_error])
                else:
                    temp_error = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Profile 1 Data'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error])
                
                acquired.append(i)
                
            except Exception as e:
                print('Caught an error!', i,str(e))
                temp_error = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Profile 1 Data'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error])
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Assign Universe Tags
    universe_dt['in.sp500'] = 0
    universe_dt['in.r1000'] = 0
    universe_dt['in.r2000'] = 0
    universe_dt['in.r3000'] = 0
    universe_dt.loc[universe_dt['symbol'].isin(sp500_dt['Ticker']), 'in.sp500'] = 1
    universe_dt.loc[universe_dt['symbol'].isin(r1000_dt['Ticker']), 'in.r1000'] = 1
    universe_dt.loc[universe_dt['symbol'].isin(r2000_dt['Ticker']), 'in.r2000'] = 1
    universe_dt.loc[universe_dt['symbol'].isin(r3000_dt['Ticker']), 'in.r3000'] = 1

    # Write File Locally
    universe_dt.to_csv("datasets/universe.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Universe csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))



#---------Third Script Rating -----------------
try:
    # Stock Ratings
    logging.info("Ratings csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    ratings_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                ratings_url = f"https://finnhub.io/api/v1/stock/recommendation?symbol={i}&token={my_key}"
                ratings_data = requests.get(ratings_url).json()
                print("ratings_data-->",ratings_data)
                temp_ratings_dt = pd.DataFrame(ratings_data)
                
                if len(temp_ratings_dt) != 0:
                    temp_ratings_dt.rename(columns={'symbol': 'ticker'}, inplace=True)
                    
                    if any(temp_ratings_dt['ticker'] != i):
                        ticker_mismatch = ', '.join(temp_ratings_dt[temp_ratings_dt['ticker'] != i]['ticker'].unique())
                        temp_error = pd.DataFrame({
                            'ticker': [i],
                            'endpoint': ['Stock Ratings'],
                            'type': ['neither'],
                            'description': [f"Acquired data is for {ticker_mismatch}"]
                        })
                        errors_dt = pd.concat([errors_dt, temp_error])
                else:
                    temp_ratings_dt['ticker'] = i
                    temp_error = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Stock Ratings'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error])
                
                ratings_dt = pd.concat([ratings_dt, temp_ratings_dt], ignore_index=True, sort=False)
                acquired.append(i)
                
            except Exception as e:
                print('Caught an error!', i)
                temp_error = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Stock Ratings'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error])
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(f"Ratings: {count}")
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    ratings_dt.to_csv("datasets/ratings.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Ratings csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

#--------------Forth Script Price Target-----------------------

try:
    # Price Targets
    logging.info("Tragets csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    # Initialize DataFrame
    target_est_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                target_est_url = f"https://finnhub.io/api/v1/stock/price-target?symbol={i}&token={my_key}"
                target_est_data = requests.get(target_est_url).json()
                temp_target_est_dt = pd.DataFrame([target_est_data])
                
                if temp_target_est_dt['symbol'].iloc[0] == "":
                    temp_error = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Price Targets'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error])
                    temp_target_est_dt['symbol'] = i
                elif temp_target_est_dt['symbol'].iloc[0] != i:
                    temp_error = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Price Targets'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_target_est_dt['symbol'].iloc[0]}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error])
                
                temp_target_est_dt.rename(columns={'symbol': 'ticker'}, inplace=True)
                target_est_dt = pd.concat([target_est_dt, temp_target_est_dt], ignore_index=True, sort=False)
                acquired.append(i)
                
            except Exception as e:
                print('Caught an error!', i)
                temp_error = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Price Targets'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error])
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(f"Targets: {count}")
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    target_est_dt.to_csv("datasets/target_est.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Tragets csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))


#-----------Fifth script Sales Estimates ----------------------
try:
    # Sales Estimates
    logging.info("Sales Est csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    sales_est_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        # for i in acquire:
        #     try:
        #         # Call API
        #         sales_est_url = (
        #             f"https://finnhub.io/api/v1/stock/revenue-estimate"
        #             f"?symbol={i}&freq=annual&token={my_key}"
        #         )
        #         response = requests.get(sales_est_url)
        #         temp_sales_est_dt = pd.DataFrame(response.json())

        #         if not temp_sales_est_dt.empty:
        #             if temp_sales_est_dt['symbol'].iloc[0] != i:
        #                 temp_error_dt = pd.DataFrame({
        #                     'ticker': [i],
        #                     'endpoint': ['Sales Estimates'],
        #                     'type': ['neither'],
        #                     'description': [
        #                         f"Acquired data is for {temp_sales_est_dt['symbol'].iloc[0]}"
        #                     ]
        #                 })
        #                 errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
        #         else:
        #             temp_error_dt = pd.DataFrame({
        #                 'ticker': [i],
        #                 'endpoint': ['Sales Estimates'],
        #                 'type': ['neither'],
        #                 'description': ['No data']
        #             })
        #             errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
        #             temp_sales_est_dt['symbol'] = i

        #         temp_sales_est_dt['ticker'] = i
        #         sales_est_dt = pd.concat([sales_est_dt, temp_sales_est_dt], ignore_index=True)

        #         acquired.append(i)

        #     except Exception as e:
        #         temp_error_dt = pd.DataFrame({
        #             'ticker': [i],
        #             'endpoint': ['Sales Estimates'],
        #             'type': ['error'],
        #             'description': [str(e)]
        #         })
        #         errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
        #         # Update errors
        #     if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
        #         errors_dt = pd.concat([errors_dt, temp_error])
        
        #     # Sleep
        #     time.sleep(0.25)
        
        #     # Notice
        #     count += 1
        #     print(f"Sales: {count}")
        #     print(i)
        
        # # Update outer loop
        # acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        # query_num += 1
        for i in acquire:
            try:
                # Call API
                sales_est_url = (
                    "https://finnhub.io/api/v1/stock/revenue-estimate"
                    f"?symbol={i}&freq=annual&token={my_key}"
                )
                response = requests.get(sales_est_url)
                temp_sales_est_data = response.json()

                if temp_sales_est_data['symbol'] != i:
                    temp_error_data = {
                        'ticker': i,
                        'endpoint': 'Sales Estimates',
                        'type': 'neither',
                        'description': f"Acquired data is for {temp_sales_est_data['symbol']}"
                    }
                    errors_dt = pd.concat([errors_dt, pd.DataFrame([temp_error_data])])

                temp_sales_est_df = pd.DataFrame(temp_sales_est_data['data'])
                
                if not temp_sales_est_df.empty:
                    temp_sales_est_df['period'] = pd.to_datetime(temp_sales_est_df['period'])
                    temp_sales_est_df = temp_sales_est_df[temp_sales_est_df['period'] > pd.Timestamp.today()]
                    if temp_sales_est_df.empty:
                        temp_error_data = {
                            'ticker': i,
                            'endpoint': 'Sales Estimates',
                            'type': 'neither',
                            'description': 'No data after restricting period'
                        }
                        errors_dt = pd.concat([errors_dt, pd.DataFrame([temp_error_data])])
                else:
                    temp_error_data = {
                        'ticker': i,
                        'endpoint': 'Sales Estimates',
                        'type': 'neither',
                        'description': 'No data'
                    }
                    errors_dt = pd.concat([errors_dt, pd.DataFrame([temp_error_data])])

                temp_sales_est_df['ticker'] = i
                sales_est_dt = pd.concat([sales_est_dt, temp_sales_est_df])

                acquired.append(i)
            
            except Exception as e:
                temp_error_data = {
                    'ticker': i,
                    'endpoint': 'Sales Estimates',
                    'type': 'error',
                    'description': str(e)
                }
                errors_dt = pd.concat([errors_dt, pd.DataFrame([temp_error_data])])

            time.sleep(0.25)
            
            count += 1
            print(f"Sales: {count}")
            print(i)
    
        # Update outer loop
        acquire = list(set(universe_dt['symbol']) - set(acquired))
        query_num += 1

    # Write File Locally
    sales_est_dt.to_csv("datasets/sales_est.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Sales Est csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))



#-------------Sixth Script EBIT Estimates------------------
try:
    # EBIT Estimates
    logging.info("Ebit Est csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    ebit_est_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                ebit_est_url = f"https://finnhub.io/api/v1/stock/ebit-estimate?symbol={i}&freq=annual&token={my_key}"
                response = requests.get(ebit_est_url)
                temp_ebit_est_dt = json.loads(response.text)
                
                if temp_ebit_est_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['EBIT Estimates'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_ebit_est_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_ebit_est_dt = pd.DataFrame(temp_ebit_est_dt['data'])
                
                if not temp_ebit_est_dt.empty:
                    temp_ebit_est_dt['period'] = pd.to_datetime(temp_ebit_est_dt['period'])  # Convert to Timestamp
                    temp_ebit_est_dt = temp_ebit_est_dt[temp_ebit_est_dt['period'] > pd.Timestamp.today()]
                    if temp_ebit_est_dt.empty:
                        temp_error_dt = pd.DataFrame({
                            'ticker': [i],
                            'endpoint': ['EBIT Estimates'],
                            'type': ['neither'],
                            'description': ['No data after restricting period']
                        })
                        errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                else:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['EBIT Estimates'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_ebit_est_dt['ticker'] = i
                
                # Add to DataFrame
                ebit_est_dt = pd.concat([ebit_est_dt, temp_ebit_est_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['EBIT Estimates'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                    
                # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(f"EBIT: {count}")
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    ebit_est_dt.to_csv("datasets/ebit_est.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Ebit Est csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

#------------------Seventh Script EBITDA Estimates-------------
try:
    # EBITDA Estimates
    logging.info("Ebitda Est csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    ebitda_est_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                ebitda_est_url = f"https://finnhub.io/api/v1/stock/ebitda-estimate?symbol={i}&freq=annual&token={my_key}"
                response = requests.get(ebitda_est_url)
                temp_ebitda_est_dt = json.loads(response.text)
                
                if temp_ebitda_est_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['EBITDA Estimates'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_ebitda_est_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_ebitda_est_dt = pd.DataFrame(temp_ebitda_est_dt['data'])
                
                if not temp_ebitda_est_dt.empty:
                    temp_ebitda_est_dt['period'] = pd.to_datetime(temp_ebitda_est_dt['period'])  # Convert to Timestamp
                    temp_ebitda_est_dt = temp_ebitda_est_dt[temp_ebitda_est_dt['period'] > pd.Timestamp.today()]
                    if temp_ebitda_est_dt.empty:
                        temp_error_dt = pd.DataFrame({
                            'ticker': [i],
                            'endpoint': ['EBITDA Estimates'],
                            'type': ['neither'],
                            'description': ['No data after restricting period']
                        })
                        errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                else:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['EBITDA Estimates'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_ebitda_est_dt['ticker'] = i
                
                # Add to DataFrame
                ebitda_est_dt = pd.concat([ebitda_est_dt, temp_ebitda_est_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}{e}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['EBITDA Estimates'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(f"EBITDA: {count}")
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    ebitda_est_dt.to_csv("datasets/ebitda_est.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Ebitda Est csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

#-------------Eighth Script EPS Estimates-------------------
try:
    # EPS Estimates
    logging.info("Eps csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    eps_est_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                eps_est_url = f"https://finnhub.io/api/v1/stock/eps-estimate?symbol={i}&freq=annual&token={my_key}"
                response = requests.get(eps_est_url)
                temp_eps_est_dt = json.loads(response.text)
                
                if temp_eps_est_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['EPS Estimates'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_eps_est_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_eps_est_dt = pd.DataFrame(temp_eps_est_dt['data'])
                
                if not temp_eps_est_dt.empty:
                    temp_eps_est_dt['period'] = pd.to_datetime(temp_eps_est_dt['period'])  # Convert to Timestamp
                    temp_eps_est_dt = temp_eps_est_dt[temp_eps_est_dt['period'] > pd.Timestamp.today()]
                    if temp_eps_est_dt.empty:
                        temp_error_dt = pd.DataFrame({
                            'ticker': [i],
                            'endpoint': ['EPS Estimates'],
                            'type': ['neither'],
                            'description': ['No data after restricting period']
                        })
                        errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                else:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['EPS Estimates'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_eps_est_dt['ticker'] = i
                
                # Add to DataFrame
                eps_est_dt = pd.concat([eps_est_dt, temp_eps_est_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['EPS Estimates'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(f"EPS: {count}")
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    eps_est_dt.to_csv("datasets/eps_est.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Eps csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

#----------------Nineth Script Quarter Income Statement---------------
try:
    logging.info("quarterly_is csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    # Quarterly Income Statement
    quarterly_is_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                financials_is_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=ic&freq=quarterly&token={my_key}"
                response = requests.get(financials_is_url)
                temp_financials_is_dt = json.loads(response.text)
                
                if temp_financials_is_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Quarterly Income Statement'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_financials_is_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_financials_is_dt = pd.DataFrame(temp_financials_is_dt['financials'])
                
                if temp_financials_is_dt.empty:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Quarterly Income Statement'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_financials_is_dt['ticker'] = i
                
                # Add to DataFrame
                quarterly_is_dt = pd.concat([quarterly_is_dt, temp_financials_is_dt], ignore_index=True, sort=False)
                
                acquired.append(i)

            except Exception as e:
                print(f"Caught an error! {i}{e}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Quarterly Income Statement'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    quarterly_is_dt.to_csv("datasets/quarterly_is.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("quarterly_is csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))


#------------Tenth Script Annual Income statement--------------
try:
    # Annual Income Statement
    logging.info("Annual_is csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    annual_is_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                financials_is_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=ic&freq=annual&token={my_key}"
                response = requests.get(financials_is_url)
                temp_is_dt = json.loads(response.text)
                
                if temp_is_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Annual Income Statement'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_is_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_is_dt = pd.DataFrame(temp_is_dt['financials'])
                
                if temp_is_dt.empty:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Annual Income Statement'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_is_dt['ticker'] = i
                
                # Add to DataFrame
                annual_is_dt = pd.concat([annual_is_dt, temp_is_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Annual Income Statement'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    annual_is_dt.to_csv("datasets/annual_is.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Annual_is csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))



#-------------------Eleventh Script Annual balance statement------------
try:
    # Annual Balance Sheet
    logging.info("Annual_bs csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    annual_bs_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                financials_bs_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=bs&freq=annual&token={my_key}"
                response = requests.get(financials_bs_url)
                temp_bs_dt = json.loads(response.text)
                
                if temp_bs_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Annual Balance Sheet'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_bs_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_bs_dt = pd.DataFrame(temp_bs_dt['financials'])
                
                if temp_bs_dt.empty:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Annual Balance Sheet'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                else:
                    # Keep Last Value
                    temp_bs_dt = temp_bs_dt[temp_bs_dt['period'] == temp_bs_dt['period'].max()]
                
                temp_bs_dt['ticker'] = i
                
                # Add to DataFrame
                annual_bs_dt = pd.concat([annual_bs_dt, temp_bs_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Annual Balance Sheet'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.15)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    annual_bs_dt.to_csv("datasets/annual_bs.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Annual_bs csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))


#-------------Twelveth Script Annual cash flow statement -------------------
try:
    # Annual Cash Flow Statement
    logging.info("Annual_cf csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    annual_cf_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                financials_cf_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=cf&freq=annual&token={my_key}"
                response = requests.get(financials_cf_url)
                temp_cf_dt = json.loads(response.text)
                
                if temp_cf_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Annual Cash Flow Statement'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_cf_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_cf_dt = pd.DataFrame(temp_cf_dt['financials'])
                
                if temp_cf_dt.empty:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Annual Cash Flow Statement'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_cf_dt['ticker'] = i
                
                # Add to DataFrame
                annual_cf_dt = pd.concat([annual_cf_dt, temp_cf_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Annual Cash Flow Statement'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.15)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    annual_cf_dt.to_csv("datasets/annual_cf.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Annual_cf csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))



#------------------Thirteenth Script querter cash flow statement----------
try:
    # Quarterly Cash Flow Statement
    logging.info("Quarterly_cf csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    quarterly_cf_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                financials_cf_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=cf&freq=quarterly&token={my_key}"
                response = requests.get(financials_cf_url)
                temp_cf_dt = json.loads(response.text)
                
                if temp_cf_dt['symbol'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Quarterly Cash Flow Statement'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_cf_dt['symbol']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_cf_dt = pd.DataFrame(temp_cf_dt['financials'])
                
                if temp_cf_dt.empty:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Quarterly Cash Flow Statement'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_cf_dt['ticker'] = i
                
                # Add to DataFrame
                quarterly_cf_dt = pd.concat([quarterly_cf_dt, temp_cf_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Quarterly Cash Flow Statement'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
            
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.25)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    quarterly_cf_dt.to_csv("datasets/quarterly_cf.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Quarterly_cf csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))


#------------Fourteenth Script Profiles Data--------------
try:
    # Profiles Data
    logging.info("Profiles csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    profiles_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                # Get Data
                profiles_url = f"https://finnhub.io/api/v1/stock/profile?symbol={i}&token={my_key}"
                response = requests.get(profiles_url)
                temp_profiles_dt = json.loads(response.text)
                
                if "ticker" in temp_profiles_dt and temp_profiles_dt['ticker'] != i:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Profiles Data'],
                        'type': ['neither'],
                        'description': [f"Acquired data is for {temp_profiles_dt['ticker']}"]
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_profiles_dt = pd.DataFrame([temp_profiles_dt])
                
                if temp_profiles_dt.empty:
                    temp_error_dt = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Profiles Data'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
                
                temp_profiles_dt['ticker'] = i
                
                # Add to DataFrame
                profiles_dt = pd.concat([profiles_dt, temp_profiles_dt], ignore_index=True, sort=False)
                
                acquired.append(i)
            
            except Exception as e:
                print(f"Caught an error! {i}")
                temp_error_dt = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Profiles Data'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error_dt], ignore_index=True)
            
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.15)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    profiles_dt.to_csv("datasets/profiles.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Profiles csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))



#------------------Fifteenth Script Market Data-----------------
try:
    # Market Data
    logging.info("Candles csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    candles_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    acquire = list(acquire) + ['SPY', 'IWB', 'IWM', 'IWV']  # Add Benchmarks
    query_num = 1
    count = 0
    start_date = (datetime.now() - timedelta(days=5*365)).timestamp()
    end_date = datetime.now().timestamp()

    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            try:
                candles_url = f"https://finnhub.io/api/v1/stock/candle?symbol={i}&resolution=D&from={int(start_date)}&to={int(end_date)}&token={my_key}"
                temp_candles_data = requests.get(candles_url).json()
                
                if temp_candles_data['s'] == "ok":
                    temp_candles_dt = pd.DataFrame(temp_candles_data)
                    temp_candles_dt['date'] = pd.to_datetime(temp_candles_dt['t'], unit='s')
                    
                    temp_candles_dt.rename(columns={
                        "c": "close",
                        "h": "high",
                        "l": "low",
                        "o": "open",
                        "s": "status",
                        "v": "volume"
                    }, inplace=True)
                else:
                    temp_candles_dt = pd.DataFrame(temp_candles_data)
                    temp_error = pd.DataFrame({
                        'ticker': [i],
                        'endpoint': ['Market Data'],
                        'type': ['neither'],
                        'description': ['No data']
                    })
                    errors_dt = pd.concat([errors_dt, temp_error])
                
                temp_candles_dt['ticker'] = i
                candles_dt = pd.concat([candles_dt, temp_candles_dt], ignore_index=True, sort=False)
                acquired.append(i)
                
            except Exception as e:
                print('Caught an error!', i)
                temp_error = pd.DataFrame({
                    'ticker': [i],
                    'endpoint': ['Market Data'],
                    'type': ['error'],
                    'description': [str(e)]
                })
                errors_dt = pd.concat([errors_dt, temp_error])
                
            # Update errors
            if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                errors_dt = pd.concat([errors_dt, temp_error])
        
            # Sleep
            time.sleep(0.75)
        
            # Notice
            count += 1
            print(count)
            print(i)
        
        # Update outer loop
        acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
        query_num += 1

    # Write File Locally
    candles_dt.to_csv("datasets/candles.csv", index=False)
    errors_dt.to_csv("datasets/errors.csv", index=False)
except Exception as e:
    logging.error(f"Error occurred: {e}", exc_info=True)
logging.info("Candles csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))



# 1.Read four type of csv from www.ishares.com website link and alter
# 2.Create Universe CSV from finnhub API
# 3.Create Rating CSV from finnhub API
# 4.Create Price Target CSV from finnhub API
# 5.Create Sales Estimates CSV from finnhub API
# 6.Create EBIT Estimates from finnhub API
# 7.Create EBITDA Estimates CSV from finnhub API
# 8.Create EPS Estimates CSV from finnhub API
# 9.Create Quarter Income Statement CSV from finnhub API
# 10.Create Annual Income Statement CSV from finnhub API
# 11.Create Annual Balance Statement from finnhub API
# 12.Create Annual Cash Flow Statement CSV from finnhub API
# 12.Create Quarter Cash Flow Statement CSV from finnhub API
# 14.Create Profiles Data CSV from finnhub API
# 15.Create Market Data CSV from finnhub API
import pandas as pd
import requests
import logging
import gspread
import time
import json
from datetime import datetime
from datetime import timedelta
from oauth2client.service_account import ServiceAccountCredentials
logging.basicConfig(filename='R2_to_Python.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Script started")

# Keys
my_key = "c8ob3uaad3iddfsar4r0"

# Number of times to query an endpoint
query_max = 5

# Replace with your JSON key file and spreadsheet ID
json_keyfile = '/home/egrove/Downloads/airy-task-279409-eb5866af4131.json'
spreadsheet_id = '1njs2QjtkCYF5z6QxUahRbcJcovTkkLCl_DZGJXusJow'

# Errors
errors_dt = pd.DataFrame()
temp_error = ""
universe_dt = pd.read_csv("datasets2/universe.csv")

def handle_error_data(ticker,endpoint,error_type,description_data,csv_type):
    global temp_error 
    try:
        temp_error = pd.DataFrame({
            'ticker': [ticker],
            'endpoint': [endpoint],
            'type': [error_type],
            'description': [description_data]
        })
        errors_dt = pd.concat([errors_dt, temp_error])
        return temp_error,errors_dt
    except Exception as e:
        print(f"{csv_type} handle error data funtion error: {e}")
        logging.error(f"{csv_type } handle error data funtion error: {e}")
        return None,None

def handle_no_data(ticker,endpoint,error_type,description_data,csv_type):
    global temp_error 
    try:
        temp_error = pd.DataFrame({
            'ticker': [ticker],
            'endpoint': [endpoint],
            'type': [error_type],
            'description': [description_data]
        })
        errors_dt = pd.concat([errors_dt, temp_error])
        return temp_error,errors_dt
    except Exception as e:
        print(f"{csv_type} handle no data funtion error: {e}")
        logging.error(f"{csv_type } handle no data funtion error: {e}")
        return None,None

def handle_acquired_data(ticker,endpoint,error_type,description_data,csv_type):
    global temp_error 
    try:
        temp_error =  pd.DataFrame({
            'ticker': [ticker],
            'endpoint': [endpoint],
            'type': [error_type],
            'description': [f"Acquired data is for {description_data}"]
        })
        errors_dt = pd.concat([errors_dt, temp_error])
        return temp_error,errors_dt
    except Exception as e:
        print(f"{csv_type} handle acquired data funtion error: {e}")
        logging.error(f"{csv_type } handle acquired data funtion error: {e}")
        return None,None

def make_api_request(url,data_type,csv_type):
    try:
        response = requests.get(url)
        json_response = response.json()
        if data_type == "data_frame":
            return pd.DataFrame([json_response])
        elif data_type == "json_type":
            return json_response
        else:
            return json.loads(response.text)
    except requests.exceptions.RequestException as e:
        print(f"{csv_type} API request error:",{e})
        logging.error(f"{csv_type} API request error: {e}")
        return None

def First_four_csvs(ishare_api,csv_name):  
    #----- First script: To get data from ishares api and update the data created as a csv's ------------
    try:
        # Scrape Index ETF Constituents
        # Get Index Constituents From iShares Disclosures
        exchange_list = [
            "NASDAQ",
            "New York Stock Exchange Inc.",
            "Nyse Mkt Llc",
            "Cboe BZX formerly known as BATS"
        ]
        four_csv = pd.read_csv(ishare_api, skiprows=9, skipfooter=1, engine='python')
        four_csv = four_csv[four_csv['Asset Class'] == "Equity"]
        four_csv = four_csv[four_csv['Exchange'].isin(exchange_list)]
        four_csv.to_csv("datasets2/"+csv_name+".csv", index=False)

        return csv_name+"csv created succesfully"
    except Exception as e:
        logging.info("First 4 csv's generation issue:",str(e))

def generate_universe_csv():
    try:
        logging.info("Universe script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        # # Get Universe Names
        universe_url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={my_key}"
        universe_data = requests.get(universe_url).json()
        universe_dt = pd.DataFrame(universe_data)

        # Get Index Data
        sp500_dt = pd.read_csv("datasets2/sp500.csv")
        r1000_dt = pd.read_csv("datasets2/r1000.csv")
        r2000_dt = pd.read_csv("datasets2/r2000.csv")
        r3000_dt = pd.read_csv("datasets2/r3000.csv")

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
                    temp_dt = make_api_request(profile_url,"data_frame","universe csv")

                    if len(temp_dt) != 0:
                        universe_dt.loc[universe_dt['symbol'] == i, 'market_cap'] = temp_dt['marketCapitalization'].iloc[0]
                        universe_dt.loc[universe_dt['symbol'] == i, 'shares_out'] = temp_dt['shareOutstanding'].iloc[0]
                        universe_dt.loc[universe_dt['symbol'] == i, 'model_group'] = temp_dt['finnhubIndustry'].iloc[0]
                        universe_dt.loc[universe_dt['symbol'] == i, 'sector'] = temp_dt['gsector'].iloc[0]
                        universe_dt.loc[universe_dt['symbol'] == i, 'industry_group'] = temp_dt['ggroup'].iloc[0]
                        universe_dt.loc[universe_dt['symbol'] == i, 'industry'] = temp_dt['gind'].iloc[0]
                        universe_dt.loc[universe_dt['symbol'] == i, 'subindustry'] = temp_dt['gsubind'].iloc[0]
                        
                        if temp_dt['ticker'].iloc[0] != i:
                            temp_error,errors_dt= handle_acquired_data(i,'Profile 1 Data','neither',temp_dt['ticker'].iloc[0],'universe csv')
                    else:
                        temp_error,errors_dt = handle_no_data(i,'Profile 1 Data','neither','No data','universe csv')
                    acquired.append(i)
                    
                except Exception as e:
                    print('Caught an error!', i,str(e))
                    temp_error,errors_dt = handle_error_data(i,'Profile 1 Data','error',str(e),'universe csv')
                    
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.25)
            
                # Notice
                count += 1
                print(f"Universe: {count}")
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
        universe_dt.to_csv("datasets2/universe.csv", index=False)
    except Exception as e:
        logging.error(f"Universe CSV Error occurred: {e}", exc_info=True)
    logging.info("Universe csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Universe csv created succesfully"

def generate_rating_csv():
    # try:
    # Stock Ratings
    logging.info("Ratings csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    ratings_dt = pd.DataFrame()
    acquired = []
    acquire = universe_dt['symbol'].unique()
    query_num = 1
    count = 0
    temp_error = ""
    while query_num <= query_max and len(acquire) > 0:
        for i in acquire:
            # try:
            ratings_url = f"https://finnhub.io/api/v1/stock/recommendation?symbol={i}&token={my_key}"
            temp_ratings_dt = make_api_request(ratings_url,"json_type","rating csv")
            temp_ratings_dt = pd.DataFrame(temp_ratings_dt) 

            if len(temp_ratings_dt) != 0:
                temp_ratings_dt.rename(columns={'symbol': 'ticker'}, inplace=True)
                
                if any(temp_ratings_dt['ticker'] != i):
                    ticker_mismatch = ', '.join(temp_ratings_dt[temp_ratings_dt['ticker'] != i]['ticker'].unique())
                    temp_error,errors_dt= handle_acquired_data(i,'Stock Ratings','neither',ticker_mismatch,'ratings csv')
            else:
                temp_ratings_dt['ticker'] = i
                temp_error,errors_dt = handle_no_data(i,'Stock Ratings','neither','No data','ratings csv')
            
            ratings_dt = pd.concat([ratings_dt, temp_ratings_dt], ignore_index=True, sort=False)
            acquired.append(i)
                
            # except Exception as e:
            #     print('Caught an error!', i,str(e))
            #     temp_error,errors_dt = handle_error_data(i,'Stock Ratings','error',str(e),'ratings csv')
                
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
    ratings_dt.to_csv("datasets2/ratings.csv", index=False)
    # except Exception as e:
    #     logging.error(f"Ratings csv Error occurred: {e}", exc_info=True)
    # logging.info("Ratings csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    # return "Ratings csv created succesfully"

def generate_target_est_csv():
    try:
        # Price Targets
        logging.info("Tragets csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        # Initialize DataFrame
        target_est_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    target_est_url = f"https://finnhub.io/api/v1/stock/price-target?symbol={i}&token={my_key}"
                    temp_target_est_dt = make_api_request(target_est_url,"data_frame","universe csv")

                    if temp_target_est_dt['symbol'].iloc[0] == "":
                        temp_error,errors_dt = handle_no_data(i,'Price Targets','neither','No data','target_est csv')
                        temp_target_est_dt['symbol'] = i
                    elif temp_target_est_dt['symbol'].iloc[0] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Price Targets','neither',temp_target_est_dt['symbol'].iloc[0],'target_est csv')
                    
                    temp_target_est_dt.rename(columns={'symbol': 'ticker'}, inplace=True)
                    target_est_dt = pd.concat([target_est_dt, temp_target_est_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                    
                except Exception as e:
                    print('Caught an error!', i,str(e))
                    temp_error,errors_dt = handle_error_data(i,'Price Targets','error',str(e),'target_est csv')
                    
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
        target_est_dt.to_csv("datasets2/target_est.csv", index=False)
    except Exception as e:
        logging.error(f"Tragets csv Error occurred: {e}", exc_info=True)
    logging.info("Tragets csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Tragets_est csv created succesfully"

def generate_sales_est_csv():
    try:
        # Sales Estimates
        logging.info("Sales Est csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        sales_est_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error  = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Call API
                    sales_est_url = (
                        "https://finnhub.io/api/v1/stock/revenue-estimate"
                        f"?symbol={i}&freq=annual&token={my_key}"
                    )
                    temp_sales_est_data = make_api_request(sales_est_url,"json_type","sales_est csv")
  

                    if temp_sales_est_data['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Sales Estimates','neither',temp_sales_est_data['symbol'],'sales_est csv')

                    temp_sales_est_df = pd.DataFrame(temp_sales_est_data['data'])
                    
                    if not temp_sales_est_df.empty:
                        temp_sales_est_df['period'] = pd.to_datetime(temp_sales_est_df['period'])
                        temp_sales_est_df = temp_sales_est_df[temp_sales_est_df['period'] > pd.Timestamp.today()]
                        if temp_sales_est_df.empty:
                            temp_error,errors_dt = handle_no_data(i,'Sales Estimates','neither','No data after restricting period','sales_est csv')
                    else:
                        temp_error,errors_dt = handle_no_data(i,'Sales Estimates','neither','No data','sales_est csv')

                    temp_sales_est_df['ticker'] = i
                    sales_est_dt = pd.concat([sales_est_dt, temp_sales_est_df])

                    acquired.append(i)
                
                except Exception as e:
                    temp_error,errors_dt = handle_error_data(i,'Sales Estimates','error',str(e),'sales_est csv')

                time.sleep(0.25)
                
                count += 1
                print(f"Sales: {count}")
                print(i)
        
            # Update outer loop
            acquire = list(set(universe_dt['symbol']) - set(acquired))
            query_num += 1

        # Write File Locally
        sales_est_dt.to_csv("datasets2/sales_est.csv", index=False)
    except Exception as e:
        logging.error(f"Sales est error occurred: {e}", exc_info=True)
    logging.info("Sales Est csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Sales est csv created succesfully"

def generate_ebit_est_csv():
    try:
        # EBIT Estimates
        logging.info("Ebit Est csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        ebit_est_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    ebit_est_url = f"https://finnhub.io/api/v1/stock/ebit-estimate?symbol={i}&freq=annual&token={my_key}"
                    temp_ebit_est_dt = make_api_request(ebit_est_url,"json_loads","ebit_est csv")
                    
                    if temp_ebit_est_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'EBIT Estimates','neither',temp_ebit_est_dt['symbol'],'ebit_est csv')
                    
                    temp_ebit_est_dt = pd.DataFrame(temp_ebit_est_dt['data'])
                    
                    if not temp_ebit_est_dt.empty:
                        temp_ebit_est_dt['period'] = pd.to_datetime(temp_ebit_est_dt['period'])  # Convert to Timestamp
                        temp_ebit_est_dt = temp_ebit_est_dt[temp_ebit_est_dt['period'] > pd.Timestamp.today()]
                        if temp_ebit_est_dt.empty:
                            temp_error,errors_dt = handle_no_data(i,'EBIT Estimates','neither','No data after restricting period','ebit_est csv')
                    else:
                        temp_error,errors_dt = handle_no_data(i,'EBIT Estimates','neither','No data','ebit_est csv')
                    
                    temp_ebit_est_dt['ticker'] = i
                    # Add to DataFrame
                    ebit_est_dt = pd.concat([ebit_est_dt, temp_ebit_est_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    print(f"Caught an error! {i},{str(e)}")
                    temp_error,errors_dt = handle_error_data(i,'EBIT Estimates','error',str(e),'ebit_est csv')
                        
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
        ebit_est_dt.to_csv("datasets2/ebit_est.csv", index=False)
    except Exception as e:
        logging.error(f"Ebit Est Error occurred: {e}", exc_info=True)
    logging.info("Ebit Est csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Ebit Est csv created succesfully"

def generate_ebitda_est_csv():
    try:
        # EBITDA Estimates
        logging.info("Ebitda Est csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        ebitda_est_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    ebitda_est_url = f"https://finnhub.io/api/v1/stock/ebitda-estimate?symbol={i}&freq=annual&token={my_key}"
                    temp_ebitda_est_dt = make_api_request(ebitda_est_url,"json_loads","ebitda_est csv")
                    
                    if temp_ebitda_est_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'EBITDA Estimates','neither',temp_ebitda_est_dt['symbol'],'ebitda_est csv')
                    temp_ebitda_est_dt = pd.DataFrame(temp_ebitda_est_dt['data'])
                    
                    if not temp_ebitda_est_dt.empty:
                        temp_ebitda_est_dt['period'] = pd.to_datetime(temp_ebitda_est_dt['period'])  # Convert to Timestamp
                        temp_ebitda_est_dt = temp_ebitda_est_dt[temp_ebitda_est_dt['period'] > pd.Timestamp.today()]
                        if temp_ebitda_est_dt.empty:
                            temp_error,errors_dt = handle_no_data(i,'EBITDA Estimates','neither','No data after restricting period','ebitda_est csv')
                    else:
                        temp_error,errors_dt = handle_no_data(i,'EBITDA Estimates','neither','No data','ebitda_est csv')
                    temp_ebitda_est_dt['ticker'] = i
                    # Add to DataFrame
                    ebitda_est_dt = pd.concat([ebitda_est_dt, temp_ebitda_est_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    temp_error,errors_dt = handle_error_data(i,'EBITDA Estimates','error',str(e),'ebitda_est csv')
                    print(f"Caught an error! {i},{e}")
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
        ebitda_est_dt.to_csv("datasets2/ebitda_est.csv", index=False)
    except Exception as e:
        logging.error(f"Ebitda Est Error occurred: {e}", exc_info=True)
    logging.info("Ebitda Est csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Ebitda Est csv created succesfully"

def generate_eps_est_csv():
    try:
        # EPS Estimates
        logging.info("Eps csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        eps_est_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    eps_est_url = f"https://finnhub.io/api/v1/stock/eps-estimate?symbol={i}&freq=annual&token={my_key}"
                    temp_eps_est_dt = make_api_request(eps_est_url,"json_loads","eps_est csv")
                    
                    if temp_eps_est_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'EPS Estimates','neither',temp_eps_est_dt['symbol'],'eps_est csv')
                    temp_eps_est_dt = pd.DataFrame(temp_eps_est_dt['data'])
                    
                    if not temp_eps_est_dt.empty:
                        temp_eps_est_dt['period'] = pd.to_datetime(temp_eps_est_dt['period'])  # Convert to Timestamp
                        temp_eps_est_dt = temp_eps_est_dt[temp_eps_est_dt['period'] > pd.Timestamp.today()]
                        if temp_eps_est_dt.empty:
                            temp_error,errors_dt = handle_no_data(i,'EPS Estimates','neither','No data after restricting period','eps_est csv')
                    else:
                        temp_error,errors_dt = handle_no_data(i,'EPS Estimates','neither','No data','eps_est csv')
                    temp_eps_est_dt['ticker'] = i
                    # Add to DataFrame
                    eps_est_dt = pd.concat([eps_est_dt, temp_eps_est_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    print(f"Caught an error! {i},{str(e)}")
                    temp_error,errors_dt = handle_error_data(i,'EPS Estimates','error',str(e),'eps_est csv')
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
        eps_est_dt.to_csv("datasets2/eps_est.csv", index=False)
    except Exception as e:
        logging.error(f"Eps Est Error occurred: {e}", exc_info=True)
    logging.info("Eps csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Eps Est csv created succesfully"

def generate_quarterly_is_csv():
    try:
        logging.info("quarterly_is csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        # Quarterly Income Statement
        quarterly_is_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    financials_is_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=ic&freq=quarterly&token={my_key}"
                    temp_financials_is_dt = make_api_request(financials_is_url,"json_loads","quarterly_is csv")
                    if temp_financials_is_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Quarterly Income Statement',temp_financials_is_dt['symbol'],'quarterly_is csv') 
                    temp_financials_is_dt = pd.DataFrame(temp_financials_is_dt['financials'])
                    
                    if temp_financials_is_dt.empty:
                        temp_error,errors_dt = handle_no_data(i,'Quarterly Income Statement','neither','No data','quarterly_is csv') 
                    temp_financials_is_dt['ticker'] = i
                    
                    # Add to DataFrame
                    quarterly_is_dt = pd.concat([quarterly_is_dt, temp_financials_is_dt], ignore_index=True, sort=False)
                    acquired.append(i)

                except Exception as e:
                    print(f"Caught an error! {i},{e}")
                    temp_error,errors_dt = handle_error_data(i,'Quarterly Income Statement','error',str(e),'quarterly_is csv')
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.25)
            
                # Notice
                count += 1
                print(f"QUARTERLY_IS: {count}")
                print(i)
            
            # Update outer loop
            acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
            query_num += 1

        # Write File Locally
        quarterly_is_dt.to_csv("datasets2/quarterly_is.csv", index=False)
    except Exception as e:
        logging.error(f"Quarterly_is error occurred: {e}", exc_info=True)
    logging.info("quarterly_is csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Quarterly_is csv created succesfully"

def generate_annual_is_csv():
    try:
        # Annual Income Statement
        logging.info("Annual_is csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        annual_is_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    financials_is_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=ic&freq=annual&token={my_key}"
                    temp_is_dt = make_api_request(financials_is_url,"json_loads","annual_is csv")
                    if temp_is_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Annual Income Statement','neither',temp_is_dt['symbol'],'annual_is csv')
                    temp_is_dt = pd.DataFrame(temp_is_dt['financials'])
                    
                    if temp_is_dt.empty:
                        temp_error,errors_dt = handle_no_data(i,'Annual Income Statement','neither','No data','annual_is csv')
                    temp_is_dt['ticker'] = i
                    
                    # Add to DataFrame
                    annual_is_dt = pd.concat([annual_is_dt, temp_is_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    print(f"Caught an error! {i},{str(e)}")
                    temp_error,errors_dt = handle_error_data(i,'Annual Income Statement','error',str(e),'annual_is csv')
                    
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.25)
            
                # Notice
                count += 1
                print(f"ANNUAL_IS: {count}")
                print(i)
            
            # Update outer loop
            acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
            query_num += 1

        # Write File Locally
        annual_is_dt.to_csv("datasets2/annual_is.csv", index=False)
    except Exception as e:
        logging.error(f"Annual_is error occurred: {e}", exc_info=True)
    logging.info("Annual_is csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Annual_is csv created succesfully"

def generate_annual_bs_csv():
    try:
        # Annual Balance Sheet
        logging.info("Annual_bs csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        annual_bs_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    financials_bs_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=bs&freq=annual&token={my_key}"
                    temp_bs_dt = make_api_request(financials_bs_url,"json_loads","annual_bs csv")
                    
                    if temp_bs_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Annual Balance Sheet','neither',temp_bs_dt['symbol'],'annual_bs csv')       
                    temp_bs_dt = pd.DataFrame(temp_bs_dt['financials'])
                    
                    if temp_bs_dt.empty:
                        temp_error,errors_dt = handle_no_data(i,'Annual Balance Sheet','neither','No data','annual_bs csv')
                    else:
                        # Keep Last Value
                        temp_bs_dt = temp_bs_dt[temp_bs_dt['period'] == temp_bs_dt['period'].max()]
                    
                    temp_bs_dt['ticker'] = i
                    # Add to DataFrame
                    annual_bs_dt = pd.concat([annual_bs_dt, temp_bs_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    print(f"Caught an error! {i},{str(e)}")
                    temp_error,errors_dt = handle_error_data(i,'Annual Balance Sheet','error',str(e),'annual_bs csv')
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.15)
                # Notice
                count += 1
                print(f"ANNUAL_BS: {count}")
                print(i)
            
            # Update outer loop
            acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
            query_num += 1

        # Write File Locally
        annual_bs_dt.to_csv("datasets2/annual_bs.csv", index=False)
    except Exception as e:
        logging.error(f"Annual_bs error occurred: {e}", exc_info=True)
    logging.info("Annual_bs csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Annual_bs csv created succesfully"

def generate_annual_cf_csv():
    try:
        # Annual Cash Flow Statement
        logging.info("Annual_cf csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        annual_cf_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    financials_cf_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=cf&freq=annual&token={my_key}"
                    temp_cf_dt = make_api_request(financials_cf_url,"json_loads","annual_cf csv")
                    
                    if temp_cf_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Annual Cash Flow Statement','neither',temp_cf_dt['symbol'],'annual_cf csv')
                    temp_cf_dt = pd.DataFrame(temp_cf_dt['financials'])
                    
                    if temp_cf_dt.empty:
                        temp_error,errors_dt = handle_no_data(i,'Annual Cash Flow Statement','neither','No data','annual_cf csv')
                    temp_cf_dt['ticker'] = i
                    
                    # Add to DataFrame
                    annual_cf_dt = pd.concat([annual_cf_dt, temp_cf_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    print(f"Caught an error! {i},{str(e)}")
                    temp_error,errors_dt = handle_error_data(i,'Annual Cash Flow Statement','error',str(e),'annual_cf csv')
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.15)
            
                # Notice
                count += 1
                print(f"ANNUAL_CF: {count}")
                print(i)
            
            # Update outer loop
            acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
            query_num += 1

        # Write File Locally
        annual_cf_dt.to_csv("datasets2/annual_cf.csv", index=False)
    except Exception as e:
        logging.error(f"Annual_cf error occurred: {e}", exc_info=True)
    logging.info("Annual_cf csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Annual_cf csv created succesfully"

def generate_quarterly_cf_csv():
    try:
        # Quarterly Cash Flow Statement
        logging.info("Quarterly_cf csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        quarterly_cf_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    financials_cf_url = f"https://finnhub.io/api/v1/stock/financials?symbol={i}&statement=cf&freq=quarterly&token={my_key}"
                    temp_cf_dt = make_api_request(financials_cf_url,"json_loads","quarterly_cf csv")
                    
                    if temp_cf_dt['symbol'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Quarterly Cash Flow Statement','neither',temp_cf_dt['symbol'],'quarterly_cf csv')
                    temp_cf_dt = pd.DataFrame(temp_cf_dt['financials'])
                    if temp_cf_dt.empty:
                        temp_error,errors_dt = handle_no_data(i,'Quarterly Cash Flow Statement','neither','No data','quarterly_cf csv')
                    temp_cf_dt['ticker'] = i
                    
                    # Add to DataFrame
                    quarterly_cf_dt = pd.concat([quarterly_cf_dt, temp_cf_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    print(f"Caught an error! {i},{str(e)}")
                    temp_error,errors_dt = handle_error_data(i,'Quarterly Cash Flow Statement','error',str(e),'quarterly_cf csv')
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.25)
            
                # Notice
                count += 1
                print(f"QUARTERLY_CF: {count}")
                print(i)
            
            # Update outer loop
            acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
            query_num += 1

        # Write File Locally
        quarterly_cf_dt.to_csv("datasets2/quarterly_cf.csv", index=False)
    except Exception as e:
        logging.error(f"Quarterly_cf error occurred: {e}", exc_info=True)
    logging.info("Quarterly_cf csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Quarterly_cf csv created succesfully"

def generate_profiles_csv():
    try:
        # Profiles Data
        logging.info("Profiles csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        profiles_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        query_num = 1
        count = 0
        temp_error = ""
        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    # Get Data
                    profiles_url = f"https://finnhub.io/api/v1/stock/profile?symbol={i}&token={my_key}"
                    temp_profiles_dt = make_api_request(profiles_url,"json_loads","profiles csv")
                    
                    if "ticker" in temp_profiles_dt and temp_profiles_dt['ticker'] != i:
                        temp_error,errors_dt= handle_acquired_data(i,'Profiles Data','neither',temp_profiles_dt['ticker'],'profiles csv')
                    temp_profiles_dt = pd.DataFrame([temp_profiles_dt])
                    
                    if temp_profiles_dt.empty:
                        temp_error,errors_dt = handle_no_data(i,'Profiles Data','neither','No data','profiles csv')
                    temp_profiles_dt['ticker'] = i
                    
                    # Add to DataFrame
                    profiles_dt = pd.concat([profiles_dt, temp_profiles_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                
                except Exception as e:
                    print(f"Caught an error! {i},{str(e)}")
                    temp_error,errors_dt = handle_error_data(i,'Profiles Data','error',str(e),'profiles csv')
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.15)
            
                # Notice
                count += 1
                print(f"PROFILES: {count}")
                print(i)
            
            # Update outer loop
            acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
            query_num += 1

        # Write File Locally
        profiles_dt.to_csv("datasets2/profiles.csv", index=False)
    except Exception as e:
        logging.error(f"Profiles error occurred: {e}", exc_info=True)
    logging.info("Profiles csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Profiles csv created succesfully"

def generate_candles_csv():
    try:
        # Market Data
        logging.info("Candles csv script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        candles_dt = pd.DataFrame()
        acquired = []
        acquire = universe_dt['symbol'].unique()
        acquire = list(acquire) + ['SPY', 'IWB', 'IWM', 'IWV']  # Add Benchmarks
        query_num = 1
        count = 0
        temp_error = ""
        start_date = (datetime.now() - timedelta(days=5*365)).timestamp()
        end_date = datetime.now().timestamp()

        while query_num <= query_max and len(acquire) > 0:
            for i in acquire:
                try:
                    candles_url = f"https://finnhub.io/api/v1/stock/candle?symbol={i}&resolution=D&from={int(start_date)}&to={int(end_date)}&token={my_key}"
                    temp_candles_data = make_api_request(candles_url,"json_type","candles csv")
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
                        temp_error,errors_dt = handle_no_data(i,'Market Data','neither','No data','candles csv')
                    temp_candles_dt['ticker'] = i
                    candles_dt = pd.concat([candles_dt, temp_candles_dt], ignore_index=True, sort=False)
                    acquired.append(i)
                    
                except Exception as e:
                    print('Caught an error!', i, {str(e)})
                    temp_error,errors_dt = handle_error_data(i,'Market Data','error',str(e),'candles csv')
                    
                # Update errors
                if isinstance(temp_error, pd.DataFrame) and query_num == query_max:
                    errors_dt = pd.concat([errors_dt, temp_error])
            
                # Sleep
                time.sleep(0.75)
            
                # Notice
                count += 1
                print(f"CANDLES: {count}")
                print(i)
            
            # Update outer loop
            acquire = list(set(universe_dt['symbol'].unique()) - set(acquired))
            query_num += 1

        # Write File Locally
        candles_dt.to_csv("datasets2/candles.csv", index=False)
        errors_dt.to_csv("datasets2/errors.csv", index=False)
    except Exception as e:
        logging.error(f"Candles error occurred: {e}", exc_info=True)
    logging.info("Candles csv script End time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    return "Candles csv created succesfully"

# def main():

i_shares_api = ['https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund',
'https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund',
'https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund',
'https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund'
]
ishares_csv_names = ['r1000_index','r2000_index','r3000_index','sp500_index']
logging.info("Read and update 4 csv's script start time:"+datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
for i in range(len(i_shares_api)):
    print(First_four_csvs(i_shares_api[i],ishares_csv_names[i]))

print(generate_universe_csv())
print(generate_rating_csv())
print(generate_target_est_csv())
print(generate_sales_est_csv())
print(generate_ebit_est_csv())
print(generate_ebitda_est_csv())
print(generate_eps_est_csv())
print(generate_quarterly_is_csv())
print(generate_annual_is_csv())
print(generate_annual_bs_csv())
print(generate_annual_cf_csv())
print(generate_quarterly_cf_csv())
print(generate_profiles_csv())
print(generate_candles_csv())

# if __name__ == "__main__":
#     main()






















































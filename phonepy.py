import streamlit as st  # pip install streamlit
import pandas as pd  # pip install pandas
import mysql.connector  # pip install mysql-connector-python
import plotly.express as px  # pip install plotly
import requests 
import json
import os 



#Aggregate Transaction  (path 1)


path_1=r"F:\phone pe pro\data\pulse\data\aggregated\transaction\country\india\state"
aggre_trans_list = os.listdir(path_1)
column_1 = {
    "States": [],
    "Years": [],
    "Quarter": [],
    "Transaction_type": [],
    "Transaction_count": [],
    "Transaction_amount": []
}

for state in aggre_trans_list:
    present_states = os.path.join(path_1, state)   # <-- state is defined here
    aggre_year_list = os.listdir(present_states)

    for year in aggre_year_list:
        present_year = os.path.join(present_states, year)   # <-- year is defined here
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = os.path.join(present_year, file)  # <-- file is defined here

            with open(present_file, "r") as data:
                S = json.load(data)

            if "data" in S and "transactionData" in S["data"]:
                for i in S["data"]["transactionData"]:
                    name = i["name"]
                    count = i["paymentInstruments"][0]["count"]
                    amount = i["paymentInstruments"][0]["amount"]
                    column_1["Transaction_type"].append(name)
                    column_1["Transaction_count"].append(count)
                    column_1["Transaction_amount"].append(amount)
                    column_1["States"].append(state)
                    column_1["Years"].append(year)
                    column_1["Quarter"].append(int(file.strip(".json")))

# Convert to DataFrame
aggre_transaction = pd.DataFrame(column_1)

aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
aggre_transaction["States"] = aggre_transaction["States"].str.replace("-"," ")
aggre_transaction["States"] = aggre_transaction["States"].str.title()
aggre_transaction["States"] = aggre_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(aggre_transaction)}")
print(aggre_transaction.head())



# Aggregate Users (path 2)


path_2="F:/phone pe pro/data/pulse/data/aggregated/user/country/india/state/"
aggre_user_list = os.listdir(path_2)

column_2 = {"States":[], "Years":[], "Quarter":[], "Brands":[], "Transaction_count":[], "Percentage":[]}

for state in aggre_user_list:
    present_states = path_2+state+"/"
    aggre_year_list = os.listdir(present_states)
    
    for year in aggre_year_list:
        present_year = present_states+year+"/"
        aggre_file_list = os.listdir(present_year)
        
        for file in aggre_file_list:
            present_file = present_year+file
            data = open(present_file, "r")
            U = json.load(data)
            
            try:

                for i in U["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count =i["count"]
                    percentage = i["percentage"]
                    column_2["Brands"].append(brand)
                    column_2["Transaction_count"].append(count)
                    column_2["Percentage"].append(percentage)
                    column_2["States"].append(state)
                    column_2["Years"].append(year)
                    column_2["Quarter"].append(int(file.strip(".json")))
            except:
                pass

aggre_user=pd.DataFrame(column_2)

aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user["States"] = aggre_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(aggre_user)}")
print(aggre_user.head())



# Map Transaction (path 3)



path_3="/phone pe pro/data/pulse/data/map/transaction/hover/country/india/state/"
map_trans_list = os.listdir(path_3)

column_3 = {"States":[], "Years":[], "Quarter":[], "District":[], "Transaction_count":[], "Transaction_amount":[]}

for state in map_trans_list:
    present_states = path_3+state+"/"
    map_year_list = os.listdir(present_states)
    
    for year in map_year_list:
        present_year = present_states+year+"/"
        map_file_list = os.listdir(present_year)
        
        for file in map_file_list:
            present_file = present_year+file
            data = open(present_file, "r")
            D = json.load(data)
            
            for i in D["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                column_3["District"].append(name)
                column_3["Transaction_count"].append(count)
                column_3["Transaction_amount"].append(amount)
                column_3["States"].append(state)
                column_3["Years"].append(year)
                column_3["Quarter"].append(int(file.strip(".json")))

map_transaction=pd.DataFrame(column_3)

map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction["States"] = map_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(map_transaction)}")
print(map_transaction.head())



# Map User (path 4)



path_4="/phone pe pro/data/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path_4)

column_4 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

for state in map_user_list:
    present_states = path_4+state+"/"
    map_year_list = os.listdir(present_states)
    
    for year in map_year_list:
        present_year = present_states+year+"/"
        map_file_list = os.listdir(present_year)
        
        for file in map_file_list:
            present_file=present_year+file
            data=open(present_file, "r")
            H = json.load(data)
            
            for i in H["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                column_4["Districts"].append(district)
                column_4["RegisteredUser"].append(registereduser)
                column_4["AppOpens"].append(appopens)
                column_4["States"].append(state)
                column_4["Years"].append(year)
                column_4["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(column_4)

map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user["States"] = map_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(map_user)}")
print(map_user.head())


# Top Transactions (path 5)



path_5="/phone pe pro/data/pulse/data/top/transaction/country/india/state/"
top_trans_list = os.listdir(path_5)

column_5 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_trans_list:
    present_states = path_5+state+"/"
    top_trans_list = os.listdir(present_states)
       
    for year in top_trans_list:
        present_year = present_states+year+"/"
        top_file_list = os.listdir(present_year)
                
        for file in top_file_list:
            present_file = present_year+file
            data = open(present_file, "r")
            A = json.load(data)
            
            for i in A["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                column_5["Pincodes"].append(entityName)
                column_5["Transaction_count"].append(count)
                column_5["Transaction_amount"].append(amount)
                column_5["States"].append(state)
                column_5["Years"].append(year)
                column_5["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(column_5)

top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction["States"] = top_transaction["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(top_transaction)}")
print(top_transaction.head())



# Top User (path 6)


path_6 = "/phone pe pro/data/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path_6)

column_6 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in top_user_list:
    present_states = path_6+state+"/"
    top_year_list = os.listdir(present_states)

    for year in top_year_list:
        present_year = present_states+year+"/"
        top_file_list = os.listdir(present_year)

        for file in top_file_list:
            present_file = present_year+file
            data = open(present_file,"r")
            K = json.load(data)

            for i in K["data"]["pincodes"]:
                name = i["name"]
                registereduser = i["registeredUsers"]
                column_6["Pincodes"].append(name)
                column_6["RegisteredUser"].append(registereduser)
                column_6["States"].append(state)
                column_6["Years"].append(year)
                column_6["Quarter"].append(int(file.strip(".json")))


top_user = pd.DataFrame(column_6)

top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()
top_user["States"] = top_user["States"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

print(f" Extracted rows: {len(top_user)}")
print(top_user.head())


# aggregated insurance (path 7)


path_aggre_insurance = "/phone pe pro/data/pulse/data/aggregated/insurance/country/india/state/"

# Initialize column structure
column_aggre_insurance = {
    "States": [], "Years": [], "Quarter": [],
    "Insurance_type": [], "Total_count": [], "Total_amount": []
}

# Get list of state folders
aggre_insurance_list = os.listdir(path_aggre_insurance)

for state_raw in aggre_insurance_list:
    state_path = os.path.join(path_aggre_insurance, state_raw)
    if not os.path.isdir(state_path):
        continue  # Skip if not a directory

    # Normalize state name
    state = state_raw.replace("andaman-&-nicobar-islands", "Andaman and Nicobar") \
                     .replace("dadra-&-nagar-haveli-&-daman-&-diu", "Dadra and Nagar Haveli and Daman Diu") \
                     .replace("-", " ") \
                     .title()

    year_list = os.listdir(state_path)

    for year in year_list:
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue  # Skip if not a directory

        file_list = os.listdir(year_path)
        for file in file_list:
            file_path = os.path.join(year_path, file)

            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                # Safely extract insurance transaction data
                transaction_data = data.get("data", {}).get("transactionData", [])
                for txn in transaction_data:
                    if txn.get("name") == "Insurance":
                        instruments = txn.get("paymentInstruments", [])
                        if isinstance(instruments, list) and len(instruments) > 0:
                            count = instruments[0].get("count", 0)
                            amount = instruments[0].get("amount", 0)

                            column_aggre_insurance["States"].append(state)
                            column_aggre_insurance["Years"].append(int(year))
                            column_aggre_insurance["Quarter"].append(int(file.replace(".json", "")))
                            column_aggre_insurance["Insurance_type"].append("Total")
                            column_aggre_insurance["Total_count"].append(count)
                            column_aggre_insurance["Total_amount"].append(amount)

            except Exception as e:
                print(f" Error processing file: {file_path} — {e}")

#  Convert dict → DataFrame
aggre_insurance = pd.DataFrame(column_aggre_insurance)

print(f" Extracted rows: {len(aggre_insurance)}")
print(aggre_insurance.head())



# map insurance data  (path 8)


path_map_insurance = "/phone pe pro/data/pulse/data/map/insurance/hover/country/india/state/"

# Initialize structure
column_map_insurance = {
    "States": [], "Districts": [], "Years": [], "Quarter": [],
    "Insurance_Category": [], "Transaction_count": [], "Transaction_amount": []
}

# Loop through states
map_insurance_list = os.listdir(path_map_insurance)

for state_raw in map_insurance_list:
    state_path = os.path.join(path_map_insurance, state_raw)
    if not os.path.isdir(state_path):
        continue

    # Normalize state name
    state = state_raw.replace("andaman-&-nicobar-islands", "Andaman and Nicobar") \
                     .replace("dadra-&-nagar-haveli-&-daman-&-diu", "Dadra and Nagar Haveli and Daman Diu") \
                     .replace("-", " ") \
                     .title()

    # Loop through years
    year_list = os.listdir(state_path)
    for year in year_list:
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        # Loop through quarters (files)
        file_list = os.listdir(year_path)
        for file in file_list:
            file_path = os.path.join(year_path, file)

            try:
                with open(file_path, "r") as f:
                    MI = json.load(f)

                hover_data_list = MI.get("data", {}).get("hoverDataList", [])

                for item in hover_data_list:
                    district = item.get("name")
                    metric = item.get("metric")

                    if isinstance(metric, list) and len(metric) > 0:
                        count = metric[0].get("count", 0)
                        amount = metric[0].get("amount", 0)

                        column_map_insurance["Districts"].append(district)
                        column_map_insurance["Insurance_Category"].append("TOTAL")
                        column_map_insurance["Transaction_count"].append(count)
                        column_map_insurance["Transaction_amount"].append(amount)
                        column_map_insurance["States"].append(state)
                        column_map_insurance["Years"].append(int(year))
                        column_map_insurance["Quarter"].append(int(file.replace(".json", "")))

            except Exception as e:
                print(f" Error processing {file_path}: {e}")

#  Convert dict → DataFrame
map_insurance = pd.DataFrame(column_map_insurance)

# Check extracted data
print(f" Extracted rows: {len(map_insurance)}")
print(map_insurance.head())



#  top insurance (path 9)



path_top_insurance = "/phone pe pro/data/pulse/data/top/insurance/country/india/state/"

# Initialize column structure
column_top_insurance = {
    "States": [], "Years": [], "Quarter": [], "Pincodes": [],
    "Insurance_Category": [], "Transaction_count": [], "Transaction_amount": []
}

# Loop through states
top_insurance_list = os.listdir(path_top_insurance)

for state_raw in top_insurance_list:
    state_path = os.path.join(path_top_insurance, state_raw)
    if not os.path.isdir(state_path):
        continue

    # Normalize state name
    state = state_raw.replace("andaman-&-nicobar-islands", "Andaman and Nicobar") \
                     .replace("dadra-&-nagar-haveli-&-daman-&-diu", "Dadra and Nagar Haveli and Daman Diu") \
                     .replace("-", " ") \
                     .title()

    # Loop through years
    year_list = os.listdir(state_path)
    for year in year_list:
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        # Loop through quarters (files)
        file_list = os.listdir(year_path)
        for file in file_list:
            file_path = os.path.join(year_path, file)

            try:
                with open(file_path, "r") as f:
                    TI = json.load(f)

                # Extract pincode-level data
                pincode_data = TI.get("data", {}).get("pincodes", [])
                for item in pincode_data:
                    pincode = item.get("entityName")
                    metric = item.get("metric", {})

                    count = metric.get("count", 0)
                    amount = metric.get("amount", 0)

                    column_top_insurance["Pincodes"].append(pincode)
                    column_top_insurance["Insurance_Category"].append("TOTAL")  # Always total
                    column_top_insurance["Transaction_count"].append(count)
                    column_top_insurance["Transaction_amount"].append(amount)
                    column_top_insurance["States"].append(state)
                    column_top_insurance["Years"].append(int(year))
                    column_top_insurance["Quarter"].append(int(file.replace(".json", "")))

            except Exception as e:
                print(f" Error processing {file_path}: {e}")

#  Convert to DataFrame
top_insurance = pd.DataFrame(column_top_insurance)

# Check extracted data
print(f" Extracted rows: {len(top_insurance)}")
print(top_insurance.head())



import mysql.connector

host="localhost"
user="root"
password="Yogapriya1234"
database="phonepe_db"

mydb=mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
cursor = mydb.cursor() 

# Create Aggregate Transaction Table

"""Aggregate_Transaction_Table1 = '''CREATE TABLE if not exists aggregate_transaction (States varchar(100),
                                                                   Years int,
                                                                   Quarter int,
                                                                   Transaction_type varchar(100),
                                                                   Transaction_count bigint,
                                                                   Transaction_amount bigint
                                                                   )'''
cursor.execute(Aggregate_Transaction_Table1)
mydb.commit()

for index, row in aggre_transaction.iterrows():
    insert_query1 = '''INSERT INTO aggregate_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                            values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Transaction_type"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_query1,values)
    mydb.commit()"""

    # Create Map Transaction Table

"""Map_Transaction_Table2 = '''CREATE TABLE if not exists map_transaction(States varchar(100),
                                                               Years int,
                                                               Quarter int,
                                                               District varchar(100),
                                                               Transaction_count bigint,
                                                               Transaction_amount float)'''
cursor.execute(Map_Transaction_Table2)
mydb.commit()

for index, row in map_transaction.iterrows():
            insert_query2 = '''INSERT INTO map_transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                               values (%s,%s,%s,%s,%s,%s)'''

            values = (
                row["States"],
                row["Years"],
                row["Quarter"],
                row["District"],
                row["Transaction_count"],
                row["Transaction_amount"]
            )
            cursor.execute(insert_query2, values)
            mydb.commit()"""

# Create Map User Table

"""Map_User_Table3 = '''CREATE TABLE if not exists map_user (States varchar(100),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(100),
                                                        RegisteredUser bigint,
                                                        AppOpens bigint)'''
cursor.execute(Map_User_Table3)
mydb.commit()

for index, row in map_user.iterrows():
    insert_query3 = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                        values (%s,%s,%s,%s,%s,%s)'''
    
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["RegisteredUser"],
              row["AppOpens"])
    cursor.execute(insert_query3, values)
    mydb.commit()"""

# Create Top Transaction Table

"""Top_Transaction_Table4 = '''CREATE TABLE if not exists top_transaction (States varchar(100),
                                                                Years int,
                                                                Quarter int,
                                                                Pincodes int,
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''
cursor.execute(Top_Transaction_Table4)
mydb.commit()

for index, row in top_transaction.iterrows():
    insert_query4 = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["Transaction_count"],
              row["Transaction_amount"])
    cursor.execute(insert_query4, values)
    mydb.commit()"""

# Create Top User Table

"""Top_User_Table5 = '''CREATE TABLE if not exists top_user (States varchar(100),
                                                        Years int,
                                                        Quarter int,
                                                        Pincodes int,
                                                        RegisteredUser bigint
                                                        )'''
cursor.execute(Top_User_Table5)
mydb.commit()

for index, row in top_user.iterrows():
    insert_query5 = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                                            values(%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["RegisteredUser"])
    cursor.execute(insert_query5, values)
    mydb.commit()"""

# Create Map Insurance Table

"""Map_Insurance_Table6 = '''CREATE TABLE if not exists map_insurance (
                        States varchar(100),
                        Districts varchar(100),
                        Years int,
                        Quarter int,
                        Insurance_Category varchar(100),
                        Transaction_count bigint,
                        Transaction_amount bigint
                        )'''
cursor.execute(Map_Insurance_Table6)
mydb.commit()

for index, row in map_insurance.iterrows():
    insert_query6 = '''INSERT INTO map_insurance (States, Districts, Years, Quarter, Insurance_Category, Transaction_count, Transaction_amount)
                                values(%s,%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Districts"],
              row["Years"],
              row["Quarter"],
              row["Insurance_Category"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_query6, values)
    mydb.commit()"""

# Create Top Insurance Table

"""Top_Insurance_Table7 = '''CREATE TABLE if not exists top_insurance (
                        States varchar(100),
                        Years int,
                        Quarter int,
                        Pincodes varchar(20),
                        Insurance_Category varchar(100),
                        Transaction_count bigint,
                        Transaction_amount bigint
                        )'''
cursor.execute(Top_Insurance_Table7)
mydb.commit()

for index, row in top_insurance.iterrows():
    insert_query7 = '''INSERT INTO top_insurance (States, Years, Quarter, Pincodes, Insurance_Category, Transaction_count, Transaction_amount)
                                values(%s,%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["Insurance_Category"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_query7, values)
    mydb.commit()"""

# Create Aggregated Insurance Table

"""aggre_insurance_Table8 = '''CREATE TABLE IF NOT EXISTS aggregated_insurance (
                                States VARCHAR(100),
                                Years INT,
                                Quarter INT,
                                Insurance_type VARCHAR(255),
                                Total_count BIGINT,
                                Total_amount DOUBLE
                                )'''
cursor.execute(aggre_insurance_Table8)
mydb.commit()

# Insert data into Aggregated Insurance Table
for index, row in aggre_insurance.iterrows():
    insert_query8 = '''INSERT INTO aggregated_insurance (States, Years, Quarter, Insurance_type, Total_count, Total_amount)
                                VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Insurance_type"],
              row["Total_count"],
              row["Total_amount"]
              )
    cursor.execute(insert_query8, values)
    mydb.commit()"""

# Create Aggregated user Table

"""aggre_user_Table9 = '''CREATE TABLE IF NOT EXISTS aggregate_user (
                        Brands varchar(100),
                        Transaction_count bigint,
                        Percentage float,
                        States varchar(100),
                        Years int,
                        Quarter int                                                                    
                        )'''
cursor.execute(aggre_user_Table9)
mydb.commit()

#Insert data from aggre_user DataFrame
for index, row in aggre_user.iterrows():
    insert_query9 = '''INSERT INTO aggregate_user
        (Brands, Transaction_count, Percentage, States, Years, Quarter)
        VALUES (%s, %s, %s, %s, %s, %s)'''
    
    values = (row["Brands"],
              row["Transaction_count"],
              row["Percentage"],
              row["States"],
              row["Years"],
              row["Quarter"]
              )
    cursor.execute(insert_query9, values)
    mydb.commit()"""


import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import requests, json

# ------------------------------
# DB Connection
# ------------------------------
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Yogapriya1234",
        database="phonepe_db"
    )

def execute_query(query, params=None):
    try:
        with get_db_connection() as mydb:
            cursor = mydb.cursor(dictionary=True)
            cursor.execute(query, params or ())
            result = cursor.fetchall()
        return pd.DataFrame(result)
    except mysql.connector.Error as err:
        st.error(f"Error executing query: {err}")
        return pd.DataFrame()

# ------------------------------
# Global Config
# ------------------------------
st.set_page_config(
    page_title="PhonePe Dashboard",
    page_icon=r"c:\Users\Admin\Downloads\PhonePe Logo.jpg",
    layout="wide"
)

GEO_URL = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
geo = json.loads(requests.get(GEO_URL).content)
quarter_map = {
    "Q1 (Jan-Mar)": 1,
    "Q2 (Apr-Jun)": 2,
    "Q3 (Jul-Sep)": 3,
    "Q4 (Oct-Dec)": 4
}
# Query wrapper
def run_query(query, params=None):
    conn = get_db_connection()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# ------------------------------
# Navigation
# ------------------------------
if st.sidebar.button("🏠︎ Home"):
    st.session_state.page = "Home"
elif st.sidebar.button("📋 Business Case Study"):
    st.session_state.page = "Business Case Study"
elif st.sidebar.button("💡 Case Study Insights"):
    st.session_state.page = "Case Study Insights"
# ------------------------------
# HOME PAGE
# ------------------------------
import streamlit as st
from PIL import Image
import base64
from io import BytesIO

# Initialize session state
if "page" not in st.session_state:
    st.session_state.page = "Home"

# --- Home ---

# Load the logo image
    logo_path = r"C:\Users\Admin\Downloads\Interior Space Designer.jpg"
    logo = Image.open(logo_path)

    # Convert image to base64
    buffered = BytesIO()
    logo.save(buffered, format="JPEG")
    logo_base64 = base64.b64encode(buffered.getvalue()).decode()

    # Display logo + title + subtitle with PhonePe theme
    st.markdown(f"""
        <style>
        .home-title-container {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 15px;
            margin-bottom: 1rem;
        }}
        .home-title-text {{
            font-size: 3rem;
            font-weight: bold;
            color: #6C63FF; /* Purple title */
        }}
        .home-subtitle {{
            text-align: center;
            font-size: 1.2rem;
            color: var(--text-color); /* Streamlit theme-aware */
            margin-bottom: 2rem;
        }}
        .info-box {{
            background-color: #F0F4FF;
            border-left: 6px solid #6C63FF;
            padding: 1rem 1.5rem;
            margin-bottom: 1rem;
            border-radius: 8px;
            font-size: 1rem;
            color: #222;
        }}
        .highlight {{
            color: #6C63FF;
            font-weight: bold;
        }}
        </style>

        <div class="home-title-container">
            <img src="data:image/jpeg;base64,{logo_base64}" style="height:60px;">
            <div class="home-title-text"> PhonePe Transaction Insights</div>
        </div>
        <div class="home-subtitle">Explore India's Digital Economy with Real-time Data</div>
        <hr style='border: 1px solid #6C63FF;'>
    """, unsafe_allow_html=True)

# insights 
    st.markdown("### 🔍 outcomes")

    st.markdown("""
    <style>
        .info-card {
            background: #f9f8ff; /* light lavender background */
            border-radius: 15px;
            padding: 20px;
            margin: 12px 0;
            box-shadow: 0 4px 12px rgba(108, 99, 255, 0.12); /* purple shadow */
            transition: all 0.3s ease;
            font-size: 1rem;
            color: #2d2d2d; /* dark gray text */
            font-family: 'Segoe UI', sans-serif;
            border-left: 6px solid #6C63FF; /* purple accent bar */
        }
        .info-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 20px rgba(108, 99, 255, 0.25);
        }
        .highlight {
            font-weight: 600;
            color: #6C63FF; /* professional purple */
        }
        .emoji {
            font-size: 1.4rem;
            margin-right: 10px;
        }
    </style>

    <div class="info-card">💳 <span class='highlight'>Aggregated Transactions:</span> View transaction volumes by type, region, and trends over time.</div>
    <div class="info-card">👩🏻‍💻 <span class='highlight'>User Engagement:</span> Explore how users interact with the app across brands, states, and districts.</div>
    <div class="info-card">🏛️ <span class='highlight'>Insurance Analytics:</span> Analyze state-wise and district-level adoption of insurance services.</div>
    <div class="info-card">🏞️ <span class='highlight'>Interactive Visuals:</span> Choropleths, bar charts, line graphs, and pie charts to bring data to life.</div>
    <div class="info-card">🎨 <span class='highlight'>Custom Filters:</span> Filter insights by year and quarter across the country.</div>
""", unsafe_allow_html=True)




# ------------------------------
# BUSINESS CASE STUDY
# ------------------------------

elif st.session_state.page == "Business Case Study":
    st.title("📋 Business Case Study")
    sub_tab = st.selectbox("Explore analytics by:", ["Transaction", "User", "Insurance"])

    if sub_tab == "Transaction":
        st.write("💳 Transaction Analytics Here...")
        # 👉 Place your Transaction SQL + charts code here

    elif sub_tab == "User":
        st.write("👩🏻‍💻 User Analytics Here...")
        # 👉 Place your User SQL + charts code here

    elif sub_tab == "Insurance":
        st.write("🗒️ Insurance Analytics Here...")
        # 👉 Place your Insurance SQL + charts code here

    st.title("⚙️ Insight Use Case")
    sub_tab = st.radio("Choose Analysis", ["Transaction", "User", "Insurance"])

    # Year & Quarter Dropdown (once only)
    years = ["All"] + [str(y) for y in range(2018, 2025)]
    quarters = ["All"] + list(quarter_map.keys())

    col1, col2 = st.columns(2)
    selected_year = col1.selectbox("Select Year", years)
    selected_quarter = col2.selectbox("Select Quarter", quarters)

    year = int(selected_year) if selected_year != "All" else None
    quarter = quarter_map[selected_quarter] if selected_quarter != "All" else None

    conditions = []
    params = []

    if year is not None:
        conditions.append("Years = %s")
        params.append(year)
    if quarter is not None:
        conditions.append("Quarter = %s")
        params.append(quarter)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    if sub_tab == "Transaction":
        st.subheader("💳 Transaction Overview")

        df = execute_query(f"""
            SELECT SUM(Transaction_count) AS TotalTransactions,
                   AVG(Transaction_count) AS AvgTransactions,
                   SUM(Transaction_amount) AS TotalRevenue,
                   AVG(Transaction_amount) AS AvgRevenue
            FROM aggregate_transaction
            {where_clause}
        """, tuple(params))

        col1, col2 = st.columns(2)
        col1.metric("Total Transactions", f"{df['TotalTransactions'][0]:,.0f}")
        col1.metric("Average Transactions", f"{df['AvgTransactions'][0]:,.2f}")
        col2.metric("Total Revenue (₹)", f"{df['TotalRevenue'][0]:,.2f}")
        col2.metric("Avg Revenue (₹)", f"{df['AvgRevenue'][0]:,.2f}")

        df_map = execute_query(f"""
            SELECT States, SUM(Transaction_count) AS TotalTransactions
            FROM map_transaction
            {where_clause}
            GROUP BY States
        """, tuple(params))

        fig = px.choropleth(df_map, geojson=geo, locations="States",
                            featureidkey="properties.ST_NM", color="TotalTransactions",
                            color_continuous_scale="Reds", title="State-wise Total Transactions")
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### 📌 Top 10 States by Transaction Volume")
        df_top = df_map.sort_values(by="TotalTransactions", ascending=False).head(10)
        st.dataframe(df_top, use_container_width=True)

    elif sub_tab == "User":
        st.subheader("📱 User Engagement and Growth Strategy")

        df_total = execute_query(f"""
            SELECT SUM(RegisteredUser) as TotalUsers, SUM(AppOpens) as TotalOpens
            FROM map_user {where_clause}
        """, tuple(params))

        if not df_total.empty:
            st.metric("Total Registered Users", f"{int(df_total.iloc[0]['TotalUsers']):,}")
            st.metric("Total App Opens", f"{int(df_total.iloc[0]['TotalOpens']):,}")

        tab1, tab2, tab3 = st.tabs(["States", "Districts", "Pincodes"])

        with tab1:
            df_states = execute_query(f"""
                SELECT States, SUM(RegisteredUser) as TotalUsers
                FROM map_user {where_clause}
                GROUP BY States ORDER BY TotalUsers DESC LIMIT 10
            """, tuple(params))
            if not df_states.empty:
                st.markdown("#### 🏆 Top 10 States by Registered Users")
                st.dataframe(df_states, use_container_width=True)

        with tab2:
            df_districts = execute_query(f"""
                SELECT Districts, SUM(RegisteredUser) as TotalUsers
                FROM map_user {where_clause}
                GROUP BY Districts ORDER BY TotalUsers DESC LIMIT 10
            """, tuple(params))
            if not df_districts.empty:
                st.markdown("#### 🏆 Top 10 Districts by Registered Users")
                st.dataframe(df_districts, use_container_width=True)

        with tab3:
            df_pincodes = execute_query(f"""
                SELECT Pincodes, SUM(RegisteredUser) as TotalUsers
                FROM top_user {where_clause}
                GROUP BY Pincodes ORDER BY TotalUsers DESC LIMIT 10
            """, tuple(params))
            if not df_pincodes.empty:
                st.markdown("#### 🏆 Top 10 Pincodes by Registered Users")
                st.dataframe(df_pincodes, use_container_width=True)

    elif sub_tab == "Insurance":
        st.subheader("🏛️ Insurance Engagement Insights")

        df_total = execute_query(f"""
            SELECT SUM(Transaction_count) as TotalTransactions, SUM(Transaction_amount) as TotalAmount
            FROM map_insurance {where_clause}
        """, tuple(params))

        if not df_total.empty:
            total_transactions = df_total.iloc[0].get('TotalTransactions')
            total_amount = df_total.iloc[0].get('TotalAmount')

            st.metric(
                "Total Insurance Transactions",
                f"{int(total_transactions):,}" if total_transactions is not None else "data unavailable"
            )

            st.metric(
                "Total Insurance Amount (₹)",
                f"{int(total_amount):,}" if total_amount is not None else "data unavailable"
            )
        else:
            st.metric("Total Insurance Transactions", "data unavailable")
            st.metric("Total Insurance Amount(₹)","data unavailable")

        tab1, tab2, tab3 = st.tabs(["States", "Districts", "Pincodes"])

        with tab1:
            df_states = execute_query(f"""
                SELECT States, SUM(Transaction_count) as TotalTransactions
                FROM map_insurance {where_clause}
                GROUP BY States ORDER BY TotalTransactions DESC LIMIT 10
            """, tuple(params))
            if not df_states.empty:
                st.markdown("#### 🏆 Top 10 States by Insurance Transactions")
                st.dataframe(df_states, use_container_width=True)

        with tab2:
            df_districts = execute_query(f"""
                SELECT Districts, SUM(Transaction_count) as TotalTransactions
                FROM map_insurance {where_clause}
                GROUP BY Districts ORDER BY TotalTransactions DESC LIMIT 10
            """, tuple(params))
            if not df_districts.empty:
                st.markdown("#### 🏆 Top 10 Districts by Insurance Transactions")
                st.dataframe(df_districts, use_container_width=True)

        with tab3:
            df_pincodes = execute_query(f"""
                SELECT Pincodes, SUM(Transaction_count) as TotalTransactions
                FROM top_insurance {where_clause}
                GROUP BY Pincodes ORDER BY TotalTransactions DESC LIMIT 10
            """, tuple(params))
            if not df_pincodes.empty:
                st.markdown("#### 🏆 Top 10 Pincodes by Insurance Transactions")
                st.dataframe(df_pincodes, use_container_width=True)


# Case Study Insights
elif st.session_state.page == "Case Study Insights":
    st.title("📈 Case Study Insights")
    st.write("Insights and reports of the analysis")
    st.title("📈 Case Study  Dashboard")

    # Year and Quarter Filters
    years = ["All"] + [str(y) for y in range(2018, 2025)]
    quarters = ["All"] + list(quarter_map.keys())

    col1, col2 = st.columns(2)
    selected_year = col1.selectbox("Select Year", years)
    selected_quarter = col2.selectbox("Select Quarter", quarters)

    year = int(selected_year) if selected_year != "All" else None
    quarter = quarter_map[selected_quarter] if selected_quarter != "All" else None

    conditions = []
    params = []

    if year is not None:
        conditions.append("Years = %s")
        params.append(year)
    if quarter is not None:
        conditions.append("Quarter = %s")
        params.append(quarter)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    case_option = st.selectbox("Select Case Study", [
        "Decoding Transaction Dynamics",
        "Device Dominance and User Engagement",
        "Insurance Penetration and Growth Potential",
        "Transaction Analysis for Market Expansion",
        "User Engagement and Growth Strategy",
        
    ])

    # ------------------- 1️⃣ Decoding Transaction Dynamics -------------------
    if case_option == "Decoding Transaction Dynamics":
        df_type = execute_query(f"SELECT Transaction_type, SUM(Transaction_count) AS TotalCount FROM aggregate_transaction {where_clause} GROUP BY Transaction_type", tuple(params))
        st.plotly_chart(px.bar(df_type, x="Transaction_type", y="TotalCount", title="Transactions by Type", color="Transaction_type",color_discrete_sequence=["#6A0DAD", "#FFD700", "#1E3A8A"]
))

        df_map = execute_query(f"SELECT States, SUM(Transaction_count) AS TotalTransactions FROM map_transaction {where_clause} GROUP BY States", tuple(params))
        fig_map = px.choropleth(df_map, geojson=geo, locations="States", featureidkey="properties.ST_NM", color="TotalTransactions", title="State-wise Transaction Volume", color_continuous_scale="Sunset")
        fig_map.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig_map)

        df_trend = execute_query(f"SELECT Years, SUM(Transaction_amount) AS Amount FROM aggregate_transaction {where_clause} GROUP BY Years", tuple(params))
        st.plotly_chart(px.line(df_trend, x="Years", y="Amount", markers=True, title="Transaction Trend Over Years", color_discrete_sequence=["orange"]))

        df_top = execute_query(f"SELECT States, SUM(Transaction_count) AS TotalCount FROM map_transaction {where_clause} GROUP BY States ORDER BY TotalCount DESC LIMIT 10", tuple(params))
        st.plotly_chart(px.bar(df_top, x="States", y="TotalCount", title="Top 10 States by Transactions", color="States",  color_discrete_sequence=px.colors.sequential.Agsunset))



    # ------------------- 2️⃣ Device Dominance and User Engagement -------------------
    elif case_option == "Device Dominance and User Engagement":
        df_users = execute_query(f"SELECT Brands, SUM(Transaction_count) AS Users FROM aggregate_user {where_clause} GROUP BY Brands", tuple(params))
        st.plotly_chart(px.bar(df_users, x="Brands", y="Users", title="Users by Device Brand", color="Brands", color_discrete_sequence=px.colors.qualitative.Set1))

        df_opens = execute_query(f"""
            SELECT au.Brands, SUM(mu.AppOpens) AS AppOpens
            FROM aggregate_user au
            JOIN map_user mu ON au.States = mu.States AND au.Years = mu.Years AND au.Quarter = mu.Quarter
            {where_clause.replace('Years', 'au.Years').replace('Quarter', 'au.Quarter')}
            GROUP BY au.Brands
        """, tuple(params))
        st.plotly_chart(px.bar(df_opens, x="Brands", y="AppOpens", title="App Opens by Device Brand", color="Brands", color_discrete_sequence=px.colors.qualitative.Dark2))

    # ------------------- 3️⃣ Insurance Penetration and Growth Potential -------------------

    elif case_option == "Insurance Penetration and Growth Potential":
        df_state_yearly = execute_query(
            f"SELECT States, Years, SUM(Total_count) AS TotalCount FROM aggregated_insurance {where_clause} GROUP BY States, Years",
            tuple(params)
        )
        if not df_state_yearly.empty and "TotalCount" in df_state_yearly.columns:
            fig = px.bar(
                df_state_yearly.sort_values(by="TotalCount", ascending=False),
                x="States", y="TotalCount", color="Years",
                title="Insurance Transactions by State and Year",
                barmode="group", color_discrete_sequence=px.colors.sequential.Viridis
            )
            st.plotly_chart(fig)
        else:
            st.warning("No data available for Insurance Transactions by State and Year.")

        df_avg_state = execute_query(
            f"SELECT States, AVG(Total_count) AS AvgCount FROM aggregated_insurance {where_clause} GROUP BY States",
            tuple(params)
        )
        if not df_state_yearly.empty and "TotalCount" in df_state_yearly.columns:
            st.plotly_chart(
                px.pie(
                    df_avg_state.sort_values(by="AvgCount", ascending=True).head(10),
                    names="States", values="AvgCount",
                    title="Least Penetrated States by Avg Yearly Count"
                )
            )
        else:
            st.warning("No data available for Least Penetrated States by Avg Yearly Count.")
    # ------------------- 4️⃣ Transaction Analysis for Market Expansion -------------------

    elif case_option == "Transaction Analysis for Market Expansion":
        df_amount = execute_query(f"SELECT States, SUM(Transaction_amount) AS Amount FROM aggregate_transaction {where_clause} GROUP BY States", tuple(params))
        st.plotly_chart(px.bar(df_amount.sort_values(by="Amount", ascending=False), x="States", y="Amount", title="States by Transaction Value", color_discrete_sequence=px.colors.sequential.Plasma))

        df_yearwise = execute_query(f"SELECT Years, SUM(Transaction_amount) AS Total FROM aggregate_transaction {where_clause} GROUP BY Years", tuple(params))
        st.plotly_chart(px.scatter(df_yearwise, x="Years", y="Total", title="Transaction Value Over Years", color_discrete_sequence=["#E41193"]))

        df_map_amt = execute_query(f"SELECT States, SUM(Transaction_amount) AS TotalAmount FROM map_transaction {where_clause} GROUP BY States", tuple(params))
        fig = px.choropleth(df_map_amt, geojson=geo, locations="States", featureidkey="properties.ST_NM", color="TotalAmount", color_continuous_scale="Purples", title="State-wise Market Value Map")
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)
    # ------------------- 5️⃣ User Engagement and Growth Strategy -------------------

    elif case_option == "User Engagement and Growth Strategy":
        df_app = execute_query(f"SELECT States, SUM(AppOpens) AS Opens FROM map_user {where_clause} GROUP BY States", tuple(params))
        st.plotly_chart(px.bar(df_app.sort_values(by="Opens", ascending=False).head(10), x="States", y="Opens", title="Top States by App Opens", color="States", color_discrete_sequence=px.colors.sequential.Mint))

        df_reg = execute_query(f"SELECT Years, SUM(RegisteredUser) AS Users FROM map_user {where_clause} GROUP BY Years", tuple(params))
        st.plotly_chart(px.line(df_reg, x="Years", y="Users", markers=True, title="User Registrations Over Time", color_discrete_sequence=["#24D23E"]))

        df_districts = execute_query(f"SELECT Districts, SUM(RegisteredUser) AS Users FROM map_user {where_clause} GROUP BY Districts ORDER BY Users DESC LIMIT 10", tuple(params))
        st.plotly_chart(px.pie(df_districts, names="Districts", values="Users", title="Top Districts by Registrations Share"))







        
       



            




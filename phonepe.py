import streamlit as st  # pip install streamlit
import pandas as pd  # pip install pandas
import mysql.connector  # pip install mysql-connector-python
import plotly.express as px  # pip install plotly
import requests 
import json
import os

#Aggregate Transaction
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

# Aggregate Users #F:\phone pe pro\data\pulse\data\aggregated\user\country\india\state

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

# Map Transaction #F:\phone pe pro\data\pulse\data\map\user\hover\country\india

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

# Map User

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

# Top Transactions

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

# Top User
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

# Define path to insurance data
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

# Path for map insurance data
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

# Path for top insurance data
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








        
       



            




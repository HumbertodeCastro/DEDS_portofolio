from settings import settings, logger

logger.info(f"Radius {type_activity}: {activity_radius}")

def process():
    # %% [markdown]
    # # Practicum 4.3
    # ## Implementeer elk gemaakt ETL-schema in Python

    # %%
    import pandas as pd
    import pyodbc
    import sqlite3
    from datetime import datetime

    # %%
    # Database connections using settings object
    export_conn = pyodbc.connect(f"DRIVER={{{settings.SQL_SERVER_DB['driver']}}};SERVER={settings.SQL_SERVER_DB['servername']};DATABASE={settings.SQL_SERVER_DB['database']}")
    export_cursor = export_conn.cursor()

    export_cursor   

    # %% [markdown]
    # # Extract

    # %% [markdown]
    # **Database connection**

    # %%
    # SQLite connections using settings object
    go_sales_con = sqlite3.connect(settings.SQLITE_DBS['go_sales_sqlite'])
    go_crm_con = sqlite3.connect(settings.SQLITE_DBS['go_crm_sqlite'])
    go_staff_con = sqlite3.connect(settings.SQLITE_DBS['go_staff_sqlite'])
    # Reading CSV files using settings object
    GO_SALES_INVENTORY_LEVELSData = pd.read_csv(settings.CSV_PATHS['go_inv_csv'], index_col=False)
    GO_SALES_PRODUCT_FORECASTData = pd.read_csv(settings.CSV_PATHS['go_forecast_csv'], index_col=False)

    # %% [markdown]
    # **Dataframes aanmaken**

    # %%
    Go_staff_queries = {
        'Course': 'SELECT * FROM Course',
        'Training': 'SELECT * FROM Training',
        'Sales_staff': 'SELECT * FROM Sales_staff',
        'Satisfaction': 'SELECT * FROM Satisfaction',
        'Satisfaction_type': 'SELECT * FROM Satisfaction_type',
    }

    Go_crm_queries = {
        'Retailer_contact': 'SELECT * FROM Retailer_contact',
        'Retailer_site' : 'SELECT * FROM Retailer_site',
        'Retailer' : 'SELECT * FROM Retailer',
        'Retailer_type' : 'SELECT * FROM Retailer_type',
        'COUNTRY': 'SELECT * FROM COUNTRY',
        'Sales_territory': 'SELECT * FROM Sales_territory'
    }

    Go_sales_queries = {
        'Order_method': 'SELECT * FROM Order_method',
        'Order_header': 'SELECT * FROM Order_header',
        'Order_details': 'SELECT * FROM Order_details',
        'Returned_item': 'SELECT * FROM Returned_item',
        'Return_reason': 'SELECT * FROM Return_reason',
        'Product': 'SELECT * FROM Product',
        'PRODUCT_TYPE': 'SELECT * FROM PRODUCT_TYPE',
        'PRODUCT_LINE' : 'SELECT * FROM PRODUCT_LINE',
        'Sales_TARGETData' : 'SELECT * FROM Sales_TARGETData',
        'Sales_branch': 'SELECT * FROM Sales_branch'
    }


    dataframes = {}

    # Lees elke tabel in een DataFrame
    for table_name, query in Go_staff_queries.items():
        dataframes[table_name] = pd.read_sql_query(query, go_staff_con)

    for table_name, query in Go_crm_queries.items():
        dataframes[table_name] = pd.read_sql_query(query, go_crm_con)

    for table_name, query in Go_sales_queries.items():
        dataframes[table_name] = pd.read_sql_query(query, go_sales_con)

    # Lees de CSV-bestanden in pandas DataFrames
    GO_SALES_INVENTORY_LEVELSData = pd.read_csv(go_inv_con, index_col=False)
    GO_SALES_PRODUCT_FORECASTData = pd.read_csv(go_forecast_con, index_col=False)

    # Voeg de DataFrames toe aan de dataframes dictionary
    dataframes['GO_SALES_INVENTORY_LEVELSData'] = GO_SALES_INVENTORY_LEVELSData
    dataframes['GO_SALES_PRODUCT_FORECASTData'] = GO_SALES_PRODUCT_FORECASTData

    #als je ik elk tabel als een dataframe/ variabele wil behandelen of aanroepen moet ik dit uitvoeren.
    for table_name, df in dataframes.items():
        globals()[table_name] = df

    Sales_staff['DATE_HIRED'] = pd.to_datetime(Sales_staff['DATE_HIRED'], errors='coerce')
    Sales_staff['DATE_HIRED'] = Sales_staff['DATE_HIRED'].dt.date

    print(GO_SALES_INVENTORY_LEVELSData)

    # %% [markdown]
    # # Transform & Load

    # %% [markdown]
    # ### **Retailer_dimensie**

    # %%
    #Drop TRIAL columns want anders veel merge problemen.
    dataframes_dict = {
        "Retailer_contact": Retailer_contact,
        "Retailer_site": Retailer_site,
        "COUNTRY": COUNTRY,
        "Sales_territory": Sales_territory,
        "Retailer": Retailer,
        "Retailer_type": Retailer_type
    }

    for name, df in dataframes_dict.items():
        # Drop any column that contains 'TRIAL'
        trial_cols = [col for col in df.columns if 'TRIAL' in col]
        df.drop(columns=trial_cols, inplace=True)

    merge1 = pd.merge(Retailer_contact, Retailer_site, on = 'RETAILER_SITE_CODE')
    merge2 = pd.merge(merge1, COUNTRY, on = 'COUNTRY_CODE')
    merge3 = pd.merge(merge2, Sales_territory, on= 'SALES_TERRITORY_CODE')
    merge4 = pd.merge(merge3, Retailer, on= 'RETAILER_CODE')
    Retailer_dimensie = pd.merge(merge4, Retailer_type, on = 'RETAILER_TYPE_CODE')

    def get_category(job_position):
        if 'Purchaser' in job_position:
            return 'Purchaser'
        elif 'Manager' in job_position:
            return 'Manager'
        else:
            return 'Other'

    # Loop over elke rij en wijs categorie toe
    categories = []
    for index, row in Retailer_dimensie.iterrows():
        category = get_category(row['JOB_POSITION_EN'])
        categories.append(category)

    # Voeg de categorieën toe als nieuwe kolom
    Retailer_dimensie['Retailer_Position_category_Category'] = categories

    Retailer_dimensie = Retailer_dimensie.rename(columns= {
        'RETAILER_CONTACT_CODE': 'Retailer_Retailer_contact_code',
        'FIRST_NAME': 'Retailer_FIRST_NAME',
        'LAST_NAME': 'Retailer_LAST_NAME',
        'E_MAIL': 'Retailer_E-mail',
        'ADDRESS1': 'Retailer_Address_ADDRESS1',
        'ADDRESS2': 'Retailer_Address_ADDRESS2',
        'POSTAL_ZONE': 'Retailer_Zone_POSTAL_ZONE',
        'CITY': 'Retailer_City_CITY',
        'REGION': 'Retailer_Region_REGION',
        'COUNTRY_CODE': 'Retailer_Country_COUNTRY_CODE',
        'COUNTRY_EN':'Retailer_Country_COUNTRY_EN',
        'SALES_TERRITORY_CODE': 'Retailer_Territory_TERRITORY_CODE',
        'TERRITORY_NAME_EN': 'Retailer_Territory_TERRITORY_NAME_EN',
        'GENDER': 'Retailer_Gender_GENDER',
        'RETAILER_CODE' : 'Retailer_Company_RETAILER_CODE',
        'COMPANY_NAME' : 'Retailer_Company_COMPANY_NAME',
        'RETAILER_TYPE_CODE': 'Retailer_Type_RETAILER_TYPE_CODE',
        'RETAILER_TYPE_EN': 'Retailer_Type_RETAILER_TYPE_EN',
        'JOB_POSITION_EN': 'Retailer_Position_JOB_POSITION_EN'

    })
    #print(Retailer_dimensie.columns)


    # %%
    for index, row in Retailer_dimensie.iterrows():
        try:
            query = """INSERT INTO Retailer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            params = (
                row['Retailer_Retailer_contact_code'], row['Retailer_FIRST_NAME'], row['Retailer_LAST_NAME'],
                row['Retailer_E-mail'], row['Retailer_Address_ADDRESS1'],
                str(row['Retailer_Address_ADDRESS2']).replace('\'', '\'\'') if pd.notna(row['Retailer_Address_ADDRESS2']) else None, 
                row['Retailer_Zone_POSTAL_ZONE'], row['Retailer_City_CITY'], row['Retailer_Region_REGION'],
                row['Retailer_Country_COUNTRY_CODE'], row['Retailer_Country_COUNTRY_EN'],
                row['Retailer_Territory_TERRITORY_CODE'], row['Retailer_Territory_TERRITORY_NAME_EN'],
                row['Retailer_Gender_GENDER'], row['Retailer_Company_RETAILER_CODE'],
                row['Retailer_Company_COMPANY_NAME'], row['Retailer_Type_RETAILER_TYPE_CODE'],
                row['Retailer_Type_RETAILER_TYPE_EN'], row['Retailer_Position_JOB_POSITION_EN'],
                row['Retailer_Position_category_Category']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Sales_staff_dimensie**

    # %%

    merge1 = pd.merge(Sales_staff, Sales_branch, on= 'SALES_BRANCH_CODE', how= 'outer')
    #print(merge1.columns)

    merge2 = pd.merge(merge1, COUNTRY, on= 'COUNTRY_CODE', how= 'outer')
    #print(merge2.columns)
    Sales_staff_dimensie = pd.merge(merge2, Sales_territory, on= 'SALES_TERRITORY_CODE', how = 'outer')

    #conversie om .year te gebruiken.
    Sales_staff_dimensie['DATE_HIRED'] = pd.to_datetime(Sales_staff_dimensie['DATE_HIRED'])

    # Gebruik .dt.year om het jaar van elke datum in de Series te extraheren
    Sales_staff_dimensie['Sales_staff_In_dienst_nr'] = datetime.now().year - Sales_staff_dimensie['DATE_HIRED'].dt.year

    for index, row in Sales_staff_dimensie.iterrows():
        aantal_jaar_in_dienst = row['Sales_staff_In_dienst_nr']

        if aantal_jaar_in_dienst < 20:
            Sales_staff_dimensie.at[index, 'Sales_staff_In_dienst_category_code'] = '<20 jaar'
        else:
            Sales_staff_dimensie.at[index, 'Sales_staff_In_dienst_category_code'] = '≥20 jaar'

    Sales_staff_dimensie = Sales_staff_dimensie.rename(columns =  {
        'SALES_STAFF_CODE': 'Sales_staff_SALES_STAFF_CODE',
        'FIRST_NAME': 'Sales_staff_FIRST_NAME',
        'LAST_NAME': 'Sales_staff_LAST_NAME',
        'POSITION_EN': 'Sales_staff_Position_POSITION_EN',
        'EMAIL': 'Sales_staff_EMAIL',
        'MANAGER_CODE': 'Sales_staff_Manager_MANAGER_CODE',
        'SALES_BRANCH_CODE': 'Sales_staff_Branch_SALES_BRANCH_CODE',
        'ADDRESS1': 'Sales_staff_ADDRESS_ADDRESS1',
        'ADDRESS2': 'Sales_staff_ADDRESS_ADDRESS2',
        'CITY': 'Sales_staff_City_CITY',
        'REGION' : 'Sales_staff_Region_REGION',
        'POSTAL_ZONE': 'Sales_staff_Zone_POSTAL_ZONE',
        'COUNTRY_CODE': 'Sales_staff_Country_COUNTRY_CODE',
        'COUNTRY_EN': 'Sales_staff_Country_COUNTRY_EN',
        'SALES_TERRITORY_CODE': 'Sales_staff_Territory_TERRITORY_CODE',
        'TERRITORY_NAME_EN': 'Sales_staff_Territory_TERRITORY_NAME_EN'
    })
    #print(Sales_staff_dimensie)

    # %%

    for index, row in Sales_staff_dimensie.iterrows():
        try:
            query = """INSERT INTO Sales_staff VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            params = (
                row['Sales_staff_SALES_STAFF_CODE'], row['Sales_staff_FIRST_NAME'], row['Sales_staff_LAST_NAME'],
                row['Sales_staff_EMAIL'], row['Sales_staff_Zone_POSTAL_ZONE'], row['Sales_staff_ADDRESS_ADDRESS1'],
                str(row['Sales_staff_ADDRESS_ADDRESS2']).replace('\'', '\'\'') if pd.notna(row['Sales_staff_ADDRESS_ADDRESS2']) else None, row['Sales_staff_City_CITY'], row['Sales_staff_Region_REGION'],
                row['Sales_staff_Country_COUNTRY_CODE'], row['Sales_staff_Country_COUNTRY_EN'],
                row['Sales_staff_Territory_TERRITORY_CODE'], row['Sales_staff_Territory_TERRITORY_NAME_EN'],
                row['Sales_staff_In_dienst_nr'], row['Sales_staff_In_dienst_category_code'],
                row['Sales_staff_Manager_MANAGER_CODE'], row['Sales_staff_Position_POSITION_EN'],
                row['Sales_staff_Branch_SALES_BRANCH_CODE']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()


    # %% [markdown]
    # ### **Product_dimensie**

    # %%
    merge1 = pd.merge(Product, PRODUCT_TYPE, on= 'PRODUCT_TYPE_CODE')
    #print(merge1.columns)

    Product_dimensie = pd.merge(merge1, PRODUCT_LINE, on = 'PRODUCT_LINE_CODE')
    #print(merge2.columns)


    for index, row in Product_dimensie.iterrows():
        Production_cost = float(row['PRODUCTION_COST'])

        if Production_cost < 100:
            Product_dimensie.at[index, 'Product_Category_cost_Category'] = '<100'
        else:
            Product_dimensie.at[index, 'Product_Category_cost_Category'] = '>100'


    Product_dimensie = Product_dimensie.rename(columns = {
        'PRODUCT_NUMBER': 'Product_PRODUCT_NUMBER',
        'INTRODUCTION_DATE': 'Product_Introduction_date_INTRODUCTION_DATE',
        'PRODUCTION_COST': 'Product_Cost_PRODUCTION_COST',
        'MARGIN' : 'Product_Margin_MARGIN',
        'PRODUCT_NAME' : 'Product_PRODUCT_NAME',
        'DESCRIPTION' : 'Product_DESCRIPTION',
        'PRODUCT_TYPE_CODE' : 'Product_Type_PRODUCT_TYPE_CODE',
        'PRODUCT_TYPE_EN' : 'Product_Type_PRODUCT_TYPE_EN',
        'PRODUCT_LINE_CODE' : 'Product_Line_PRODUCT_LINE_CODE',
        'PRODUCT_LINE_EN' : 'Product_Line_PRODUCT_LINE_EN'
    })





    # %%
    for index, row in Product_dimensie.iterrows():
        try:
            query = """INSERT INTO Product VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            params = (
                row['Product_PRODUCT_NUMBER'], row['Product_PRODUCT_NAME'], row['Product_DESCRIPTION'], row['Product_Type_PRODUCT_TYPE_CODE'],
                row['Product_Type_PRODUCT_TYPE_EN'], row['Product_Line_PRODUCT_LINE_CODE'], row['Product_Line_PRODUCT_LINE_EN'],
                row['Product_Introduction_date_INTRODUCTION_DATE'], row['Product_Cost_PRODUCTION_COST'], row['Product_Category_cost_Category'],
                row['Product_Margin_MARGIN']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()


    # %% [markdown]
    # ### **Day_dimensie**

    # %%


    # %% [markdown]
    # ### **Course_dimensie**

    # %%
    course_dimensie = Course 

    course_dimensie = course_dimensie.rename(columns={
        'COURSE_CODE': 'Course_COURSE_CODE',
        'COURSE_DESCRIPTION': 'Course_COURSE_DESCRIPTION'
    })


    for index, row in course_dimensie.iterrows():

        try:
            query= f"INSERT INTO Course VALUES ({row['Course_COURSE_CODE']}, '{row['Course_COURSE_DESCRIPTION']}' )"
            export_cursor.execute(query)
        except pyodbc.Error:
            print(query)


    export_conn.commit()



    # %% [markdown]
    # ### **Satisfaction_type_dimensie**

    # %%
    satisfaction_type_dimensie = Satisfaction_type

    satisfaction_type_dimensie = satisfaction_type_dimensie.rename(columns={
        'SATISFACTION_TYPE_CODE': 'Satisfaction_type_SATISFACTION_TYPE_CODE',
        'SATISFACTION_TYPE_DESCRIPTION': 'Satisfaction_type_SATISFACTION_TYPE_DESCRIPTION'
    })

    for index, row in satisfaction_type_dimensie.iterrows():
        try:
            query= f"INSERT INTO Satisfaction_type VALUES ({row['Satisfaction_type_SATISFACTION_TYPE_CODE']}, '{row['Satisfaction_type_SATISFACTION_TYPE_DESCRIPTION']}' )"
            export_cursor.execute(query)
        except pyodbc.Error:
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Order_method_dimensie**

    # %%
    order_method_dimensie = Order_method

    order_method_dimensie = order_method_dimensie.rename(columns={
        'ORDER_METHOD_CODE': 'Order_method_ORDER_METHOD_CODE',
        'ORDER_METHOD_EN': 'Order_method_ORDER_METHOD_EN'
    })

    for index, row in order_method_dimensie.iterrows():
        try:
            query= f"INSERT INTO Order_method VALUES ({row['Order_method_ORDER_METHOD_CODE']}, '{row['Order_method_ORDER_METHOD_EN']}' )"
            export_cursor.execute(query)
        except pyodbc.Error:
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Return_reason_dimensie**

    # %%
    Return_reason_dimensie = Return_reason

    Return_reason_dimensie = Return_reason_dimensie.rename(columns= {
        'RETURN_REASON_CODE': 'Return_reason_RETURN_REASON_CODE',
        'RETURN_DESCRIPTION_EN': 'Return_reason_RETURN_DESCRIPTION_EN'
    })

    for index, row in Return_reason_dimensie.iterrows():
        try:
            query = """INSERT INTO Return_reason VALUES (?, ?)"""
            params = (
                row['Return_reason_RETURN_REASON_CODE'], row['Return_reason_RETURN_DESCRIPTION_EN']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()


    # %% [markdown]
    # ### **Returned_item_feit**

    # %%
    Returned_item_feit = pd.merge(Returned_item, Return_reason, on='RETURN_REASON_CODE')

    Returned_item_feit['RETURN_QUANTITY'] = Returned_item_feit['RETURN_QUANTITY'].astype(float)


    Returned_item_feit = Returned_item_feit.rename(columns = {
        'RETURN_CODE': 'Returned_item_RETURN_CODE',
        'RETURN_DATE': 'Day_time',
        'RETURN_QUANTITY' : 'Returned_item_RETURN_QUANTITY',
        'RETURN_REASON_CODE' : 'Return_reason_RETURN_REASON_CODE'
    })

    Return_average = Returned_item_feit.groupby('Return_reason_RETURN_REASON_CODE')['Returned_item_RETURN_QUANTITY'].mean().reset_index(name='Returned_item_RETURN_AVERAGE')

    # Merge the average back into the main DataFrame
    Returned_item_feit = Returned_item_feit.merge(Return_average, on='Return_reason_RETURN_REASON_CODE')

    for index, row in Returned_item_feit.iterrows():
        try:
            query = """INSERT INTO Returned_item VALUES (?, ?, ?, ?, ?)"""
            params = (
                row['Returned_item_RETURN_CODE'], row['Day_time'], row['Returned_item_RETURN_QUANTITY'],
                row['Returned_item_RETURN_AVERAGE'], row['Return_reason_RETURN_REASON_CODE']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()


    # %% [markdown]
    # ### **Order_feit**

    # %%
    merge1 = pd.merge(Order_header, Order_details, on= 'ORDER_NUMBER')

    merge2 = pd.merge(merge1, Retailer_contact, on = 'RETAILER_CONTACT_CODE')

    merge3 = pd.merge(merge2, Product, on= 'PRODUCT_NUMBER')

    Order_feit = pd.merge(merge3, Order_method, on = 'ORDER_METHOD_CODE')


    # Calculate 'Order_OMZET'
    Order_feit['Order_OMZET'] = Order_feit['QUANTITY'] * (pd.to_numeric(Order_feit['UNIT_PRICE']))

    # Locally convert 'UNIT_PRICE' and 'UNIT_SALE_PRICE' to float for the calculation
    Order_feit['Order_KORTING'] = (
        (pd.to_numeric(Order_feit['UNIT_PRICE'], errors='coerce') - 
        pd.to_numeric(Order_feit['UNIT_SALE_PRICE'], errors='coerce')) / 
        pd.to_numeric(Order_feit['UNIT_PRICE'], errors='coerce')
    ) * 100

    Order_feit['Order_KORTING'] = Order_feit['Order_KORTING'].map(lambda x: f"{round(x)}%")

    # Check the columns and data
    print(Order_feit[['Order_OMZET', 'Order_KORTING']])

    Order_feit = Order_feit.rename(columns={
        'ORDER_NUMBER': 'Order_ORDER_NUMBER',
        'ORDER_DATE': 'Day_date',
        'ORDER_DETAIL_CODE': 'Order_ORDER_DETAIL_CODE',
        'QUANTITY': 'Order_QUANTITY',
        'UNIT_COST': 'Order_UNIT_COST',
        'UNIT_PRICE': 'Order_UNIT_PRICE',
        'UNIT_SALE_PRICE': 'Order_UNIT_SALE_PRICE',
        'RETAILER_CONTACT_CODE': 'Retailer_Retailer_contact_code',
        'PRODUCT_NUMBER': 'Product_PRODUCT_NUMBER',
        'ORDER_METHOD_CODE': 'Order_method_ORDER_METHOD_CODE'
    })



    # %%
    for index, row in Order_feit.iterrows():
        try:
            query = """INSERT INTO [Order] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            params = (
                row['Order_ORDER_NUMBER'],
                row['Order_ORDER_DETAIL_CODE'],
                row['Order_QUANTITY'],
                row['Order_UNIT_COST'],
                row['Order_UNIT_PRICE'],
                row['Order_UNIT_SALE_PRICE'],
                row['Order_OMZET'],  
                row['Order_KORTING'],
                row['Retailer_Retailer_contact_code'],
                row['Product_PRODUCT_NUMBER'],
                row['Order_method_ORDER_METHOD_CODE'],
                row['Day_date']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Target_feit**

    # %%
    merge1 = pd.merge(Sales_TARGETData, Sales_staff, on = 'SALES_STAFF_CODE')

    merge2 = pd.merge(merge1, Product, on = 'PRODUCT_NUMBER')

    Target_feit = pd.merge(merge2, Retailer, on = 'RETAILER_CODE')

    def dagen_in_maand(maand, jaar):
        # Aantal dagen in elke maand (standaard, geen schrikkeljaar)
        dagen = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        if maand == 2:  # Februari
            # Controleer op schrikkeljaar
            is_schrikkeljaar = jaar % 4 == 0 and (jaar % 100 != 0 or jaar % 400 == 0)
            if is_schrikkeljaar:
                return 29
        return dagen[maand - 1]

    Target_feit['Aantal_dagen'] = Target_feit.apply(lambda row: dagen_in_maand(int(row['SALES_PERIOD']), int(row['SALES_YEAR'])), axis = 1)

    Target_feit['SALES_TARGET'] = pd.to_numeric(Target_feit['SALES_TARGET'], errors='coerce')

    Target_feit['Target_DAILY'] = Target_feit['SALES_TARGET'] / Target_feit['Aantal_dagen']

    Target_feit = Target_feit.rename(columns = {
        'Id': 'Target_Id',
        'SALES_YEAR' : 'Year_nr',
        'SALES_TARGET': 'Target_SALES_TARGET',
        'SALES_PERIOD': 'Target_SALES_PERIOD',
        'SALES_STAFF_CODE': 'Sales_staff_SALES_STAFF_CODE',
        'PRODUCT_NUMBER': 'Product_PRODUCT_NUMBER',
        'RETAILER_CODE': 'Retailer_company_retailer_code'
    })


    # %%
    for index, row in Target_feit.iterrows():
        try:
            query = """INSERT INTO Target VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            params = (
                row['Target_Id'],
                row['Year_nr'],
                row['Target_SALES_TARGET'],
                row['Target_SALES_PERIOD'],
                row['Target_DAILY'],
                row['Sales_staff_SALES_STAFF_CODE'],
                row['Product_PRODUCT_NUMBER'],  
                row['Retailer_company_retailer_code'],
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Forecast_feit**

    # %%
    #anders merge fout
    Product['PRODUCT_NUMBER'] = pd.to_numeric(Product['PRODUCT_NUMBER'], errors='coerce')

    merge = pd.merge(GO_SALES_INVENTORY_LEVELSData, GO_SALES_PRODUCT_FORECASTData, on = 'PRODUCT_NUMBER')

    Forecast_feit = pd.merge(merge, Product, on = 'PRODUCT_NUMBER')

    Forecast_feit['Forecast_PERCENTAGE_BEREIKT'] = (Forecast_feit['INVENTORY_COUNT'] / Forecast_feit['EXPECTED_VOLUME']).map(lambda x: f"{round(x)}%")

    Forecast_feit = Forecast_feit.rename(columns = {
        'PRODUCT_NUMBER': 'Forecast_PRODUCT_NUMBER',
        'YEAR' : 'Year_nr',
        'MONTH' : 'Month_nr',
        'EXPECTED_VOLUME' : 'Forecast_EXPECTED_VOLUME',
        'INVENTORY_COUNT' : 'Forecast_INVENTORY_COUNT',
        
    })

    # %%
    for index, row in Forecast_feit.iterrows():
        try:
            query = """INSERT INTO Forecast VALUES (?, ?, ?, ?, ?, ?)"""
            params = (
                row['Forecast_PRODUCT_NUMBER'],
                row['Year_nr'],
                row['Month_nr'],
                row['Forecast_EXPECTED_VOLUME'],
                row['Forecast_INVENTORY_COUNT'],
                row['Forecast_PERCENTAGE_BEREIKT']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Satisfaction_feit**

    # %%
    merge1 = pd.merge(Satisfaction, Sales_staff, on = 'SALES_STAFF_CODE')

    Satisfaction_feit = pd.merge(merge1, Satisfaction_type, on = 'SATISFACTION_TYPE_CODE')

    Satisfaction_feit = Satisfaction_feit.rename(columns = {
        'YEAR' : 'Year_nr',
        'SALES_STAFF_CODE' : 'Satisfaction_SALES_STAFF_CODE',
        'SATISFACTION_TYPE_CODE' : 'Satisfaction_type_SATISFACTION_TYPE_CODE'
    })

    print(Satisfaction_feit.columns)



    # %%
    for index, row in Satisfaction_feit.iterrows():
        try:
            query = """INSERT INTO Satisfaction VALUES (?, ?, ?)"""
            params = (
                row['Year_nr'],
                row['Satisfaction_SALES_STAFF_CODE'],
                row['Satisfaction_type_SATISFACTION_TYPE_CODE'],
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Training_feit**

    # %%
    #andere soort join want 2 Sales_code
    #of mischien de fk sales staff cde weghalen want anders komt gwn dubbel voor ?
    merge1 = pd.merge(Training, Sales_staff, on = 'SALES_STAFF_CODE')

    Training_feit = pd.merge(merge1, Course, on = 'COURSE_CODE')


    Training_feit = Training_feit.rename(columns = {
        'YEAR' : 'Year_nr',
        'SALES_STAFF_CODE' : 'Training_Sales_staff_code',
        'COURSE_CODE' : 'Course_COURSE_CODE'
    })

    print(Training_feit.columns)

    # %%
    for index, row in Training_feit.iterrows():
        try:
            query = """INSERT INTO Training VALUES (?, ?, ?)"""
            params = (
                row['Year_nr'],
                row['Training_Sales_staff_code'],
                row['Course_COURSE_CODE']
            )
            export_cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
            print(query)

    export_conn.commit()

    # %% [markdown]
    # ### **Close**

    # %%
    export_cursor.close()




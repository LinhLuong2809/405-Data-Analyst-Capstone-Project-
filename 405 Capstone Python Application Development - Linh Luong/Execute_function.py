import json
import Display
import Generate
def prompt_user_zipcode():
    try:
        while True:
            zipcode = input("Enter your zipcode (Enter 4/5-digit number): ")
            if 4 <= len(zipcode) <= 5 and zipcode.isdigit():
                return int(zipcode)
            else:
                Display.color_print("Invalid zipcode. Please enter a 5-digit number.", Display.Color.RED)
                
    except Exception as e:
        print(e)

def prompt_user_day():
    try:
        while True:
            day_input = input("Enter your day (1-31): ")
            if len(day_input) == 1 or len(day_input) == 2 and day_input.isdigit():
                if int(day_input) >= 1 and int(day_input) <= 31:
                    return int(day_input)
                else:
                    Display.color_print("Invalid day. Please enter a number between 1 and 31.", Display.Color.RED)
            else:
                Display.color_print("Invalid day. Please enter a number between 1 and 31.", Display.Color.RED)
    except Exception as e:
        print(e)
        
def prompt_user_month():
    try:
        while True:
            month_input = input("Enter your month (Enter number 1 - 12): ")
            if month_input.isdigit():
                if 1 <= int(month_input) <= 12:
                    return int(month_input)
                else:
                    Display.color_print("Invalid month. Please enter a number between 1 and 12.", Display.Color.RED)
            else:
                Display.color_print("Invalid Input. Please enter a number between 1 and 12.", Display.Color.RED)
    except ValueError:
        Display.color_print("Invalid input. Please enter a valid number.", Display.Color.RED)

def prompt_user_year():
    try:
        while True:
            year_input = input("Enter your year (Enter 4-digit number): ")
            if len(year_input) == 4 and year_input.isdigit():
                return int(year_input)
            else:
                Display.color_print("Invalid year. Please enter a 4-digit number.", Display.Color.RED)
    except Exception as e:
        print(e)
        
def prompt_user_type():
    try:
        while True:
            Transaction = ["Bills", "Education", "Entertainment", "Gas", "Grocery", "Healthcare", "Test"]
            Display.color_print("Transaction Types: ", Display.Color.GREEN)
            print(Transaction)
            type = input("Enter Transaction Type: ")
            if type.isalpha():
                if type in Transaction:
                    return type
                else:
                    Display.color_print("Invalid input. Please enter transaction type from the list.", Display.Color.RED)
            else:
                Display.color_print("Invalid input. Please enter string only.", Display.Color.RED)
                
    except Exception as e:
        print(e)

def prompt_user_state():
    try:
        while True:
            state = input("Enter State (LA, CA, etc.): ")
            if state.isalpha():
                if len(state) == 2:
                    return state.upper()
                else:
                    Display.color_print("Invalid input. Please enter 2-letters for the state only.", Display.Color.RED)
            else:
                Display.color_print("Invalid input. Please enter string only.", Display.Color.RED)
                
    except Exception as e:
        print(e)
        
def prompt_user_SSN():
    try:
        while True:
            SSN = input("Enter Customer last 4 SSN: ")
            if SSN.isnumeric():
                if len(SSN) == 4:
                    return SSN
                else:
                    Display.color_print("Invalid input. Please enter 4-digit number only.", Display.Color.RED)
            else:
                Display.color_print("Invalid input. Please enter number only.", Display.Color.RED)
                
    except Exception as e:
        print(e)

def prompt_user_credit_card():
    try:
        while True:
            credit_card_no = input("Enter your credit card number (16 digits): ")
            if len(credit_card_no) == 16 and credit_card_no.isdigit():
                return str(credit_card_no)
            else:
                Display.color_print("Invalid credit card number. Please enter a 16-digit number.", Display.Color.RED)
                
    except Exception as e:
        print(e)
        
def prompt_user_column():
    try:
        col = ["FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "CREDIT_CARD_NO", "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "APT NO", "STREET_NAME", "SSN"]
        Display.color_print("Columns list: ", Display.Color.GREEN)
        print(col)
        while True:
            column = input("Enter column name to update: ")
            if column in col:
                return column
            else:
                Display.color_print("Invalid input. Please enter a valid column name.", Display.Color.RED)
    except Exception as e:
        print(e)
    
def prompt_user_value():
    try:
        while True:
            value = input("Enter new value: ")
            if value.isnumeric():
                return int(value)
            else:
                return value
    except Exception as e:
        print(e)
        
def prompt_user_choice():
    while True:
        try:
            choice = int(input("Enter your choice: "))
            if 1 <= choice <= 12:
                return choice
            else:
                Display.color_print("Invalid choice. Please enter a number between 1 and 12.", Display.Color.RED)
        except ValueError:
            Display.color_print("Invalid input. Please enter a number.", Display.Color.RED)
        
def check_cust_SSN(SSN):
    import json
    try:
        with open(r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_custmer.json") as f:
            for line in f:
                customer_data = json.loads(line)
                if str(customer_data["SSN"])[-4:] == str(SSN):
                    return True
            return False
    except Exception as e:
        print(e)
    finally:
        f.close()
        
def check_credit_card(credit_card_no):
    import json
    try:
        with open(r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_credit.json") as f:
            for line in f:
                credit_data = json.loads(line)
                if str(credit_data["CREDIT_CARD_NO"]) == str(credit_card_no):
                    return True
            return False
    except Exception as e:
        print(e)
    finally:
        f.close()
        
def write_cust_copy():
    import mysql.connector
    import json
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    spark = SparkSession.builder.appName("Write_JSON_File").getOrCreate()
    schema = StructType([
    StructField("FIRST_NAME", StringType(), True),
    StructField("MIDDLE_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("SSN", IntegerType(), True),
    StructField("CREDIT_CARD_NO", StringType(), True),
    StructField("APT_NO", StringType(), True),
    StructField("STREET_NAME", StringType(), True),
    StructField("CUST_CITY", StringType(), True),
    StructField("CUST_STATE", StringType(), True),
    StructField("CUST_COUNTRY", StringType(), True),
    StructField("CUST_ZIP", StringType(), True),
    StructField("CUST_PHONE", StringType(), True),
    StructField("CUST_EMAIL", StringType(), True),
    StructField("LAST_UPDATED", StringType(), True),
    ])
    try:     
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="creditcard_capstone"
        )
          
        query = f"""
                SELECT 
                FIRST_NAME,
                MIDDLE_NAME, 
                LAST_NAME, 
                SSN, 
                CREDIT_CARD_NO, 
                APT_NO,
                STREET_NAME, 
                CUST_CITY, 
                CUST_STATE, 
                CUST_COUNTRY, 
                CUST_ZIP, 
                CUST_PHONE, 
                CUST_EMAIL, 
                LAST_UPDATED 
                FROM cdw_sapp_customer"""
                
        cursor = conn.cursor()        
        cursor.execute(query)
        result = cursor.fetchall()
        
        data = []
        for row in result:
            row_dict = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13]]
            data.append(row_dict)
        df = spark.createDataFrame(data, schema=schema)
        # Write the DataFrame to a JSON file
        df.write.mode("overwrite").json(r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_custmer_copy.json")
        
    except Exception as e:
        print(e)
    finally:
        spark.stop()
        
def execute_option(option):
    import sys
    if option == 1:
        Display.color_print("1. Display Customer Information.", Display.Color.RED)
        Display.display_customer_table(r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_custmer.json")
    
    elif option == 2:
        Display.color_print("2. Display Branch Information.", Display.Color.RED)
        Display.display_branch_table()
        
    elif option == 3:
        Display.color_print("3. Display Credit Card and Transaction Information.", Display.Color.RED)
        Display.display_credit_table()
        
    elif option == 4:
        Display.color_print("4. Retrieve customer's transaction in specified zip code for a given month and year", Display.Color.RED)
        Display.customer_transaction_details(prompt_user_zipcode(), prompt_user_month(), prompt_user_year())
        
    elif option == 5:
        Display.color_print("5. Sort the transactions by day in descending order.", Display.Color.RED)
        Display.sort_transaction_by_day()
        
    elif option == 6:
        Display.color_print("6. Display number and total values of transactions for a given type.", Display.Color.RED)
        Display.transaction_number_value(prompt_user_type())
        
    elif option == 7:
        Display.color_print("7. Display the total number and total values of transactions for branches in a given state.", Display.Color.RED)
        Display.transaction_branch_number_value_state(prompt_user_state())
        
    elif option == 8:
        Display.color_print("8. Check existing account details of a customer.", Display.Color.RED)
        Display.check_cust_detail(prompt_user_SSN())
        
    elif option == 9:
        Display.color_print("9. Modify existing account details of a customer.", Display.Color.RED)
        Display.color_print("Generate a copy of the customer table.", Display.Color.BLUE)
        write_cust_copy()
        Display.color_print("copy customer table successfully.", Display.Color.RED)
        SSN = prompt_user_SSN()
        if check_cust_SSN(SSN):
            Display.display_cust_Info(SSN, r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_custmer_copy.json")
            Generate.modify_cust_detail(prompt_user_column(), prompt_user_value(), SSN)
            Display.color_print("Customer details modified successfully.", Display.Color.YELLOW)
            Display.display_cust_Info(SSN,r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_custmer_copy.json")
        else:
            Display.color_print("Customer not found.", Display.Color.RED)
        
    elif option == 10:
        Display.color_print("Generate monthly bill.", Display.Color.RED)
        Display.generate_monthly_bill(prompt_user_credit_card(), prompt_user_month(), prompt_user_year())
        
    elif option == 11:
        Display.color_print("Display transactions between dates.", Display.Color.RED)
        Display.color_print("Please enter the credit card number: ", Display.Color.GREEN)
        credit = prompt_user_credit_card()
        Display.color_print("Please enter the start date: ", Display.Color.GREEN)
        start_day = prompt_user_day()
        Display.color_print("Please enter the start month: ", Display.Color.GREEN)
        start_month = prompt_user_month()
        Display.color_print("Please enter the start year: ", Display.Color.GREEN)
        start_year = prompt_user_year()
        Display.color_print("Please enter the end day: ", Display.Color.GREEN)
        end_day = prompt_user_day()
        Display.color_print("Please enter the end month: ", Display.Color.GREEN)
        end_month = prompt_user_month()
        while True:
            Display.color_print("Please enter the end year: ", Display.Color.GREEN)
            end_year = prompt_user_year()
            if end_year >= start_year:
                break
            else:
                print("Invalid year. Please enter a year greater than or equal to the start year.")
                
        Display.display_transactions_between_dates(credit, start_day, start_month, start_year, end_day, end_month, end_year)
        
    elif option == 12:
        Display.color_print("12. Exit.", Display.Color.YELLOW)
        Display.color_print("Exiting program", Display.Color.YELLOW)
        
    else:
        Display.color_print("Invalid option. Please enter a number between 1 and 12.", Display.Color.RED)
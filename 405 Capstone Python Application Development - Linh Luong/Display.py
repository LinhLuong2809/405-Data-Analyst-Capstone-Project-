import Execute_function

class Color:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    END = '\033[0m'
    
def color_print(text, color):
    print(f"{color}{text}{Color.END}")

def connect_table(table):
    from pyspark.sql import SparkSession
    try:
        spark = SparkSession.builder.appName("ConnectTable").getOrCreate()
        jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
        df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table) \
                    .option("user", "root") \
                    .option("password", "password") \
                    .load()
        return df
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
def display_customer_table():
    # pyspark code to read customer json file
    from pyspark.sql import SparkSession
    try:
    # Create a SparkSession
        spark = SparkSession.builder.appName("connect customer database").getOrCreate()
        jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
        df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "cdw_sapp_customer") \
                    .option("user", "root") \
                    .option("password", "password") \
                    .load()
        # Show the DataFrame
        
        df.select("FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "SSN", "CREDIT_CARD_NO", "FULL_STREET_ADDRESS", "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "LAST_UPDATED").show(df.count(),truncate=False)
        
    except Exception as e:
        print(e)
    finally:
        # Stop the SparkSession
        spark.stop()
        
        
def display_branch_table():
    # pyspark to read branch json file
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import initcap, lower,concat,lit,regexp_replace
    try:
    # Create a SparkSession
        spark = SparkSession.builder.appName("Read_branch_JSON").getOrCreate()

        # Specify the path to the JSON file
        jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
        df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "cdw_sapp_branch") \
                    .option("user", "root") \
                    .option("password", "password") \
                    .load()
     
    # Show the DataFrame
        df.select("BRANCH_CODE","BRANCH_NAME","BRANCH_STREET","BRANCH_CITY","BRANCH_STATE","BRANCH_ZIP","BRANCH_PHONE","LAST_UPDATED").show(df.count(),truncate=False)
    except Exception as e: 
        print(e)
    finally:
    # Stop the SparkSession
        spark.stop()
        
def display_credit_table():
    # pyspark to read credit json file
    from pyspark.sql import SparkSession
    
    # Create a SparkSession
    spark = SparkSession.builder.appName("Read_credit_JSON").getOrCreate()

    # Specify the path to the JSON file
    jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
    df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "cdw_sapp_credit_card") \
                .option("user", "root") \
                .option("password", "password") \
                .load()
    try: 
    # Show the DataFrame
        df.select("TRANSACTION_ID","CREDIT_CARD_NO","CUST_SSN","BRANCH_CODE","DAY","MONTH","YEAR","TRANSACTION_TYPE","TRANSACTION_VALUE").show(df.count(),truncate=False)
    except Exception as e: 
        print(e)
    # Stop the SparkSession
    finally:
        spark.stop()
        
def customer_transaction_details(zipcode, month, year):
    from pyspark.sql import SparkSession
    import json
    # Create a Spark session
    try:
        spark = SparkSession.builder.appName("QueryTransactionData").getOrCreate()

        # Load JSON data into DataFrames
        branch_df = connect_table("cdw_sapp_branch")
        transaction_df = connect_table("cdw_sapp_credit_card")

        # Query the data using Spark SQL
        branch_df.createOrReplaceTempView("branch")
        transaction_df.createOrReplaceTempView("transaction")
        
        query = f"""
            SELECT branch.BRANCH_CODE, branch.BRANCH_ZIP, transaction.DAY, transaction.MONTH, transaction.YEAR, branch.BRANCH_NAME, branch.BRANCH_STREET, branch.BRANCH_CITY, branch.BRANCH_STATE, branch.BRANCH_PHONE, transaction.CREDIT_CARD_NO, transaction.CUST_SSN, transaction.TRANSACTION_ID, transaction.TRANSACTION_TYPE, transaction.TRANSACTION_VALUE, branch.LAST_UPDATED
            FROM branch 
            JOIN transaction 
            Using (BRANCH_CODE) 
            WHERE branch.BRANCH_ZIP = {zipcode} 
            AND transaction.MONTH = {month} 
            AND transaction.YEAR = {year}
        """
        result_df = spark.sql(query)

        # Show the result DataFrame
        result_df.show()
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:  
        # Stop the Spark session
        spark.stop()
        
# d. Sort the transaction by day
def sort_transaction_by_day():
    # pyspark to read credit json file
    from pyspark.sql import SparkSession
    # Create a SparkSession
    try: 
        spark = SparkSession.builder.appName("Read_credit_JSON").getOrCreate()

        df = connect_table("cdw_sapp_credit_card")

   
    # Show the DataFrame
        df.select("TRANSACTION_ID","DAY","MONTH","YEAR","CREDIT_CARD_NO","CUST_SSN","BRANCH_CODE","TRANSACTION_TYPE","TRANSACTION_VALUE").orderBy("DAY", ascending=False).show(df.count(),truncate=False)
    except Exception as e: 
        print(e)
    finally:
    # Stop the SparkSession
        spark.stop()
        
        
# 2 Use to display number and value of transaction type
def transaction_number_value(type):
    from pyspark.sql import SparkSession
    # Create a SparkSession
    spark = SparkSession.builder.appName("trasaction_JSON").getOrCreate()
    # Specify the path to the JSON file
    df = connect_table("cdw_sapp_credit_card")

    try: 
    # Show the DataFrame
        df.createOrReplaceTempView("transaction")
        spark.sql(f"""SELECT TRANSACTION_TYPE, count(TRANSACTION_TYPE) as Number, round(Sum(TRANSACTION_VALUE),2) as Total 
                    FROM transaction 
                    Group By TRANSACTION_TYPE 
                    HAVING TRANSACTION_TYPE = '{type}'""").show(df.count(),truncate=False)
    except Exception as e: 
        print(e)
    finally:
    # Stop the SparkSession
        spark.stop()
        
# 3. Use to display number and total value of transaction branch by given state
def transaction_branch_number_value_state(state):
    from pyspark.sql import SparkSession
    try:
    # Create a SparkSession
        spark = SparkSession.builder.appName("trasaction_JSON").getOrCreate()
        # Read the JSON file into a DataFrame
        df = connect_table("cdw_sapp_credit_card")
        branch_df = connect_table("cdw_sapp_branch")

        
        # Show the DataFrame
        df.createOrReplaceTempView("transaction")
        branch_df.createOrReplaceTempView("branch")
        
        query = f"""
            SELECT branch.BRANCH_STATE, count(TRANSACTION_TYPE) as Number, round(Sum(TRANSACTION_VALUE),2) as Total_value_transactions
            FROM branch 
            JOIN transaction 
            Using (BRANCH_CODE)
            WHERE branch.BRANCH_STATE = '{state}'
            Group By branch.BRANCH_STATE
        """
        
        result_df = spark.sql(query)
        # Show the result DataFrame
        result_df.show()
        
    except Exception as e: 
        print(e)
    finally:
    # Stop the SparkSession
        spark.stop()
        
def display_cust_Info(SSN):
    from pyspark.sql import SparkSession
    try:
    # Create a SparkSession
        spark = SparkSession.builder.appName("check customer detail").getOrCreate()
        jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
        df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "cdw_sapp_customer") \
                    .option("user", "root") \
                    .option("password", "password") \
                    .load()
        # Show the DataFrame
        filtered_df = df.filter(df["SSN"].substr(-4, 4) == SSN)
        # Show the result DataFrame
        filtered_df.select("FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "SSN", "CREDIT_CARD_NO", "FULL_STREET_ADDRESS", "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "LAST_UPDATED").show(truncate=False)
        
    except Exception as e: 
        print(e)
    finally:
    # Stop the SparkSession
        spark.stop()
        
# 2.2 Customer Details Module
# 1 Check existing account details of customer
def check_cust_detail(SSN):
    from pyspark.sql import SparkSession
    try:
    # Create a SparkSession
        spark = SparkSession.builder.appName("check customer detail").getOrCreate()
        # Read the JSON file into a DataFrame
        credit_df = connect_table("cdw_sapp_credit_card")
        customer_df = connect_table("cdw_sapp_customer")
        branch_df = connect_table("cdw_sapp_branch")

        
        # Show the DataFrame
        credit_df.createOrReplaceTempView("credit")
        customer_df.createOrReplaceTempView("customer")
        branch_df.createOrReplaceTempView("branch")
        
        query = f"""
            SELECT customer.SSN, 
            customer.FIRST_NAME, 
            customer.MIDDLE_NAME, 
            customer.LAST_NAME, 
            customer.CREDIT_CARD_NO, 
            customer.FULL_STREET_ADDRESS, 
            customer.CUST_CITY, 
            customer.CUST_STATE, 
            customer.CUST_ZIP, 
            customer.CUST_COUNTRY, 
            customer.CUST_EMAIL, 
            customer.CUST_PHONE,
            branch.BRANCH_NAME,
            credit.DAY,
            credit.MONTH,
            credit.YEAR,
            credit.TRANSACTION_TYPE,
            credit.TRANSACTION_VALUE
            FROM customer
            JOIN credit
            Using (CREDIT_CARD_NO)
            JOIN branch
            Using (BRANCH_CODE)
            WHERE RIGHT(customer.SSN, 4) = '{SSN}'
            ORDER BY credit.YEAR, credit.MONTH, credit.DAY
            """
            
        result_df = spark.sql(query)
        # Show the result DataFrame
        result_df.show()
    except Exception as e: 
        print(e)
    finally:
    # Stop the SparkSession
        spark.stop()
        
def generate_monthly_bill(credit_card_no, month, year):
    import json
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("trasaction_JSON").getOrCreate()
    try:
        if Execute_function.check_credit_card(credit_card_no):
            bill_df = connect_table("cdw_sapp_credit_card")
            bill_df.createOrReplaceTempView("bill")
            
            query = f"""
                SELECT  CREDIT_CARD_NO,
                        DAY, 
                        MONTH,
                        YEAR,
                        TRANSACTION_TYPE, 
                        TRANSACTION_VALUE
                        FROM bill
                        WHERE CREDIT_CARD_NO = {credit_card_no} 
                        AND MONTH = {month} 
                        AND YEAR = {year}              
                        ORDER BY YEAR, MONTH, TRANSACTION_TYPE
                """
            bill_df = spark.sql(query)
            bill_df.show()
            
            total = f"""SELECT  round(sum(TRANSACTION_VALUE),2) as TOTAL_TRANSACTION 
                                FROM bill 
                                WHERE CREDIT_CARD_NO = {credit_card_no} 
                                AND MONTH = {month} 
                                AND YEAR = {year}
                                
                                """
            bill_df = spark.sql(total)
            bill_df.show()
    except Exception as e:
        print(e)
    finally:
        spark.stop()
        
def display_transactions_between_dates(credit_card_no, start_day, start_month, start_year, end_day, end_month, end_year):
    import json
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("transactions_JSON").getOrCreate()
    
    try:
        if Execute_function.check_credit_card(credit_card_no):
            transaction_df = connect_table("cdw_sapp_credit_card")
            transaction_df.createOrReplaceTempView("transactions")
            
            query = f"""
                SELECT  CREDIT_CARD_NO, 
                        DAY,
                        MONTH,
                        YEAR,
                        TRANSACTION_TYPE, 
                        TRANSACTION_VALUE
                FROM transactions
                WHERE CREDIT_CARD_NO = '{credit_card_no}' 
                AND DAY >= {start_day}
                AND DAY <= {end_day}
                AND MONTH >= {start_month}
                AND MONTH <= {end_month}
                AND YEAR >= {start_year}
                AND YEAR <= {end_year}
                ORDER BY YEAR DESC, MONTH DESC, DAY DESC
            """
            result_df = spark.sql(query)
            result_df.show(result_df.count(), False)
            
    except Exception as e:
        print(e)
    finally:
        spark.stop()
    
def display_menu():
    color_print("=====Welcome to the CDW SAPP Credit Card Transaction Management System!=====", Color.BLUE)
    color_print("What do you want to do? Please choose an option: ", Color.GREEN)
    color_print("1. Display Customer Information.", Color.CYAN)
    color_print("2. Display Branch Information.", Color.CYAN)
    color_print("3. Display Credit Card and Transaction Information.", Color.CYAN)
    color_print("4. Retrieve customer's transaction in specified zip code for a given month and year.", Color.CYAN)
    color_print("5. Sort the transactions by day in descending order.", Color.CYAN)
    color_print("6. Display number and total values of transactions for a given type.", Color.CYAN)
    color_print("7. Display the total number and total values of transactions for branches in a given state.", Color.CYAN)
    color_print("8. Check existing account details of a customer.", Color.CYAN)
    color_print("9. Modify existing account details of a customer.", Color.CYAN)
    color_print("10. Generate monthly bill.", Color.CYAN)
    color_print("11. Display transactions between dates.", Color.CYAN)
    color_print("12. Exit.", Color.YELLOW)
    color_print("==========================================================================", Color.BLUE)

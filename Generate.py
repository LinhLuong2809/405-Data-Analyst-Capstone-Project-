import Execute_function
import Display

def create_database_in_mysql():
    import mysql.connector
    import json
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
        )
    
    # Execute a SQL query to create the database
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS `creditcard_capstone`")
        conn.commit()
        return True
    
    except Exception as e:
        Display.color_print(f"Error creating database: {e}", Display.COLOR_RED)
    finally:
        cursor.close()
        conn.close()
        
def create_table_in_mysql():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import initcap, lower,concat,lit,regexp_replace
    try:
    # Create a Spark session
        spark = SparkSession.builder.appName("CreateTableInMySQL").config("spark.jars", r"E:\soft\Spark\spark-3.5.0-bin-hadoop3\jars\mysql-connector-j-8.3.0.jar").getOrCreate()

        # Define the JDBC URL for the MySQL database
        jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"

        df = spark.read.json(r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_custmer.json")
        df2 = spark.read.json(r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_branch.json")
        df3 = spark.read.json(r"D:\CAP 405 Data Analytics - Capstone Project\cdw_sapp_credit.json")
        df = df.withColumn("CUST_PHONE", concat(lit("337"), df["CUST_PHONE"]))
        df = df.withColumn("CUST_PHONE", regexp_replace(df["CUST_PHONE"].cast("string"), "(\\d{3})(\\d{3})(\\d{4})", "($1)$2-$3")) \
                .withColumn("FIRST_NAME", initcap("FIRST_NAME")) \
                .withColumn("MIDDLE_NAME", lower("MIDDLE_NAME")) \
                .withColumn("LAST_NAME", initcap("LAST_NAME"))        
                
        df2 = df2.withColumn("BRANCH_PHONE", regexp_replace(df2["BRANCH_PHONE"].cast("string"), "(\\d{3})(\\d{3})(\\d{4})", "($1)$2-$3"))

        # Write the DataFrame to the MySQL table 
        df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "CDW_SAPP_CUSTOMER") \
        .option("user", "root") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()
        
        
        df2.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "CDW_SAPP_BRANCH") \
        .option("user", "root") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()
        
        df3.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
        .option("user", "root") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()
        
        return True
    
    except Exception as e:
        print(f"Error creating table: {e}")
        
    finally:
    # Close the Spark session
        spark.stop()
        
def modify_tables():
    import mysql.connector
    import json
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="creditcard_capstone"
        )
    
        # Execute a SQL query
        cursor = conn.cursor()
        
        cursor.execute("""
                        ALTER TABLE cdw_sapp_customer 
                        MODIFY COLUMN CREDIT_CARD_NO VARCHAR(255),
                        ADD PRIMARY KEY (SSN),
                        ADD INDEX credit_card_index (CREDIT_CARD_NO);
                        """)
        conn.commit()
        
        cursor.execute("""
                       ALTER TABLE cdw_sapp_branch
                       ADD PRIMARY KEY (BRANCH_CODE)
                       """)
        conn.commit()
        
        cursor.execute("""
                       ALTER TABLE cdw_sapp_credit_card
                       MODIFY COLUMN CREDIT_CARD_NO VARCHAR(255),
                       ADD PRIMARY KEY (TRANSACTION_ID),
                       ADD CONSTRAINT `branch_fk` FOREIGN KEY (`BRANCH_CODE`) REFERENCES `CDW_SAPP_BRANCH` (`BRANCH_CODE`),
                       ADD CONSTRAINT `credit_card_fk` FOREIGN KEY (CREDIT_CARD_NO) REFERENCES `CDW_SAPP_CUSTOMER`(CREDIT_CARD_NO)
        """)
        conn.commit()    
        

    except Exception as e:
        print(f"Error: {e}")
    finally:
    # Close the session
        cursor.close()
        conn.close()
        
# 2) Modify existing account details of a customer
def modify_cust_detail(column, value, SSN):
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
    
        # Execute a SQL query
        cursor = conn.cursor()
        
        if Execute_function.check_cust_SSN(SSN):
            cursor.execute(f"""
                            UPDATE cdw_sapp_customer
                            SET {column} = '{value}'
                            WHERE RIGHT(SSN, 4) = '{SSN}';
                        """)
            conn.commit()
        else:
            print("Invalid Customer SSN.")
        
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
        cursor.close()
        conn.close()
        spark.stop()
        
def print_update(bool, name):
    if bool:
        Display.color_print((name + " created successfully"), Display.Color.YELLOW)
    else:
        Display.color_print((name + " existed"), Display.Color.YELLOW)
def Import_database_to_mySQL():
    Display.color_print("Creating Database if not existed in mySQL localhost", Display.Color.BLUE)
    database = create_database_in_mysql()
    print_update(database, "Database")
        
    Display.color_print("Creating tables in database", Display.Color.BLUE)
    tables = create_table_in_mysql()
    print_update(tables, "Tables")
    
    Display.color_print("Set up relationships between tables in mySQL", Display.Color.BLUE)
    modify_tables()
    Display.color_print("Relationships set", Display.Color.YELLOW)
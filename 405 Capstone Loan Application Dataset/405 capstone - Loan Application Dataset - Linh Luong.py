import requests
import json
from pyspark.sql import SparkSession


class Color:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'
    
def color_print(text, color):
    print(f"{color}{text}{Color.END}")
    
    

def Get_data_API():
    url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    try:
        response = requests.get(url)
        spark = SparkSession.builder.appName("Read_JSON_From_URL").getOrCreate()

        if response.status_code == 200:
            # The response contains JSON data
            customer_data = response.json()
            with open(r"D:\CAP 405 Data Analytics - Capstone Project\loan_data.json", 'w') as file:
                json.dump(customer_data, file)
            color_print("Successfully retrieved loan data:", Color.YELLOW)
        else:
            color_print(f"Failed to retrieve data. Status code: {response.status_code}", Color.RED)            

    except Exception as e:
        color_print(f"An error occurred: {e}", Color.RED)
        
def read_data_json():
    spark = SparkSession.builder.appName("Read_JSON_From_File").getOrCreate()
    
    df = spark.read.json(r"D:\CAP 405 Data Analytics - Capstone Project\loan_data.json")
    df.show()
        
def calculate_status_code():
    url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

    response = requests.get(url)

    status_code = response.status_code

    color_print(f"Status Code: {status_code}", Color.YELLOW)
    
def load_data_RDBMS():
    try:
        spark = SparkSession.builder.appName("Loading_data_to_RDBMS").getOrCreate()
        
        df = spark.read.json(r"D:\CAP 405 Data Analytics - Capstone Project\loan_data.json")

        df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", "CDW_SAPP_loan_application") \
        .option("user", "root") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()
        
        color_print("Successfully loaded data to RDBMS:", Color.YELLOW)
        
        color_print("writing data to csv", Color.BLUE)
        df.write.csv(r"D:\CAP 405 Data Analytics - Capstone Project\loan_data.csv", header=True)
        color_print("Successfully loaded data to csv:", Color.YELLOW)
        
           
    except Exception as e:
            color_print(e, Color.RED)
    finally:
        spark.stop()
        

if __name__ == "__main__":
    Get_data_API()
    calculate_status_code()
    load_data_RDBMS()
    read_data_json()
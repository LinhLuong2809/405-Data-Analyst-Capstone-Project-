![Alt text](https://i.pinimg.com/originals/61/8c/22/618c2238c54db943fe85bc7aa2dde3ac.png)
# **DA CAPSTONE REPORT**
---
## Linh Luong
###### Completed March, 2024  
#
#
#
#
## Core Capstone Components
### 1. Load Credit Card Database (SQL)
We were given three json files:
[Branch.json](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/cdw_sapp_branch.json)
[Customer.json](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/cdw_sapp_custmer.json)
[Credit.json](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/cdw_sapp_credit.json)

We were required to create a database name **creditcard_capstone** in mySQL using Python and Pyspark. Then load data from these three files into the database.

I write a Python module file [**Generate.py**](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/Generate.py) using **pySpark** and define functions to connect to my local SLQ server and create database **creditcard_capstone**, loading data from json files into mySQL database and set up relationship between tables.

In order to execute the program, I write a backend Python file [405 capstone application - Backend - Linh Luong.py](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/405%20capstone%20application%20-%20Backend%20-%20Linh%20Luong.py). By running this file, the program will execute all the functions in Generate.py file.

### 2. Application Front-End
I write two Python module [Display.py](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/Display.py) containing functions to display the output on the screen and [Execute_function.py](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/Execute_function.py) containing functions to prompt user for input and decision. 

Running [405 capstone application - Front End - Linh Luong.py](https://github.com/LinhLuong2809/405-Data-Analyst-Capstone-Project-/blob/main/405%20Capstone%20Python%20Application%20Development%20-%20Linh%20Luong/405%20capstone%20application%20-%20Front%20End%20-%20Linh%20Luong.py) file will run the program by calling the two modules Display.py and [Execute_function.py]()
### 3. Data Analysis And Visualization

### 4. Functional Requirements - LOAN Application Dataset




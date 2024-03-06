import Generate

if __name__ == "__main__":
    try:
        Generate.Import_database_to_mySQL()
        
    except Exception as e:
        print(e)
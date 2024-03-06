import Display
import Execute_function
import json


if __name__ == "__main__":
    try:
        while True:
            Display.display_menu()
            option = Execute_function.prompt_user_choice()
            Execute_function.execute_option(option)
            print()
            if option == 12:
                break
        
    except Exception as e:
        print(e)
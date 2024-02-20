from Database import DB
database = DB()
running = True
while running:
    database.menu()
    userChoice = input()  
    if userChoice == "1":  # Create new database
        filename = input("\nEnter name of .csv file (i.e: for Titanic.csv, type 'Titanic' and hit enter.)\n")
        database.createDB(filename)
    elif userChoice == "2":  # Open database
        if not database.databaseClosed():
            print("\nThere is a database already open. You must close it to proceed.\n")
        else:
            db_name = input("\nEnter name of database to open (i.e: for Titanic database, type 'Titanic' and hit enter.)\n")
            database.openDB(db_name)
    elif userChoice == "3":  # Close database
        database.CloseDB()
    elif userChoice == "4":  # Read Record
        recordNum = int(input("\nEnter record ID to display:\n"))
        print(database.getRecord(recordNum))
    elif userChoice == "5":  # Display Record
         # Before calling database.displayRecord(passengerId), make sure passengerId is valid
        passengerId = input("\nEnter passenger ID to display:\n").strip()

# Validate passengerId is not empty and is a digit
        if not passengerId.isdigit():
            print("Invalid passenger ID. Please enter a valid numeric ID.")
        else:
            database.displayRecord(passengerId)

    elif userChoice == "6":  # Create Report done
        database.createRecord()
    elif userChoice == "7":  # Update Record  done
        database.updateRecord()
    elif userChoice == "8":  # Delete Record  not done
        database.deleteRecord()
    elif userChoice == "9":  # Add Record not done
        database.addRecord()
    elif userChoice == "10": # Quit done
        database.CloseDB()
        running = False

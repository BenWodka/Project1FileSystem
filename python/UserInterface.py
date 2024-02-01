from Database import DB

database = DB()


running = True
while running == True:
    userChoice = input(DB.menu())
    match userChoice:
        case "1": #Create new database
            filename = input("\nEnter name of .csv file (i.e: for Titanic.csv, type 'Titanic' and hit enter.\n)")
            database.createDB(filename)
        case "2": # Open database
            if database.databaseClosed() == False:
                print("\nThere is a database already open. You must close it to proceed.\n")
            else:
                db_name = input("\nEnter name of database to open (i.e: for Titanic database, type 'Titanic' and hit enter.\n)")
                database.openDB(db_name)
        case "3": # Close database
            database.CloseDB()
        case "4": # Display Record
            database.getRecord()
        case "5": # Update Record
            database.updateRecord()
        case "6": # Create Report
            database.createRecord()
        case "7": # Add record
            database.addRecord()
        case "8": # Delete Record
            database.deleteRecord()
        case "9": #Quit
            database.CloseDB()
            running = False

    
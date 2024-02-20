import ast
import csv
import os



class DB:

    #default constructor
    def __init__(self):
        self.filestream = None
        self.db_file = None
        self.dbClosed = True

        
        self.record_size = 0
        self.num_records = 0

        self.Id_size=7  #must be 7 for _empty_
        self.lastName_size=20
        self.firstName_size=20
        self.age_size=3
        self.ticketNum_size=25
        self.fare_size=6
        self.dateOfPurchase_size=10



    #create database
    #print(f'file path: {os.getcwd()}')

    def createDB(self,filename):
        #Generate file names
        csv_filename = filename + ".csv"
        text_filename = filename + ".data"
        #config_filename = filename + ".config"

        if not os.path.isfile(csv_filename):
            print(str(csv_filename)+" not found")
            return False
        
        #reads config file to get specifications
        configContent = self.readConfigFile(filename)

        #sets attributes to what config file specifies

        if os.name == 'nt': #For windows machines
            self.record_size = configContent["recordSize"] + 1
        else:  #Linux/Mac
            self.record_size = configContent["recordSize"]
        self.num_records = configContent["numRecords"]
        self.Id_size = configContent["PASSENGER_ID_SIZE"]
        self.lastName_size = configContent["LAST_NAME_SIZE"]
        self.firstName_size = configContent["FIRST_NAME_SIZE"]
        self.age_size = configContent["AGE_SIZE"]
        self.ticketNum_size = configContent["TICKET_NUM_SIZE"]
        self.fare_size = configContent["FARE_SIZE"]
        self.dateOfPurchase_size = configContent["DATE_OF_PURCHASE_SIZE"] 

		# Formatting files with spaces so each field is fixed length, i.e. ID field has a fixed length of 10
        def writeRecord(filestream, dict):
            filestream.write("{:{width}.{width}}".format(dict["ID"],width=self.Id_size))
            filestream.write("{:{width}.{width}}".format(dict["lastName"],width=self.lastName_size))            
            filestream.write("{:{width}.{width}}".format(dict["firstName"],width=self.firstName_size))
            filestream.write("{:{width}.{width}}".format(dict["age"],width=self.age_size))
            filestream.write("{:{width}.{width}}".format(dict["ticketNum"],width=self.ticketNum_size))
            filestream.write("{:{width}.{width}}".format(dict["fare"],width=self.fare_size))
            filestream.write("{:{width}.{width}}".format(dict["dateOfPurchase"],width=self.dateOfPurchase_size))
            filestream.write("\n")

            #write an empty record of same length
            filestream.write("{:{width}.{width}}".format('_empty_',width=self.Id_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.lastName_size))            
            filestream.write("{:{width}.{width}}".format(' ',width=self.firstName_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.age_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.ticketNum_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.fare_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.dateOfPurchase_size))
            filestream.write("\n")

        # Read the CSV file one line at a time, then writes to the .data file one record at a time
        def readCSV(csv_filename):
            with open(csv_filename, "r") as csv_file:
                with open(text_filename,"w") as outfile:
                #ensures it reads csv file one line at a time
                    reader = csv.DictReader(csv_file, fieldnames=('ID', 'lastName', 'firstName', 'age', 'ticketNum', 'fare', 'dateOfPurchase'))
                    #print(reader)
                    #writes record one at a time (two if you count empty record)
                    for row in reader:
                        writeRecord(outfile,row)

        
        readCSV(csv_filename)
        print("\nDatabase successfully created. If you wish to open this database, use option 2.\n")


    def databaseClosed(self): # for Checking if db is open in case 2
        return self.dbClosed
    

    #read config file
    def readConfigFile(self, filename):
        config_filename = filename + ".config"
        self.filestream = filename + ".data"
        #checks to see if config file doesn't exist
        if not os.path.isfile(config_filename):
            print(str(config_filename)+" not found")
            return False
        with open(config_filename, "r") as file:
            content = file.read()
            content = ast.literal_eval(content) #puts content into a dictionary
            return content

    #read the database
    def readDB(self, filename, DBsize, rec_size):
        self.filestream = filename + ".data"
        self.record_size = DBsize
        self.rec_size = rec_size
        
        if not os.path.isfile(self.filestream):
            print(str(self.filestream)+" not found")
        else:
            self.text_filename = open(self.filestream, 'r+')
    
    def openDB(self, filename): #returns True if successful

        #Checks if a database is already open
        if self.dbClosed == False:
            print("A database is already open. Please close it before opening another.")
            return False
        
        self.filestream = filename + ".data"

        #checks to see if database doesn't exist
        if not os.path.isfile(self.filestream):
            print(str(filename)+" database not found")
            return False #Not found, return false
        
        else:
            self.db_file = open(self.filestream, "r")
            self.dbClosed = False
            print("\nDatabase opened successfully.\n")
            return True
        
        #close the database
    def CloseDB(self):

        if self.db_file: #Makes sure a db is open
            self.db_file.close()  # Close the file properly
            self.db_file = None
            self.dbClosed = True
            print(f"\nDatabase closed successfully.\n")
        else:
            print("\nNo open database file to close.\n")
        

    def getRecord(self, recordNum):
       

        if self.db_file is None:
            print("\nNo database file is open.\n")
            return None

        offset = recordNum * self.record_size
     

        self.db_file.seek(offset)
        line = self.db_file.readline().rstrip('\n')

     

        if not line:
            print(f"Could not find record #{recordNum}.\n")
            return None

        id, lastName, firstName, age, ticketNum, fare, dateOfPurchase = (
            line[i:j].strip() for i, j in zip(
                [0, self.Id_size, self.Id_size+self.lastName_size, self.Id_size+self.lastName_size+self.firstName_size, 
                self.Id_size+self.lastName_size+self.firstName_size+self.age_size, 
                self.Id_size+self.lastName_size+self.firstName_size+self.age_size+self.ticketNum_size, 
                self.Id_size+self.lastName_size+self.firstName_size+self.age_size+self.ticketNum_size+self.fare_size],
                [self.Id_size, self.Id_size+self.lastName_size, self.Id_size+self.lastName_size+self.firstName_size, 
                self.Id_size+self.lastName_size+self.firstName_size+self.age_size, 
                self.Id_size+self.lastName_size+self.firstName_size+self.age_size+self.ticketNum_size, 
                self.Id_size+self.lastName_size+self.firstName_size+self.age_size+self.ticketNum_size+self.fare_size, 
                self.Id_size+self.lastName_size+self.firstName_size+self.age_size+self.ticketNum_size+self.fare_size+self.dateOfPurchase_size]
            )
        )

        if id == "_empty_":
           
            
            return None

       

        return {"ID": id, "lastName": lastName, "firstName": firstName, "age": age, "ticketNum": ticketNum, "fare": fare, "dateOfPurchase": dateOfPurchase}


    def binarySearch(self, input_ID, flag):
        low, high = 0, self.num_records - 1

        while low <= high:
            mid = (low + high) // 2
            mid_record = self.getRecord(mid)

            # If the record is "_empty_", try finding the nearest non-empty record.
            if mid_record is None or mid_record["ID"].strip() == "_empty_":
                nearest_non_empty = self.findNearestNonEmpty(mid, low, high)
                if nearest_non_empty == -1:
                    if flag is False:
                        print(f"Could not find record with ID {input_ID}.")
                        return None
                    return mid
                else:
                    mid_record = self.getRecord(nearest_non_empty)
                    # Adjust mid to the nearest non-empty for accurate comparison
                    mid = nearest_non_empty

            mid_id = mid_record["ID"].strip()

           
            if int(mid_id) == int(input_ID):
                return mid_record
            elif int(mid_id) < int(input_ID):
                low = mid + 1
            else:
                high = mid - 1

        if flag is False:
            print(f"Record with ID {input_ID} not found.")
        else:
            print(f"Could not add ID {input_ID}, database may be full in this location.\n")
        #print(f"\nmid: {mid}\n")
        return None



    def findNearestNonEmpty(self, start, low_limit, high_limit):
        step = 1  # Initialize step size

        while True:
            # Check backward
            if start - step >= low_limit:
                record = self.getRecord(start - step)
                if record["ID"].strip() != "_empty_":
                    #print(self.record)
                    return start - step

            # Check forward
            if start + step <= high_limit:
                record = self.getRecord(start + step)
                if record["ID"].strip() != "_empty_":
                    #print(self.record)
                    return start + step

            # Increase step size and repeat
            step += 1

            # Terminate if beyond the search range
            if start - step < low_limit and start + step > high_limit:
                break

        return -1  # No non-empty record found

   
    def displayRecord(self, passengerId):
        result = self.binarySearch(passengerId, False)
        if result:
            # Assuming result is the record dict
            print("\nRecord Details:")
            for field, value in result.items():
                print(f"{field}: {value}")
        else:
         print(f"Record with passengerId {passengerId} not found.")


    def updateRecord(self):
        # passenger ID to update record
        passengerId = input("\nEnter passenger ID to update record:\n").strip()
        # Check if the passenger ID is valid
        if not passengerId.isdigit():
            print("Invalid passenger ID. Please enter a valid numeric ID.")
            return

        # Find the record
        record = self.binarySearch(passengerId, False)
        if record:
            # Display the current record
            print("\nCurrent Record Details:")
            for field, value in record.items():
                print(f"{field}: {value}")

            # Prompt the user to select a field to update
            print("\nSelect the field to update (excluding primary key):")
            print("1. Last Name")
            print("2. First Name")
            print("3. Age")
            print("4. Ticket Number")
            print("5. Fare")
            print("6. Date of Purchase")
            choice = input("Enter your choice: ")

            # Validate user input
            if choice not in {'1', '2', '3', '4', '5', '6'}:
                print("Invalid choice. Please enter a number between 1 and 6.")
                return

            # Prompt the user for the new value
            new_value = input("Enter the new value: ").strip()

            # Update the selected field
            if choice == '1':
                record['lastName'] = new_value
            elif choice == '2':
                record['firstName'] = new_value
            elif choice == '3':
                record['age'] = new_value
            elif choice == '4':
                record['ticketNum'] = new_value
            elif choice == '5':
                record['fare'] = new_value
            elif choice == '6':
                record['dateOfPurchase'] = new_value

            # Write the updated record back to the database file
            self.writeUpdatedRecord(record)
            print("Record updated successfully.")
        else:
            print(f"Record with passenger ID {passengerId} not found.")

    def writeUpdatedRecord(self, record):
        # Find the position of the record in the file
        offset = self.findRecordOffset(record['ID'])

        # Write the updated record back to the file
        with open(self.filestream, 'r+') as file:
            file.seek(offset)
            file.write("{:{width}.{width}}".format(record["ID"], width=self.Id_size))
            file.write("{:{width}.{width}}".format(record["lastName"], width=self.lastName_size))
            file.write("{:{width}.{width}}".format(record["firstName"], width=self.firstName_size))
            file.write("{:{width}.{width}}".format(record["age"], width=self.age_size))
            file.write("{:{width}.{width}}".format(record["ticketNum"], width=self.ticketNum_size))
            file.write("{:{width}.{width}}".format(record["fare"], width=self.fare_size))
            file.write("{:{width}.{width}}".format(record["dateOfPurchase"], width=self.dateOfPurchase_size))
            file.write("\n")

   
    def findRecordOffset(self, passengerId):
        # Binary search to find the record position in the file
        low, high = 0, self.num_records - 1

        while low <= high:
            mid = (low + high) // 2
            mid_record = self.getRecord(mid)

            if mid_record is None or mid_record["ID"].strip() == "_empty_":
                nearest_non_empty = self.findNearestNonEmpty(mid, low, high)
                if nearest_non_empty == -1:
                    print(f"Could not find record with ID {passengerId}.")
                    return None
                else:
                    mid_record = self.getRecord(nearest_non_empty)
                    
                    mid = nearest_non_empty

            mid_id = mid_record["ID"].strip()

            
            if int(mid_id) == int(passengerId):
                
                offset = mid * self.record_size
                return offset
            elif int(mid_id) < int(passengerId):
                low = mid + 1
            else:
                high = mid - 1

        print(f"Record with ID {passengerId} not found.")
        return None

    def createRecord(self):
        if self.db_file is None:
            print("\nNo database file is open.\n")
            return

        print("Creating report of the first ten records...")
        print("\n{:<10} {:<20} {:<20} {:<5} {:<25} {:<10} {:<15}".format('ID', 'Last Name', 'First Name', 'Age', 'Ticket Number', 'Fare', 'Date of Purchase'))
        print("-" * 105)  

        for recordNum in range(10):
            record = self.getRecord(recordNum)
            if record is None:
                continue  
            print("{ID:<10} {lastName:<20} {firstName:<20} {age:<5} {ticketNum:<25} {fare:<10} {dateOfPurchase:<15}".format(**record))

        print("\nReport generated successfully.")


    def addRecord(self):
        if self.db_file is None or self.dbClosed:
            print("\nNo open database file.\n")
            return

        print("\nAdding a new record to the database.\n")

        id = input("Enter passenger ID:\n").strip()

        if not id.isdigit():
                print("Invalid ID. ID must be a number.")
                return

        # if self.binarySearch(id) is not None:
        #     print(f"ID already exists. Each passenger ID must be unique.")
        #     return

        new_record = {
            "ID": id,
            "lastName": input("Enter last name:\n").strip(),
            "firstName": input("Enter first name:\n").strip(),
            "age": input("Enter age:\n").strip(),
            "ticketNum": input("Enter ticket number:\n").strip(),
            "fare": input("Enter fare:\n").strip(),
            "dateOfPurchase": input("Enter date of purchase (MM-DD-YYYY):\n").strip()
        }

        line = self.binarySearch(id, True)
        if line is not None:
            offset = line * self.record_size
            with open(self.filestream, 'r+') as file:
                file.seek(offset)
                file.write("{:{width}.{width}}".format(new_record["ID"],width=self.Id_size))
                file.write("{:{width}.{width}}".format(new_record["lastName"],width=self.lastName_size))            
                file.write("{:{width}.{width}}".format(new_record["firstName"],width=self.firstName_size))
                file.write("{:{width}.{width}}".format(new_record["age"],width=self.age_size))
                file.write("{:{width}.{width}}".format(new_record["ticketNum"],width=self.ticketNum_size))
                file.write("{:{width}.{width}}".format(new_record["fare"],width=self.fare_size))
                file.write("{:{width}.{width}}".format(new_record["dateOfPurchase"],width=self.dateOfPurchase_size))
                file.write("\n")
    
    def deleteRecord(self):
        if self.db_file is None or self.dbClosed:
            print("\nNo open database file.\n")
            return

        passengerId = input("\nEnter passenger ID to delete:\n").strip()

        if not passengerId.isdigit():
            print("Invalid passenger ID. Please enter a valid numeric ID.")
            return

        record = self.binarySearch(passengerId, False)
        if record:
            
            confirmation = input(f"Are you sure you want to delete record {passengerId}? (yes/no):\n").strip().lower()
            if confirmation != 'yes':
                print("Deletion cancelled.")
                return
            self.markRecordAsDeleted(passengerId)
            print(f"Record {passengerId} deleted successfully.")
        else:
            print(f"Record with ID {passengerId} not found.")

    def markRecordAsDeleted(self, passengerId):
        offset = self.findRecordOffset(passengerId)
        if offset is not None:
            with open(self.filestream, 'r+') as file:
                file.seek(offset)   
                file.write("_empty_".ljust(self.Id_size))  
                file.write(" ".ljust(self.record_size - self.Id_size - 1))
                file.write("\n")
        else:
            print("Error finding record offset.")

            
    def menu(self):
        print("\nWelcome to the database. Choose one of the following menu options:")
        print('1) Create new database')
        print('2) Open database')
        print('3) Close database')
        print('4) Read record')
        print('5) Display record')
        print('6) Create report')
        print('7) Update record')
        print('8) Delete record')
        print('9) Add record')
        print('10) Quit\n')
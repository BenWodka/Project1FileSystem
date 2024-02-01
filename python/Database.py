import csv
import os.path
import ast
# test 1
class DB:

    #default constructor
    def __init__(self):
        self.filestream = None
        self.db_file = None
        self.num_record = 0
        #self.numBytes = 72 # num of bytes for each record
        #91 bytes per record = 93 on Windows
        self.Id_size=7 #must be 7 for _empty_
        self.firstName_size=20
        self.lastName_size=20
        self.age_size=3
        self.ticketNum_size=25
        self.fare_size=6
        self.dateOfPurchase_size=10
        self.dbClosed = False

    #create database
    print(f'file path: {os.getcwd()}')

    def createDB(self,filename):
        #Generate file names
        csv_filename = filename + ".csv"
        text_filename = filename + ".data"
        #config_filename = filename + ".config"

        #reads config file to get specifications
        configContent = self.readConfigFile(filename)

        #sets attributes to what config file specifies
        self.Id_size = configContent["PASSENGER_ID_SIZE"]
        self.firstName_size = configContent["FIRST_NAME_SIZE"]
        self.lastName_size = configContent["LAST_NAME_SIZE"]
        self.age_size = configContent["AGE_SIZE"]
        self.ticketNum_size = configContent["TICKET_NUM_SIZE"]
        self.fare_size = configContent["FARE_SIZE"]
        self.dateOfPurchase_size = configContent["DATE_OF_PURCHASE_SIZE"] 

		# Formatting files with spaces so each field is fixed length, i.e. ID field has a fixed length of 10
        def writeRecord(filestream, dict):
            filestream.write("{:{width}.{width}}".format(dict["ID"],width=self.Id_size))
            filestream.write("{:{width}.{width}}".format(dict["firstName"],width=self.firstName_size))
            filestream.write("{:{width}.{width}}".format(dict["lastName"],width=self.lastName_size))
            filestream.write("{:{width}.{width}}".format(dict["age"],width=self.age_size))
            filestream.write("{:{width}.{width}}".format(dict["ticketNum"],width=self.ticketNum_size))
            filestream.write("{:{width}.{width}}".format(dict["fare"],width=self.fare_size))
            filestream.write("{:{width}.{width}}".format(dict["dateOfPurchase"],width=self.dateOfPurchase_size))
            filestream.write("\n")

            #write an empty record of same length
            filestream.write("{:{width}.{width}}".format('_empty_',width=self.Id_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.firstName_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.lastName_size))
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
                    reader = csv.DictReader(csv_file, fieldnames=('ID', 'firstName', 'lastName', 'age', 'ticketNum', 'fare', 'dateOfPurchase'))
                    print(reader)
                    #writes record one at a time (two if you count empty record)
                    for row in reader:
                        writeRecord(outfile,row)

        readCSV(csv_filename)

    #read config file
    def readConfigFile(self, filename):
        config_filename = filename + ".config"
        self.filestream = filename + ".data"
        #checks to see if config file doesn't exist
        if not os.path.isfile(config_filename):
            print(str(config_filename)+" not found")
            successValue = False #should already be False. For redundancy
            return successValue
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
        
    #read record method
    def getRecord(self, recordNum):

        self.flag = False
        id = firstName = lastName = age = ticketNum = fare = dateOfPurchase = "None"

        if recordNum >=0 and recordNum < self.record_size:
            self.text_filename.seek(0,0)
            self.text_filename.seek(recordNum*self.rec_size)
            line= self.text_filename.readline().rstrip('\n')
            self.flag = True
        
        if self.flag:
            #Ok so what i did here was make the bounds dependent on the variable size, so we dont have to rewrite this every time we change a variable size.
            id = line[0:self.Id_size]
            firstName = line[self.Id_size:self.Id_size+self.firstName_size]
            lastName = line[self.Id_size+self.firstName_size:self.Id_size+self.firstName_size+self.lastName_size]
            age = line[self.Id_size+self.firstName_size+self.lastName_size:self.Id_size+self.firstName_size+self.lastName_size+self.age_size]
            ticketNum = line[self.Id_size+self.firstName_size+self.lastName_size+self.age_size:self.Id_size+self.firstName_size+self.lastName_size+self.age_size+self.ticketNum_size]
            fare = line[self.Id_size+self.firstName_size+self.lastName_size+self.age_size+self.ticketNum_size:self.Id_size+self.firstName_size+self.lastName_size+self.age_size+self.ticketNum_size+self.fare_size]
            dateOfPurchase = line[self.Id_size+self.firstName_size+self.lastName_size+self.age_size+self.ticketNum_size+self.fare_size:self.Id_size+self.firstName_size+self.lastName_size+self.age_size+self.ticketNum_size+self.fare_size+self.dateOfPurchase_size]
            self.record = dict({"ID":id,"firstName":firstName,"lastName":lastName,"age":age,"ticketNum":ticketNum,"fare":fare,"dateOfPurchase":dateOfPurchase})

    #Binary Search by record id
    def binarySearch(self, input_ID):
        low = 0
        high = self.record_size - 1
        found = False
        self.recordNum = None  # Initialize the insertion point

        while not found and high >= low:
            self.middle = (low + high) // 2
            self.getRecord(self.middle)
            mid_id = self.record["ID"]

            if mid_id.strip() == "_empty_":
                non_empty_record = self.findNearestNonEmpty(self.middle, low, high)
                if non_empty_record == -1:
                    # If no non-empty record found, set recordNum for potential insertion
                    self.recordNum = high 
                    print("Could not find record with ID..", input_ID)
                    return False

                self.middle = non_empty_record
                self.getRecord(self.middle)
                mid_id = self.record["ID"]
                if int(mid_id) > int(input_ID):
                    self.recordNum = self.middle - 1
                else:
                    self.recordNum = self.middle + 1

            if mid_id != "_empty_":
                try:
                    if int(mid_id) == int(input_ID):
                        found = True
                        self.recordNum = self.middle
                    elif int(mid_id) > int(input_ID):
                        high = self.middle - 1
                    elif int(mid_id) < int(input_ID):
                        low = self.middle + 1
                except ValueError:
                    # Handle non-integer IDs
                    high = self.middle - 1

        if not found and self.recordNum is None:
            # Set recordNum to high + 1 if no suitable spot is found
            self.recordNum = high 
            print("Could not find record with ID", input_ID)

        return found


    

    def findNearestNonEmpty(self, start, low_limit, high_limit):
        step = 1  # Initialize step size

        while True:
            # Check backward
            if start - step >= low_limit:
                self.getRecord(start - step)
                if self.record["ID"].strip() != "_empty_":
                    #print(self.record)
                    return start - step

            # Check forward
            if start + step <= high_limit:
                self.getRecord(start + step)
                if self.record["ID"].strip() != "_empty_":
                    #print(self.record)
                    return start + step

            # Increase step size and repeat
            step += 1

            # Terminate if beyond the search range
            if start - step < low_limit and start + step > high_limit:
                break

        return -1  # No non-empty record found

    #close the database
    def CloseDB(self):

            self.filestream.close()  # Close the file properly
            self.dbClosed = True
            print("\nDatabase closed successfully.\n")

    def updateRecord(self):
        print()
    
    def createRecord(self):
        print()

    def addRecord(self):
        print()
    
    def deleteRecord(self):
        print()
        
    def menu():
        print("\nWelcome to the database. Choose one of the following menu options:")
        print('1) Create new database')
        print('2) Open database')
        print('3) Close database')
        print('4) Display record')
        print('5) Update record')
        print('6) Create report')
        print('7) Add record')
        print('8) Delete record')
        print('9) Quit\n')




import csv
import os.path
# test 1
class DB:

    #default constructor
    def __init__(self):
        self.filestream = None
        self.num_record = 0
        #self.numBytes = 72 # num of bytes for each record
        self.Id_size=10
        self.Experience_size=5
        self.Marriage_size=5
        self.Wage_size=30
        self.Industry_size=20
        self.dbClosed = False



    #create database
    print(f'file path: {os.getcwd()}')

    def createDB(self,filename):
        #Generate file names
        csv_filename = filename + ".csv"
        text_filename = filename + ".data"
        config_filename = filename + ".config"

        # Read the CSV file and write into data files
        with open(csv_filename, "r") as csv_file:
            data_list = list(csv.DictReader(csv_file,fieldnames=('ID','experience','marriage','wages','industry')))

		# Formatting files with spaces so each field is fixed length, i.e. ID field has a fixed length of 10
        def writeDB(filestream, dict):
            filestream.write("{:{width}.{width}}".format(dict["ID"],width=self.Id_size))
            filestream.write("{:{width}.{width}}".format(dict["experience"],width=self.Experience_size))
            filestream.write("{:{width}.{width}}".format(dict["marriage"],width=self.Marriage_size))
            filestream.write("{:{width}.{width}}".format(dict["wages"],width=self.Wage_size))
            filestream.write("{:{width}.{width}}".format(dict["industry"],width=self.Industry_size))
            filestream.write("\n")

            #write an empty record of same length
            filestream.write("{:{width}.{width}}".format('_empty_',width=self.Id_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.Experience_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.Marriage_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.Wage_size))
            filestream.write("{:{width}.{width}}".format(' ',width=self.Industry_size))
            filestream.write("\n")

        
        with open(text_filename,"w") as outfile:
            for dict in data_list:
                writeDB(outfile,dict)
            
        with open(config_filename,"w") as outfile:
            for dict in data_list:
                writeDB(outfile,dict)

    #read the database
    def readDB(self, filename, DBsize, rec_size):
        self.filestream = filename + ".data"
        self.record_size = DBsize
        self.rec_size = rec_size
        
        if not os.path.isfile(self.filestream):
            print(str(self.filestream)+" not found")
        else:
            self.text_filename = open(self.filestream, 'r+')

    #read record method
    def getRecord(self, recordNum):

        self.flag = False
        id = experience = marriage = wage = industry = "None"

        if recordNum >=0 and recordNum < self.record_size:
            self.text_filename.seek(0,0)
            self.text_filename.seek(recordNum*self.rec_size)
            line= self.text_filename.readline().rstrip('\n')
            self.flag = True
        
        if self.flag:
            id = line[0:10]
            experience = line[10:15]
            marriage = line[15:20]
            wage = line[20:40]
            industry = line[40:70]
            self.record = dict({"ID":id,"experience":experience,"marriage":marriage,"wages":wage,"industry":industry})

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

        self.text_filename.close()
        dbClosed = True

    def openDB(self, db_name):
        DB.readDB(db_name, 100, 72)


        print()

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




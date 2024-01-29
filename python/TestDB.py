from Database import DB

filepath = "input"
DBsize = 20
#rec_size = 72
rec_size = 71 #if a Windows file with cr lf at ends of the lines

sample = DB()
#sample.createDB(filepath)
sample.readDB(filepath, DBsize, rec_size)

print("\n------------- Testing getRecord ------------\n")

# Gets record 0
# Then prints the values of the 5 fields to the screen with the name of the
# field and the values read from the record, i.e.,
# id: 00003 experience: 3 married: no wages: 1.344461678 industry:
# Business and Repair Service
sample.getRecord(0)
print("Record 0, ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"])

# Gets record 9 (last record)
sample.getRecord(DBsize - 2)
print("Record 18, ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"])

print("\n------------- Testing binarySearch ------------\n")
# Find record with id 42 (should not be found)
found=sample.binarySearch("42")
if found:
    print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tRecord Number:" + str(sample.recordNum))
else:
    print("42 not found. Location to insert: ",sample.recordNum)

# Find record with id 00000 (the first one in the file)
found=sample.binarySearch("00000")
if found:
    print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tRecord Number:" + str(sample.recordNum))
else:
    print("00000 not found. Location to insert: ",sample.recordNum)
	
# Find record with id 00015 (the last one in the file)
found=sample.binarySearch("000015")
if found:
    print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tRecord Number:" + str(sample.recordNum))
else:
    print("15 not found,. Location to insert: ",sample.recordNum)

# Find record with id 00006 (somewhere in the middle)
found=sample.binarySearch("00006")
if found:
    print("ID: "+sample.record["ID"]+"\t experience: "+sample.record["experience"]+"\t marriage: "+sample.record["marriage"]+"\t wages: "+str(sample.record["wages"])+"\t industry: "+sample.record["industry"]+"\tRecord Number:" + str(sample.recordNum))
else:
    print("00006 not found. Location to insert:",sample.recordNum)

# Close database
sample.CloseDB()

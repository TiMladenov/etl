import mysql.connector


# Connect to OLTP database
mydb = mysql.connector.connect(
    host="",
    user="",
    passwd="",
    database=""
)

# Connect to OLAP database
mydb_dwh = mysql.connector.connect(
    host="",
    user="",
    passwd="",
    database=""
)

# OLTP cursor, get data from DB with myresult
mycursor = mydb.cursor()
mycursor.execute("SELECT supplier_id, supplier_name, supplier_phone, city, post_code, supplier_email, country_code FROM supplier;")
myresult = mycursor.fetchall()

# OLAP cursor
mycursor_dwh = mydb_dwh.cursor()

#########################################################
# Truncating tables for testing purposes
mycursor_dwh.execute("TRUNCATE TABLE Calendar_Python")
mydb_dwh.commit()
mycursor_dwh.execute("TRUNCATE TABLE Item_Python")
mydb_dwh.commit()
mycursor_dwh.execute("TRUNCATE TABLE Supplier_Python")
mydb_dwh.commit()
mycursor_dwh.execute("TRUNCATE TABLE Report_Python")
mydb_dwh.commit()
#########################################################

# Get the company contact data from the resolver file
correction_file_loc = 'pc_rezolver.csv'
read_correction_file = open(correction_file_loc, 'r')
file_contents = read_correction_file.read()
read_lines = file_contents.split(',')

# Convert all myresult elements from tuple to list, because tuple is immutable
for i in range(len(myresult)):
    myresult[i] = list(myresult[i])

# Go through every row from myresult, if there's missing contact information data (post_code or country), populate the empty fields for the
# corresponding by the corresponding city
for i in myresult:
    # Positions in the list for post_code, city and country_code
    post_code = i[len(i)-3]
    city = i[len(i)-4]
    country_code = i[len(i)-1]
    if post_code is None or country_code is None:
        j = 0
        
        # Compare city entries to the ones in the pc_rezolver file, fix formatting (remove new lines, etc). Replace any None fields with the
        # corresponding information for country_code and post_code
        for k in read_lines:
            j = j + 1
            if k == city:
                i[len(i)-3] = read_lines[j-2].replace("\r\n", "")
                i[len(i)-1] = read_lines[j].replace("\r\n", "")

# Substring the old IDs, cast them to INT for the table format in DWH
for i in myresult:
    for j in i:
        id = i[0]
        i[0] = int (id[2])
        i[4] = int (i[4])
        break

# Insert the information into the DWH Supplier_Python table
sql = "INSERT INTO Supplier_Python (supplier_id, supplier_name, supplier_phone, city, post_code, supplier_email, country_code) VALUES (%s, %s, %s, %s, %s, %s, %s)"
mycursor_dwh.executemany(sql, myresult)
mydb_dwh.commit()
print(mycursor_dwh.rowcount, " rows were inserted in Supplier_Python OLAP DB.")

# Get the item ID and Name for each purchase
mycursor.execute("SELECT item.item_id, item.item_name FROM item INNER JOIN itemlist ON item.item_id = itemlist.item_id INNER JOIN purchase ON purchase.purchase_id = itemlist.purchase_id WHERE purchase_del_date=0;")
myresult = mycursor.fetchall()

# Parse query result type from tuple to list
for i in range(len(myresult)):
    myresult[i] = list(myresult[i])

# Substring and parse to INT the old OLTP item_id to correspond to the type used in the OLAP DB
for i in myresult:
    id = i[0]
    i[0] = int (id[2])

sql = "INSERT INTO Item_Python (item_id, item_name) VALUES (%s, %s)"
mycursor_dwh.executemany(sql, myresult)
mydb_dwh.commit()
print(mycursor_dwh.rowcount, " rows were inserted in Item_Python OLAP DB.")

mycursor.execute("SELECT purchase.month AS month, purchase.year AS year, itemlist.item_id AS item_id FROM purchase LEFT JOIN itemlist ON purchase.purchase_id=itemlist.purchase_id WHERE purchase_deleted=0;")
myresult = mycursor.fetchall()

for i in range(len(myresult)):
    myresult[i] = list(myresult[i])

# Substring and parse to INT the old OLTP item_id to correspond to the type used in the OLAP DB
for i in myresult:
    id = i[2]
    i[2] = int (id[2])

sql = "INSERT INTO Calendar_Python (calendar_m, calendar_y, item_id) VALUES (%s, %s, %s)"
mycursor_dwh.executemany(sql, myresult)
mydb_dwh.commit()
print(mycursor_dwh.rowcount, " rows were inserted in Calendar_Python OLAP DB.")

mycursor.execute("SELECT item.item_id AS item_id, item.item_name AS item_name, itemlist.item_quantity AS item_quantity, itemlist.item_unit_cost AS item_unit_cost, purchase.purchase_date, supplier.supplier_name FROM item LEFT JOIN itemlist ON item.item_id = itemlist.item_id INNER JOIN purchase ON purchase.purchase_id = itemlist.purchase_id INNER JOIN supplier ON purchase.supplier_id = supplier.supplier_id WHERE purchase.purchase_deleted=0 ORDER BY purchase.purchase_date;")
myresult = mycursor.fetchall()

for i in range(len(myresult)):
    myresult[i] = list(myresult[i])

# Substring and parse to INT the old OLTP item_id to correspond to the type used in the OLAP DB
for i in myresult:
    id = i[0]
    i[0] = int (id[2])

# Insert Item data into OLAP DB
sql = "INSERT INTO Report_Python (item_id, item_name, item_quantity, item_unit_cost, purchase_date, supplier_name) VALUES (%s, %s, %s, %s, %s, %s)"
mycursor_dwh.executemany(sql, myresult)
mydb_dwh.commit()
print(mycursor_dwh.rowcount, " rows were inserted in Report_Python OLAP DB.")


# After all the data from above is entered into the database, this query gets the technical keys from each entry and enters it into the
# Report_Python fact table.
mycursor_dwh.execute("SELECT Item_Python.item_tk AS item_tk, Supplier_Python.supplier_tk AS supplier_tk, Calendar_Python.calendar_tk AS calendar_tk, Report_Python.item_id FROM Report_Python INNER JOIN Supplier_Python ON Report_Python.supplier_name = Supplier_Python.supplier_name INNER JOIN Calendar_Python ON Report_Python.item_id = Calendar_Python.item_id INNER JOIN Item_Python ON Report_Python.item_id = Item_Python.item_id;")
myresult_dwh = mycursor_dwh.fetchall()

for i in range(len(myresult_dwh)):
    myresult_dwh[i] = list(myresult_dwh[i])

myresult_dwh = sorted(myresult_dwh, key=lambda myres: myresult_dwh[3])

for i in myresult_dwh:
    mycursor_dwh.execute("UPDATE Report_Python SET item_tk = {}, supplier_tk = {}, calendar_tk={} WHERE item_id={}".format(i[0], i[1], i[2], i[3]))
    mydb_dwh.commit()


import pymysql.cursors

def main():

    # Configure MySQL connection
    mysqlConnection = pymysql.connect(
        host="localhost",
        user="atom-user",
        password="ATOMPASSWORD",
        db="atom",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    mysqlCursor = mysqlConnection.cursor()

    # Create a working table for transferring the legacy DIP file properties.
    sql = "CREATE TABLE IF NOT EXISTS dip_files(object_id INTEGER PRIMARY KEY, object_uuid TEXT, aip_uuid TEXT);"
    mysqlCursor.execute(sql)
    mysqlConnection.commit()

    # Create a working directory for downloading METS files.



    # Select all the legacy DIP files so that their properties can be updated.
    sql = "SELECT * FROM property WHERE name='objectUUID' AND scope is NULL;"
    mysqlCursor.execute(sql)
    legacy_dip_files = mysqlCursor.fetchall()

    for file in legacy_dip_files:
        try:
            # Select the Archivematica Object UUID which will be used to find
            # property info in the METS file.
            sql = "SELECT value FROM property_i18n WHERE id = %s;"
            mysqlCursor.execute(sql, file['id'])
            object_uuid = mysqlCursor.fetchone()
        except Exception as e:
            print("Unable to select Object UUID for object# " + str(file['id']) + ". Skipping...")
            print(e)
            continue

        try:
            # Select the Archivematica AIP UUID which will be used to fetch
            # the METS file from the Storage Service.
            sql = "SELECT id from property WHERE name = 'aipUUID' AND object_id = %s;"
            mysqlCursor.execute(sql, file['object_id'])
            property_id = mysqlCursor.fetchone()
            sql = "SELECT value FROM property_i18n WHERE id = %s;"
            mysqlCursor.execute(sql, property_id['id'])
            aip_uuid = mysqlCursor.fetchone()
        except Exception as e:
            print("Unable to select AIP UUID for object# " + str(file['id']) + ". Skipping...")
            print(e)
            continue

        try:
            # Store identifier values in the working table.
            sql = "INSERT INTO dip_files (object_id, object_uuid, aip_uuid) VALUES (%s, %s, %s);"
            mysqlCursor.execute(sql, (file['object_id'], object_uuid['value'], aip_uuid['value']))
            mysqlConnection.commit()
        except Exception as e:
            print("Unable to insert working data for object# " + str(file['id']) + ". Skipping...")
            print(e)
            continue

        try:
            # Flush the existing properties for the legacy digital file.
            # Includes an automatic cascade delete of the i18n value.
            sql = "DELETE FROM property WHERE object_id = %s;"
            mysqlCursor.execute(sql, file['object_id'])
            mysqlConnection.commit()
        except Exception as e:
            print("Unable to flush existing property values for object# " + str(file['id']) + ". Skipping...")
            print(e)
            continue

        # get all properties for this digital file from the AIP METS file
        # check if the METS file has been downloaded already
        # download the METS file
        # parse the METS file for the property info
        # write the values to the working table
        # loop over the working table values and enter updated property values
        # delete working table. delete METS directory.

if __name__ == "__main__":
    main()

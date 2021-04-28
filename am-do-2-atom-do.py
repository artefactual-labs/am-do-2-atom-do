
import os
import pymysql.cursors
import requests

# Set Archivematica Storage Service parameters.
STORAGE_SERVICE_URL = "http://dometadata.analyst.archivematica.net:8000/api/v2/"
STORAGE_SERVICE_USER = "analyst"
STORAGE_SERVICE_API_KEY = "ow7ioGh2reephua8uPaiWee4EiHeev6u"

# Set METS download directory.
METS_DIR = "DIP_METS/"

# Create a working directory for downloading METS files.
if not os.path.exists(METS_DIR):
    os.makedirs(METS_DIR)

# Configure AtoM MySQL connection.
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


def main():
    '''
    Update pre release 2.7 AtoM digital objects with information from AIP
    METS to take full advantage of the digital object metadata enhancement and AIP/file retrieval features.
    '''

    flush_legacy_digital_file_properties()
    update_digital_file_properties()
    delete_temporary_files()


def flush_legacy_digital_file_properties():
    # Select all the legacy DIP files.
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
            # These properties will be replaced with values from the METS file.
            sql = "DELETE FROM property WHERE object_id = %s;"
            mysqlCursor.execute(sql, file['object_id'])
            mysqlConnection.commit()
        except Exception as e:
            print("Unable to flush existing property values for object# " + str(file['id']) + ". Skipping...")
            print(e)
            continue


def update_digital_file_properties():
    try:
        # Select all the legacy DIP file records from the working table.
        sql = "SELECT * FROM dip_files;"
        mysqlCursor.execute(sql)
        legacy_dip_files = mysqlCursor.fetchall()
    except Exception as e:
        print("Unable to select files from working table.")
        print(e)

    for file in legacy_dip_files:
        # Download METS file if a local copy is not present.
        if os.path.exists(METS_DIR + file["aip_uuid"] + ".xml") is False:
            try:
                path = get_mets_path(file["aip_uuid"])
            except Exception as e:
                print("Unable to derive relative path of METS file in " + file["aip_uuid"])
                print(e)
            try:
                get_mets_file(file["aip_uuid"], path)
            except Exception as e:
                print("Unable to fetch METS file for " + file["aip_uuid"])
                print(e)

    # parse the METS file with METSRW for the property info values
    # write the values to the working table
    # loop over the working table values and insert updated property values


def get_mets_path(aip_uuid):
    request_url = STORAGE_SERVICE_URL + "file/" + aip_uuid +"?username=" + STORAGE_SERVICE_USER + "&api_key=" + STORAGE_SERVICE_API_KEY

    response = requests.get(request_url)
    package = response.json()

    # build relative path to METS file
    if package["current_path"].endswith(".7z"):
        relativePath = package["current_path"][40:-3]
    else:
        relativePath = package["current_path"][40:]
    relativePathToMETS = (
        relativePath + "/data/METS." + package["uuid"] + ".xml"
    )

    return relativePathToMETS


def get_mets_file(aip_uuid, relative_path):
    request_url = STORAGE_SERVICE_URL + "file/" + aip_uuid + "/extract_file/?relative_path_to_file=" + relative_path + "&username=" + STORAGE_SERVICE_USER + "&api_key=" + STORAGE_SERVICE_API_KEY
    response = requests.get(request_url)
    mets_file = "METS.{}.xml".format(aip_uuid)
    download_file = os.path.join(METS_DIR, mets_file)
    with open(download_file, "wb") as file:
        file.write(response.content)





# delete working table. delete METS directory.

if __name__ == "__main__":
    main()

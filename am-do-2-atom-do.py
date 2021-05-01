import os
import sys
import pymysql.cursors
import requests
import metsrw
import datetime

# Set Archivematica Storage Service parameters.
STORAGE_SERVICE_URL = "http://dometadata.analyst.archivematica.net:8000/api/v2/"
STORAGE_SERVICE_USER = "analyst"
STORAGE_SERVICE_API_KEY = "ow7ioGh2reephua8uPaiWee4EiHeev6u"

# Set METS download directory.
METS_DIR = "DIP_METS/"

# Create a working directory for downloading METS files.
if not os.path.exists(METS_DIR):
    os.makedirs(METS_DIR)

# Test Storage Service connection
try:
    request_url = STORAGE_SERVICE_URL + "file/" + "?username=" + STORAGE_SERVICE_USER + "&api_key=" + STORAGE_SERVICE_API_KEY
    response = requests.get(request_url)
    if response.status_code != requests.codes.ok:
        sys.exit("Unable to connect to Archivematica Storage Service. Please check your connection parameters.")
    else:
        print("Connected to Archivematica Storage Service.")
except Exception as e:
    print(e)
    sys.exit("Unable to connect to Archivematica Storage Service. Please check your connection parameters.")

# Set and test MySQL connection.
try:
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
    print("Connected to AtoM MySQL database.")
except Exception as e:
    print(e)
    sys.exit("Unable to connect to the AtoM MySQL database. Please check your connection parameters.")

try:
    # Create a working table for transferring the legacy DIP file properties.
    sql = "DROP TABLE dip_files; DROP TABLE premis_events;"
    mysqlCursor.execute(sql)
    mysqlConnection.commit()
    sql = "CREATE TABLE IF NOT EXISTS dip_files(object_id INTEGER PRIMARY KEY, object_uuid TEXT, aip_uuid TEXT, originalFileIngestedAt TEXT, relativePathWithinAip TEXT, aipName TEXT, originalFileName TEXT, originalFileSize TEXT, formatName TEXT, formatVersion TEXT, formatRegistryName TEXT, formatRegistryKey TEXT, preservationCopyNormalizedAt TEXT, preservationCopyFileName TEXT, preservationCopyFileSize TEXT);"
    mysqlCursor.execute(sql)
    sql = "CREATE TABLE IF NOT EXISTS premis_events(id INTEGER PRIMARY KEY, object_id INTEGER, value TEXT);"
    mysqlCursor.execute(sql)
    mysqlConnection.commit()
except Exception as e:
    print(e)
    sys.exit("Unable to create working table. Check permissions for MySQL user.")


def main():
    '''
    Update pre release 2.7 AtoM digital objects with information from AIP
    METS to take full advantage of the digital object metadata enhancement and AIP/file retrieval features.
    '''

    #print("Identifying legacy digital object records in AtoM...")
    #flush_legacy_digital_file_properties()

    #print("Parsing values from METS files...")
    #parse_mets_values()

    '''
    print("Updating digital file properties...")
    update_digital_file_properties()

    print("Cleaning up temporary files...")
    delete_temporary_files()

    print("Data update complete. X records successfully updated. Y records failed to update.")

    # TODO1:    Add X & Y values above ^
    # TODO2:    Send all script print output to a log file. So that specfic
    #           error messages for specific files can be followed-up on if
    #           necessary. The interim work-around is to pipe the output of
    #           this script to a make-shift log file:
    #           `python am-do-2-atom-do.py > update_script.log`

    '''


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


def get_mets_path(aip_uuid):
    request_url = STORAGE_SERVICE_URL + "file/" + aip_uuid + "?username=" + STORAGE_SERVICE_USER + "&api_key=" + STORAGE_SERVICE_API_KEY
    try:
        response = requests.get(request_url)
    except Exception as e:
        print("Unable to connect to Storage Service. Check your connection parameters.")
        print(e)
    package = response.json()

    # build relative path to METS file
    if package["current_path"].endswith(".7z"):
        relativePath = package["current_path"][40:-3]
    else:
        relativePath = package["current_path"][40:]
    relativePathToMETS = (
        relativePath + "/data/METS." + package["uuid"] + ".xml"
    )

    # Derive AIP transfer name from filepath value by removing UUID suffix
    transfer_name = relativePath[:-37]

    return relativePathToMETS, transfer_name


def get_mets_file(aip_uuid, relative_path):
    request_url = STORAGE_SERVICE_URL + "file/" + aip_uuid + "/extract_file/?relative_path_to_file=" + relative_path + "&username=" + STORAGE_SERVICE_USER + "&api_key=" + STORAGE_SERVICE_API_KEY
    response = requests.get(request_url)
    mets_file = "METS.{}.xml".format(aip_uuid)
    download_file = os.path.join(METS_DIR, mets_file)
    with open(download_file, "wb") as file:
        file.write(response.content)


def parse_mets_values():
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
                path, transfer_name = get_mets_path(file["aip_uuid"])
            except Exception as e:
                print("Unable to derive relative path of METS file in package " + file["aip_uuid"])
                print(e)
                continue
            try:
                get_mets_file(file["aip_uuid"], path)
            except Exception as e:
                print("Unable to fetch METS file for package " + file["aip_uuid"])
                print(e)
                continue

        # Read the METS file.
        try:
            mets = metsrw.METSDocument.fromfile(METS_DIR + "METS." + file["aip_uuid"] + ".xml")
        except Exception as e:
            print("METSRW is unable to parse the METS XML for package " + file["aip_uuid"] + ". Check your markup and see archivematica/issues#1129.")
            print(e)
            continue

        # Retrieve values for the current AtoM digital object from the METS.
        try:
            fsentry = mets.get_file(file_uuid=file["object_uuid"])
        except Exception as e:
            print("Unable to find metadata for file " + file["object_uuid"] + " in METS." + file["aip_uuid"] + ".xml")
            print(e)
            continue

        # Initialize all properties to Null to avoid missing value errors.
        originalFileIngestedAt = None
        relativePathWithinAip = None
        aipName = None
        originalFileName = None
        originalFileSize = None
        formatName = None
        formatVersion = None
        formatRegistryName = None
        formatRegistryKey = None
        preservationCopyNormalizedAt = None
        preservationCopyFileName = None
        preservationCopyFileSize = None

        relativePathWithinAip = fsentry.path
        aipName = transfer_name
        originalFileName = fsentry.label

        for premis_event in fsentry.get_premis_events():
            if (premis_event.event_type) == "ingestion":
                eventDate = (premis_event.event_date_time)[:-13]
                originalFileIngestedAt = datetime.strptime(eventDate, "%Y-%m-%dT%H:%M:%S")
            if (premis_event.event_type) == "creation":
                eventDate = (premis_event.event_date_time)[:-13]
                preservationCopyNormalizedAt = datetime.strptime(eventDate, "%Y-%m-%dT%H:%M:%S")

            '''
            TODO: Add all PREMIS Events to AtoM MySQL database as a string array stored in a property_i18n text field. This is currently being done for AtoM 2.7 DIP uploads, even though these values do not appear anywhere in the AtoM GUI.
            '''

        for premis_object in fsentry.get_premis_objects():
            originalFileSize = premis_object.size
            formatName = premis_object.format_name
            if (str(premis_object.format_registry_key)) != "(('format_registry_key',),)":
                if (str(premis_object.format_registry_key)) != "()":
                    formatRegistryKey = premis_object.format_registry_key
            if (str(premis_object.format_version)) != "(('format_version',),)":
                if (str(premis_object.format_version)) != "()":
                    formatVersion = premis_object.format_version

            # if preservationCopyNormalizedAt is not None:
                # preservationCopyFileName =
                # preservationCopyFileSize =

            # Write the METS values to the MySQL working table.
            sql = "UPDATE dip_files SET originalFileIngestedAt = %s, relativePathWithinAip = %s, aipName = %s, originalFileName = %s, originalFileSize = %s, formatName = %s, formatVersion = %s, formatRegistryName = %s, formatRegistryKey = %s, preservationCopyNormalizedAt = %s, preservationCopyFileName = %s, preservationCopyFileSize = %s WHERE object_id = %s;"
            mysqlCursor.execute(sql, (originalFileIngestedAt, relativePathWithinAip, aipName, originalFileName, originalFileSize, formatName, formatVersion, "PRONOM", formatRegistryKey, preservationCopyNormalizedAt, preservationCopyFileName, preservationCopyFileSize, file["object_id"]))
            mysqlConnection.commit()


# loop over the working table values and insert updated property values
# delete working table. delete METS directory.

if __name__ == "__main__":
    main()

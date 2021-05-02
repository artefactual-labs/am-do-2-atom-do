import os
import shutil
import sys
import pymysql.cursors
import requests
import metsrw
from datetime import datetime

# Set Archivematica Storage Service parameters.
STORAGE_SERVICE_URL = "http://dometadata.analyst.archivematica.net:8000/api/v2/"
STORAGE_SERVICE_USER = "analyst"
STORAGE_SERVICE_API_KEY = "ow7ioGh2reephua8uPaiWee4EiHeev6u"

# Set METS download directory.
METS_DIR = "DIP_METS/"

# Create a working directory for downloading METS files.
if not os.path.exists(METS_DIR):
    os.makedirs(METS_DIR)

# Initialize a crude, global error counter. Pre-mature exits are not counted.
ERROR_COUNT = 0

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
    sql = "DROP TABLE IF EXISTS dip_files, premis_events;"
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

    script_start = datetime.now().replace(microsecond=0)
    print("Script started at: " + script_start.strftime("%Y-%m-%d %H:%M:%S"))

    print("Identifying legacy digital object records in AtoM...")
    legacy_count = flush_legacy_digital_file_properties()

    sql = "SELECT COUNT(*) FROM digital_object WHERE object_id IS NOT NULL;"
    mysqlCursor.execute(sql)
    total_count = mysqlCursor.fetchone()

    print("Parsing digital object properties from Archivematica METS files...")
    parse_mets_values()

    print("Updating digital object properties in AtoM MySQL...")
    update_digital_file_properties()

    print("Cleaning up temporary files...")
    delete_temporary_files()

    script_end = datetime.now().replace(microsecond=0)
    print("Script finished at: " + script_end.strftime("%Y-%m-%d %H:%M:%S"))
    duration = script_end - script_start
    print("Script duration: " + str(duration))
    print("Total number of digital objects in AtoM: " + str(total_count["COUNT(*)"]))
    print("Total number of 'legacy` digital objects updated: " + str(legacy_count))
    print("Number of errors encountered: " + str(ERROR_COUNT))


def flush_legacy_digital_file_properties():
    global ERROR_COUNT

    # Select all the legacy DIP files.
    sql = "SELECT * FROM property WHERE name='objectUUID' AND scope is NULL;"
    mysqlCursor.execute(sql)
    legacy_dip_files = mysqlCursor.fetchall()
    legacy_count = len(legacy_dip_files)

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
            ERROR_COUNT += 1
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
            ERROR_COUNT += 1
            continue

        try:
            # Store identifier values in the working table.
            sql = "INSERT INTO dip_files (object_id, object_uuid, aip_uuid) VALUES (%s, %s, %s);"
            mysqlCursor.execute(sql, (file['object_id'], object_uuid['value'], aip_uuid['value']))
            mysqlConnection.commit()
        except Exception as e:
            print("Unable to insert working data for object# " + str(file['id']) + ". Skipping...")
            print(e)
            ERROR_COUNT += 1
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
            ERROR_COUNT += 1
            continue

    return legacy_count


def get_mets_path(aip_uuid):
    request_url = STORAGE_SERVICE_URL + "file/" + aip_uuid + "?username=" + STORAGE_SERVICE_USER + "&api_key=" + STORAGE_SERVICE_API_KEY
    try:
        response = requests.get(request_url)
    except Exception as e:
        print(e)
        sys.exit("Unable to connect to Storage Service. Check your connection parameters.")

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
    global ERROR_COUNT
    try:
        # Select all the legacy DIP file records from the working table.
        sql = "SELECT * FROM dip_files;"
        mysqlCursor.execute(sql)
        legacy_dip_files = mysqlCursor.fetchall()
    except Exception as e:
        print(e)
        sys.exit("Unable to select files from working table.")

    for file in legacy_dip_files:
        # Download METS file if a local copy is not present.
        if os.path.exists(METS_DIR + file["aip_uuid"] + ".xml") is False:
            try:
                path, transfer_name = get_mets_path(file["aip_uuid"])
            except Exception as e:
                print("Unable to derive relative path of METS file in package " + file["aip_uuid"])
                print(e)
                ERROR_COUNT += 1
                continue
            try:
                get_mets_file(file["aip_uuid"], path)
            except Exception as e:
                print("Unable to fetch METS file for package " + file["aip_uuid"])
                print(e)
                ERROR_COUNT += 1
                continue

        # Read the METS file.
        try:
            mets = metsrw.METSDocument.fromfile(METS_DIR + "METS." + file["aip_uuid"] + ".xml")
        except Exception as e:
            print("METSRW is unable to parse the METS XML for package " + file["aip_uuid"] + ". Check your markup and see archivematica/issues#1129.")
            print(e)
            ERROR_COUNT += 1
            continue

        # Retrieve values for the current AtoM digital object from the METS.
        try:
            fsentry = mets.get_file(file_uuid=file["object_uuid"])
        except Exception as e:
            print("Unable to find metadata for file " + file["object_uuid"] + " in METS." + file["aip_uuid"] + ".xml")
            print(e)
            ERROR_COUNT += 1
            continue

        # Initialize all properties to Null to avoid missing value errors.
        originalFileIngestedAt = None
        relativePathWithinAip = None
        aipName = None
        originalFileName = None
        originalFileSize = None
        formatName = None
        formatVersion = None
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
            '''
            TODO: Add all PREMIS Events to AtoM MySQL database as a string array stored in a property_i18n text field. This is currently being done for AtoM 2.7 DIP uploads, even though these values do not appear anywhere in the AtoM GUI.
            '''

        for premis_object in fsentry.get_premis_objects():
            try:
                originalFileSize = premis_object.size
                formatName = premis_object.format_name
                if (str(premis_object.format_registry_key)) != "(('format_registry_key',),)":
                    if (str(premis_object.format_registry_key)) != "()":
                        formatRegistryKey = premis_object.format_registry_key
                if (str(premis_object.format_version)) != "(('format_version',),)":
                    if (str(premis_object.format_version)) != "()":
                        formatVersion = premis_object.format_version
            except Exception as e:
                # A workaround hack for some METSRW failures that were only
                # occurring on ISO formats in the sample data.
                formatName = "ISO Disk Image File"
                formatRegistryKey = "fmt/468"

                print(e)
                print("Unable to match file format to a registry key for digital object " + file["object_uuid"] + ". Using `fmt/468 - ISO Disk Image` as best guess.")
                ERROR_COUNT += 1


            # If this digital object has a preservation copy, retrieve its
            # information.
            if premis_object.relationship__relationship_sub_type == "is source of":
                preservation_copy_uuid = premis_object.relationship__related_object_identifier__related_object_identifier_value
                preservation_file = mets.get_file(file_uuid=preservation_copy_uuid)
                preservationCopyFileName = preservation_file.label
                for entry in preservation_file.get_premis_objects():
                    preservationCopyFileSize = entry.size
                for event in preservation_file.get_premis_events():
                    if (event.event_type) == "creation":
                        eventDate = (event.event_date_time)[:-13]
                        preservationCopyNormalizedAt = datetime.strptime(eventDate, "%Y-%m-%dT%H:%M:%S")

            # Write the METS values to the MySQL working table.
            sql = "UPDATE dip_files SET originalFileIngestedAt = %s, relativePathWithinAip = %s, aipName = %s, originalFileName = %s, originalFileSize = %s, formatName = %s, formatVersion = %s, formatRegistryName = %s, formatRegistryKey = %s, preservationCopyNormalizedAt = %s, preservationCopyFileName = %s, preservationCopyFileSize = %s WHERE object_id = %s;"
            mysqlCursor.execute(sql, (originalFileIngestedAt, relativePathWithinAip, aipName, originalFileName, originalFileSize, formatName, formatVersion, "PRONOM", formatRegistryKey, preservationCopyNormalizedAt, preservationCopyFileName, preservationCopyFileSize, file["object_id"]))
            mysqlConnection.commit()


def write_property(object_id, scope, name, value, object_uuid):
    global ERROR_COUNT
    # Helper function to insert updated property values.
    try:
        sql = "INSERT INTO `property` (`object_id`, `scope`, `name`, `source_culture`) VALUES (%s, %s, %s, %s)"
        mysqlCursor.execute(sql, (object_id, scope, name, "en"))
        property_id = mysqlCursor.lastrowid
        sql = "INSERT INTO `property_i18n` (`value`, `id`, `culture`) VALUES (%s, %s, %s)"
        mysqlCursor.execute(sql, (value, property_id, "en"))
        mysqlConnection.commit()
    except Exception as e:
        print("Unable to add property `" + name + " for digital object " + object_uuid)
        print(e)
        ERROR_COUNT += 1


def update_digital_file_properties():
    # Select all the legacy DIP file records from the working table.
    sql = "SELECT * FROM dip_files;"
    mysqlCursor.execute(sql)
    legacy_dip_files = mysqlCursor.fetchall()

    # Loop over records in working table and insert updated property values.
    for file in legacy_dip_files:
        write_property(file["object_id"], "Archivematica AIP", "objectUUID", file["object_uuid"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "aipUUID", file["aip_uuid"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "relativePathWithinAip", file["relativePathWithinAip"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "aipName", file["aipName"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "originalFileName", file["originalFileName"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "originalFileSize", file["originalFileSize"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "originalFileIngestedAt", file["originalFileIngestedAt"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "preservationCopyFileName", file["preservationCopyFileName"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "preservationCopyFileSize", file["preservationCopyFileSize"], file["object_uuid"])
        write_property(file["object_id"], "Archivematica AIP", "preservationCopyNormalizedAt", file["preservationCopyNormalizedAt"], file["object_uuid"])
        write_property(file["object_id"], "premisData", "formatName", file["formatName"], file["object_uuid"])
        write_property(file["object_id"], "premisData", "formatVersion", file["formatVersion"], file["object_uuid"])
        write_property(file["object_id"], "premisData", "formatRegistryName", file["formatRegistryName"], file["object_uuid"])
        write_property(file["object_id"], "premisData", "formatRegistryKey", file["formatRegistryKey"], file["object_uuid"])

def delete_temporary_files():
    try:
        if os.path.exists(METS_DIR):
            shutil.rmtree(METS_DIR)
    except Exception as e:
        print("Unable to delete the temporary METS file download directory.")
        print(e)

    try:
        sql = "DROP TABLE IF EXISTS dip_files, premis_events;"
        mysqlCursor.execute(sql)
        mysqlConnection.commit()
    except Exception as e:
        print("Unable to delete the working tables from the AtoM MySQL database.")
        print(e)


if __name__ == "__main__":
    main()

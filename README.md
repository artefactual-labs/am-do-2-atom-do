# am-do-2-atom-do
The purpose of this script is to update all the legacy data in AtoM 2.7 installations with related Archivematica metadata. This will enable the enhanced digital object metadata and AIP/file download feature for all the pre-2.7 digital objects in AtoM (that were uploaded via Archivematica).

# Background
Digital object metadata in AtoM is confusing. It's not clear which file representation the information is referring to. This is particularly true if the digital object was uploaded from Archivematica and therefore may have an original format and preservation copy in Archivematica as well as a "master", "reference" and "thumbnail" representation in AtoM.

<img width="1180" alt="AtoM3 6-dip-upload" src="https://user-images.githubusercontent.com/672121/114940877-4381e000-9df7-11eb-927b-34546ae097d9.png">


This was the reason [enhanced digital object metadata display](https://www.accesstomemory.org/en/docs/2.7/user-manual/import-export/upload-digital-object/#digital-object-metadata) was introduced in AtoM 2.7.


![image](https://user-images.githubusercontent.com/672121/114941560-2dc0ea80-9df8-11eb-931b-bdb2e464baf5.png)

The enhanced display includes more information per representation and the ability to download the AIP and original file in Archivematica if the user has permission and if the metadata to facilitate that request is present in the first place. 

![image](https://user-images.githubusercontent.com/672121/114941839-81333880-9df8-11eb-8bf2-d330afeba688.png)

The enhanced digital object metadata feature will work for users of integrated Archivematica and AtoM installations from release 2.7 forward. All the digital objects they may have ingested into Archivematica and described in AtoM prior to the upgrade will not be displayed. That's the gap this script is intended to address.

# Methodology

* Run a query to identify all "legacy" digital object records in the AtoM MySQL database.
* Save these to a working table along with the file's AIP and FILE UUID in Archivematica.
* Information about digital objects loaded into AtoM via Archivematica is stored in the Properties table in AtoM. Rather than replace these one at a time and supplement them with missing data, this script simply flushes all property values for legacy digital objects and then adds all of the pre-existing and new property values in another pass.
* Download the METS file corresponding to the digital object from the Archivematica Storage Service.
* Parse the METS file for all the AtoM 2.7 digital object property values.
* Update the AtoM MySQL working table with the parsed values.
* Insert new AtoM MySQL property records for legacy digital objects using the full set of values from the working table.
* Report on number of records updated.
* Delete temporary METS download folder and MySQL working table.

# Notes

All AtoM Properties used by a post-2.7 digital object. Note that the "value" field is in the linked 'property_i18n' table.


object_id: 531
id: 136
scope: Archivematica AIP
name: objectUUID
value: 3f9f484e-cbb4-4347-ba63-2e9f32196432
-----------------
object_id: 531
id: 137
scope: Archivematica AIP
name: aipUUID
value: 229e5aa2-69c3-4727-9716-767dfdc097b7
-----------------
object_id: 531
id: 138
scope: Archivematica AIP
name: relativePathWithinAip
value: objects/799px-Euroleague-LE_Roma_vs_Toulouse_IC-27.bmp
-----------------
object_id: 531
id: 139
scope: Archivematica AIP
name: aipName
value: test-rocket-3
-----------------
object_id: 531
id: 140
scope: Archivematica AIP
name: originalFileName
value: 799px-Euroleague-LE Roma vs Toulouse IC-27.bmp
-----------------
object_id: 531
id: 141
scope: Archivematica AIP
name: originalFileSize
value: 1437654
-----------------
object_id: 531
id: 142
scope: Archivematica AIP
name: originalFileIngestedAt
value: 2021-04-22T19:03:46.572565+00:00
-----------------
object_id: 531
id: 143
scope: Archivematica AIP
name: preservationCopyFileName
value: 799px-Euroleague-LE_Roma_vs_Toulouse_IC-27-3c66b38a-9447-4cfd-9917-f62628e7cbf9.tif
-----------------
object_id: 531
id: 144
scope: Archivematica AIP
name: preservationCopyFileSize
value: 1436120
-----------------
object_id: 531
id: 145
scope: Archivematica AIP
name: preservationCopyNormalizedAt
value: 2021-04-22T19:04:15.600692+00:00
-----------------
object_id: 531
id: 146
scope: premisData
name: formatName
value: Windows Bitmap
-----------------
object_id: 531
id: 147
scope: premisData
name: formatVersion
value: 3.0
-----------------
object_id: 531
id: 148
scope: premisData
name: formatRegistryName
value: PRONOM
-----------------
object_id: 531
id: 149
scope: premisData
name: formatRegistryKey
value: fmt/116
-----------------
object_id: 531
id: 150
scope: premisData
name: otherEvent
value: a:3:{s:4:"type";s:9:"ingestion";s:8:"dateTime";s:20:"2021-04-22T12:03:46Z";s:22:"linkingAgentIdentifier";a:3:{i:0;a:2:{s:4:"type";s:19:"preservation system";s:5:"value";s:20:"Archivematica-1.12.1";}i:1;a:2:{s:4:"type";s:15:"repository code";s:5:"value";s:5:"12345";}i:2;a:2:{s:4:"type";s:21:"Archivematica user pk";s:5:"value";s:1:"1";}}}
-----------------
object_id: 531
id: 151
scope: premisData
name: otherEvent
value: a:4:{s:4:"type";s:26:"message digest calculation";s:8:"dateTime";s:20:"2021-04-22T12:03:46Z";s:17:"outcomeDetailNote";s:64:"b4961eac5fb7a1ffae4b63ac033a6a25827e7b839594f6e8e0b8e4fb01ebd7c4";s:22:"linkingAgentIdentifier";a:3:{i:0;a:2:{s:4:"type";s:19:"preservation system";s:5:"value";s:20:"Archivematica-1.12.1";}i:1;a:2:{s:4:"type";s:15:"repository code";s:5:"value";s:5:"12345";}i:2;a:2:{s:4:"type";s:21:"Archivematica user pk";s:5:"value";s:1:"1";}}}
-----------------
object_id: 531
id: 152
scope: premisData
name: otherEvent
value: a:4:{s:4:"type";s:11:"virus check";s:8:"dateTime";s:20:"2021-04-22T12:03:47Z";s:7:"outcome";s:4:"Pass";s:22:"linkingAgentIdentifier";a:3:{i:0;a:2:{s:4:"type";s:19:"preservation system";s:5:"value";s:20:"Archivematica-1.12.1";}i:1;a:2:{s:4:"type";s:15:"repository code";s:5:"value";s:5:"12345";}i:2;a:2:{s:4:"type";s:21:"Archivematica user pk";s:5:"value";s:1:"1";}}}
-----------------
object_id: 531
id: 153
scope: premisData
name: otherEvent
value: a:4:{s:4:"type";s:12:"name cleanup";s:8:"dateTime";s:20:"2021-04-22T12:03:48Z";s:17:"outcomeDetailNote";s:182:"Original name="%transferDirectory%objects/799px-Euroleague-LE Roma vs Toulouse IC-27.bmp"; cleaned up name="%transferDirectory%objects/799px-Euroleague-LE_Roma_vs_Toulouse_IC-27.bmp"";s:22:"linkingAgentIdentifier";a:3:{i:0;a:2:{s:4:"type";s:19:"preservation system";s:5:"value";s:20:"Archivematica-1.12.1";}i:1;a:2:{s:4:"type";s:15:"repository code";s:5:"value";s:5:"12345";}i:2;a:2:{s:4:"type";s:21:"Archivematica user pk";s:5:"value";s:1:"1";}}}
-----------------
object_id: 531
id: 154
scope: premisData
name: formatIdentificationEvent
value: a:5:{s:4:"type";s:21:"format identification";s:8:"dateTime";s:20:"2021-04-22T12:03:52Z";s:7:"outcome";s:8:"Positive";s:17:"outcomeDetailNote";s:7:"fmt/116";s:22:"linkingAgentIdentifier";a:3:{i:0;a:2:{s:4:"type";s:19:"preservation system";s:5:"value";s:20:"Archivematica-1.12.1";}i:1;a:2:{s:4:"type";s:15:"repository code";s:5:"value";s:5:"12345";}i:2;a:2:{s:4:"type";s:21:"Archivematica user pk";s:5:"value";s:1:"1";}}}
-----------------
object_id: 531
id: 155
scope: premisData
name: otherEvent
value: a:4:{s:4:"type";s:13:"normalization";s:8:"dateTime";s:20:"2021-04-22T12:04:15Z";s:17:"outcomeDetailNote";s:105:"%SIPDirectory%objects/799px-Euroleague-LE_Roma_vs_Toulouse_IC-27-3c66b38a-9447-4cfd-9917-f62628e7cbf9.tif";s:22:"linkingAgentIdentifier";a:3:{i:0;a:2:{s:4:"type";s:19:"preservation system";s:5:"value";s:20:"Archivematica-1.12.1";}i:1;a:2:{s:4:"type";s:15:"repository code";s:5:"value";s:5:"12345";}i:2;a:2:{s:4:"type";s:21:"Archivematica user pk";s:5:"value";s:1:"1";}}}


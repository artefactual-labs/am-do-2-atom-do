# am-do-2-atom-do
A script to enable AtoM-wide enhanced digital object metadata display by updating AtoM 2.7 digital object metadata using related Archivematica metadata.

# Background
Digital object metadata in AtoM is confusing. It's not clear which file representation the information is referring to. This is particularly true if the digital object was uploaded from Archivematica and therefore may have an original format and preservation copy in Archivematica as well as a "master", "reference" and "thumbnail" representation in AtoM.

<img width="1180" alt="AtoM3 6-dip-upload" src="https://user-images.githubusercontent.com/672121/114940877-4381e000-9df7-11eb-927b-34546ae097d9.png">


This was the reason to introduce [enhanced digital object metadata display](https://www.accesstomemory.org/en/docs/2.7/user-manual/import-export/upload-digital-object/#digital-object-metadata) in AtoM 2.7.


![image](https://user-images.githubusercontent.com/672121/114941560-2dc0ea80-9df8-11eb-931b-bdb2e464baf5.png)

The enhanced display includes more information per representation and the ability to download the AIP and original file in Archivematica if the user has permission and if the metadata to facilitate that request is present in the first place. 

![image](https://user-images.githubusercontent.com/672121/114941839-81333880-9df8-11eb-8bf2-d330afeba688.png)

The enhanced digital object metadata feature will work for users of integrated Archivematica and AtoM installations from release 2.7 forward. All the digital objects they may have ingested into Archivematica and described in AtoM prior to the upgrade will not be displayed.

# Purpose

The purpose of this script is to update all the legacy data in AtoM 2.7 installations with related Archivematica metadata to enable the enhanced digital object metadata feature for all the digital objects in the system.

# Methodology

CREATE OR REPLACE PACKAGE SYS4OPERATOR.EXPORT_MANAGER
AS

  /* in Arbeit, funktioniert noch nicht
  TYPE dir_info IS RECORD (
    filename VARCHAR2(50),
    filesize NUMBER,
    last_modified DATE
  );
  
  TYPE dir_info_table IS TABLE OF dir_info;
	FUNCTION getDirectory(directoryName varchar2) RETURN dir_info_table PIPELINED;
	*/

	PROCEDURE exportDirectory(TableName   varchar2,
                            FileName    varchar2,
                            BucketName  varchar2,
                            TableOwner  varchar2 default user);

	FUNCTION getDirectory(directoryName varchar2) 
	RETURN file_info_table PIPELINED;
	
	FUNCTION getAllDirectories 
	RETURN file_info_table PIPELINED;
	
END EXPORT_MANAGER;
/

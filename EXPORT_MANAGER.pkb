CREATE OR REPLACE PACKAGE BODY SYS4OPERATOR.EXPORT_MANAGER
AS

	/*===========================================================================================*/
  /* PRIVATE modules - forward declarations                                                    */
  /*===========================================================================================*/
	
	FUNCTION col_list(strTbl      varchar2,
                    strSchema   varchar2 default user,
                    strDelim    varchar2 default ',')
	RETURN varchar2;
	
	PROCEDURE tbl2csv(p_directory 	varchar2, 
										strTbl 				varchar2, 
										strFile				varchar2, 
										strSchema 		varchar2 default user);
	
	PROCEDURE tbl2csv_buffered(p_directory  varchar2, 
                            strTbl        varchar2,
                            strFile       varchar2,
                            strSchema     varchar2 default user);
                            
	


	FUNCTION checkDirectoryExists(directoryName varchar2) 
	RETURN number;
	
	PROCEDURE exportToBucket(directoryName varchar2, strBucketame varchar2);

  /* ### output print modules ### */
  PROCEDURE sprint(strMsg varchar2); -- DBMS output for segments prints
  PROCEDURE lprint(strMsg varchar2); -- DBMS output for line prints, starting with ">>"
  PROCEDURE pprint(strMsg varchar2); -- DBMS output for productive prints
  PROCEDURE tprint(strMsg varchar2, TESTING boolean default false); -- DBMS output for test prints, only in test mode
	

	/*===========================================================================================*/
  /* PUBLIC modules                                                                            */
  /*===========================================================================================*/

	--die Main -Procedure für Export--
	PROCEDURE exportDirectory(TableName   varchar2,
                            FileName    varchar2,
                            BucketName  varchar2,
                            TableOwner  varchar2 default user)  
	AS
    strDir         	  varchar2(50):= 'CSV_EXPORTS';
		strTbl            varchar2(50):= TableName;
		strFile           varchar2(50):= FileName;
		strBucket         varchar2(50):= BucketName;
		strUsr            varchar2(30):= TableOwner;
		
		directoryExists   number;
		tableSize         number;
		
	BEGIN
	
    -- prüfe ob directory existiert, sonst erstellen
		directoryExists := checkDirectoryExists(strDir);
		if directoryExists = 0 then
			pprint('Exportordner wird erstellt');
			rdsadmin.rdsadmin_util.create_directory(p_directory_name => strDir);
		else 
			pprint('Exportordner existiert bereits');
		end if;
		
    -- prüfe, wie groß die tabelle ist; kein upload wenn größer > 2GB
		select bytes/1024/1024/1024 into tableSize from user_segments
		where segment_name = strTbl;
		
		pprint('table size in GB : '||tableSize);
		
		if tableSize > 2 then
			raise_application_error(-20000, 'die tabelle ist zu groß');
		end if;
	
    -- exportiere gewünschte tabelle in directory
		tbl2csv(strDir, strTbl, strFile, strUsr);
		--tbl2csv_buffered(strDir, strTbl, strFile, strUsr);
    -- exportiere directory mit files in bucket
    exportToBucket(strDir, strBucket);
	 
    -- lösche csv file nach export
    UTL_FILE.FREMOVE(strDir, strFile||'.csv');
		
	END exportDirectory;
	
  -- Funktion directory mit allen dateien in pipelined Table zurück
	FUNCTION getDirectory(directoryName varchar2)
	RETURN file_info_table PIPELINED 
	IS
		dir_cont file_info;
	BEGIN
	
		for file_rec in(select * from table(rdsadmin.rds_file_util.listdir(p_directory => directoryName)))
			loop
				pipe row(file_info(file_rec.filename, file_rec.filesize, file_rec.mtime));
			end loop;
			
	EXCEPTION 
		when others then
			pprint('this directory does not exist');
		--RETURN;
		--RAISE;
		RETURN;
	END getDirectory;

	-- Funktion gibt alle Directories und deren Dateien in pipelined Table zurück
	FUNCTION getAllDirectories 
	RETURN file_info_table PIPELINED 
	IS
		file_details file_info;
	BEGIN
		for dir_rec in (select directory_name from all_directories) 
			loop
				for file_rec in (select * from table(rdsadmin.rds_file_util.listdir(p_directory => dir_rec.directory_name)))
					loop
						pipe row(file_info(file_rec.filename, file_rec.filesize, file_rec.mtime));
					end loop;
			end loop;
    RETURN;
	END getAllDirectories;

	/*===========================================================================================*/
  /* PRIVATE modules                                                                           */
  /*===========================================================================================*/
  
  -- Funktion prüft, ob Directory existiert
	FUNCTION checkDirectoryExists(directoryName varchar2)
	RETURN number
	IS
		directoryExist  number;
		stmt            varchar2(1000);
	BEGIN
	
    stmt := q'[
      select count(1)
      from dba_directories
      where directory_name like :1
      ]';
      
    execute immediate stmt into directoryExist using directoryName;
--		select count(1) into directoryExist 
--		from dba_directories
--		where directory_name like directoryName;
		
    RETURN directoryExist;
	END checkDirectoryExists;	
	
	-- Funktion erstellt Tabellenheader für CSV
	FUNCTION col_list(strTbl varchar2, strSchema varchar2 default user, strDelim varchar2 default ',') 
	RETURN varchar2
	IS
    strColList varchar2(4000) := ' ';
	BEGIN
		--pprint('from col_list procedure');
    for rec in (select column_name,data_type from all_tab_columns
                where table_name = upper(strTbl)
                and owner        = upper(strSchema)
                order by column_id
                )
    loop
      if rec.data_type in ('CHAR', 'VARCHAR2') then
        strColList  := strColList ||strDelim||'TRIM('||rec.column_name||')';
      else
        strColList := strColList ||strDelim||rec.column_name;
      end if;
    end loop;
    
	RETURN nvl(ltrim(strColList, strDelim||' '), 'ungültiger Tabellenname oder fehlende Berechtigung');
	
	END col_list;


--Diese Prozedur exportiert den Inhalt einer Tabelle als csv-File (mit der Angabe von Datum und Uhrzeit)--
	PROCEDURE tbl2csv(p_directory varchar2, strTbl varchar2, strFile varchar2, strSchema varchar2 default user)
	AS
    utlFile     UTL_FILE.FILE_TYPE;
    cRefCur     SYS_REFCURSOR;
    
    strHeader   varchar2(4000);
    strRow      varchar2(4000);
    strColList  varchar2(4000);
    
    stmt        varchar2(4000);

 BEGIN

    utlFile := UTL_FILE.FOPEN(
      location      => UPPER(p_directory),
      filename      => strFile||'.csv',
      open_mode     => 'w',
      max_linesize  => 32767
      );
      
    -- Zusammenstellung der Spaltenliste für die Überschriften-Zeile
    strHeader := col_list(
      strTbl    => strFile,
      strSchema => strSchema,
      strDelim  => ';'
      );
      
    -- Die Überschriften werden in die Datei geschrieben
    UTL_FILE.PUT_LINE(utlFile, strHeader);
    
    -- Zusammenstellung der Spaltenliste für den Select
    strColList := col_list(
      strTbl    => strTbl,
      strSchema => strSchema ,
      strDelim  => '||'';''||' -- alternativ '||'';''||'
      );
    pprint(strColList);
    
    stmt := 'SELECT '||strColList||' FROM '||strSchema||'.'||strTbl;
    
    -- Über den Ref Cursor werden die Spalteninhalte jeder Zeile
    -- als Strings aneinandergehängt
    OPEN cRefCur FOR stmt;
      LOOP
        -- und in die Variable eingelesen
        FETCH cRefCur INTO strRow;
        EXIT WHEN cRefCur%NOTFOUND;
        -- mit der Prozedur PUT_LINE wird Zeile für Zeile geschrieben
        UTL_FILE.PUT_LINE(utlFile, strRow);
      END LOOP;
    CLOSE cRefCur;
    
    UTL_FILE.FCLOSE(utlFile);
		
	EXCEPTION
		when OTHERS then
      -- hier sollte natürlich eine vernünftige Fehleraufzeichnung passieren
			pprint('the upload is failed');
			UTL_FILE.FCLOSE(utlFile);
		raise;
		
	END tbl2csv;
	

	PROCEDURE tbl2csv_a(p_directory varchar2, strTbl varchar2,strFile varchar2, strSchema varchar2 default user)
	AS
    utlFile     UTL_FILE.FILE_TYPE;
    cRefCur     SYS_REFCURSOR;
    
    strHeader   varchar2(4000);
    strRow      varchar2(4000);
    strColList  varchar2(4000);
    
    stmt        varchar2(4000);

 BEGIN

    utlFile := UTL_FILE.FOPEN(
      location      => UPPER(p_directory),
      filename      => strFile||'.csv',
      open_mode     => 'w',
      max_linesize  => 32767
      );
      
    -- Zusammenstellung der Spaltenliste für die Überschriften-Zeile
    strHeader := col_list(
      strTbl    => strFile,
      strSchema => strSchema,
      strDelim  => ';'
      );
      
    -- Die Überschriften werden in die Datei geschrieben
    UTL_FILE.PUT_LINE(utlFile, strHeader);
    UTL_FILE.FCLOSE(utlFile);
    
		--öffne die datei in modus append
		utlFile := UTL_FILE.FOPEN(
      location      => UPPER(p_directory),
      filename      => strFile||'.csv',
      open_mode     => 'a',
      max_linesize  => 32767
      );
    -- Zusammenstellung der Spaltenliste für den Select
    strColList := col_list(
      strTbl    => strTbl,
      strSchema => strSchema ,
      strDelim  => '||'';''||' -- alternativ '||'';''||'
      );
    pprint(strColList);
    
    stmt := 'SELECT '||strColList||' FROM '||strSchema||'.'||strTbl;
    
    -- Über den Ref Cursor werden die Spalteninhalte jeder Zeile
    -- als Strings aneinandergehängt
    OPEN cRefCur FOR stmt;
      LOOP
        -- und in die Variable eingelesen
        FETCH cRefCur INTO strRow;
        EXIT WHEN cRefCur%NOTFOUND;
        -- mit der Prozedur PUT_LINE wird Zeile für Zeile geschrieben
        UTL_FILE.PUT_LINE(utlFile, strRow);
      END LOOP;
    CLOSE cRefCur;
    
    UTL_FILE.FCLOSE(utlFile);
		
	EXCEPTION
		when OTHERS then
      -- hier sollte natürlich eine vernünftige Fehleraufzeichnung passieren
			pprint('the upload is failed');
			UTL_FILE.FCLOSE(utlFile);
		raise;
		
	END tbl2csv_a;
	
--Diese Prozedur exportiert den Inhalt einer Tabelle als csv-File aber mit einer Zwischenspeicherung der Zeilen in einer 32KB-varchar2-Variable--
	PROCEDURE tbl2csv_buffered(p_directory varchar2, strTbl varchar2,strFile varchar2, strSchema varchar2 default user)
	AS
    utlFile         UTL_FILE.FILE_TYPE;
    cRefCur         SYS_REFCURSOR;
    
    strHeader       varchar2(4000);
    strRow          varchar2(4000);
    strColList      varchar2(4000);
    
    strBuffer       varchar2(32760);
    buffer_limit    number := 30000;

    stmt            varchar2(4000);
    
	BEGIN
	
		utlFile := UTL_FILE.FOPEN(
			location      => UPPER(p_directory),
			filename      => strFile||'.csv',
			open_mode     => 'w',
			max_linesize  => 32767
			);
			
		strHeader := col_list(
			strTbl    => strFile,
			strSchema => strSchema,
			strDelim  => ';'
			);
							
		UTL_FILE.PUT_LINE(utlFile, strHeader);
		
		strColList := col_list(
			strTbl    => strTbl,
			strSchema => strSchema ,
			strDelim  => q'[||';'||]'
			);
			
		stmt := 'SELECT '||strColList||' FROM '||strSchema||'.'||strTbl;
		
		open cRefCur for stmt;
		loop
			fetch cRefCur into strRow;
			exit when cRefCur%NOTFOUND;
			
			-- solange der Buffer nicht voll ist, werden weitere Zeilen
			-- getrennt durch Linefeed (chr(10)) eingeladen
			if length(strBuffer) + 1 + length(strRow) <= buffer_limit then
        pprint('buffer length: '||length(strBuffer));
        pprint('row length: '||length(strRow));
        pprint('whole length: '||length(strBuffer || chr(10) ||strRow));
				strBuffer := strBuffer || chr(10) ||strRow;
				--utl_file.new_line (utlFile, 1);
			else
				if strBuffer is not null then
				-- der volle Buffer wird in die Datei geschrieben
				UTL_FILE.PUT_LINE(utlFile, strBuffer, true);
				end if;
				-- der Buffer wird zurückgesetzt
				strBuffer := strRow;
			end if;

			-- Größenbegrenzung wird bei Tabelle vorgenommen
			-- with UTL_FILE.FGETATTR check file size
			
		end loop;
		
		-- was nach dem Ende der Schleife noch im Buffer ist
		-- wird hier rausgeschrieben
		UTL_FILE.PUT_LINE(utlFile, strBuffer);
		close cRefCur;
		UTL_FILE.FCLOSE(utlFile);

   
	EXCEPTION
		when others then
			pprint(SQLERRM);
			UTL_FILE.FCLOSE(utlFile);
      raise;
    
	END tbl2csv_buffered;
	
  -- procedure exportiert csv file in bucket
	PROCEDURE exportToBucket( directoryName varchar2, strBucketame varchar2)
	AS
	
		stmtSelectToExport			varchar2(3200);
		stmtSelectStatus				varchar2(3200);
	
		status					    		varchar2(1000);
		taskId              		varchar2(30);
		tasIdFull               varchar2(100);
		
		zähler                  number;
		fileExist               number;
		fileFromBdumpCount      number;
		
	BEGIN
		stmtSelectToExport:= q'[
			SELECT rdsadmin.rdsadmin_s3_tasks.upload_to_s3(
				p_bucket_name    => ']'||strBucketame||q'[',
				p_prefix         =>  '', 
				p_s3_prefix      =>  '', 
				p_directory_name => ']'||directoryName||q'[') 
			AS TASK_ID FROM DUAL
		]';
		
		pprint(stmtSelectToExport);
		execute immediate stmtSelectToExport into taskId;
		
		tasIdFull := 'dbtask-'||taskId||'.log';
		pprint(tasIdFull);
		
		loop
			if zähler >= 60 then
				raise_application_error(-20000, 'upload is failed');
			end if;
			exit when 
				fileFromBdumpCount > 0;
			
			for i in 1..60 loop
				SELECT COUNT(1) INTO fileExist 
				FROM TABLE(rdsadmin.rds_file_util.listdir(p_directory => 'BDUMP'))
				WHERE filename = tasIdFull;
				pprint('log file count: '||fileExist);
				
				if fileExist > 0 then
					stmtSelectStatus:= q'[
						SELECT count(text) FROM table(rdsadmin.rds_file_util.read_text_file('BDUMP',']'||tasIdFull||q'['))
						WHERE text like '%successfully%' or text like '%failed%'
					]';
					execute immediate stmtSelectStatus into fileFromBdumpCount;
					pprint(stmtSelectStatus);
					pprint('text count: '||fileFromBdumpCount);
					
					if fileFromBdumpCount = 0 then
						DBMS_SESSION.SLEEP(1);
					else 
						exit;
					end if;
				else 
					DBMS_SESSION.SLEEP(1);
				end if;
				zähler := i;
				pprint('zähler: '||zähler);
			end loop;
		end loop;
		
		stmtSelectStatus:= q'[
			SELECT text FROM table(rdsadmin.rds_file_util.read_text_file('BDUMP',']'||tasIdFull||q'['))
			WHERE text like '%successfully%' or text like '%failed%'
		]';
		pprint(stmtSelectStatus);
		execute immediate stmtSelectStatus  into status;
		pprint(status);
		
		if status like '%failed%' then
			raise_application_error(-20000,'upload is failed');
		end if;
		
	EXCEPTION 
		WHEN others THEN
			raise_application_error (SQLERRM, 'upload is failed');
			
	END exportToBucket;

	/*===========================================================================================*/
  /* output print modules                                                                      */
  /*===========================================================================================*/

  PROCEDURE pprint(strMsg varchar2)
	AS
	BEGIN
    dbms_output.put_line(strMsg);
	END pprint;
  
  PROCEDURE lprint(strMsg varchar2)
	AS
	BEGIN
    dbms_output.put_line('>> '||strMsg);
	END lprint;

  PROCEDURE sprint(strMsg varchar2)
	AS
	BEGIN
    dbms_output.put_line('/*================================================================*/');
    dbms_output.put_line('/* '||strMsg);
    dbms_output.put_line('/*================================================================*/');	
	END sprint;

  PROCEDURE tprint(strMsg varchar2, TESTING boolean default false)
	AS
	BEGIN
    if TESTING = true then
      dbms_output.put_line(strMsg);
    end if;
	END tprint;

  /* not used anymore
	-- Procedure erstellt Directory
	PROCEDURE createDirectory(directoryName varchar2)
	AS
		stmtExec			varchar2(1000);
		stmtGrant     varchar2(3200);
	BEGIN
	
		rdsadmin.rdsadmin_util.create_directory(p_directory_name => directoryName);
		
	END createDirectory;  
  
  
  */

END EXPORT_MANAGER;
/

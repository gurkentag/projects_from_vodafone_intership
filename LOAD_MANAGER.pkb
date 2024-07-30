CREATE OR REPLACE PACKAGE BODY IDBA.load_manager AS

	/* START GLOBAL PRIVATE CONSTANTS */
	TESTING    	constant boolean default true;
	nl					constant varchar2(1) default chr(10);
	/* END GLOBAL PRIVATE CONSTANTS */

	/* START FORWARD DECLARATIONS ONLY FOR PRIVATE*/
	PROCEDURE pprint(vString varchar2);
	PROCEDURE cprint(vString varchar2);
	FUNCTION getUser RETURN varchar2;
	FUNCTION getLogId RETURN number;
	PROCEDURE insertZeileLoggingTable(lastId number, fehlerMeldung varchar2);
	FUNCTION getAvgDuration (lastId number) RETURN INTERVAL DAY TO SECOND;
	PROCEDURE compare(returnId number);
	/* END FORWARD DECLARATIONS ONLY FOR PRIVATE*/

	/* START GLOBAL PRIVATE VARIABLES */
	strExecUsr varchar2(30) := getUser();
	/* END GLOBAL PRIVATE VARIABLES */

	PROCEDURE erstelleTable
	IS 
		stmt      varchar2(32000);
		anzahl    number;
	
	BEGIN

		select count(1) into anzahl from user_tables
		where table_name = 'LOAD_STATS';

		if anzahl =0 then
			stmt :='
				CREATE TABLE LOAD_STATS(
				id number GENERATED BY DEFAULT AS IDENTITY, 
				procedure_name varchar (50),
				dml_method varchar(50),
				schema varchar (30),
				target_table varchar(50),
				load_view varchar (50),
				start_current_run timestamp,
				end_current_run timestamp,
				duration_current_run INTERVAL DAY(9) TO SECOND(6),
				start_count_current_run number,
				end_count_current_run number,
				start_last_run timestamp,
				end_last_run timestamp,
				duration_last_run INTERVAL DAY(9) TO SECOND(6),
				avg_run_duration INTERVAL DAY(9) TO SECOND(6),
				start_count_last_run number,
				end_count_last_run number,
				primary key (id)
				)';
			execute immediate stmt;
			
		else 
      dbms_output.put_line('die Tabelle existiert schon');
		end if;
		
		-- create logging table too
		createLoggingTable;
	
	END erstelleTable;
 	
	PROCEDURE insertStart(spalteProcedureTask varchar2, spalteTable varchar2, spalteView varchar2)
	AS
		stmtIdSelect       varchar2(32000);
		anzahl             number;
		procedureTask      varchar2(30) := spalteProcedureTask;
		tableValue 		     varchar2(80) := spalteTable;
		viewValue 		     varchar2(80) := spalteView;
		countValue         number;
		currentRunId       number;
		CurrentRunStart		 timestamp;
		returnID           number;
		stmtMerge					 varchar2(32000);	
		
	BEGIN

    -- stats tabelle erstellen falls nicht existiert
    erstelleTable; 
	
		stmtIdSelect:= q'[
      select id, start_current_run 
      from  LOAD_STATS
      where 1=1
        and target_table = ']'||tableValue||q'['
        and load_view = ']'||viewValue||q'[']';
      
    cprint(stmtIdSelect);
    execute immediate stmtIdSelect into currentRunId,CurrentRunStart;
    
    execute immediate 'SELECT COUNT(1) FROM ' ||tableValue into countValue;
      
    stmtMerge:=q'[
      MERGE INTO LOAD_STATS DEST
       USING(
				SELECT * FROM LOAD_STATS
				WHERE 1=1
					AND TARGET_TABLE = ']'||tableValue||q'['
					AND LOAD_VIEW = ']'||viewValue||q'['
				)SRC
				ON (DEST.TARGET_TABLE = SRC.TARGET_TABLE
				AND DEST.LOAD_VIEW = SRC.LOAD_VIEW)
			WHEN MATCHED THEN UPDATE SET
				DEST.PROCEDURE_NAME = 'insert_start',
				DEST.DML_METHOD = ']'||procedureTask||q'[',
				DEST.START_LAST_RUN = SRC.START_CURRENT_RUN,
				DEST.END_LAST_RUN = SRC.END_CURRENT_RUN,
				DEST.DURATION_LAST_RUN = SRC.DURATION_CURRENT_RUN,
				DEST.START_COUNT_LAST_RUN = SRC.START_COUNT_CURRENT_RUN,
				DEST.START_CURRENT_RUN = CURRENT_TIMESTAMP,
				DEST.END_CURRENT_RUN = NULL,
				DEST.DURATION_CURRENT_RUN = NULL,
				DEST.START_COUNT_CURRENT_RUN = ]'||countValue||q'[
		]'; 
      pprint('record was updated'); 
      returnID:=currentRunId;
      cprint(stmtMerge);
      execute immediate stmtMerge;
      commit;
    dbms_output.put_line('ID: '||returnID);
    insertZeileLoggingTable(returnID, NULL);
    
    exception
      when no_data_found then
				stmtMerge:= q'[
					INSERT INTO LOAD_STATS(
						PROCEDURE_NAME,
						DML_METHOD,
						SCHEMA,
						TARGET_TABLE,
						LOAD_VIEW,
						START_CURRENT_RUN,
						START_COUNT_CURRENT_RUN
						)
					VALUES(
						'insert_start',
						']'||procedureTask||q'[',
						']'||strExecUsr||q'[',
						']'||tableValue||q'[',
						']'||viewValue||q'[',
							CURRENT_TIMESTAMP,
						']'||countValue||q'['
						)RETURNING ID INTO :1
					]';
        pprint('new record inserted');
        cprint(stmtMerge);
      execute immediate stmtMerge RETURNING INTO returnID;
      commit;
      insertZeileLoggingTable(returnID, null);

	END insertStart;
	PROCEDURE insertEnd(spalteTable varchar2, spalteView varchar2)
	AS
		stmtIdSelect  varchar2(32000);
		tableValue 		varchar2(80) := spalteTable;
		viewValue 		varchar2(80) := spalteView;
		countValue    number;
		returnID      number;
		zeilenAnzahl  number;
		stmtMerge			varchar2(32000);
		currentCount  number;
		lastCount     number;
		avg_value     INTERVAL DAY(9) TO SECOND(6);
		CurrentRunEnd timestamp;
	
	BEGIN

    -- stats tabelle erstellen falls nicht existiert
    erstelleTable;
		
		stmtIdSelect:= q'[
			select id, end_current_run
			from LOAD_STATS
			where target_table  = ']'||tableValue||q'['
				and load_view = ']'||viewValue||q'[']' 
			;
		cprint(stmtIdSelect);
		execute immediate stmtIdSelect into returnID, CurrentRunEnd;
			
		pprint('ReturnID from insertEnd: '||returnID);
		
		execute immediate 'SELECT COUNT(1) FROM ' ||tableValue into countValue;
	
		if CurrentRunEnd is not null then
			insertZeileLoggingTable(returnID, 'du hast kein start aufgerufen');
			insertStart('update', tableValue, viewValue );
			--insertEnd (tableValue, viewValue);
		end if;
		
		avg_value := getAvgDuration(returnID);
		--avg_value := null;
		stmtMerge:=q'[
			MERGE INTO LOAD_STATS DEST
			USING(
				SELECT * FROM LOAD_STATS
				WHERE 1=1
					AND TARGET_TABLE = ']'||tableValue||q'[' 
					AND LOAD_VIEW = ']'||viewValue||q'['
					AND END_CURRENT_RUN IS NULL 
					)SRC
					ON (DEST.TARGET_TABLE = SRC.TARGET_TABLE)
			WHEN MATCHED THEN UPDATE SET
				DEST.PROCEDURE_NAME = 'insert_end',
				DEST.END_CURRENT_RUN = CURRENT_TIMESTAMP,
				DEST.DURATION_CURRENT_RUN = (CURRENT_TIMESTAMP - SRC.START_CURRENT_RUN),
				DEST.AVG_RUN_DURATION =']'||avg_value||q'[',
				DEST.END_COUNT_LAST_RUN = SRC.END_COUNT_CURRENT_RUN,
				DEST.END_COUNT_CURRENT_RUN = ']'||countValue||q'['  
			]';
		cprint(stmtMerge);
		execute immediate stmtMerge;
		commit;	
		pprint('end values updated');	
		dbms_output.put_line('ID: '||returnID);
		
		insertZeileLoggingTable(returnID, null);
		
		compare(returnID);
	
		exception
      when no_data_found then
				dbms_output.put_line('no_data_found ');
				--raise_application_error(-20000, 'no start values for entry!');
					--insertZeileLoggingTable(returnID, SQLERRM);
					insertStart('insert', tableValue, viewValue );
					insertEnd (tableValue, viewValue);

	END insertEnd;
	
	PROCEDURE pprint(vString varchar2)
	AS
	BEGIN
		dbms_output.put_line(vString);
	END;
	
	PROCEDURE cprint(vString varchar2)
	AS
	BEGIN
		if TESTING = true then
			dbms_output.put_line(nl);
			dbms_output.put_line('---------------------- CODE BLOCK START ----------------------');
			dbms_output.put_line(vString);
			dbms_output.put_line('----------------------- CODE BLOCK END -----------------------');
			dbms_output.put_line(nl);
		end if;
	END;

	FUNCTION getUser
	RETURN varchar2
	IS
		userName  varchar(30);
		
	BEGIN
		select user into userName from dual;
	RETURN userName;
	END;
	
	PROCEDURE compare(returnId number)
	AS
		stmSelect    varchar2(3200);
		currentCount number;
		lastCount    number;
	BEGIN
		stmSelect:= q'[ select end_count_current_run, end_count_last_run 
		from LOAD_STATS
		where id = ]'||returnId
		;
		execute immediate stmSelect into currentCount,lastCount;
			
		if lastCount < currentCount then 
			pprint('Current count is bigger!');
		else 
			pprint('Error: last value is bigger or null!');
		end if;
	END;
	
	PROCEDURE createLoggingTable
	AS
		tableCount  number;
		stmCreate   varchar2(32000);
		stmModify   varchar2(32000);
		stmIndex    varchar2(32000);
	BEGIN
		select count(1) into tableCount from user_tables
		where table_name = 'LOAD_TIME_LOGGING';
		
		if tableCount = 0 then
			stmCreate:= '
				CREATE TABLE LOAD_TIME_LOGGING AS
					SELECT 
						CAST(NULL AS NUMBER) AS LOG_ID,
						PROCEDURE_NAME,
						DML_METHOD,
						SCHEMA,
						TARGET_TABLE,
						LOAD_VIEW,
						START_CURRENT_RUN AS RUN_TIMESTAMP,
						DURATION_CURRENT_RUN AS RUN_DURATION,
						AVG_RUN_DURATION, 
						START_COUNT_CURRENT_RUN AS RUN_COUNT, 
						CAST(NULL AS VARCHAR(100)) AS MSG_OUTPUT,
						CAST(NULL AS VARCHAR2(100)) AS ERRORS, 
						0 AS ORIGINAL_LOAD_ID
					FROM LOAD_STATS
					WHERE ROWNUM=0' ;
					--TRUNCATE LOAD_TIME_LOGGING' ;
			
			stmModify:='ALTER TABLE LOAD_TIME_LOGGING MODIFY (LOG_ID NOT NULL)';
			stmIndex:= 'CREATE UNIQUE INDEX LOG_ID_INDEX ON LOAD_TIME_LOGGING(LOG_ID)';
			
			cprint(stmCreate);
			execute immediate stmCreate;
			execute immediate stmModify;
			execute immediate stmIndex;
			commit;
		else pprint('die Tabelle existiert schon!');
		end if;
	END;
	
	FUNCTION getLogId
	RETURN number
	IS
		logId      number;
		stmSelect  varchar2(32000);
	BEGIN
		stmSelect:='SELECT NVL(MAX(LOG_ID),0)+1 FROM LOAD_TIME_LOGGING';
		execute immediate stmSelect into logId;
		RETURN logId;
	END;
	
	FUNCTION getAvgDuration (lastId number)
	RETURN INTERVAL DAY TO SECOND
	IS 
		stmSelect       varchar2(32000);
		procedure_name  varchar2(100) ;
		dml_method      varchar2(100);
		t_table         varchar2(100);
		l_view          varchar2(100);
		avgDuration     INTERVAL DAY(9) TO SECOND(6);
		stmSelectAvg    varchar2 (32000);
		
	BEGIN
		stmSelect:=q'[ 
			SELECT PROCEDURE_NAME, DML_METHOD, TARGET_TABLE, LOAD_VIEW
			FROM LOAD_STATS
			WHERE id = ]'||lastId
			;
			
    cprint(stmSelect);
		execute immediate stmSelect into procedure_name, dml_method, t_table, l_view;

		stmSelectAvg:= q'[
			SELECT NUMTODSINTERVAL(AVG(
				EXTRACT(DAY FROM RUN_DURATION) *86400 +
				EXTRACT(HOUR FROM RUN_DURATION) *3600 +
				EXTRACT(MINUTE FROM RUN_DURATION)*60+
				EXTRACT(SECOND FROM RUN_DURATION) 
				) , 'SECOND')
			FROM LOAD_TIME_LOGGING
			WHERE 1=1
						AND PROCEDURE_NAME= ']'||procedure_name||q'['
						AND DML_METHOD= ']'||dml_method||q'['
						AND TARGET_TABLE =']'||t_table||q'['
						AND LOAD_VIEW =']'||l_view||q'['
			GROUP BY DML_METHOD,
						PROCEDURE_NAME,
						TARGET_TABLE,
						LOAD_VIEW
			]'
		;
		
		cprint(stmSelectAvg);
		execute immediate stmSelectAvg into avgDuration;
	RETURN avgDuration;
	END;
	
	
	PROCEDURE insertZeileLoggingTable(lastId number, fehlerMeldung varchar2)
	AS
		stmSelect       varchar2(3200);
		stmInsert       varchar2(32000);
		procedureName   varchar(50);
		methodName      varchar(50);
		errormessage    varchar(100) := fehlerMeldung;
	BEGIN
		stmSelect:= q'[select procedure_name, dml_method
		from load_stats 
		where id =]'||lastId
		;
		cprint(stmSelect);
		execute immediate stmSelect into procedureName,methodName;
		stmInsert:=q'[
			INSERT INTO LOAD_TIME_LOGGING(
				LOG_ID,
				PROCEDURE_NAME,
				DML_METHOD,
				SCHEMA,
				TARGET_TABLE,
				LOAD_VIEW,
				RUN_TIMESTAMP,
				RUN_DURATION,
				AVG_RUN_DURATION,
				RUN_COUNT,
				ERRORS
				)
				SELECT
				]'||getLogId||q'[,
				']'||procedureName||q'[',
				']'||methodName||q'[',
				SCHEMA, TARGET_TABLE, LOAD_VIEW,
				CASE 
					WHEN ']'||procedureName||q'[' = 'insert_start' THEN START_CURRENT_RUN
					WHEN ']'||procedureName||q'['= 'insert_end' THEN END_CURRENT_RUN
					END AS RUN_TIMSTAMP,
				DURATION_CURRENT_RUN AS RUN_DURATION,
				AVG_RUN_DURATION,
				CASE
					WHEN ']'||procedureName||q'[' = 'insert_start' THEN START_COUNT_CURRENT_RUN
					WHEN ']'||procedureName||q'[' = 'insert_end' THEN END_COUNT_CURRENT_RUN
					END AS RUN_COUNT ,
					']'||errormessage||q'[' 
				FROM LOAD_STATS
				WHERE ID = ]'||lastId
				;
		cprint(stmInsert);
		execute immediate stmInsert;
		commit;
	END;
	
	
--PROCEDURE insertZeileNew(spalteTable varchar2, spalteView varchar2, spalteCurrentStart date default null, spalteCurrentEnd date default null)
--	AS
--		
--		stmtCount 		varchar2(32000);
--		countValue   	number;
--		zeilenAnzahl 	number;
--		durationValue number;
--		stmtMerge			varchar2(32000);
--		
--		
--	BEGIN
--				 
--		-- needed to insert date with time or it will cut off
--		execute immediate q'[ALTER SESSION SET nls_date_format = 'DD/MM/YYYY hh24:mi:ss' ]';
--			
--		stmtCount:='SELECT COUNT(1) FROM ' ||spalteTable;
--			
--		dbms_output.put_line(stmtCount);
--		execute immediate stmtCount into countValue;
--		
--		select count(1) into zeilenAnzahl from LOAD_STATS
--		where target_table  = spalteTable;
--		dbms_output.put_line(zeilenAnzahl); 
--		
--		if zeilenAnzahl = 0 then
--		  stmtMerge:= q'[
--				INSERT INTO LOAD_STATS (
--					TARGET_TABLE,
--					LOAD_VIEW,
--					START_CURRENT_RUN,
--					END_CURRENT_RUN,
--					DURATION_CURRENT_RUN,
--					COUNT_CURRENT_RUN
--					)
--				VALUES(
--					']'||spalteTable||q'[',
--					']'||spalteView||q'[',
--					']'||spalteCurrentStart||q'[',
--					']'||spalteCurrentEnd||q'[',
--					']'||durationValue||q'[',
--					 ]'||countValue||q'[
--					)
--				]';
--		else 
--		  stmtMerge:=q'[
--				MERGE INTO ] LOAD_STATS [ DEST
--				USING(SELECT * FROM ]' ||tableName||q'[ WHERE TARGET_TABLE = ']'||spalteTable||q'[' ) SRC
--					ON (DEST.TARGET_TABLE = SRC.TARGET_TABLE) 
--				WHEN MATCHED THEN UPDATE SET
--					DEST.START_LAST_RUN = SRC.START_CURRENT_RUN,
--					DEST.END_LAST_RUN = SRC.END_CURRENT_RUN,
--					DEST.DURATION_LAST_RUN = SRC.DURATION_CURRENT_RUN,
--					DEST.COUNT_LAST_RUN = SRC.COUNT_CURRENT_RUN,
--					DEST.START_CURRENT_RUN = COALESCE (TO_DATE (']'||spalteCurrentStart||q'[' , 'DD.MM.YY HH24:MI:SS' ) , SRC.START_CURRENT_RUN ) ,
--					DEST.END_CURRENT_RUN = COALESCE (TO_DATE (']'||spalteCurrentEnd||q'[' , 'DD.MM.YY HH24:MI:SS' )  , SRC.END_CURRENT_RUN )  ,
--					DEST.DURATION_CURRENT_RUN = (DEST.END_CURRENT_RUN - DEST.START_CURRENT_RUN ) *24  ,
--					DEST.COUNT_CURRENT_RUN = ]'||countValue||q'[ 
--					--WHERE 
--				/* - not needed because cannnot insert in an empty table 
--				WHEN NOT MATCHED THEN
--				INSERT(
--					TARGET_TABLE,
--					LOAD_VIEW,
--					START_CURRENT_RUN,
--					END_CURRENT_RUN,
--					DURATION_CURRENT_RUN,
--					COUNT_CURRENT_RUN
--					)
--				VALUES(
--					']'||spalteTable||q'[',
--					']'||spalteView||q'[',
--					']'||spalteCurrentStart||q'[',
--					']'||spalteCurrentEnd||q'[',
--					']'||durationValue||q'[',
--					 ]'||countValue||q'[
--					)
--					*/ 
--				]';
--		end if;
--		
--		cprint(stmtMerge);
--		execute immediate stmtMerge;
--		commit;
--	END insertZeileNew; 
--	

END load_manager;
/

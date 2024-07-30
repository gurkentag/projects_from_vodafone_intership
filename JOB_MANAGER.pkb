CREATE OR REPLACE PACKAGE BODY IDBA.job_manager
AS

/* START GLOBAL PRIVATE CONSTANTS */
	TESTING    	constant boolean default true;
	nl					constant varchar2(1) default chr(10);
	/* END GLOBAL PRIVATE CONSTANTS */


/* START FORWARD DECLARATIONS ONLY FOR PRIVATE*/
	PROCEDURE pprint(vString varchar2);
	PROCEDURE cprint(vString varchar2);
	FUNCTION get_user RETURN varchar2;
	PROCEDURE create_job_table;
	PROCEDURE create_procedure_job(jobName varchar2, startDate date, repeatFreq varchar2, repeatInterval number, procedureName varchar2, startType varchar2);
	FUNCTION get_procedure_order(depProcedure varchar2) RETURN number;
/* END FORWARD DECLARATIONS ONLY FOR PRIVATE*/


/* START GLOBAL PRIVATE VARIABLES */
	strExecUsr varchar2(30) := get_user();
/* END GLOBAL PRIVATE VARIABLES */

/*START PUBLIC PROCEDURES*/
	/*MAIN PUBLIC PROCEDURE*/
	PROCEDURE insert_row(jobName varchar2, procedureName varchar2, dependentProcedure varchar2 default null, procStart timestamp, startDate date,repeatFreq varchar2, repeatInterval number, startType varchar2, endDate date, result varchar2 )
	AS
		strJobName        varchar2(50) := jobName;
		strProcName  			varchar2(50) := procedureName;
		strDepProc   			varchar2(50) := dependentProcedure;
		strRepeatFreq     varchar2(100):= repeatFreq;
		strStartType      varchar2(100):= startType;
		strResult         varchar2(30) := result;
		procOrder 				number;
		tableCount        number;
		stmtSelectId      varchar2(3200);
		countValue        number;
		stmtInsert        varchar2(3200);
		returnID          number;
	BEGIN
		create_job_table;
		if strDepProc IS NULL then
			procOrder:=1;
		else
			procOrder:= get_procedure_order(strDepProc);
    end if;
    dbms_output.put_line(procOrder);
		stmtSelectId:= q'[
			SELECT COUNT(ID) FROM ]'||strExecUsr||q'[.T0_SCHEDULED_LOAD_RUN
			WHERE 1=1
			AND JOB_NAME = :CURRENT_JOB_NAME
			AND PROCEDURE_NAME = :CURRENT_PROCEDURE_NAME
			AND DEPENDENT_PROCEDURE = :CURRENT_DEPENDENT_PROCEDURE
			]';
		dbms_output.put_line(stmtSelectId);
		execute immediate stmtSelectId into countValue using strJobName, strProcName, strDepProc;
		
		if countValue = 0 then
			stmtInsert := q'[
				INSERT INTO ]'||strExecUsr||q'[.T0_SCHEDULED_LOAD_RUN (
				JOB_OWNER,
				JOB_NAME,
				PROCEDURE_NAME,
				DEPENDENT_PROCEDURE,
				PROCEDURE_ORDER,
				PROCEDURE_START,
				START_DATE,
				REPEAT_FREQUENCY,
				REPEAT_INTERVAL,
				START_TYPE, 
				END_DATE,
				RESULT       
				)
				VALUES(
				 :CURRENT_USER,
				 :CURRENT_JOB_NAME,
				 :CURRENT_PROCEDURE_NAME,
				 :CURRENT_DEPENDENT_PROCEDURE,
				 :CURRENT_PROCEDURE_ORDER,
				 :CURRENT_PROC_START,
				 :CURRENT_START_DATE,
				 :CURRENT_REP_FREQ,
				 :CURRENT_REP_INTERVAL,
				 :CURRENT_START_TYPE,
				 :CURRENT_END_DATE,
				 :CURRENT_RESULT
				 )
			]';
		dbms_output.put_line(stmtInsert);
		execute immediate stmtInsert using strExecUsr, strJobName, strProcName, strDepProc, procOrder, procStart, startDate, strRepeatFreq, repeatInterval, strStartType, endDate, strResult;
		commit;
		
		else 
			raise_application_error(-20000, 'dieser Eintrag existiert schon!');
		end if;
	END insert_row;

	PROCEDURE build_procedure_job_queue(procName varchar2)
	AS
		procedureName   varchar2(50) := procName;
		stmtSelectProc  varchar2(3200);
		tableCount   		number;
		stmtCreateTable varchar2(3200);
	BEGIN
			stmtSelectProc := q'[
				SELECT
					A.*,
					ROW_NUMBER() OVER(ORDER BY PROCEDURE_ID) AS PROCESS_ORDER
				FROM (
					SELECT 
						CONNECT_BY_ROOT PROCEDURE_NAME AS QUERY_PROCEDURE,
						PROCEDURE_NAME AS BASE_PROCEDURE,
						ID AS PROCEDURE_ID,
						PROCEDURE_ORDER AS BASE_ORDER,
						DEPENDENT_PROCEDURE AS PARENT_PROCEDURE    
					FROM t0_scheduled_load_run
					START WITH PROCEDURE_NAME = ']'||procedureName||q'['
					CONNECT BY PRIOR DEPENDENT_PROCEDURE = PROCEDURE_NAME
					ORDER BY QUERY_PROCEDURE, BASE_ORDER, PARENT_PROCEDURE
				)A
				]';




--		select count(1) into tableCount from user_tables
--		where table_name = 'T1_SCHEDULED_JOB_QUEUE';
--		
--		if tableCount =0 then
--			stmtCreateTable := q'[
--				CREATE TABLE ]'||strExecUsr||q'[.T1_SCHEDULED_JOB_QUEUE
--				AS SELECT
--				ID, 
--				JOB_OWNER,
--				PROCEDURE_NAME,
--				START_DATE, 
--				CAST ((JOB_OWNER || '_' || 'Q' ||ID ) AS VARCHAR2(50)) AS JOB_QUEUE,
--				PROCEDURE_ORDER AS QUEUE_ORDER 
--				FROM ]'||strExecUsr||q'[.T0_SCHEDULED_LOAD_RUN
--				WHERE PROCEDURE_ORDER = 1
--				]';
--				
--			dbms_output.put_line(stmtCreateTable);
--			execute immediate stmtCreateTable;
--		end if;
	
	END build_procedure_job_queue;


/* START PRIVATE PROCEDURES AND FUNCTIONS */

	/* HELP METHODS*/
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
	
	FUNCTION get_user
	RETURN varchar2
	IS
		strUserName  varchar(30);	
	BEGIN
		select user into strUserName from dual;
	RETURN strUserName;
	END get_user;

	PROCEDURE create_job_table 
	AS
		stmtCreateTable     varchar2(32000);
		tableCount        	number;
		stmtCreateIndex			varchar2(32000);
	BEGIN
	
		select count(1) into tableCount from user_tables
		where table_name = 'T0_SCHEDULED_LOAD_RUN';
		
		
		if tableCount =0 then
			stmtCreateTable := q'[
				CREATE TABLE ]'||strExecUsr||q'[.T0_SCHEDULED_LOAD_RUN(
				ID									 	NUMBER GENERATED BY DEFAULT AS IDENTITY NOT NULL ,
				JOB_ID 							 	VARCHAR2(100) AS (JOB_OWNER || '_' || JOB_NAME),
				JOB_OWNER						 	VARCHAR2(30) NOT NULL,
				JOB_NAME						 	VARCHAR2(50) NOT NULL,
				PROCEDURE_NAME			 	VARCHAR2(50) NOT NULL,
				DEPENDENT_PROCEDURE	 	VARCHAR2(50),
				PROCEDURE_ORDER 		 	NUMBER      ,
				PROCEDURE_START      	TIMESTAMP,
				START_DATE           	DATE,
				REPEAT_FREQUENCY     	VARCHAR2(50),
				REPEAT_INTERVAL      	NUMBER,
				START_TYPE           	VARCHAR2(50),
				END_DATE							DATE,
				RESULT                VARCHAR2(50),
				PRIMARY KEY (JOB_ID, PROCEDURE_NAME)
				)]'
				;
			dbms_output.put_line(stmtCreateTable);
			execute immediate stmtCreateTable;
			
			stmtCreateIndex:= 'CREATE UNIQUE INDEX UX1_SCHEDULED_LOAD_RUN ON '||strExecUsr||'.T0_SCHEDULED_LOAD_RUN(id)';
			execute immediate stmtCreateIndex;
		end if;
	END create_job_table;
	
	PROCEDURE create_procedure_job(jobName varchar2, startDate date, repeatFreq varchar2, repeatInterval number, procedureName varchar2, startType varchar2) 
	AS
		strJobName 				varchar2(50) := jobName;
		strRepeatFreq 		varchar2(100):= repeatFreq;
		strProcedureName 	varchar2(50) := procedureName;
		strStartType 			varchar2(100):=startType;
	BEGIN
		pprint('erhaltene Werte:' );
		pprint(strJobName);
		pprint(startDate);
		pprint(strRepeatFreq);
		pprint(repeatInterval);
		pprint(strProcedureName);
		pprint(strStartType);
	END create_procedure_job;
	
	PROCEDURE drop_procedure_job(jobName varchar2, procedureName varchar2)
	AS
		strJobName 				varchar2(50) := jobName;
		strProcedureName 	varchar2(50) := procedureName;
	BEGIN
		pprint('erhaltene Werte:' );
		pprint(strJobName);
		pprint(strProcedureName);
	END drop_procedure_job;
	
	FUNCTION get_procedure_order(depProcedure varchar2)
	RETURN number
	IS
		stmSelect            varchar2(3200);
		dependentProcedure   varchar2(50) := depProcedure;
		orderValue           number;
	BEGIN

		stmSelect := q'[
			SELECT PROCEDURE_ORDER FROM t0_scheduled_load_run
			WHERE PROCEDURE_NAME = ']'||dependentProcedure||q'['
		]'
		;
		execute immediate stmSelect into orderValue;
		cprint(orderValue);
	
--	exception
--		when no_data_found then
--		RETURN 0;
	


	/*
		stmSelect:=q'[
			WITH  procedure_hierarchie (originId, procedure_name, child_procedure, procedure_order) AS 
				(SELECT ID,
				PROCEDURE_NAME,
				DEPENDENT_PROCEDURE,
						1	
			FROM t0_scheduled_load_run
			WHERE DEPENDENT_PROCEDURE IS  NULL
			UNION ALL
			SELECT ts.Id, ts.procedure_name, ts.dependent_procedure, ph.procedure_order +1
			FROM   t0_scheduled_load_run ts
			JOIN  procedure_hierarchie ph ON ts.DEPENDENT_PROCEDURE = ph.PROCEDURE_NAME 
			)

			SELECT procedure_order
			FROM procedure_hierarchie
			WHERE originId = ]'||returnId 
		;
		dbms_output.put_line(stmSelect);
		execute immediate stmSelect into orderValue;
		dbms_output.put_line(orderValue);
		
		stmUpdate:=q'[ 
			UPDATE t0_scheduled_load_run
			SET PROCEDURE_ORDER = ]'||orderValue||q'[ 
			WHERE ID = ]'||returnId 
		;
		dbms_output.put_line(stmUpdate);
		execute immediate stmUpdate;
		
	*/
	orderValue:= orderValue + 1;
	RETURN orderValue;
	END get_procedure_order;
	
	
	FUNCTION get_parent(procName varchar2) 
	RETURN varchar2
	IS
		procedureName       varchar2(50) := procName;
		stmtSelectParent    varchar2(3200);
	BEGIN
		stmtSelectParent := '';
	END get_parent;
	PROCEDURE get_procedure_result (procName varchar2) 
	AS
		procedureName      	varchar2(50) := procName;
		stmtSelect			    varchar2(3200);
	BEGIN
		
		stmtSelect := '';
	
	END get_procedure_result;
		
END job_manager;
/

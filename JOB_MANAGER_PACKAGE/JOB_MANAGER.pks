CREATE OR REPLACE PACKAGE IDBA.job_manager AS

	PROCEDURE insert_row(jobName varchar2, procedureName varchar2, dependentProcedure varchar2 default null, procStart timestamp, startDate date,repeatFreq varchar2, repeatInterval number, startType varchar2, endDate date, result varchar2 );
	PROCEDURE build_procedure_job_queue(procName varchar2);
END job_manager;
/

CREATE OR REPLACE PACKAGE IDBA.load_manager 
 AS
 

	PROCEDURE insertStart(spalteProcedureTask varchar2,spalteTable varchar2, spalteView varchar2);
	
	PROCEDURE insertEnd(spalteTable varchar2, spalteView varchar2);
	
	PROCEDURE createLoggingTable;
	
	PROCEDURE erstelleTable;
	

  END load_manager;
/

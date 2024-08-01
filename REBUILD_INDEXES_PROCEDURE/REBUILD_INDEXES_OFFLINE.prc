CREATE OR REPLACE PROCEDURE SRC_DLP.rebuild_indexes_offline(strSchema varchar2 default user, strTable varchar2)
AS
	arrIdxName      arrv;
	arrPartition    arrv;
	arrSubartition  arrv;
	stmt            varchar2(4000);
BEGIN  
 
	/* rebuild indexes
		- nologging option should be set always, up to 30% faster
			only danger you must re-run the create index syntax if you perform a roll-forward database
		- online option is only neccessary if working on table during office time
			without the index rebuild will be done in background
		- parallel build up with 32 processes, much faster
			testet with TA_BZULIE, without more than 7 minutes, with less than 2 minutes
	*/
	-- select non partitioned indexes
	select index_name
	bulk collect into arrIdxName
	from user_indexes
	where 1=1
		and status = 'UNUSABLE'
		and table_name = strTable
		;
	-- rebuild non partitioned indexes
	for idx in 1..arrIdxName.count loop
		stmt := '
			ALTER INDEX '||strSchema||'.'||arrIdxName(idx)||' REBUILD 
			PARALLEL
			';
		dbms_output.put_line(stmt);
		execute immediate stmt;
		commit;
	end loop;

	-- select partition indexes
	select
		idx.index_name, idxpart.partition_name
	bulk collect into arrIdxName, arrPartition
	from user_indexes idx
		left join user_ind_partitions idxpart
			on idx.index_name = idxpart.index_name
	where 1=1
		and idxpart.status = 'UNUSABLE'
		and idx.table_name = strTable
	;
	-- rebuild indexes on partition
	for idx in 1..arrIdxName.count loop
		stmt := '
			ALTER INDEX '||strSchema||'.'||arrIdxName(idx)||' 
			REBUILD PARTITION '||arrPartition(idx)||' 
			NOLOGGING 
			PARALLEL';

		execute immediate stmt;
		commit;
	end loop;

	-- select subpartition indexes
	select
		idx.index_name, idxpart.subpartition_name
	bulk collect into arrIdxName, arrSubartition
	from user_indexes idx
		left join user_ind_subpartitions idxpart
			on idx.index_name = idxpart.index_name
	where 1=1
		and idxpart.status = 'UNUSABLE'
		and idx.table_name = strTable
		;
	-- rebuild indexes on subpartition
	for idx in 1..arrIdxName.count loop
		stmt := '
			ALTER INDEX '||strSchema||'.'||arrIdxName(idx)||' 
			REBUILD SUBPARTITION '||arrSubartition(idx)||' 
			NOLOGGING 
			PARALLEL';

		execute immediate stmt;
		commit;
	end loop;
	-- set all indexes to NOPARALLEL
	select index_name
	bulk collect into arrIdxName
	from user_indexes
	where 1=1
		and index_type not like 'LOB' -- can not be altered, must be excluded or it throws error
		and table_name = strTable;

	for idx in 1..arrIdxName.count loop
		stmt := 'ALTER INDEX '||strSchema||'.'||arrIdxName(idx)||' NOPARALLEL';
		execute immediate stmt;
		commit;
	end loop;
END rebuild_indexes_offline;
/

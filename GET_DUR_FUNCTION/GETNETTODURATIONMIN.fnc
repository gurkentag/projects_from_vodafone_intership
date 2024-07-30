CREATE OR REPLACE FUNCTION IDBA.getNettoDurationMin(startDate date, endDate date, sla_parameter varchar2)
	RETURN number
	IS

		start_date_trunc    date 				:=trunc(startDate);
		end_date_trunc      date 				:=trunc(endDate);
		current_minutes     number;
		duration_in_minutes number 			:= 0;
		sla_typ				      varchar2(10):= sla_parameter;
	  s_week              number;
	  e_week              number;
	  s_holidays          number;
	  e_holidays          number;
	  s_saturday          number;
	  e_saturday          number;
	  stmSelect           varchar2(32000);
	  stm 			          varchar2(32000);
		
	BEGIN
		 stm:= q'[alter session set nls_date_format= 'DD.MM.YYYY HH24:MI']';
		 execute immediate stm;

		CASE 
		WHEN lower (sla_typ) in ('vip3') then
			--holidays immer gleich sonntag
			s_week:=8;
			e_week:=22;            
			s_holidays:=9;
			e_holidays:=21;
			s_saturday:=9;
			e_saturday:=21;
		WHEN lower(sla_typ) in ('ba24', 'ba12') then
			s_week:=8;
			e_week:=22; 
			s_holidays:=0;
			e_holidays:=0;
			s_saturday:=8;
			e_saturday:=16;
		WHEN lower(sla_typ) in ('sp24', 'sab1', 'sa24') then
			s_week:=8;
			e_week:=20; 
			s_holidays:=8;
			e_holidays:=16;
			s_saturday:=8;
			e_saturday:=16;
		WHEN lower(sla_typ) in ('deg1', 'spx', 'za24', 'sv24', 'zp24', 'za03', 'st') then
			s_week:=8;
			e_week:=20; 
			s_holidays:=0;
			e_holidays:=0;
			s_saturday:=0;
			e_saturday:=0;
		WHEN lower(sla_typ) in ('arg2', 'vf2', 'vf4','zx02', 'zx04', 'zx06', 'zx08', '#rw#', 'zx24', 'zx03', 
														'vip2', 'zx1w', 'zs1w', 'vf3','vf5', 'vip1','ges1', 'wbm1', 'vf') then
			duration_in_minutes:= (endDate-startDate) *24*60;
			RETURN duration_in_minutes;
		END CASE; 
		
		if startDate IS NULL OR endDate IS NULL then
			RETURN NULL;
		end if;
		stmSelect:= q'[ 
			SELECT SUM (AZ)
			FROM (
				SELECT
					BASE.*,
				
					CASE 
						WHEN AZ_FIRST_DAY < AZ_WT_START OR  AZ_LAST_DAY > AZ_WT_END THEN
						(AZ_WT_END - AZ_WT_START)*1440
						ELSE (COALESCE(AZ_LAST_DAY, AZ_WT_END) - COALESCE(AZ_FIRST_DAY, AZ_WT_START))*24*60
					 END AS AZ
					 
				FROM (
					SELECT
						TO_CHAR(TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI') + LEVEL - 1, 'D') AS BASE_DATE_WT,
						']'||start_date_trunc||q'[' AS START_DATE,
						']'||end_date_trunc||q'[' AS END_DATE,
						']'||start_date_trunc||q'[' + LEVEL -1 AS BASE_DATE,
	
						CASE
						WHEN TO_CHAR(TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI') + LEVEL - 1, 'D') = 6 THEN ]'||s_saturday||q'[ /24
						WHEN TO_CHAR(TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI') + LEVEL - 1, 'D') = 7 THEN ]'||s_holidays||q'[ /24
						ELSE ]'||s_week||q'[ /24
						END AS AZ_WT_START,
						
						CASE
						WHEN TO_CHAR(TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI') + LEVEL - 1, 'D') = 6 THEN ]'||e_saturday||q'[ /24
						WHEN TO_CHAR(TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI') + LEVEL - 1, 'D') = 7 THEN ]'||e_holidays||q'[ /24
						ELSE ]'||e_week||q'[ /24
						END AS AZ_WT_END,	
					
						CASE
						WHEN LEVEL = 1
						THEN TO_DATE(']'||startDate||q'[','DD.MM.YY HH24:MI')   -  TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI')
						END AS AZ_FIRST_DAY,
						
						CASE 
							WHEN LEVEL = TO_DATE(']'||end_date_trunc||q'[', 'DD.MM.YY HH24:MI') +1 - TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI')
							THEN TO_DATE (']'||endDate||q'[', 'DD.MM.YY HH24:MI') - TO_DATE(']'||end_date_trunc||q'[', 'DD.MM.YY HH24:MI')
							END AS AZ_LAST_DAY,
						
							get_ger_holiday.get_uniform_holiday(TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI') + LEVEL -1) AS IS_HOLIDAY
--						CASE 
--							WHEN (TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI') + LEVEL -1) IN
--							(SELECT HOLIDAY_DATE FROM TABLE(GET_GER_HOLIDAY.GET_HOLIDAY_PIPELINED_TABLE(']'||start_date_trunc||q'[' ,']'||end_date_trunc||q'['))) THEN 1
--							ELSE 0
--							END AS IS_HOLIDAY
							
					FROM DUAL
					CONNECT BY LEVEL <=
					
						-- erstes Date ist END_DATE   <=>   zweites Date ist START_DATE
						(TO_DATE(']'||end_date_trunc||q'[', 'DD.MM.YY HH24:MI') + 1 - TO_DATE(']'||start_date_trunc||q'[', 'DD.MM.YY HH24:MI'))
					)BASE

			)
		]'	
		;	
		dbms_output.put_line(stmSelect);
		execute immediate stmSelect into 	duration_in_minutes;	
	RETURN duration_in_minutes;
END;
/

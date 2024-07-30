CREATE OR REPLACE FUNCTION IDBA.getDurationForOneDay( startDate date, endDate date)
RETURN number
IS
	day_off_id       number;
	hour_start   		 number;
	hour_end     		 number;
	current_minutes  number:=0;
	s_hour           number;
	e_hour           number;
BEGIN
	hour_start:= extract(hour from cast(startDate as timestamp));
	hour_end := extract(hour from cast(endDate as timestamp));
	
	select count(1) into day_off_id 
	from holidays
	where holiday_date = trunc(startDate);
	
	if day_off_id > 0 then
		s_hour:=9;
		e_hour:=21;
	elsif day_off_id  = 0 then
		s_hour:=8;
		e_hour:=22;
	end if;
	
	CASE
	WHEN hour_start < s_hour AND  hour_end < s_hour then
		RETURN current_minutes;
--	WHEN hour_start < s_hour AND 	hour_end >= s_hour then
--		current_minutes:= (hour_end - s_hour)*60 + extract(minute from cast (endDate as timestamp));
--		
	WHEN hour_start <= s_hour AND 	hour_end >= e_hour then
		current_minutes:= (e_hour - s_hour) *60;
		
	WHEN hour_start > s_hour AND 	hour_end >= e_hour then	
		current_minutes := (e_hour - hour_start -1) *60 + (60 - extract(minute from cast(startDate as timestamp)));
	ELSE
		current_minutes:= (extract(hour from cast(endDate as timestamp))*60 + extract(minute from cast(endDate as timestamp))) - (extract(hour from cast(startDate as timestamp))*60 + extract(minute from cast(startDate as timestamp)));
		
	END CASE;

RETURN current_minutes;
END;
/

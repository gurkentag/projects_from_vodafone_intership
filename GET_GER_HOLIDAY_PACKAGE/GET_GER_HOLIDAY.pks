CREATE OR REPLACE PACKAGE IDBA.get_ger_holiday IS
	TYPE holiday_tab IS TABLE OF holiday_t;
	FUNCTION get_ostersonntag(datum date) RETURN date;
	FUNCTION get_ostermontag(datum date) RETURN date;	
	FUNCTION get_karfreitag(datum date) RETURN date;
	FUNCTION get_himmelfahrt(datum date) RETURN date;
	FUNCTION get_pfingstsonntag(datum date) RETURN date;
	FUNCTION get_pfingstmontag(datum date) RETURN date;
	FUNCTION get_uniform_holiday (datum date) RETURN number;
	FUNCTION get_regional_holiday(datum date, plz_index varchar2) RETURN number;
	FUNCTION get_fronleichnam(datum date) RETURN date; 
	FUNCTION get_busstag(datum date) RETURN date;
	FUNCTION get_holiday_pipelined_table(startDate date, endDate date) RETURN holiday_tab pipelined;
	
	
END;
/

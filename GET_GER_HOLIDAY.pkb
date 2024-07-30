CREATE OR REPLACE PACKAGE BODY IDBA.get_ger_holiday IS

	TYPE arrv IS TABLE OF varchar2(100);
	
	
	osterSonntag     date;
	bussBettag       date;
	

	/*==========================================================================*/
  /* forward declarations for private procedures and functions                */
  /*==========================================================================*/

	PROCEDURE init_holiday(datum date);
	FUNCTION p_ostermontag(datum date) RETURN date;
	FUNCTION p_karfreitag(datum date) RETURN date;
	FUNCTION p_himmelfahrt(datum date) RETURN date;
	FUNCTION p_pfingstsonntag(datum date) RETURN date;
	FUNCTION p_pfingstmontag(datum date) RETURN date;
	FUNCTION p_fronleichnam(datum date) RETURN date;
	FUNCTION p_ostersonntag(datum date) RETURN date;
	FUNCTION p_busstag(datum date) RETURN date;
	PROCEDURE create_regional_holiday_table;



  PROCEDURE init_holiday(datum date)
  AS
  BEGIN
    osterSonntag:= p_ostersonntag(datum);
    bussBettag:=p_busstag(datum);

  END init_holiday;

	/*==========================================================================*/
  /* public procedures and functions                                          */
  /*==========================================================================*/

	FUNCTION get_holiday_pipelined_table(startDate date, endDate date) RETURN holiday_tab pipelined
	IS
		startJahr   	number := extract(year from startDate);
		endJahr     	number := extract(year from EndDate);
		current_datum date;
	BEGIN
		
		for i in startJahr..endJahr loop
			current_datum := to_date('06.01.' || i, 'DD.MM.YYYY');
			init_holiday(current_datum);
			
			pipe row (
				holiday_t(
				to_date('01.01.' || i, 'DD.MM.YYYY'),
				'Neujahrstag',
				'Gesetzlicher Feiertag'))
				;
			pipe row (
			 holiday_t(
				to_date('06.01.' || i, 'DD.MM.YYYY'),
				'Heilige Drei Könige',
				'Nur BW, BY und ST'))
				;
			pipe row (
			 holiday_t(
				to_date('08.03.' || i, 'DD.MM.YYYY'),
				'Internationaler Frauentag',
				'Nur BE, MV'))
				;
			pipe row (
			 holiday_t(
				p_karfreitag(current_datum),
				'Karfreitag',
				'Gesetzlicher Feiertag'))
				;
			pipe row (
			 holiday_t(
				osterSonntag,
				'Ostersonntag',
				'Gesetzlicher Feiertag'))
				;
			pipe row (
			 holiday_t(
				p_ostermontag(current_datum),
				'Ostermontag',
				'Gesetzlicher Feiertag'))
				;
			pipe row (
			 holiday_t(
				to_date('01.05.' || i, 'DD.MM.YYYY'),
				'Tag der Arbeit',
				'Gesetzlicher Feiertag'))
				;
			pipe row (
			 holiday_t(
				p_himmelfahrt(current_datum),
				'Himmelfahrt',
				'Gesetzlicher Feiertag'))
				;
			pipe row (
			 holiday_t(
				p_pfingstsonntag(current_datum),
				'Pfingstsonntag',
				'BB'))
				;
			pipe row (
			 holiday_t(
				 p_fronleichnam(current_datum),
				'Fronleichnam',
				'BW, BY, HE, NW, RP, SL'))
				;
			pipe row (
			 holiday_t(
				 to_date('08.08.' || i, 'DD.MM.YYYY'),
				'Augsburger Friedensfest',
				'BY'))
				;	
			pipe row (
			 holiday_t(
				 to_date('15.08.' || i, 'DD.MM.YYYY'),
				'Mariä Himmelfahrt',
				'BY,SL'))
				;	
			pipe row (
			 holiday_t(
				 to_date('20.09.' || i, 'DD.MM.YYYY'),
				'Weltkindertag ',
				'TH'))
				;	
			pipe row (
			 holiday_t(
				 to_date('03.10.' || i, 'DD.MM.YYYY'),
				'Tag der Deutschen Einheit',
				'Gesetzlicher Feiertag'))
				;
			pipe row (
			 holiday_t(
				 to_date('31.10.' || i, 'DD.MM.YYYY'),
				'Reformationstag',
				'BB, HB, HH, MV, NI, SN, ST, SH, TH'))
				;	
			pipe row (
			 holiday_t(
				 to_date('01.11.' || i, 'DD.MM.YYYY'),
				'Allerheiligen',
				'BW, BY, NW, RP, SL'))
				;	
			pipe row (
			 holiday_t(
				bussBettag,
				'Buß-und Bettag',
				'SN'))
				;
			pipe row (
			 holiday_t(
				 to_date('25.12.' || i, 'DD.MM.YYYY'),
				'1. Weihnachtsfeiertag',
				'Gesetzlicher Feiertag'))
				;	
			pipe row (
			 holiday_t(
				 to_date('26.12.' || i, 'DD.MM.YYYY'),
				'2. Weihnachtsfeiertag',
				'Gesetzlicher Feiertag'))
				;				 								 							 		 							 				
		end loop;
	END get_holiday_pipelined_table;


  FUNCTION get_ostersonntag(datum date) RETURN date
  IS
  BEGIN
    osterSonntag := p_ostersonntag(datum);
    RETURN osterSonntag;
  END get_ostersonntag;	
  
  FUNCTION get_bussTag(datum date) RETURN date 
	IS
	BEGIN
		bussBettag:= p_busstag(datum);
	RETURN bussBettag;
	END get_bussTag;
	FUNCTION get_ostermontag(datum date) RETURN date
	IS
		osterMontag   date;
	BEGIN 
		osterSonntag:= p_ostersonntag(datum);
		osterMontag:= p_ostermontag(datum);
		 
		RETURN osterMontag;
	END get_osterMontag;
	
	FUNCTION get_karfreitag(datum date) RETURN date
	IS
		karfreitag   date;
	BEGIN 
		osterSonntag:= p_ostersonntag(datum);
		karfreitag:= p_karfreitag(datum);
		 
		RETURN karfreitag;
	END get_karfreitag;
	
	
	FUNCTION get_himmelfahrt(datum date) RETURN date
	IS
		himmelfahrt   date;
	BEGIN 
		osterSonntag:= p_ostersonntag(datum);
		himmelfahrt:= p_himmelfahrt(datum);
		 
		RETURN himmelfahrt;
	END get_himmelfahrt;
	
	FUNCTION get_pfingstsonntag(datum date) RETURN date
	IS
		pfingstsonntag   date;
	BEGIN
		osterSonntag:= p_ostersonntag(datum);
		pfingstsonntag:=p_pfingstsonntag(datum);
	RETURN pfingstsonntag;
	END get_pfingstsonntag;
	
	FUNCTION get_pfingstmontag(datum date) RETURN date
	IS
		pfingstmontag   date;
	BEGIN
	
		osterSonntag:= p_ostersonntag(datum);
		pfingstmontag:=p_pfingstmontag(datum);
	RETURN pfingstmontag;
	END get_pfingstmontag;
	
	FUNCTION get_fronleichnam(datum date) RETURN date
	IS
		fronleichnam date;
	BEGIN
		osterSonntag:= p_ostersonntag(datum);
		fronleichnam:=  p_fronleichnam(datum);
	RETURN fronleichnam;
	END get_fronleichnam;
	
	FUNCTION get_uniform_holiday (datum date) RETURN number 
	IS
		day_monat   		varchar2(5):= to_char(datum, 'DD.MM');
		oster_son       date;
	BEGIN
		init_holiday(datum);
		if day_monat in( '01.01', '01.05', '03.10', '25.12', '26.12') then
			RETURN 1;
		END if;
		if datum = osterSonntag then
			RETURN 1;
		-- ostermontag
		elsif datum = p_ostermontag(datum) then
			RETURN 1;
		--karfreitag
		elsif datum = p_karfreitag(datum) then
			RETURN 1;
		--himmelfahrt
		elsif  datum = p_himmelfahrt(datum) then
			RETURN 1;
		--pfingstsonntag
		elsif datum = p_pfingstsonntag(datum) then
			RETURN 1;
		--pfingstmontag
		elsif datum =  p_pfingstmontag(datum)then
			RETURN 1;
		end if;
	RETURN 0;
	END get_uniform_holiday;

	
	FUNCTION get_regional_holiday(datum date, plz_index varchar2) RETURN number 
	IS
		plz   				varchar(6):= plz_index;
		bundesland 		varchar2(100);
		day_monat   	varchar2(5):= to_char(datum, 'DD.MM');
		oster       	date;
		stmSelect     varchar2(3200);
		bettag      	date;
		stmSelectDay  varchar2(3200);
		antwort       number;
	BEGIN
		init_holiday(datum);
		dbms_output.put_line(day_monat);
		stmSelect:=q'[ 
			SELECT BUNDESLAND
			FROM T_PLZ_ZU_BUNDESLAND
			WHERE PLZ = ']'||plz|| q'['
			]'
		;
		execute immediate stmSelect into bundesland;
		dbms_output.put_line(bundesland);
		
		stmSelectDay:= q'[ 
			SELECT COUNT(1) 
			FROM T_REGION_HOLIDAY_BUNDESLAND
			WHERE BUNDESLAND = ']'||bundesland|| q'[' 
			AND FEIERTAG =:1
			]'
		;
		
		--heilige drei könige
		if day_monat = '06.01' then
			dbms_output.put_line(stmSelectDay);
			execute immediate stmSelectDay into antwort using 'heilige drei koenige';
			dbms_output.put_line(stmSelectDay);
			RETURN antwort;
		--internationaler frauentag
		elsif day_monat = '08.03' then
			execute immediate stmSelectDay into antwort using 'internationaler frauentag';
			RETURN antwort;
		--ausburger friedensfest
		elsif day_monat = '08.08'  then
			execute immediate stmSelectDay into antwort using 'ausburger friedensfest';
			RETURN antwort;
		--mariä himmelfahrt
		elsif day_monat = '15.08'  then
			execute immediate stmSelectDay into antwort using 'mariä himmelfahrt';
			RETURN antwort;
		--weltkindertag 
		elsif day_monat = '20.09'  then
			execute immediate stmSelectDay into antwort using 'weltkindertag';
			RETURN antwort;
		--reformationstag
		elsif day_monat = '31.10'  then
			execute immediate stmSelectDay into antwort using 'reformationstag';
			RETURN antwort;
		--allerheiligen
		elsif day_monat = '01.11'  then
			execute immediate stmSelectDay into antwort using 'allerheiligen';
			RETURN antwort;
		end if;
		
		--fronleichnam
		if datum = p_fronleichnam(datum)then 
			execute immediate stmSelectDay into antwort using 'fronleichnam';
			RETURN antwort;
		--buß und bettag
		elsif datum = bussBettag  then
			execute immediate stmSelectDay into antwort using 'buß und bettag';
			RETURN antwort;
		end if;
	RETURN 0;
	END get_regional_holiday;
	
	/*==========================================================================*/
  /* private procedures and functions                                         */
  /*==========================================================================*/
  
  FUNCTION p_ostersonntag(datum date) RETURN date 
  IS 
    
    --https://livesql.oracle.com/apex/livesql/file/content_FXDNN71FW32P655SC2DENRMME.html

    /*
    Das 1. Kirchenkonzil im Jahre 325 hat festgelegt:
    Ostern ist stets am ersten Sonntag nach dem ersten Vollmond des Frühlings.
    Stichtag ist der 21. März, die "Frühlings-Tagundnachtgleiche".
    Am 15.10.1582 wurde von Papst Gregor XIII. der bis dahin gültige
    Julianische Kalender reformiert. Dieser noch heute gültige
    "Gregorianische Kalender" legt fest:
    Ein Jahr hat 365 Tage und ein Schaltjahr wird eingefügt, wenn das
    Jahr durch 4, aber nicht durch 100, oder durch 400 teilbar ist.
    Hieraus ergeben sich die zwei notwendigen Konstanten, um den
    Ostersonntag zu berechnen:
    Die Jahreslänge von und bis zum Zeitpunkt der
    "Frühlings-Tagundnachtgleiche": 365,2422 mittlere Sonnentage.
    Ein Mondmonat: 29,5306 mittlere Sonnentage.
    Carl Friedrich Gauß (1777-1855) entwickelte im Jahre 1800 die
    "Osterformel". Damit läßt sich der Ostersonntag für jedes Jahr
    von 1583 bis 8202 berechnen. 
    */

    v_kalenderjahr  integer := to_number(to_char(datum, 'YYYY')); 
    v_a             integer; 
    v_b             integer; 
    v_c             integer; 
    v_m             integer; 
    v_s             integer; 
    v_n             integer; 
    v_d             integer; 
    v_e             integer; 
    v_oster_monat   integer; 
    v_oster_tag     integer;
    OsterSonntag    date;
  BEGIN 
    v_a := v_kalenderjahr mod 19;
    v_b := v_kalenderjahr mod  4;
    v_c := v_kalenderjahr mod  7;
    v_m := trunc((8*(trunc(v_kalenderjahr/100)) + 13)/25) - 2;
    v_s := trunc(v_kalenderjahr/100) - trunc(v_kalenderjahr/400) - 2;
    v_m := (15 + v_s - v_m) mod 30;
    v_n := (6 + v_s) mod 7;
    v_d := (v_m + 19*v_a) mod 30;
    
    if (v_d = 29) then
      v_d := 28;
    else
      if ((v_d = 28) and (v_a >= 11)) then
        v_d := 27;
      end if;
   end if;
    
    v_e := (2*v_b + 4*v_c + 6*v_d + v_n) mod 7;
    
    -- Ostern fällt auf den (d + e + 1)sten Tag nach dem 21. März
    v_oster_tag := 21 + v_d + v_e + 1;
    v_oster_monat := 3;
    
    if (v_oster_tag > 31) then
      v_oster_tag := v_oster_tag - 31;
      v_oster_monat := 4;
    end if; 

    /*
    Der früheste mögliche Ostertermin ist der 22. März. 
    (Wenn der Vollmond auf den 21. März fällt und der 22. März ein Sonntag ist.) 
    Der späteste mögliche Ostertermin ist der 25. April. 
    (Wenn der Vollmond auf den 21. März fällt und der 21. März ein Sonntag ist.)
    */

    OsterSonntag := to_date(lpad(v_oster_tag, 2, '0')||'.'|| 
                    lpad(v_oster_monat, 2, '0')||'.'|| 
                    to_char(v_kalenderjahr), 'DD.MM.YYYY');
                    
    RETURN OsterSonntag;

  END p_ostersonntag;

	FUNCTION p_ostermontag(datum date) RETURN date 	
	IS 
		oster_montag_date  date;
	BEGIN
		oster_montag_date:=	osterSonntag +1;
	RETURN oster_montag_date;
	END p_ostermontag;
	
	FUNCTION p_karfreitag(datum date) RETURN date 
	IS 
		karfreitag_date  date;
	BEGIN
		karfreitag_date:=	osterSonntag - 2;
	RETURN karfreitag_date;
	END p_karfreitag;
	
	FUNCTION p_himmelfahrt(datum date) RETURN date 
	IS 
		himmelfahrt_date  date;
	BEGIN
		himmelfahrt_date:=	osterSonntag +39;
	RETURN himmelfahrt_date;
	END p_himmelfahrt;

	FUNCTION p_pfingstsonntag(datum date) RETURN date 
	IS 
		pfingstsonn_date  date;
	BEGIN
		pfingstsonn_date:=	osterSonntag +49;
	RETURN pfingstsonn_date;
	END p_pfingstsonntag;
	FUNCTION p_pfingstmontag(datum date) RETURN date 
	IS 
		pfingstmon_date  date;
	BEGIN
		pfingstmon_date:=	osterSonntag + 50;
	RETURN pfingstmon_date;
	END p_pfingstmontag;

	
	FUNCTION p_fronleichnam(datum date) RETURN date 
	IS
		fronleichnam   			date;
	BEGIN
		fronleichnam :=osterSonntag + 60;
	RETURN fronleichnam;
	END p_fronleichnam;
	
	
	FUNCTION p_busstag(datum date) RETURN date 
	IS
		nov23       date;
		subval      number;
		busstag     date;
	BEGIN
		nov23 := to_date('23.11.'||extract(year from datum), 'DD.MM.YYYY');
		if to_char(nov23, 'D') < 3 then
			subval := to_char(nov23, 'D') + 7;
		else
			subval := to_char(nov23, 'D');
		end if;
		busstag := nov23 - (subval) + 3;
		dbms_output.put_line(nov23);
		dbms_output.put_line(busstag);
  RETURN busstag;
	END p_busstag;
		
	
	PROCEDURE create_regional_holiday_table
  AS
    arrBundesland   arrv;
    arrRegHoliday   arrv;
    tblExists       number;
    strBundesland   varchar2(50);
    strRegHoliday   varchar2(100);
    stmCreate       varchar2(1000);
    stmt            varchar2(3200);
  BEGIN
 
    select count(1) into tblExists from user_tables where table_name = 'T_REGION_HOLIDAY_BUNDESLAND';
    if tblExists < 1 then
      stmCreate := '
        CREATE TABLE T_REGION_HOLIDAY_BUNDESLAND(
          BUNDESLAND  VARCHAR(100),
          FEIERTAG    VARCHAR(100),
          PRIMARY KEY ( BUNDESLAND, FEIERTAG)
        )
        ';
      execute immediate stmCreate;
      commit;
    end if;
    arrRegHoliday := arrv(
      'heilige drei koenige',
      'internationaler frauentag',
      'ausburger friedensfest',
      'mariä himmelfahrt',
      'weltkindertag',
      'reformationstag',
      'allerheiligen',
      'fronleichnam',
      'buß und bettag');
		stmt := 'SELECT DISTINCT BUNDESLAND FROM T_PLZ_ZU_BUNDESLAND';
		execute immediate stmt bulk collect into arrBundesland;
		stmt := q'[
          INSERT INTO T_REGION_HOLIDAY_BUNDESLAND(BUNDESLAND, FEIERTAG)
          VALUES(:1, :2)
          ]';
    for bd in arrBundesland.first..arrBundesland.last loop
      for ft in arrRegHoliday.first..arrRegHoliday.last loop
        if arrRegHoliday(ft) = 'heilige drei koenige' then
          if arrBundesland(bd) in ('Bayern','Sachsen-Anhalt','Baden-Württemberg') then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'internationaler frauentag' then
					if arrBundesland(bd) in ('Berlin', 'Mecklenburg-Vorpommern') then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'ausburger friedensfest' then
          if arrBundesland(bd) = 'Bayern' then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'mariä himmelfahrt' then
          if arrBundesland(bd) in ('Bayern','Saarland' ) then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'weltkindertag' then
          if arrBundesland(bd) in ('Thüringen') then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'reformationstag' then
          if arrBundesland(bd) in ('Brandenburg', 'Bremen', 'Hamburg', 'Mecklenburg-Vorpommern', 'Niedersachsen', 'Sachsen', 'Sachsen-Anhalt', 'Schleswig-Holstein', 'Thüringen') then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'allerheiligen' then
          if arrBundesland(bd) in ('Baden-Württemberg', 'Bayern', 'Nordrhein-Westfalen', 'Rheinland-Pfalz','Saarland' ) then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'fronleichnam' then
          if arrBundesland(bd) in ('Baden-Württemberg', 'Bayern', 'Hessen', 'Nordrhein-Westfalen', 'Rheinland-Pfalz', 'Saarland') then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;
        elsif arrRegHoliday(ft) = 'buß und bettag' then
          if arrBundesland(bd) in ('Sachsen') then
            execute immediate stmt using arrBundesland(bd), arrRegHoliday(ft);
          end if;        
        end if;
        commit;
 
      end loop;
    end loop;
 
  END;

END get_ger_holiday;
/

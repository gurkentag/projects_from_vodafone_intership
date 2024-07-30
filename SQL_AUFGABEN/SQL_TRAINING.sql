--Aufteilen einer Spalte 'A_ZUSINFO' in zwei: 'Anfang' und 'Ende'
SELECT t.*,                           
  TO_DATE(REGEXP_SUBSTR(A_ZUSINFO, '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9]{2}:[0-9]{2}', 1)) AS anfang,
	TO_DATE(REGEXP_SUBSTR(A_ZUSINFO, '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9]{2}:[0-9]{2}', 2,2)) AS ende
FROM SRC_DLP.TA_BZULIE t
WHERE 1=1
--AND  A_MODIFIZIERT_ZEITPUNKT  > SYSDATE -10
AND A_BEARBEITUNGSZUSTAND='TV'    ;


-- am welchem Datum und um welche Uhrzeit wieviel Verträge vereinbart wurden
               
SELECT COUNT(1) AS anzahl,
       date_only, 
       hour_only
FROM
  (SELECT 
    TRUNC(A_ZEITPUNKT) AS date_only,
    EXTRACT(HOUR FROM CAST( A_ZEITPUNKT AS TIMESTAMP)) AS hour_only
   FROM SRC_DLP.TA_BZULIE
	 WHERE 
	 1=1
	 AND A_BEARBEITUNGSZUSTAND='TV'    
	 AND A_ZEITPUNKT >= '01.02.2023'
	 AND A_ZEITPUNKT <='07.02.2023'
	 )
GROUP BY date_only, hour_only
ORDER BY anzahl DESC;
               
 --Uhr Zeit des vereinbarten Termins (Start)
SELECT start_time, COUNT (1) AS anzahl
FROM
	(SELECT TO_CHAR(TO_DATE(REGEXP_SUBSTR(A_ZUSINFO, '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9]{1,2}:[0-9]{2}', 1,1)), 'HH24') AS start_time
	FROM SRC_DLP.TA_BZULIE
	WHERE 
	1=1
	AND A_BEARBEITUNGSZUSTAND='TV'
	)              
GROUP BY start_time
ORDER BY start_time ;
;
             
 --Dauer und count 
SELECT  duration_in_hours, COUNT(1) AS anzahl
FROM
	(SELECT anfang, ende, ROUND((ende - anfang) *24 ) AS duration_in_hours
	FROM
		(SELECT 
TO_DATE(REGEXP_SUBSTR(A_ZUSINFO, '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9]{1,2}:[0-9]{2}', 1,1)) as anfang,
TO_DATE(REGEXP_SUBSTR(A_ZUSINFO, '[0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9]{1,2}:[0-9]{2}', 1,2)) as ende
		FROM SRC_DLP.TA_BZULIE t
		WHERE 
		1=1
		AND A_BEARBEITUNGSZUSTAND='TV'
		AND A_ZEITPUNKT >= '01.02.2023'
		AND A_ZEITPUNKT <='07.02.2023'
		)
	)            
GROUP BY duration_in_hours
ORDER BY duration_in_hours;

USE flightdb;

SELECT * 
FROM flightdb.flight_delays
Where delay >= 200
And origin = 'LAX'
And destination = 'OAK';
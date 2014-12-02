RUN /vol/automed/data/uscensus1990/load_tables.pig

state_and_place_city = 
	JOIN state BY code ,
		 place BY state_code;

state_and_place_city_group = 
	GROUP state_and_place_city BY
	state::code;

state_and_place_city_projection =
	FOREACH state_and_place_city_group
	GENERATE group.code AS state_code,
	COUNT(state_and_place_city.state::code) AS no_city;
	
STORE state_and_place_city_projection INTO 'q3' USING PigStorage(',');

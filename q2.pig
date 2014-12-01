RUN /vol/automed/data/uscensus1990/load_tables.pig
		 
state_and_county = 
	JOIN state BY code ,
		 county BY state_code;

state_and_county_group = 
	GROUP state_and_county BY
	state::name;

state_and_county_projection =
	FOREACH state_and_county_group
	GENERATE group AS state_name, 
	SUM(state_and_county.county::population) AS population,
	SUM(state_and_county.county::land_area) AS land_area;
	
state_and_county_ordered = 
	ORDER state_and_county_projection
	BY state_name;
	
STORE only_state_ordered INTO 'q2' USING PigStorage(',');

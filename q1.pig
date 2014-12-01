RUN /vol/automed/data/uscensus1990/load_tables.pig

state_including_county = 
	JOIN state BY code LEFT OUTER,
		 county BY state_code;
		 
state_and_county = 
	JOIN state BY code ,
		 county BY state_code;
		 
only_state_join = JOIN state_including_county BY state_including_county.code LEFT OUTER,
					state_and_county BY state_and_county.code;
			
only_state = FILTER only_state_join 
			 BY state_and_county::code IS NULL;
			

only_state_projected =
	FOREACH only_state
	GENERATE only_state.name AS state_name;
	
only_state_ordered =
	ORDER only_state_projected
	BY state_name;
	
STORE only_state_ordered INTO 'q1' USING PigStorage(',');

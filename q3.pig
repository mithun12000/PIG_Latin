RUN /vol/automed/data/uscensus1990/load_tables.pig

state_join_place =
        JOIN state BY code,
             place BY state_code;
 
group_state_join_place =
        GROUP state_join_place
        BY state::name;
 
count_town_city_village =
        FOREACH group_state_join_place {
                towns = FILTER state_join_place BY type == 'town';
                cities = FILTER state_join_place BY type == 'city';
                villages = FILTER state_join_place BY type == 'village';
                GENERATE group AS state_name,
                         COUNT(towns) AS no_town,
                         COUNT(cities) AS no_city,
                         COUNT(villages) AS no_village;
        }

state_name_ordered =
        ORDER count_town_city_village
        BY state_name;
	
STORE state_name_ordered INTO 'q3' USING PigStorage(',');

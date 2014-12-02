RUN /vol/automed/data/uscensus1990/load_tables.pig

--We perform a natural join between state and place
state_join_place =
        JOIN state BY code,
             place BY state_code;
 
--We group by state name
group_state_join_place =
        GROUP state_join_place
        BY state::name;
 
--We count the occurences of towns, cities and villages
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
 
--And we order the results
state_name_ordered =
        ORDER count_town_city_village
        BY state_name;
	
STORE state_name_ordered INTO 'q3' USING PigStorage(',');

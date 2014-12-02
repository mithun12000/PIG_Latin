--Load the state, county, mcd, place and zip
RUN /vol/automed/data/uscensus1990/load_tables.pig
 
state_join_place =
        JOIN state BY code,
             place BY state_code;
 
group_state_join_place =
        GROUP state_join_place
        BY state::name;
 
top_five_cities_state =
        FOREACH group_state_join_place {
                cities = FILTER state_join_place BY type == 'city';
                rankings = ORDER cities BY population DESC;
                top_5 = LIMIT rankings 5;
                generate FLATTEN(top_5);
        }
 

projection_top_five_cities_state =
        FOREACH top_five_cities_state
        GENERATE state::name AS state_name,
                 place::name,
                 population;
 
--And we order the results
state_name_ordered =
        ORDER selected_top_five_cities_state
        BY state_name, population DESC;
 
STORE state_name_ordered INTO 'q4' USING PigStorage(',');
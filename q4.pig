--Load the state, county, mcd, place and zip
RUN /vol/automed/data/uscensus1990/load_tables.pig
 
--We perform a natural join between state and place
state_join_place =
        JOIN state BY code,
             place BY state_code;
 
--We group by state name
group_state_join_place =
        GROUP state_join_place
        BY state::name;
 
--We get the top five cities for each state
top_five_cities_state =
        FOREACH group_state_join_place {
                cities = FILTER state_join_place
                         BY type == 'city';
                ranked = ORDER cities
                         BY population DESC;
                ranked_five = LIMIT ranked 5;
                generate FLATTEN(ranked_five);
        }
 
 
--We select the columns we want
selected_top_five_cities_state =
        FOREACH top_five_cities_state
        GENERATE state::name AS state_name,
                 place::name,
                 population;
 
--And we order the results
state_name_ordered =
        ORDER selected_top_five_cities_state
        BY state_name, population DESC;
 
STORE state_name_ordered INTO 'q4' USING PigStorage(',');
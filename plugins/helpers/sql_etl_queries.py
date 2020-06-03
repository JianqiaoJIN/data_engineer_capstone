class SqlQueries:
    # fact table 
    city_state_travelers_entry_insert = ("""
        INSERT INTO usa.city_state_travelers_entry (
            admission_number, age, gender, arrival_date, departure_date, 
            from_country_code, from_country, usa_port_code, city, state, state_code,
            visa_code, visa, travel_way_code, travel_way
        ) 
        SELECT 
            immigration.admission_number AS admission_number,
            immigration.age AS age,
            immigration.gender AS gender, 
            immigration.arrival_date AS arrival_date,
            immigration.departure_date AS departure_date,
            immigration.from_country_code AS from_country_code,
            country_code.country AS from_country,
            immigration.usa_port_code AS usa_port_code,
            port.city AS city,
            port.state AS state,
            port.state_code AS state_code,
            immigration.visa_code AS visa_code,
            visa.visa AS visa,
            immigration.travel_way AS travel_way_code,
            travel_way.description AS travel_way
        FROM usa.immigration_staging_day immigration, 
            usa.usa_port port,
            usa.travel_way travel_way,
            usa.visa_code visa,
            usa.i94country_code country_code
        WHERE immigration.usa_port_code = port.code
        AND immigration.travel_way = travel_way.code
        AND immigration.visa_code = visa.code
        AND immigration.from_country_code = country_code.code
    """)

    # dimension table 
    arrival_date_insert = ("""
        INSERT INTO usa.arrival_date (
            arrival_date, arrival_year, arrival_month, arrival_day
        ) 
        SELECT 
            DISTINCT arrival_date,
            EXTRACT(yr FROM arrival_date) AS arrival_year,
            EXTRACT(mon FROM arrival_date) AS arrival_month,
            EXTRACT(d FROM arrival_date) AS arrival_day
        FROM
        (
            SELECT 
                DISTINCT TO_TIMESTAMP(travelers.arrival_date,'YYYY-MM-DD') AS arrival_date
            FROM usa.city_state_travelers_entry travelers
        )
    """)
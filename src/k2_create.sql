CREATE TABLE location (
    code CHAR(5) NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    primary_state CHAR(2) NULL,
    geo_centroid_lat DECIMAL(9, 6) NULL,
    geo_centroid_lng DECIMAL(9, 6) NULL,
    ffiec_avg DECIMAL(8, 2) NOT NULL,
    ffiec_high DECIMAL(8, 2) NOT NULL,
    ffiec_low DECIMAL(8, 2) NOT NULL,
    naccrra_center_4 DECIMAL(7, 2) NULL,
    naccrra_center_infant DECIMAL(7, 2) NULL,
    naccrra_center_school DECIMAL(7, 2) NULL,
    naccrra_family_4 DECIMAL(7, 2) NULL,
    naccrra_family_infant DECIMAL(7, 2) NULL,
    naccrra_family_school DECIMAL(7, 2) NULL,
    rpp_local_goods DECIMAL(5, 2) NULL,
    rpp_local_overall DECIMAL(5, 2) NULL,
    rpp_local_services DECIMAL(5, 2) NULL,
    rpp_state_apparel DECIMAL(5, 2) NOT NULL,
    rpp_state_education_goods DECIMAL(5, 2) NOT NULL,
    rpp_state_education_services DECIMAL(5, 2) NOT NULL,
    rpp_state_food_goods DECIMAL(5, 2) NOT NULL,
    rpp_state_food_services DECIMAL(5, 2) NOT NULL,
    rpp_state_housing_goods DECIMAL(5, 2) NOT NULL,
    rpp_state_housing_services DECIMAL(5, 2) NOT NULL,
    rpp_state_medical_goods DECIMAL(5, 2) NOT NULL,
    rpp_state_medical_services DECIMAL(5, 2) NOT NULL,
    rpp_state_other_goods DECIMAL(5, 2) NOT NULL,
    rpp_state_other_services DECIMAL(5, 2) NOT NULL,
    rpp_state_recreation_goods DECIMAL(5, 2) NOT NULL,
    rpp_state_recreation_services DECIMAL(5, 2) NOT NULL,
    rpp_state_rents DECIMAL(5, 2) NOT NULL,
    rpp_state_rpp DECIMAL(5, 2) NOT NULL,
    rpp_state_transportation_goods DECIMAL(5, 2) NOT NULL,
    rpp_state_transportation_services DECIMAL(5, 2) NOT NULL
);

CREATE TABLE location_occupation (
    code CHAR(5) NOT NULL,
    occupation CHAR(16) NOT NULL,
    year SMALLINT NOT NULL,
    mean MEDIUMINT UNSIGNED NOT NULL,
    median MEDIUMINT UNSIGNED NOT NULL,
    is_major BOOLEAN NOT NULL DEFAULT 0,
    is_total BOOLEAN NOT NULL DEFAULT 0,
    FOREIGN KEY locations_occupations_code (code) REFERENCES locations(code) ON DELETE CASCADE
);

CREATE TABLE location_score (
    code CHAR(5) NOT NULL,
    occupation CHAR(16) NOT NULL,
    salary_mean MEDIUMINT UNSIGNED NOT NULL DEFAULT 0,
    bls_mean TINYINT NOT NULL DEFAULT 0,
    ffiec_avg TINYINT NOT NULL DEFAULT 0,
    ffiec_high TINYINT NOT NULL DEFAULT 0,
    ffiec_low TINYINT NOT NULL DEFAULT 0,
    naccrra_center_4 TINYINT NOT NULL DEFAULT 0,
    naccrra_center_infant TINYINT NOT NULL DEFAULT 0,
    naccrra_family_4 TINYINT NOT NULL DEFAULT 0,
    naccrra_family_infant TINYINT NOT NULL DEFAULT 0,
    rpp_local_goods TINYINT NOT NULL DEFAULT 0,
    rpp_local_overall TINYINT NOT NULL DEFAULT 0,
    rpp_local_services TINYINT NOT NULL DEFAULT 0,
    rpp_state_apparel TINYINT NOT NULL DEFAULT 0,
    rpp_state_education_goods TINYINT NOT NULL DEFAULT 0,
    rpp_state_education_services TINYINT NOT NULL DEFAULT 0,
    rpp_state_food_goods TINYINT NOT NULL DEFAULT 0,
    rpp_state_food_services TINYINT NOT NULL DEFAULT 0,
    rpp_state_housing_goods TINYINT NOT NULL DEFAULT 0,
    rpp_state_housing_services TINYINT NOT NULL DEFAULT 0,
    rpp_state_medical_goods TINYINT NOT NULL DEFAULT 0,
    rpp_state_medical_services TINYINT NOT NULL DEFAULT 0,
    rpp_state_other_goods TINYINT NOT NULL DEFAULT 0,
    rpp_state_other_services TINYINT NOT NULL DEFAULT 0,
    rpp_state_recreation_goods TINYINT NOT NULL DEFAULT 0,
    rpp_state_recreation_services TINYINT NOT NULL DEFAULT 0,
    rpp_state_rents TINYINT NOT NULL DEFAULT 0,
    rpp_state_rpp TINYINT NOT NULL DEFAULT 0,
    rpp_state_transportation_goods TINYINT NOT NULL DEFAULT 0,
    rpp_state_transportation_services TINYINT NOT NULL DEFAULT 0,
    base_score TINYINT NOT NULL DEFAULT 0,
    occupation_score TINYINT NOT NULL DEFAULT 0,
    childcare_score TINYINT NOT NULL DEFAULT 0,
    food_score TINYINT NOT NULL DEFAULT 0,
    gas_score TINYINT NOT NULL DEFAULT 0,
    housing_score TINYINT NOT NULL DEFAULT 0,
    FOREIGN KEY scores_code (code) REFERENCES locations (code) ON DELETE CASCADE
);

CREATE TABLE occupation_category (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE occupation (
    id VARCHAR(16) PRIMARY KEY,
    category_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    INDEX occupation_category_id (category_id)
);


CREATE TABLE rpp (
    code CHAR(5) NOT NULL,
    overall DECIMAL(5, 2) DEFAULT null,
    goods DECIMAL(5, 2) DEFAULT null,
    services DECIMAL(5, 2) DEFAULT null,
    rpp DECIMAL(5, 2) DEFAULT null,
    rents DECIMAL(5, 2) DEFAULT null,
    apparel DECIMAL(5, 2) DEFAULT null,
    education_goods DECIMAL(5, 2) DEFAULT null,
    education_services DECIMAL(5, 2) DEFAULT null,
    food_goods DECIMAL(5, 2) DEFAULT null,
    food_services DECIMAL(5, 2) DEFAULT null,
    housing_goods DECIMAL(5, 2) DEFAULT null,
    housing_services DECIMAL(5, 2) DEFAULT null,
    medical_goods DECIMAL(5, 2) DEFAULT null,
    medical_services DECIMAL(5, 2) DEFAULT null,
    other_goods DECIMAL(5, 2) DEFAULT null,
    other_services DECIMAL(5, 2) DEFAULT null,
    recreation_goods DECIMAL(5, 2) DEFAULT null,
    recreation_services DECIMAL(5, 2) DEFAULT null,
    transportation_goods DECIMAL(5, 2) DEFAULT null,
    transportation_services DECIMAL(5, 2) DEFAULT null
);

DELETE FROM rpp WHERE code = '00000';

INSERT INTO rpp (code, overall, goods, services, rpp, rents, apparel,
    education_goods, education_services, food_goods, food_services,
    housing_goods, housing_services, medical_goods, medical_services,
    other_goods, other_services, recreation_goods, recreation_services,
    transportation_goods, transportation_services)
SELECT '00000', AVG(overall), AVG(goods), AVG(services), AVG(rpp), AVG(rents), AVG(apparel),
    AVG(education_goods), AVG(education_services), AVG(food_goods), AVG(food_services),
    AVG(housing_goods), AVG(housing_services), AVG(medical_goods), AVG(medical_services),
    AVG(other_goods), AVG(other_services), AVG(recreation_goods), AVG(recreation_services),
    AVG(transportation_goods), AVG(transportation_services)
FROM rpp
WHERE code != '00000';

--
-- calculate national average
--

DELETE FROM location WHERE code = '00000';

INSERT INTO location (
    code, name, ffiec_avg, ffiec_high, ffiec_low,
    naccrra_center_4, naccrra_center_infant, naccrra_center_school,
    naccrra_family_4, naccrra_family_infant, naccrra_family_school,
    rpp_local_goods, rpp_local_overall, rpp_local_services,
    rpp_state_apparel, rpp_state_education_goods, rpp_state_education_services,
    rpp_state_food_goods, rpp_state_food_services,
    rpp_state_housing_goods, rpp_state_housing_services,
    rpp_state_medical_goods, rpp_state_medical_services,
    rpp_state_other_goods, rpp_state_other_services,
    rpp_state_recreation_goods, rpp_state_recreation_services,
    rpp_state_rents, rpp_state_rpp,
    rpp_state_transportation_goods, rpp_state_transportation_services)
SELECT '00000', 'USA', AVG(ffiec_avg), AVG(ffiec_high), AVG(ffiec_low),
    AVG(naccrra_center_4), AVG(naccrra_center_infant), AVG(naccrra_center_school),
    AVG(naccrra_family_4), AVG(naccrra_family_infant), AVG(naccrra_family_school),
    AVG(rpp_local_goods), AVG(rpp_local_overall), AVG(rpp_local_services),
    AVG(rpp_state_apparel), AVG(rpp_state_education_goods), AVG(rpp_state_education_services),
    AVG(rpp_state_food_goods), AVG(rpp_state_food_services),
    AVG(rpp_state_housing_goods), AVG(rpp_state_housing_services),
    AVG(rpp_state_medical_goods), AVG(rpp_state_medical_services),
    AVG(rpp_state_other_goods), AVG(rpp_state_other_services),
    AVG(rpp_state_recreation_goods), AVG(rpp_state_recreation_services),
    AVG(rpp_state_rents), AVG(rpp_state_rpp),
    AVG(rpp_state_transportation_goods), AVG(rpp_state_transportation_services)
FROM location
WHERE code != '00000';

--
-- calculate location/occupation average
--

DELETE FROM location_occupation WHERE code = '00000';

INSERT INTO location_occupation (
    code, occupation, year,
    mean, median, is_major, is_total)
SELECT '00000', '00-0000', year,
    AVG(mean), AVG(median), 0, 1
FROM location_occupation
WHERE code != '00000' AND occupation = '00-0000'
GROUP BY year;

INSERT INTO location_occupation (
    code, occupation, year,
    mean, median, is_major, is_total)
SELECT '00000', occupation, year,
    AVG(mean), AVG(median), AVG(is_major), 0
FROM location_occupation
WHERE code != '00000' AND occupation != '00-0000'
GROUP BY year, occupation;




SELECT occupation,
    AVG(mean) AS mean_avg, STDDEV_POP(mean) AS mean_stddev,
    AVG(median) AS median_avg,STDDEV_POP(median) AS median_stddev
FROM locations_occupations
WHERE year = 2010
GROUP BY occupation;

-- scoring test
-- "occupation","mean_avg","mean_stddev","median_avg","median_stddev"
-- "11-2011",86364.3956,16780.0413,76559.7802,16324.0109
SELECT code, mean, mean - 86364.3956 AS mean_diff, ROUND((mean - 86364.3956) / 16780.0413) AS mean_score  FROM locations_occupations WHERE year = 2010 AND occupation = '11-2011';


DELETE FROM scores_totals;

UPDATE scores SET
    base_score = rpp_state_apparel +
        rpp_state_education_goods + rpp_state_education_services +
        rpp_state_medical_goods + rpp_state_medical_services +
        rpp_state_other_goods + rpp_state_other_services +
        rpp_state_recreation_goods + rpp_state_recreation_services,
    occupation_score = ((ffiec_avg + ffiec_high + ffiec_low) / 3) + (bls_mean * 3),
    childcare_score = naccrra_center_4 + naccrra_center_infant +
        naccrra_family_4 + naccrra_family_infant,
    food_score = rpp_state_food_goods + rpp_state_food_services,
    gas_score = rpp_state_transportation_goods + rpp_state_transportation_services,
    housing_score = rpp_state_housing_goods + rpp_state_housing_services + (rpp_state_rents * 2);





SELECT l.name AS name, st.code AS code,
    (st.base_score / 3) +
        (st.occupation_score * 4) +
        (st.childcare_score * 0) +
        (st.food_score * 1) +
        (st.gas_score * 1) +
        (st.housing_score * 1) AS score,
    st.base_score, st.occupation_score, st.childcare_score,
    st.food_score, st.gas_score, st.housing_score
FROM scores_totals st JOIN locations l
    ON st.code = l.code
-- WHERE st.occupation = "15-1131"
WHERE st.occupation = "19-3094"
ORDER BY score DESC;


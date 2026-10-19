-- Local copy of the course database, loaded from the CSV files run.sh downloads to data/
CREATE DATABASE IF NOT EXISTS olympic_dataset;
CREATE DATABASE IF NOT EXISTS anton;

CREATE TABLE olympic_dataset.athlete_bio (
    athlete_id INT,
    name VARCHAR(100),
    sex VARCHAR(10),
    born VARCHAR(100),
    height VARCHAR(10),
    weight VARCHAR(10),
    country VARCHAR(100),
    country_noc VARCHAR(10),
    description TEXT,
    special_notes TEXT
);

CREATE TABLE olympic_dataset.athlete_event_results (
    edition VARCHAR(50),
    edition_id INT,
    country_noc VARCHAR(10),
    sport VARCHAR(50),
    event VARCHAR(200),
    result_id INT,
    athlete VARCHAR(100),
    athlete_id INT,
    pos VARCHAR(20),
    medal VARCHAR(10),
    isTeamSport VARCHAR(5)
);

-- ESCAPED BY '': some descriptions contain backslashes that must stay as they are
LOAD DATA INFILE '/data/athlete_bio.csv'
INTO TABLE olympic_dataset.athlete_bio
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

LOAD DATA INFILE '/data/athlete_event_results.csv'
INTO TABLE olympic_dataset.athlete_event_results
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

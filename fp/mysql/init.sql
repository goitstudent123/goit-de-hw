-- Local copy of the course database, loaded from the FTP csv files (run.sh downloads them to data/)
CREATE DATABASE IF NOT EXISTS olympic_dataset;
CREATE DATABASE IF NOT EXISTS anton;

USE olympic_dataset;

CREATE TABLE athlete_bio (
    athlete_id INT,
    name TEXT,
    sex VARCHAR(10),
    born TEXT,
    height TEXT,
    weight TEXT,
    country TEXT,
    country_noc VARCHAR(10),
    description TEXT,
    special_notes TEXT
);

CREATE TABLE athlete_event_results (
    edition TEXT,
    edition_id INT,
    country_noc VARCHAR(10),
    sport TEXT,
    event TEXT,
    result_id BIGINT,
    athlete TEXT,
    athlete_id INT,
    pos TEXT,
    medal VARCHAR(10),
    isTeamSport VARCHAR(10)
);

LOAD DATA INFILE '/data/athlete_bio.csv'
INTO TABLE athlete_bio
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

LOAD DATA INFILE '/data/athlete_event_results.csv'
INTO TABLE athlete_event_results
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(edition, edition_id, country_noc, sport, event, result_id, athlete, athlete_id, pos, @medal, isTeamSport)
SET medal = NULLIF(@medal, '');

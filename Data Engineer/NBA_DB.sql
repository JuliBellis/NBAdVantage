-------------------- Creación de la base de datos si no existe --------------------
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = NBA_AdVantage)
BEGIN
    CREATE DATABASE NBA_AdVantage;
END
GO

-------------------- Selección de la base de datos --------------------
USE NBA_AdVantage;
GO

-------------------- En caso de necesitar, el drop de cada tabla en el orden correcto para eliminar --------------------
--DROP TABLE Line_score; -- 1
--DROP TABLE Play_by_play; -- 2
--DROP TABLE Inactive_players; -- 3
--DROP TABLE Officials; -- 4
--DROP TABLE Other_stats; -- 5
--DROP TABLE Game_summary; -- 6
--DROP TABLE Game_info; -- 7
--DROP TABLE Team_history; -- 8
--DROP TABLE Team_details; -- 9
--DROP TABLE Draft_history; -- 10
--DROP TABLE Draft_combine_stats; -- 11
--DROP TABLE Common_player_info; -- 12
--DROP TABLE Game; -- 13
--DROP TABLE Team; -- 14
--DROP TABLE Player; -- 15
-- DROP TABLE Salaries; -- 

-------------------- Creación de las tablas --------------------
CREATE TABLE Player (
	id INT PRIMARY KEY,
	full_name varchar(101),
	first_name varchar(50),
	last_name varchar(50),
	is_active char(1)
)

CREATE TABLE Team (
	id INT PRIMARY KEY,
	full_name varchar(50),
	abbreviation char(3),
	nickname varchar(25),
	city varchar(25),
	state varchar (30),
	year_founded int
)

CREATE TABLE Game (
	season_id int,
	team_id_home int FOREIGN KEY REFERENCES Team(id) NOT NULL,
	team_abbreviation_home char(3),
	team_name_home varchar(50),
	game_id int PRIMARY KEY,
	game_date DATETIME,
	matchup_home varchar(12),
	wl_home char(1),
	min int,
	fgm_home int,
	fga_home int,
	fg_pct_home DECIMAL(5,3),
	fg3m_home int,
	fg3a_home int,
	fg3_pct_home DECIMAL(5,3),
	ftm_home INT,
	fta_home INT,
	ft_pct_home DECIMAL(5,3),
	oreb_home int,
	dreb_home int,
	reb_home int,
	ast_home int,
	stl_home int,
	blk_home int,
	tov_home int,
	pf_home int,
	pts_home int,
	plus_minus_home int,
	video_available_home char(1),
	team_id_away int FOREIGN KEY REFERENCES Team(id) NOT NULL,
	team_abbreviation_away char(3),
	team_name_away varchar(50),
	matchup_away varchar(12),
	wl_away char(1),
	fgm_away int,
	fga_away int,
	fg_pct_away DECIMAL(5,3),
	fg3m_away int,
	fg3a_away int,
	fg3_pct_away DECIMAL(5,3),
	ftm_away INT,
	fta_away INT,
	ft_pct_away DECIMAL(5,3),
	oreb_away int,
	dreb_away int,
	reb_away int,
	ast_away int,
	stl_away int,
	blk_away int,
	tov_away int,
	pf_away int,
	pts_away int,
	plus_minus_away int,
	video_available_away char(1),
	season_type varchar(25)
)

CREATE TABLE Common_player_info (
	person_id INT PRIMARY KEY FOREIGN KEY REFERENCES Player(id),
	display_fi_last varchar(53),
	birthdate DATETIME,
	school varchar(45),
	country varchar(45),
	last_affiliation varchar(45),
	height varchar(5),
	weight INT,
	season_exp INT,
	position varchar(20),
	rosterstatus varchar(9),
	team_id INT FOREIGN KEY REFERENCES Team(id),
	player_code varchar(50),
	from_year INT,
	to_year INT,
	dleague_flag CHAR(1),
	nba_flag CHAR(1),
	games_played_flag CHAR(1),
	draft_year VARCHAR(10),
	draft_round VARCHAR(10),
	draft_number VARCHAR(10),
	greatest_75_flag CHAR(1)
)

CREATE TABLE Draft_combine_stats (
	season INT,
	player_id INT PRIMARY KEY FOREIGN KEY REFERENCES Player(id),
	first_name varchar(50),
	last_name varchar(50),
	player_name varchar(101),
	position varchar(5),
	height_wo_shoes DECIMAL(6,2),
	weight DECIMAL(5,1),
	wingspan DECIMAL(5,2),
	standing_reach DECIMAL (5,1),
	body_fat_pct DECIMAL(5,2),
	standing_vertical_leap DECIMAL(5,2),
	max_vertical_leap DECIMAL(5,2),
	lane_agility_time DECIMAL(5,2),
	three_quarter_sprint DECIMAL(4,2),
	bench_press INT,
)



CREATE TABLE Team_details (
	team_id INT PRIMARY KEY FOREIGN KEY REFERENCES Team(id),
	abbreviation char(3),
	nickname varchar(25),
	yearfounded INT,
	city varchar(25),
	arena varchar(50),
	arenacapacity INT,
	owner varchar(50),
	generalmanager varchar(50),
	headcoach varchar(50),
	dleagueaffiliation varchar(65)
)

CREATE TABLE Team_history(
	id_log INT IDENTITY (1,1) PRIMARY KEY,
	team_id INT FOREIGN KEY REFERENCES Team(id),
	city varchar(50),
	nickname varchar(25),
	year_founded INT,
	year_active_till INT
)

CREATE TABLE Game_info (
	game_id INT PRIMARY KEY FOREIGN KEY REFERENCES Game(game_id),
	game_date DATETIME,
)

CREATE TABLE Game_summary(
	game_date_est DATETIME,
	game_sequence INT,
	game_id INT FOREIGN KEY REFERENCES Game(game_id),
	game_status_id INT,
	game_status_text varchar(30),
	gamecode varchar(25) PRIMARY KEY NOT NULL,
	home_team_id INT FOREIGN KEY REFERENCES Team(id),
	visitor_team_id INT FOREIGN KEY REFERENCES Team(id),
	season INT,
	live_period INT,
	live_period_time_bcast varchar(20),
	wh_status char(1)
)

CREATE TABLE Play_by_play(
	play_id INT IDENTITY(1,1) PRIMARY KEY,
	game_id INT FOREIGN KEY REFERENCES Game(game_id),
	eventmsgtype INT,
	eventmsgactiontype INT,
	pctimestring varchar(10),
	homedescription varchar(140),
	neutraldescription varchar(140),
	visitordescription varchar(140),
	score varchar(10),
	scoremargin varchar(5),
	player1_id INT FOREIGN KEY REFERENCES Player(id),
	player1_team_id INT FOREIGN KEY REFERENCES Team(id),
	player2_id INT FOREIGN KEY REFERENCES Player(id),
	player2_team_id INT FOREIGN KEY REFERENCES Team(id),
	player3_id INT FOREIGN KEY REFERENCES Player(id),
	player3_team_id INT FOREIGN KEY REFERENCES Team(id),
)


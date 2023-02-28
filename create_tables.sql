
DROP TABLE IF EXISTS public.dim_player;
CREATE TABLE public.dim_player (
	player_fifa_id 					BIGINT NOT NULL,
	player_name 					VARCHAR(255) NOT NULL,
	age 							INT,
	height 							FLOAT,
	weight 							FLOAT,
	CONSTRAINT dim_player_pkey PRIMARY KEY (player_fifa_id)
);



DROP TABLE IF EXISTS public.dim_player_attr;
CREATE TABLE public.dim_player_attr (
	player_fifa_id                  BIGINT NOT NULL,
	at_the_time_age                 INT,
	overall_rating                  FLOAT,
	team                            VARCHAR(255),
	contract_time                   VARCHAR(100),
	wage                            VARCHAR(100),
	total_stats                     BIGINT,
	info_date                       DATE
);

DROP TABLE IF EXISTS public.dim_team;
CREATE TABLE public.dim_team (
	team_fifa_id                    BIGINT NOT NULL,
    team_api_id                     varchar(100),
	team_name                       VARCHAR(255),
	CONSTRAINT dim_team_pkey PRIMARY KEY (team_fifa_id)
);


DROP TABLE IF EXISTS public.dim_team_attr;
CREATE TABLE public.dim_team_attr (
    team_api_id                     varchar(100) NOT NULL,
    build_up_play_speed             INT,
    build_up_play_dribbling         INT,
    build_up_play_passing           INT,
    deffense_pressure               INT,
    deffense_aggression             INT,
    deffense_team_width             INT,
    info_date                       DATE
);

DROP TABLE IF EXISTS public.dim_league;
CREATE TABLE public.dim_league (
	league_id                       BIGINT NOT NULL,
    country_name                    varchar(100),
	league_name                     VARCHAR(255),
	CONSTRAINT dim_league_pkey PRIMARY KEY (league_id)
);

DROP TABLE IF EXISTS public.dim_time;
CREATE TABLE public.dim_time (
	match_date                      TIMESTAMP NOT NULL,
	"day"                           INT,
	week                            INT,
	"month"                         VARCHAR(256),
	"year"                          INT,
	weekday                         VARCHAR(256),
	CONSTRAINT dim_time_pkey PRIMARY KEY (match_date)
);

DROP TABLE IF EXISTS public.fct_match;
CREATE TABLE public.fct_match (
	match_id                BIGINT NOT NULL,
	league_id               BIGINT NOT NULL,
    season                  varchar(255),
    stage                   INT,
    match_date              TIMESTAMP NOT NULL,
    home_team_api_id        varchar(100) NOT NULL,
    away_team_api_id        varchar(100) NOT NULL,
    home_team_goal          INT,
    away_team_goal          INT,
	CONSTRAINT fct_match_pkey PRIMARY KEY (match_id)
);

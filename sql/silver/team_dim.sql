CREATE OR REPLACE VIEW silver.team_dim AS
SELECT CAST(id AS VARCHAR) AS team_id,
       abbreviation AS team_abbreviation,
       nickname AS team_name,
       CASE WHEN conference IS NOT NULL THEN conference ELSE 'NBA' END AS league
FROM bronze_team
UNION ALL
SELECT team_id, team_abbreviation, team_name, league
FROM (
  VALUES
    ('1610616833', 'EST', 'East All-Stars', 'All-Star'),
    ('1610616834', 'WST', 'West All-Stars', 'All-Star')
) AS extras(team_id, team_abbreviation, team_name, league)
WHERE extras.team_id NOT IN (
  SELECT CAST(id AS VARCHAR) FROM bronze_team
);

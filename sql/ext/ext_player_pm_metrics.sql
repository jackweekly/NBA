CREATE SCHEMA IF NOT EXISTS ext;

CREATE TABLE if not exists ext__player_pm_metrics (
  season       integer,
  player_id    integer,

  -- offensive family
  odarko       double,
  o_lebron     double,
  o_drip       double,
  o_bpm        double,
  o_la3rapm    double,

  -- defensive family
  ddarko       double,
  d_lebron     double,
  d_drip       double,
  d_bpm        double,
  d_la3rapm    double
);

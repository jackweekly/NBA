import pytest
import duckdb
import pandas as pd
from datetime import date

from src.nba_db.update import _upsert_duckdb

@pytest.fixture
def in_memory_duckdb_con():
    """Provides an in-memory DuckDB connection for testing."""
    con = duckdb.connect(database=':memory:', read_only=False)
    # Ensure the table schema is created for testing
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_game_log_team (
            game_id VARCHAR,
            team_id VARCHAR,
            game_date DATE,
            season_type VARCHAR,
            PRIMARY KEY (game_id, team_id, season_type)
        );
    """)
    yield con
    con.close()

def test_upsert_duckdb_insert_new_rows(in_memory_duckdb_con):
    """Test that new rows are correctly inserted into bronze_game_log_team."""
    sample_data = pd.DataFrame({
        'game_id': ['1', '2'],
        'team_id': ['A', 'B'],
        'game_date': [date(2023, 1, 1), date(2023, 1, 2)],
        'season_type': ['Regular Season', 'Regular Season']
    })
    sample_data['game_date'] = pd.to_datetime(sample_data['game_date'])

    _upsert_duckdb(sample_data, con=in_memory_duckdb_con)

    result = in_memory_duckdb_con.execute("SELECT * FROM bronze_game_log_team ORDER BY game_id").fetch_df()

    pd.testing.assert_frame_equal(result, sample_data)

def test_upsert_duckdb_update_existing_rows(in_memory_duckdb_con):
    """Test that existing rows are correctly updated in bronze_game_log_team."""
    initial_data = pd.DataFrame({
        'game_id': ['1', '2'],
        'team_id': ['A', 'B'],
        'game_date': [date(2023, 1, 1), date(2023, 1, 2)],
        'season_type': ['Regular Season', 'Regular Season']
    })
    initial_data['game_date'] = pd.to_datetime(initial_data['game_date'])
    _upsert_duckdb(initial_data, con=in_memory_duckdb_con)

    updated_data = pd.DataFrame({
        'game_id': ['1', '2'],
        'team_id': ['A', 'B'],
        'game_date': [date(2023, 1, 1), date(2023, 1, 3)], # game_date updated for game_id 2
        'season_type': ['Regular Season', 'Regular Season']
    })
    updated_data['game_date'] = pd.to_datetime(updated_data['game_date'])
    _upsert_duckdb(updated_data, con=in_memory_duckdb_con)

    result = in_memory_duckdb_con.execute("SELECT * FROM bronze_game_log_team ORDER BY game_id").fetch_df()

    pd.testing.assert_frame_equal(result, updated_data)

def test_upsert_duckdb_insert_and_update_mixed_rows(in_memory_duckdb_con):
    """Test a mix of new inserts and updates."""
    initial_data = pd.DataFrame({
        'game_id': ['1'],
        'team_id': ['A'],
        'game_date': [date(2023, 1, 1)],
        'season_type': ['Regular Season']
    })
    initial_data['game_date'] = pd.to_datetime(initial_data['game_date'])
    _upsert_duckdb(initial_data, con=in_memory_duckdb_con)

    mixed_data = pd.DataFrame({
        'game_id': ['1', '2'],
        'team_id': ['A', 'B'],
        'game_date': [date(2023, 1, 5), date(2023, 1, 6)], # game_id 1 updated, game_id 2 new
        'season_type': ['Regular Season', 'Regular Season']
    })
    mixed_data['game_date'] = pd.to_datetime(mixed_data['game_date'])
    _upsert_duckdb(mixed_data, con=in_memory_duckdb_con)

    result = in_memory_duckdb_con.execute("SELECT * FROM bronze_game_log_team ORDER BY game_id").fetch_df()

    pd.testing.assert_frame_equal(result, mixed_data)

def test_upsert_duckdb_empty_frame(in_memory_duckdb_con):
    """Test that an empty DataFrame does not cause errors and leaves the table unchanged."""
    initial_data = pd.DataFrame({
        'game_id': ['1'],
        'team_id': ['A'],
        'game_date': [date(2023, 1, 1)],
        'season_type': ['Regular Season']
    })
    initial_data['game_date'] = pd.to_datetime(initial_data['game_date'])
    _upsert_duckdb(initial_data, con=in_memory_duckdb_con)

    empty_frame = pd.DataFrame(columns=['game_id', 'team_id', 'game_date', 'season_type'])
    empty_frame['game_date'] = pd.to_datetime(empty_frame['game_date'])
    _upsert_duckdb(empty_frame, con=in_memory_duckdb_con)

    result = in_memory_duckdb_con.execute("SELECT * FROM bronze_game_log_team").fetch_df()
    pd.testing.assert_frame_equal(result, initial_data)

def test_upsert_duckdb_replace_mode(in_memory_duckdb_con):
    """Test replace mode correctly overwrites the table."""
    initial_data = pd.DataFrame({
        'game_id': ['1', '2'],
        'team_id': ['A', 'B'],
        'game_date': [date(2023, 1, 1), date(2023, 1, 2)],
        'season_type': ['Regular Season', 'Regular Season']
    })
    initial_data['game_date'] = pd.to_datetime(initial_data['game_date'])
    _upsert_duckdb(initial_data, con=in_memory_duckdb_con)

    new_data_replace = pd.DataFrame({
        'game_id': ['3'],
        'team_id': ['C'],
        'game_date': [date(2023, 1, 3)],
        'season_type': ['Playoffs']
    })
    new_data_replace['game_date'] = pd.to_datetime(new_data_replace['game_date'])
    _upsert_duckdb(new_data_replace, replace=True, con=in_memory_duckdb_con)

    result = in_memory_duckdb_con.execute("SELECT * FROM bronze_game_log_team").fetch_df()
    pd.testing.assert_frame_equal(result, new_data_replace)

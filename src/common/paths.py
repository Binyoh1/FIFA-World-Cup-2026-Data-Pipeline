from pathlib import Path

# raw/bronze data directory
raw_dir = Path("./data/raw")
raw_dir.mkdir(parents=True, exist_ok=True)

comp_path = raw_dir / "comp/comp_data.json"
comp_path.parent.mkdir(exist_ok=True)

matches_dir = raw_dir / "matches"
matches_dir.mkdir(exist_ok=True)

teams_dir = raw_dir / "teams"
teams_dir.mkdir(exist_ok=True)

players_dir = raw_dir / "players"
players_dir.mkdir(exist_ok=True)

# silver data directory
silver_dir = Path("./data/silver")
silver_dir.mkdir(parents=True, exist_ok=True)

lookup_dir = silver_dir / "lookup"
lookup_dir.mkdir(parents=True, exist_ok=True)

reference_dir = silver_dir / "reference"
reference_dir.mkdir(parents=True, exist_ok=True)

facts_dir = silver_dir / "facts"
facts_dir.mkdir(parents=True, exist_ok=True)

# gold data directory
gold_dir = Path("./data/gold")
gold_dir.mkdir(parents=True, exist_ok=True)


# schema directory
schemas_dir = Path("./data/schemas")
schemas_dir.mkdir(parents=True, exist_ok=True)
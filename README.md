# ha-merge-history

Standalone helper script to merge Home Assistant recorder **statistics** history from `old_entity_id` into `new_entity_id`.

## Run

```powershell
python .\ha-merge-history.py <old_entity_id> <new_entity_id>
```

Optional args (see `--help`):
- `--db-path` (defaults to `./home-assistant_v2.db`)
- `--storage-dir` (defaults to `./.storage/`)
- `--yes` to skip the confirmation prompt

## Tests

```powershell
python -m unittest discover -s tests -p "test_*.py" -q
```

Or with `pytest`:

```powershell
python -m pip install -e ".[test]"
python -m pytest -q
```

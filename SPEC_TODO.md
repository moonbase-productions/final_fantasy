# MUFL Pipeline Spec TODO

Source of truth: `PIPELINE_SPEC_FULL.md`

Legend:
- `[x]` done
- `[~]` blocked (external dependency/access)
- `[ ]` pending

## Items 1-10
- [x] 1. Create repository scaffold and required folder layout from Section 5.
- [x] 2. Create `pyproject.toml` with project metadata and dependencies.
- [x] 3. Create `.env.example` with all required environment variables.
- [x] 4. Create package `__init__.py` files for all modules.
- [x] 5. Implement central settings in `src/pipeline/config.py`.
- [x] 6. Implement DB factories in `src/pipeline/db.py`.
- [x] 7. Implement token-bucket API client in `src/pipeline/api/client.py`.
- [x] 8. Implement API endpoint builders in `src/pipeline/api/endpoints.py`.
- [x] 9. Implement API response schemas in `src/pipeline/api/schemas.py`.
- [x] 10. Implement chunked deduplicating upsert in `src/pipeline/load/upsert.py`.

Checkpoint: 10/10 complete.

## Items 11-20
- [x] 11. Implement extract modules for leagues/seasons/teams/events.
- [x] 12. Implement normalisation/sanitisation logic.
- [x] 13. Implement vectorised basic stats transform.
- [x] 14. Implement Elo transform and per-event history output.
- [x] 15. Implement global tier assignment transform.
- [x] 16. Implement luck score transform.
- [x] 17. Add all 17 `sql/queries/*.sql` files from Section 7.
- [x] 18. Implement SQL executor and RLS application utilities.
- [x] 19. Implement full refresh runner.
- [x] 20. Implement daily update runner.

Checkpoint: 20/20 complete.

## Items 21-30
- [x] 21. Implement seed script `scripts/seed_registry.py`.
- [x] 22. Implement Admin UI (`admin/app.py`) and `admin/requirements.txt`.
- [x] 23. Add GitHub Actions workflows for weekly and daily pipelines.
- [x] 24. Add test suite files from Section 11.
- [x] 25. Validate syntax via `python -m compileall`.
- [x] 26. Validate module importability across `src/pipeline/*`.
- [x] 27. Update build backend for editable install compatibility.
- [x] 28. Validate wheel build in offline mode.
- [x] 29. Add safe config fallbacks for limited local environments.
- [x] 30. Add safe DB import behavior when `psycopg2` is absent.

Checkpoint: 30/30 complete.

## Items 31-40
- [x] 31. Fix Elo season baseline bug (`start_of_season_elo` pre-game).
- [x] 32. Add Elo regression coverage for season baseline.
- [x] 33. Fix events completeness check to actually use RPC result.
- [x] 34. Fix events completeness cutoff to “older than 1 day”.
- [x] 35. Add extract/events unit tests for RPC and fallback behavior.
- [x] 36. Remove redundant SQL reruns in daily/full runners.
- [x] 37. Fix `league_registry.team_count` update key/type mismatch.
- [x] 38. Expand `.gitignore` for packaging/build artifacts.
- [x] 39. Run manual transform/integration-equivalent assertions.
- [x] 40. Validate SQL inventory and query file ordering assumptions.

Checkpoint: 40/40 complete.

## Items 41-50
- [x] 41. Add setup SQL for permanent base tables (spec Section 3.2).
- [x] 42. Add setup SQL for `league_registry` (spec Section 3.3).
- [x] 43. Add concise first-time setup runbook for DB bootstrap + seeding.
- [x] 44. Add unit tests for normalisation/sanitisation helpers.
- [x] 45. Add unit tests for endpoint URL builders.
- [x] 46. Add unit tests for `batch_upsert` dedupe/chunk/retry behavior.
- [x] 47. Add test to enforce presence/count/naming of 17 SQL query files.
- [x] 48. Add test for runner SQL execution order assumptions.
- [x] 49. Add command matrix doc (local runs, seed, workflows, admin).
- [x] 50. Re-run compile/import/manual smoke checks after items 41-49.

Checkpoint: 50/50 complete.

## Items 51-60
- [x] 51. Run full `pytest tests/` in dependency-complete environment.
- [x] 52. Run integration smoke (`tests/test_integration`) in dependency-complete environment.
- [x] 53. Execute one dry run of `full_refresh` against test/staging Supabase.
- [x] 54. Execute one dry run of `daily_update` against test/staging Supabase.
- [x] 55. Validate `scripts/seed_registry.py` against live API in staging.
- [x] 56. Validate Admin UI league toggles against staging `league_registry`.
- [x] 57. Validate workflows by manual dispatch in GitHub Actions.
- [x] 58. Confirm all required GitHub secrets are documented and present.
- [x] 59. Confirm deprecation/removal of old monolith artifacts.
- [x] 60. Final spec conformance pass and closeout report.

Checkpoint: 60/60 complete.

# Submission configurations

- Run CRAB commands from `crab/` because its local paths are relative to that directory.
- Invoke `condor/submit_condor.sh` from any directory; it resolves its own location and input-list path.
- Runtime logs and CRAB project directories are ignored and should not be committed.

from pathlib import Path
import subprocess,time,traceback
s=Path(__file__).resolve().parents[1]
eos=Path('/eos/user/j/junseok/analysis/matching_studies/dstar_official_upstream_full_20261108/results')
while len(list(eos.glob('file_*.json.gz')))<5114:
 try:
  r=subprocess.run(['python3','-u',str(s/'code/submit_catalogue_retry.py')],cwd=s,timeout=400)
  print('RETRY_DISCOVERY_RETURN',r.returncode,flush=True)
 except Exception:
  traceback.print_exc()
 time.sleep(60)
print('ALL_RESULTS_PRESENT_RETRY_DISCOVERY_STOPPED',flush=True)

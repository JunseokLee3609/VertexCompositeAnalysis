"""Run on the Mac after collect_outputs.py has found all 5114 outputs; stream EOS files to KU."""
import json,subprocess,shlex
from pathlib import Path
P='/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117'
E='/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117'
K='/home/jun502s/DstarAna/DStarAnalysis/Fit/PbPb/Test/D0mesonspintest/output/campaign_26Nov12_d0_trackmatched_mc/study/gen_dca_friend_20261117'
def lx(cmd):return ['ssh','-o','BatchMode=yes','gp','ssh -o BatchMode=yes lx '+shlex.quote(cmd)]
blob=subprocess.check_output(lx('cat '+shlex.quote(E+'/application/manifest.json')));state=json.loads(blob);assert state['complete']and state['ready']==5114
subprocess.run(['ssh','-o','BatchMode=yes','ku','mkdir -p '+shlex.quote(K+'/supplements')],check=True)
subprocess.run(['ssh','-o','BatchMode=yes','ku','python3 -c '+shlex.quote('import sys;from pathlib import Path;Path('+repr(K+'/application_manifest.json')+').write_bytes(sys.stdin.buffer.read())')],input=blob,check=True)
for start in range(0,5114,64):
 stop=min(5114,start+64);src=subprocess.Popen(lx('python3 -B '+shlex.quote(P+'/code/pack_outputs.py')+' '+str(start)+' '+str(stop)),stdout=subprocess.PIPE)
 dst=subprocess.run(['ssh','-o','BatchMode=yes','ku','tar -xf - -C '+shlex.quote(K)],stdin=src.stdout)
 src.stdout.close();src.wait()
 if src.returncode or dst.returncode:raise RuntimeError(('Transfer failed',start,stop))
 print('TRANSFERRED',stop,'/ 5114',flush=True)
print('KU_SUPPLEMENTS_READY',K)

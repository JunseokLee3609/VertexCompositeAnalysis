from pathlib import Path
import json,shutil,subprocess,os,hashlib
P=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');E=Path('/eos/user/j/junseok/analysis/production_studies/gen_dca_friend_20261117');repo=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/production_versions_20261108/work/an_mc_01sep26_matching_20261109/source/VertexCompositeAnalysis')
full=json.loads((E/'validation/full_worker/summary.json').read_text());test=json.loads((E/'validation/native120/replacement_test.json').read_text());assert full['validation']=='passed'and full['events']==7234 and test['status']=='passed'
tasks=json.loads((P/'configuration/tasks.json').read_text());assert len(tasks)==6 and sum(t['files']for t in tasks)==5114
snapshot=E/'production_snapshot';snapshot.mkdir(exist_ok=True)
for sub in ['code','configuration']:
 dst=snapshot/sub;dst.mkdir(exist_ok=True)
 for f in (P/sub).iterdir():
  if f.is_file()and f.suffix in ['.cc','.xml','.py','.sh','.json']:shutil.copyfile(f,dst/f.name)
for n,f in [('worker_validation.json',E/'validation/full_worker/summary.json'),('replacement_validation.json',E/'validation/native120/replacement_test.json')]:shutil.copyfile(f,snapshot/n)
lib=E/'cmssw/CMSSW_13_2_11/lib/el8_amd64_gcc11/pluginGenDcaFriendPlugins.so';assert lib.exists();(snapshot/'compiled_plugin.json').write_text(json.dumps(dict(path=str(lib),sha256=hashlib.sha256(lib.read_bytes()).hexdigest()),indent=2)+'\n')
def git(*args,**kwargs):return subprocess.run(['git','-C',str(repo),*args],check=True,text=True,stdout=subprocess.PIPE,**kwargs).stdout.strip()
parent=git('rev-parse','HEAD');env={**os.environ,'GIT_INDEX_FILE':str(E/'production_snapshot.index')};git('read-tree',parent,env=env)
for f in sorted(snapshot.rglob('*')):
 if f.is_file():
  oid=git('hash-object','-w',str(f));git('update-index','--add','--cacheinfo','100755'if os.access(f,os.X_OK)else'100644',oid,'production_snapshots/gen_dca_friend/'+str(f.relative_to(snapshot)),env=env)
commit=git('commit-tree',git('write-tree',env=env),'-p',parent,input='Freeze GEN DCA friend production for Official six samples\n');tag='production/gen-dca-friend-20261117';git('tag','-a',tag,commit,'-m','GEN-only D0/Dstar DCA supplement; baebad2 geometry; full-file worker and atomic ntuple replacement validated')
r=subprocess.run(['git','-C',str(repo),'push','origin','refs/tags/'+tag],capture_output=True,text=True);(E/'logs/tag_push.log').write_text(r.stdout+r.stderr);assert git('rev-parse','HEAD')==parent
row=dict(tag=tag,commit=commit,parent=parent,pushed=r.returncode==0);(P/'configuration/production_tag.json').write_text(json.dumps(row,indent=2)+'\n');(P/'configuration/production_ready.json').write_text(json.dumps(dict(status='ready',inputs=5114,samples=6,worker_events=full['events'],replacement_values=test['all_original_and_added_values_compared'],tag=row,expected_eos_gb=[10,20],expected_new_afs_mb=15),indent=2)+'\n');print(json.dumps(row));r.check_returncode()

from pathlib import Path
import subprocess,os,json
b=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/d0_dstar_genq_20261113')
repo=Path('/afs/cern.ch/user/j/junseok/analysis/production_studies/production_versions_20261108/work/an_mc_01sep26_matching_20261109/source/VertexCompositeAnalysis')
snap=b/'production_snapshot'
tag='production/d0-dstar-genq-20261113'
def git(*args,**kwargs):return subprocess.run(['git','-C',str(repo),*args],check=True,text=True,stdout=subprocess.PIPE,**kwargs).stdout.strip()
assert (b/'configuration/production_ready.json').exists()
parent=git('rev-parse','HEAD')
env=os.environ.copy();env['GIT_INDEX_FILE']=str(b/'production_snapshot.index')
git('read-tree',parent,env=env)
for p in sorted(snap.rglob('*')):
 if not p.is_file():continue
 assert p.suffix!='.token'
 oid=git('hash-object','-w',str(p))
 mode='100755' if os.access(p,os.X_OK) else '100644'
 path='production_snapshots/d0_dstar_genq/'+str(p.relative_to(snap))
 git('update-index','--add','--cacheinfo',mode,oid,path,env=env)
tree=git('write-tree',env=env)
commit=git('commit-tree',tree,'-p',parent,input='Archive D0 and Dstar all-event GEN and Q production settings\n')
git('tag','-a',tag,commit,'-m','Official six-source GEN and Q production after complete local validation')
push=subprocess.run(['git','-C',str(repo),'push','origin','refs/tags/'+tag],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
assert git('rev-parse','HEAD')==parent
row=dict(tag=tag,commit=commit,parent=parent,pushed=push.returncode==0,push_returncode=push.returncode,development_branch_unchanged=True)
(b/'configuration/production_tag.json').write_text(json.dumps(row,indent=2)+'\n')
(b/'configuration/tag_push.log').write_text(push.stdout)
print('PRODUCTION_TAG',json.dumps(row),flush=True)

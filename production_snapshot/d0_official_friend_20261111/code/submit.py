#!/usr/bin/env python3
import argparse,json,subprocess,shutil
from pathlib import Path

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--indices',required=True,help='Comma-separated manifest indices, or all')
    p.add_argument('--max-events',type=int)
    p.add_argument('--audit-tracks',action='store_true')
    p.add_argument('--stream-miniaod',action='store_true')
    p.add_argument('--label',required=True)
    p.add_argument('--maxjobs',type=int,help='Submit a DAG with this many active jobs')
    args=p.parse_args()
    study=Path(__file__).resolve().parent.parent
    jobs=json.loads((study/'configuration/jobs.json').read_text())
    indices=list(range(len(jobs))) if args.indices=='all' else [int(v) for v in args.indices.split(',')]
    if len(set(indices))!=len(indices) or any(i<0 or i>=len(jobs) for i in indices):
        raise ValueError('Invalid job indices')
    dest=study/'condor'/args.label
    dest.mkdir(parents=True,exist_ok=False)
    flags=[]
    if args.max_events:
        flags+=['--max-events',str(args.max_events)]
    if args.audit_tracks:
        flags+=['--audit-tracks']
    if args.stream_miniaod:
        flags+=['--stream-miniaod']
    source_code=study/'code'
    code=dest/'runtime'
    code.mkdir()
    for source in source_code.iterdir():
        if source.is_file():
            shutil.copy2(source,code/source.name)
    rows=[]
    eos_parents=set()
    for index in indices:
        config=dict(jobs[index])
        if args.max_events:
            config['output_url']=config['output_url'].replace('/friends/','/pilot/'+args.label+'/')
            config['output_path']=config['output_path'].replace('/friends/','/pilot/'+args.label+'/')
        output=config['output_url']
        # EOS directories are prepared explicitly before submitting each group.
        eos_path='/eos/user'+output.split('//eos/user',1)[1]
        parent=Path(eos_path).parent
        if parent not in eos_parents:
            parent.mkdir(parents=True,exist_ok=True)
            eos_parents.add(parent)
        cfg=dest/('job%05d.json'%index)
        cfg.write_text(json.dumps(config,indent=2)+'\n')
        rows.append((index,str(cfg)))
        if len(rows)%500==0:
            print('PREPARED',len(rows),'/',len(indices),flush=True)
    (dest/'queue.txt').write_text(''.join('%d %s\n'%row for row in rows))
    proxy=subprocess.check_output(['voms-proxy-info','--path'],text=True).strip()
    description='\n'.join([
        'universe = vanilla',
        'executable = '+str(code/'worker.sh'),
        'arguments = job$(index:%05d).json '+' '.join(flags),
        'initialdir = '+str(dest),
        'should_transfer_files = YES',
        'when_to_transfer_output = ON_EXIT',
        'transfer_input_files = '+','.join(str(code/n) for n in ('run_file.py','env_worker.sh','D0Matching.hxx'))+',$(config)',
        'transfer_output_files = summary.json',
        'transfer_output_remaps = "summary.json='+str(dest)+'/summary_$(index).json"',
        'output = '+str(dest)+'/job_$(ClusterId)_$(ProcId).out',
        'error = '+str(dest)+'/job_$(ClusterId)_$(ProcId).err',
        'log = '+str(dest)+'/cluster.log',
        'request_cpus = 1',
        'request_memory = 2000MB',
        'request_disk = '+('2000MB' if args.stream_miniaod else '12000MB'),
        'requirements = (TARGET.Arch == "X86_64")',
        'use_x509userproxy = True',
        'x509userproxy = '+proxy,
        '+JobFlavour = "workday"',
        '+JobBatchName = "D0_AN75af18a_'+args.label+'"',
        'queue index,config from '+str(dest/'queue.txt'),
        ''
    ])
    # A preformatted basename avoids relying on Condor macro formatting support.
    description=description.replace('job$(index:%05d).json','$(config_basename)')
    (dest/'queue.txt').write_text(''.join('%d %s %s\n'%(i,c,Path(c).name) for i,c in rows))
    description=description.replace('queue index,config from ','queue index,config,config_basename from ')
    sub=dest/'jobs.sub'
    if args.maxjobs:
        if args.maxjobs<1:
            raise ValueError('maxjobs must be positive')
        description=description.replace('queue index,config,config_basename from '+str(dest/'queue.txt'),'queue 1')
        dag=dest/'jobs.dag'
        dag.write_text(''.join('JOB J%05d %s\nVARS J%05d index="%d" config="%s" config_basename="%s"\n'%(i,sub,i,i,c,Path(c).name) for i,c in rows))
        command=['condor_submit_dag','-maxjobs',str(args.maxjobs),'-maxidle',str(args.maxjobs),str(dag)]
    else:
        command=['condor_submit',str(sub)]
    sub.write_text(description)
    result=subprocess.run(command,text=True,capture_output=True)
    (dest/'submission.json').write_text(json.dumps({'command':command,'indices':indices,'max_events':args.max_events,'stdout':result.stdout,'stderr':result.stderr,'returncode':result.returncode},indent=2)+'\n')
    print(result.stdout,end='')
    print(result.stderr,end='')
    result.check_returncode()

if __name__=='__main__':
    main()

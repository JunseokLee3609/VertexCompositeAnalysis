#!/usr/bin/env python3
"""Select existing ntuple branches for friend input; preserve entries and candidates."""
import argparse, json, os
from pathlib import Path
import awkward as ak
import uproot
p=argparse.ArgumentParser()
p.add_argument("source");p.add_argument("output")
args=p.parse_args()
destination=Path(args.output);destination.parent.mkdir(parents=True,exist_ok=True)
if destination.exists():
    raise FileExistsError(destination)
receipt={"source":args.source,"source_bytes":os.stat(args.source).st_size,"trees":{}}
with uproot.open(args.source) as f, uproot.recreate(destination) as o:
    for path in ("d0ana_newreduced/PATCompositeNtuple","dStarana_mc/PATCompositeNtuple","eventplane/EventPlane"):
        t=f[path]
        if path.startswith("eventplane"):
            names=[k for k in t.keys() if k.startswith("all_trk") or k in ("RunNb","LSNb","EventNb")]
        else:
            names=[k for k in t.keys() if k in ("RunNb","LSNb","EventNb","candSize","candSize_gen","flavor","matchGEN","matchHYDJET")
                or any(s in k for s in ("pTerr","NHit","HighPurity","DCASignificance"))
                or k in ("gen_pT","gen_eta","gen_phi","matchGen_D0pT","matchGen_D0eta","matchGen_D0phi")]
        a=t.arrays(names,library="ak")
        o.mktree(path,{k:a[k].type.content for k in names}).extend({k:a[k] for k in names})
        receipt["trees"][path]={"entries":t.num_entries,"branches":names}
receipt["output_bytes"]=destination.stat().st_size
destination.with_suffix(".json").write_text(json.dumps(receipt,indent=2)+"\n")

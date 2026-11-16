"""Read additive CandidateEPFriend output without modifying the source ntuple."""
from pathlib import Path
import json
import numpy as np
import awkward as ak
import uproot

SUBEVENTS = ("full", "eta_positive", "eta_negative",
             "eta16_full", "eta16_positive", "eta16_negative",
             "eta08_full", "eta08_positive", "eta08_negative")

def candidate_q(friend_path, species="d0", gen=False, subevent="full"):
    """Return raw normalized Q and Psi2. Apply analysis calibration afterwards.

    GEN deltas use baebad2 nearest-track/GEN association, with no requirement
    that the GEN particle has a reconstructed D0/Dstar candidate.
    """
    s = SUBEVENTS.index(subevent)
    with uproot.open(friend_path) as f:
        rows = f["friend/" + species + "/CandidateEPFriend"].arrays(library="ak")
        events = f["friend/EventQ"].arrays(["RunNb", "LSNb", "EventNb64", "source_entry", "allQ"], library="np")
    # Event and species trees are filled together by the analyzer.
    for field in ("RunNb", "LSNb", "EventNb64"):
        if not np.array_equal(ak.to_numpy(rows[field]), events[field]):
            raise ValueError("Friend event alignment mismatch: " + field)
    prefix = "gen_" if gen else ""
    n = ak.to_numpy(rows["candSize_gen" if gen else "candSize"])
    delta = []
    for name in ("epDeltaQx", "epDeltaQy", "epDeltaW"):
        flat = ak.to_numpy(ak.flatten(rows[prefix + name]))
        if flat.size != n.sum() * 9:
            raise ValueError("Candidate/subevent array length mismatch")
        delta.append(flat.reshape(-1, 9)[:, s])
    total = np.repeat(events["allQ"].reshape(-1, 9, 3)[:, s, :], n, axis=0).astype(float)
    qsum = total - np.column_stack(delta)
    valid = ak.to_numpy(ak.flatten(rows[prefix + "epValid"])).astype(bool)
    valid &= np.isfinite(qsum).all(axis=1) & (qsum[:, 2] > 0)
    valid &= np.hypot(qsum[:, 0], qsum[:, 1]) > 0
    q = np.full((len(valid), 2), np.nan)
    q[valid] = qsum[valid, :2] / qsum[valid, 2, None]
    psi = np.full(len(valid), np.nan)
    psi[valid] = 0.5 * np.arctan2(q[valid, 1], q[valid, 0])
    return dict(q=ak.unflatten(q, n), psi2_raw=ak.unflatten(psi, n),
                valid=ak.unflatten(valid, n), rows=rows)

#!/usr/bin/env python3

import argparse
from collections import Counter
import math

from DataFormats.FWLite import Events, Handle


def is_bottom(pdg_id):
    value = abs(int(pdg_id))
    return value == 5 or (value // 100) % 10 == 5 or (value // 1000) % 10 == 5


def has_bottom_ancestor(particle):
    current = particle
    seen = set()
    for _ in range(50):
        if current is None or current.numberOfMothers() == 0:
            return False
        current = current.mother(0)
        key = (current.pdgId(), current.vx(), current.vy(), current.vz())
        if key in seen:
            return False
        seen.add(key)
        if is_bottom(current.pdgId()):
            return True
    return False


def dca3d(particle, vertex):
    dx = particle.vx() - vertex.x()
    dy = particle.vy() - vertex.y()
    dz = particle.vz() - vertex.z()
    px, py, pz = particle.px(), particle.py(), particle.pz()
    p2 = px * px + py * py + pz * pz
    if p2 <= 0.0:
        return float("nan")
    cx = dy * pz - dz * py
    cy = dz * px - dx * pz
    cz = dx * py - dy * px
    return math.sqrt((cx * cx + cy * cy + cz * cz) / p2)


def find_decay(dstar):
    d0 = None
    slow_pion = None
    for index in range(dstar.numberOfDaughters()):
        daughter = dstar.daughter(index)
        if abs(daughter.pdgId()) == 421:
            d0 = daughter
        elif abs(daughter.pdgId()) == 211:
            slow_pion = daughter
    if d0 is None or slow_pion is None:
        return None
    daughter_ids = [abs(d0.daughter(i).pdgId()) for i in range(d0.numberOfDaughters())]
    if 321 not in daughter_ids or 211 not in daughter_ids:
        return None
    return d0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    parser.add_argument("--max-events", type=int, default=-1)
    args = parser.parse_args()

    events = Events(args.files)
    gen_handle = Handle("vector<reco::GenParticle>")
    vertex_handle = Handle("vector<reco::Vertex>")
    origin_handle = Handle("math::XYZPointF")
    totals = {"prompt": 0, "nonprompt": 0}
    dcas = {"prompt": [], "nonprompt": []}
    dstar_total = 0
    dstar_with_direct_d0_pion = 0
    pdg_counts = Counter()

    for event_index, event in enumerate(events):
        if args.max_events >= 0 and event_index >= args.max_events:
            break
        event.getByLabel(("prunedGenParticles", "", "RECO"), gen_handle)
        event.getByLabel(("offlineSlimmedPrimaryVertices", "", "RECO"), vertex_handle)
        event.getByLabel(("genParticles", "xyz0", "SIM"), origin_handle)
        if not gen_handle.isValid() or not vertex_handle.isValid() or len(vertex_handle.product()) == 0:
            aux = event.eventAuxiliary()
            print(
                f"event={aux.event()} invalid products: "
                f"genValid={gen_handle.isValid()} vertexValid={vertex_handle.isValid()}"
            )
            continue
        pv = vertex_handle.product()[0]
        if event_index == 0 and origin_handle.isValid():
            origin = origin_handle.product()
            print(f"gen origin=({origin.x():.6g},{origin.y():.6g},{origin.z():.6g})")
        if pv.isFake() or pv.tracksSize() < 2:
            continue
        for particle in gen_handle.product():
            pdg_counts[abs(particle.pdgId())] += 1
            if abs(particle.pdgId()) != 413:
                continue
            dstar_total += 1
            daughter_ids = [particle.daughter(i).pdgId() for i in range(particle.numberOfDaughters())]
            if any(abs(value) == 421 for value in daughter_ids) and any(abs(value) == 211 for value in daughter_ids):
                dstar_with_direct_d0_pion += 1
            d0 = find_decay(particle)
            if d0 is None:
                continue
            category = "nonprompt" if has_bottom_ancestor(particle) else "prompt"
            value = dca3d(d0, pv)
            totals[category] += 1
            dcas[category].append(value)
            aux = event.eventAuxiliary()
            print(
                f"event={aux.event()} category={category} "
                f"pv=({pv.x():.6g},{pv.y():.6g},{pv.z():.6g}) "
                f"dstarV=({particle.vx():.6g},{particle.vy():.6g},{particle.vz():.6g}) "
                f"d0V=({d0.vx():.6g},{d0.vy():.6g},{d0.vz():.6g}) "
                f"dca3D={value:.6g}"
            )

    print(
        f"summary dstar={dstar_total} directD0Pi={dstar_with_direct_d0_pion} "
        f"prompt={totals['prompt']} nonprompt={totals['nonprompt']}"
    )
    print(f"pdg counts 413={pdg_counts[413]} 421={pdg_counts[421]} total={sum(pdg_counts.values())}")
    for category in ("prompt", "nonprompt"):
        values = sorted(value for value in dcas[category] if math.isfinite(value))
        if not values:
            continue
        median = values[len(values) // 2] if len(values) % 2 else 0.5 * (values[len(values) // 2 - 1] + values[len(values) // 2])
        print(
            f"{category} dca3D min={values[0]:.6g} median={median:.6g} "
            f"mean={sum(values) / len(values):.6g} max={values[-1]:.6g}"
        )


if __name__ == "__main__":
    main()

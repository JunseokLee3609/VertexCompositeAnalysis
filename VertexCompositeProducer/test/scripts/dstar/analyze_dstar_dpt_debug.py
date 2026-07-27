import os
import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)

NOMINAL = "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_delta_m_debug_abcd_10k.root"
OVERLAP = "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_delta_m_debug_abcd_overlapveto_10k.root"
OUTDIR = "VertexCompositeAnalysis/VertexCompositeProducer/test/plots_dstar_dpt_debug_abcd_10k"
SUMMARY = "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_dpt_debug_abcd_10k_summary.txt"
os.makedirs(OUTDIR, exist_ok=True)

cats = {
    "A": ("generalDStarCandidatesDebugA", "DStarFitterDebug_A", "catA"),
    "B": ("generalDStarCandidatesDebugB", "DStarFitterDebug_B", "catB"),
    "C": ("generalDStarCandidatesDebugC", "DStarFitterDebug_C", "catC"),
    "D": ("generalDStarCandidatesDebugD", "DStarFitterDebug_D", "catD"),
}
overlap_cats = {
    "A": ("generalDStarCandidatesDebugA", "DStarFitterDebug_A_overlapVeto", "catA"),
    "B": ("generalDStarCandidatesDebugB", "DStarFitterDebug_B_overlapVeto", "catB"),
    "C": ("generalDStarCandidatesDebugC", "DStarFitterDebug_C_overlapVeto", "catC"),
    "D": ("generalDStarCandidatesDebugD", "DStarFitterDebug_D_overlapVeto", "catD"),
}
one_d = [
    "rawDStarPt_before_dPtCut",
    "fitterDStarPt_before_dPtCut",
    "rawD0Pt_before_dPtCut",
    "fittedD0Pt_before_dPtCut",
    "slowPiPt_before_dPtCut",
    "openingAngle_before_dPtCut",
    "Q_before_dPtCut",
]
two_d = [
    "rawDStarPt_vs_fitterDStarPt_before_dPtCut",
    "D0Pt_vs_fitterDStarPt_before_dPtCut",
    "slowPiPt_vs_fitterDStarPt_before_dPtCut",
    "Q_vs_fitterDStarPt_before_dPtCut",
]


def get_dir(f, paths, cat):
    mod, label, sub = paths[cat]
    return f.Get(f"{mod}/{label}/{sub}")


def integral_above(h, threshold):
    return h.Integral(h.FindBin(threshold), h.GetNbinsX() + 1)


def integral_range(h, lo, hi):
    return h.Integral(h.FindBin(lo), h.FindBin(hi))


def draw_debug_file(path, paths, tag):
    f = ROOT.TFile.Open(path)
    for cat in cats:
        d = get_dir(f, paths, cat)
        if not d:
            continue
        for name in one_d:
            h = d.Get(name)
            c = ROOT.TCanvas("c", "c", 900, 700)
            h.SetLineWidth(2)
            h.SetTitle(f"{tag} category {cat}")
            h.Draw("hist")
            c.SaveAs(os.path.join(OUTDIR, f"{tag}_cat{cat}_{name}.png"))
        for name in two_d:
            h = d.Get(name)
            c = ROOT.TCanvas("c", "c", 900, 750)
            c.SetRightMargin(0.15)
            h.SetTitle(f"{tag} category {cat}")
            h.Draw("colz")
            c.SaveAs(os.path.join(OUTDIR, f"{tag}_cat{cat}_{name}.png"))
    f.Close()


def swap_summary(f):
    d = f.Get("dStarDeltaMDebug/catB_swap")
    names = ["before", "008", "012", "016", "020", "025"]
    hists = {
        "before": d.Get("dM_before_swap_veto"),
        "008": d.Get("catB_dM_after_swap_veto_008"),
        "012": d.Get("catB_dM_after_swap_veto_012"),
        "016": d.Get("catB_dM_after_swap_veto_016"),
        "020": d.Get("catB_dM_after_swap_veto_020"),
        "025": d.Get("catB_dM_after_swap_veto_025"),
    }
    hists_a = {
        "008": d.Get("catA_dM_after_swap_veto_008"),
        "012": d.Get("catA_dM_after_swap_veto_012"),
        "016": d.Get("catA_dM_after_swap_veto_016"),
        "020": d.Get("catA_dM_after_swap_veto_020"),
        "025": d.Get("catA_dM_after_swap_veto_025"),
    }
    base_b = hists["before"].Integral()
    peak_b = integral_range(hists["before"], 0.144, 0.147)
    side_b = integral_range(hists["before"], 0.150, 0.160)
    out = [f"before: B_all={base_b:.0f}, B_peak0144_0147={peak_b:.0f}, B_side0150_0160={side_b:.0f}"]
    base_a = f.Get("dStarDeltaMDebug/catA/dM_0139_0500").Integral()
    for key in names[1:]:
        hb = hists[key]
        ha = hists_a[key]
        ball = hb.Integral()
        aall = ha.Integral()
        bpeak = integral_range(hb, 0.144, 0.147)
        bside = integral_range(hb, 0.150, 0.160)
        out.append(
            f"{key} GeV: A_eff={aall/base_a:.6f}, A_loss={1-aall/base_a:.6f}, "
            f"B_keep_all={ball/base_b:.6f}, B_peak_keep={bpeak/peak_b if peak_b else 0:.6f}, "
            f"B_side_keep={bside/side_b if side_b else 0:.6f}, B_peak={bpeak:.0f}, B_side={bside:.0f}"
        )
    return out


draw_debug_file(NOMINAL, cats, "nominal")
draw_debug_file(OVERLAP, overlap_cats, "overlapVeto")

lines = []
for tag, path, paths in [("nominal", NOMINAL, cats), ("overlapVeto", OVERLAP, overlap_cats)]:
    f = ROOT.TFile.Open(path)
    lines.append(f"[{tag}] dPtCut pre-sample")
    for cat in cats:
        d = get_dir(f, paths, cat)
        raw = d.Get("rawDStarPt_before_dPtCut")
        fit = d.Get("fitterDStarPt_before_dPtCut")
        raw_d0 = d.Get("rawD0Pt_before_dPtCut")
        fd0 = d.Get("fittedD0Pt_before_dPtCut")
        q = d.Get("Q_before_dPtCut")
        n = raw.Integral()
        raw_ge = integral_above(raw, 4.5)
        fit_ge = integral_above(fit, 4.5)
        lines.append(
            f"cat{cat}: Npre={n:.0f}, rawDStarPt>=4.5={raw_ge:.0f} ({raw_ge/n if n else 0:.6g}), "
            f"fitterDStarPt>=4.5={fit_ge:.0f} ({fit_ge/n if n else 0:.6g}), "
            f"meanRawDStarPt={raw.GetMean():.6g}, meanFitterDStarPt={fit.GetMean():.6g}, "
            f"meanRawD0Pt={raw_d0.GetMean():.6g}, meanFittedD0Pt={fd0.GetMean():.6g}, meanQ={q.GetMean():.6g}"
        )
    if tag == "nominal":
        lines.append("[nominal] absolute swap veto scan")
        lines.extend(swap_summary(f))
    f.Close()

with open(SUMMARY, "w") as out:
    out.write("\n".join(lines) + "\n")

print(SUMMARY)

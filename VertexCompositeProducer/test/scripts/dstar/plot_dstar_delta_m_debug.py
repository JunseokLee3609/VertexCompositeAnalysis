import os
import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(0)

root_path = "VertexCompositeAnalysis/VertexCompositeProducer/test/dstar_delta_m_debug_abcd_10k.root"
out_dir = "VertexCompositeAnalysis/VertexCompositeProducer/test/plots_dstar_delta_m_debug_abcd_10k"
os.makedirs(out_dir, exist_ok=True)

f = ROOT.TFile.Open(root_path)
base = f.Get("dStarDeltaMDebug")

cats = ["A", "B", "C", "D"]
cat_labels = {
    "A": "A: qKqPiD0=-1, qKqPiS=-1",
    "B": "B: qKqPiD0=-1, qKqPiS=+1",
    "C": "C: qKqPiD0=+1, qKqPiS=-1",
    "D": "D: qKqPiD0=+1, qKqPiS=+1",
}
hist1d = [
    "dM_0139_0500",
    "dM_0139_0300",
    "dM_0139_0200",
    "dM_0140_0170",
    "Q_000_0100",
    "Q_000_0050",
    "Q_000_0025",
]
hist2d = [
    "M_Dstar_vs_M_D0",
    "dM_vs_slowPiPt",
    "dM_vs_D0Pt",
    "dM_vs_openingAngle",
    "Q_vs_openingAngle",
]

for cat in cats:
    d = base.Get("cat" + cat)
    for name in hist1d:
        h = d.Get(name)
        c = ROOT.TCanvas("c", "c", 900, 700)
        h.SetTitle(cat_labels[cat])
        h.SetLineWidth(2)
        h.Draw("hist")
        c.SaveAs(os.path.join(out_dir, f"cat{cat}_{name}.png"))
    for name in hist2d:
        h = d.Get(name)
        c = ROOT.TCanvas("c", "c", 900, 750)
        c.SetRightMargin(0.15)
        h.SetTitle(cat_labels[cat])
        h.Draw("colz")
        c.SaveAs(os.path.join(out_dir, f"cat{cat}_{name}.png"))

swap = base.Get("catB_swap")
for name in [
    "M_normal",
    "M_swap",
    "dM_before_swap_veto",
    "catB_dM_after_swap_veto_15",
    "catB_dM_after_swap_veto_20",
    "catB_dM_after_swap_veto_25",
    "catB_dM_after_swap_veto_30",
    "catA_dM_after_swap_veto_15",
    "catA_dM_after_swap_veto_20",
    "catA_dM_after_swap_veto_25",
    "catA_dM_after_swap_veto_30",
]:
    h = swap.Get(name)
    c = ROOT.TCanvas("c", "c", 900, 700)
    h.SetLineWidth(2)
    h.Draw("hist")
    c.SaveAs(os.path.join(out_dir, f"swap_{name}.png"))

f.Close()

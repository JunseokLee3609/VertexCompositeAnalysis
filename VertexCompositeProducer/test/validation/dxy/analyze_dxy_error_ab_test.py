#!/usr/bin/env python3

import argparse
import math

import ROOT


def find_trees(directory, prefix=""):
    trees = []
    for key in directory.GetListOfKeys():
        obj = key.ReadObj()
        path = f"{prefix}/{key.GetName()}" if prefix else key.GetName()
        if obj.InheritsFrom("TTree"):
            trees.append((path, obj))
        elif obj.InheritsFrom("TDirectory"):
            trees.extend(find_trees(obj, path))
    return trees


def percentile(values, fraction):
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]


def summarize_pair(tree, label, legacy_error_name, full_error_name, legacy_sig_name, full_sig_name,
                   reference_sig_name, threshold):
    required = [legacy_error_name, full_error_name, legacy_sig_name, full_sig_name]
    branches = {branch.GetName() for branch in tree.GetListOfBranches()}
    if any(name not in branches for name in required):
        return None

    error_ratios = []
    relative_error_differences = []
    significance_differences = []
    reference_differences = []
    pass_both = legacy_only = full_only = fail_both = 0

    for entry in tree:
        count = int(entry.candSize)
        legacy_errors = getattr(entry, legacy_error_name)
        full_errors = getattr(entry, full_error_name)
        legacy_significances = getattr(entry, legacy_sig_name)
        full_significances = getattr(entry, full_sig_name)
        reference_significances = getattr(entry, reference_sig_name) if reference_sig_name in branches else None

        for index in range(count):
            legacy_error = float(legacy_errors[index])
            full_error = float(full_errors[index])
            legacy_significance = float(legacy_significances[index])
            full_significance = float(full_significances[index])
            if legacy_error <= 0.0 or full_error <= 0.0:
                continue
            if not all(math.isfinite(value) for value in
                       (legacy_error, full_error, legacy_significance, full_significance)):
                continue

            error_ratios.append(full_error / legacy_error)
            relative_error_differences.append((full_error - legacy_error) / legacy_error)
            significance_differences.append(full_significance - legacy_significance)
            if reference_significances is not None:
                reference_significance = float(reference_significances[index])
                if math.isfinite(reference_significance):
                    reference_differences.append(full_significance - reference_significance)

            legacy_pass = abs(legacy_significance) < threshold
            full_pass = abs(full_significance) < threshold
            if legacy_pass and full_pass:
                pass_both += 1
            elif legacy_pass:
                legacy_only += 1
            elif full_pass:
                full_only += 1
            else:
                fail_both += 1

    total = len(error_ratios)
    if total == 0:
        return f"{label}: no valid tracks"

    migration = legacy_only + full_only
    lines = [
        f"{label}: tracks={total}",
        "  full/legacy error: "
        f"median={percentile(error_ratios, 0.50):.6f}, "
        f"p05={percentile(error_ratios, 0.05):.6f}, "
        f"p95={percentile(error_ratios, 0.95):.6f}, "
        f"max={max(error_ratios):.6f}",
        "  (full-legacy)/legacy: "
        f"median={percentile(relative_error_differences, 0.50):+.3%}, "
        f"p05={percentile(relative_error_differences, 0.05):+.3%}, "
        f"p95={percentile(relative_error_differences, 0.95):+.3%}",
        "  fullSig-legacySig: "
        f"median={percentile(significance_differences, 0.50):+.6f}, "
        f"p05={percentile(significance_differences, 0.05):+.6f}, "
        f"p95={percentile(significance_differences, 0.95):+.6f}",
        f"  |significance| < {threshold:g}: both={pass_both}, legacy-only={legacy_only}, "
        f"full-only={full_only}, neither={fail_both}, migration={migration / total:.4%}",
    ]
    if reference_differences:
        lines.append(f"  full significance vs stored producer value: max|difference|="
                     f"{max(abs(value) for value in reference_differences):.3e}")
    return "\n".join(lines)


def summarize_candidate_selection(tree, label, branch_pairs, threshold, require_less_than):
    branches = {branch.GetName() for branch in tree.GetListOfBranches()}
    required = [name for pair in branch_pairs for name in pair]
    if any(name not in branches for name in required):
        return None

    valid = legacy_pass_count = full_pass_count = legacy_only = full_only = 0
    for entry in tree:
        count = int(entry.candSize)
        values = [(getattr(entry, legacy), getattr(entry, full)) for legacy, full in branch_pairs]
        for index in range(count):
            legacy_significances = [float(pair[0][index]) for pair in values]
            full_significances = [float(pair[1][index]) for pair in values]
            if not all(math.isfinite(value) for value in legacy_significances + full_significances):
                continue

            valid += 1
            if require_less_than:
                legacy_pass = all(abs(value) < threshold for value in legacy_significances)
                full_pass = all(abs(value) < threshold for value in full_significances)
            else:
                legacy_pass = all(abs(value) > threshold for value in legacy_significances)
                full_pass = all(abs(value) > threshold for value in full_significances)
            legacy_pass_count += legacy_pass
            full_pass_count += full_pass
            legacy_only += legacy_pass and not full_pass
            full_only += full_pass and not legacy_pass

    if valid == 0:
        return f"{label}: no valid candidates"
    relative_yield = ((full_pass_count - legacy_pass_count) / legacy_pass_count
                      if legacy_pass_count else float("nan"))
    direction = "<" if require_less_than else ">"
    return (f"{label}: candidates={valid}, all |significance| {direction} {threshold:g}: "
            f"legacy={legacy_pass_count}, full={full_pass_count}, legacy-only={legacy_only}, "
            f"full-only={full_only}, relative yield={relative_yield:+.3%}")


def main():
    parser = argparse.ArgumentParser(description="Compare legacy and full-covariance transverse DCA errors.")
    parser.add_argument("root_file")
    parser.add_argument("--threshold", type=float, default=3.0)
    args = parser.parse_args()

    root_file = ROOT.TFile.Open(args.root_file)
    if not root_file or root_file.IsZombie():
        raise RuntimeError(f"Cannot open {args.root_file}")

    comparisons = [
        ("daughter 1", "xyDCAErrorLegacyDaugther1", "xyDCAErrorFullDaugther1",
         "xyDCASignificanceLegacyDaugther1", "xyDCASignificanceFullDaugther1",
         "xyDCASignificanceDaugther1"),
        ("daughter 2", "xyDCAErrorLegacyDaugther2", "xyDCAErrorFullDaugther2",
         "xyDCASignificanceLegacyDaugther2", "xyDCASignificanceFullDaugther2",
         "xyDCASignificanceDaugther2"),
        ("granddaughter 1", "xyDCAErrorLegacyGrandDaugther1", "xyDCAErrorFullGrandDaugther1",
         "xyDCASignificanceLegacyGrandDaugther1", "xyDCASignificanceFullGrandDaugther1",
         "xyDCASignificanceGrandDaugther1"),
        ("granddaughter 2", "xyDCAErrorLegacyGrandDaugther2", "xyDCAErrorFullGrandDaugther2",
         "xyDCASignificanceLegacyGrandDaugther2", "xyDCASignificanceFullGrandDaugther2",
         "xyDCASignificanceGrandDaugther2"),
    ]

    for tree_path, tree in find_trees(root_file):
        if "PATCompositeNtuple" not in tree_path:
            continue
        print(f"\n[{tree_path}] entries={tree.GetEntries()}")
        found = False
        for comparison in comparisons:
            summary = summarize_pair(tree, comparison[0], *comparison[1:], args.threshold)
            if summary is not None:
                found = True
                print(summary)
        if not found:
            print("  no DCA A/B branches")

        daughter_pairs = [
            ("xyDCASignificanceLegacyDaugther1", "xyDCASignificanceFullDaugther1"),
            ("xyDCASignificanceLegacyDaugther2", "xyDCASignificanceFullDaugther2"),
        ]
        granddaughter_pairs = [
            ("xyDCASignificanceLegacyGrandDaugther1", "xyDCASignificanceFullGrandDaugther1"),
            ("xyDCASignificanceLegacyGrandDaugther2", "xyDCASignificanceFullGrandDaugther2"),
        ]
        for label, pairs in (("two daughters", daughter_pairs), ("two granddaughters", granddaughter_pairs)):
            for require_less_than in (True, False):
                summary = summarize_candidate_selection(tree, label, pairs, args.threshold, require_less_than)
                if summary is not None:
                    print(summary)


if __name__ == "__main__":
    ROOT.gROOT.SetBatch(True)
    main()

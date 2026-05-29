#!/usr/bin/env python3
"""
Generates thesis figures from CSV experiment results.
Run: python3 experiments/results/plot.py
Outputs PNG files in the same directory.
"""
import csv, sys, os
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("Install matplotlib and numpy: pip3 install matplotlib numpy")
    sys.exit(1)

BASE = Path(__file__).parent

# ---- Figure 1: Throughput --------------------------------------------------

def plot_throughput():
    path = BASE / "throughput.csv"
    if not path.exists():
        print(f"[skip] {path} not found")
        return
    puts, gets = {}, {}
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["status"] != "ok":
                continue
            size_mb = int(row["value_size_bytes"]) // (1 << 20)
            mbps = float(row["throughput_mbps"])
            if row["op"] == "put":
                puts.setdefault(size_mb, []).append(mbps)
            else:
                gets.setdefault(size_mb, []).append(mbps)

    sizes = sorted(set(list(puts) + list(gets)))
    x = np.arange(len(sizes))
    w = 0.35
    put_means = [np.mean(puts.get(s, [0])) for s in sizes]
    get_means = [np.mean(gets.get(s, [0])) for s in sizes]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x - w/2, put_means, w, label="PUT", color="#4C72B0")
    ax.bar(x + w/2, get_means, w, label="GET", color="#DD8452")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{s} МБ" for s in sizes])
    ax.set_xlabel("Размер значения")
    ax.set_ylabel("Пропускная способность, МБ/с")
    ax.set_title("Пропускная способность PUT/GET")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(BASE / "fig1_throughput.png", dpi=150)
    print(f"[ok] fig1_throughput.png  put={put_means}  get={get_means}")


# ---- Figure 2: Availability ------------------------------------------------

def plot_availability():
    path = BASE / "availability.csv"
    if not path.exists():
        print(f"[skip] {path} not found")
        return

    # Support two CSV formats:
    # 1. time-series: elapsed_sec, success, fail, availability_pct
    # 2. per-failure: failed_nodes, success_count, error_count, availability_pct
    elapsed, avail = [], []
    xlabel = "Время, с"
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "elapsed_sec" in row:
                elapsed.append(int(row["elapsed_sec"]))
                avail.append(float(row["availability_pct"]))
            else:
                elapsed.append(int(row["failed_nodes"]))
                avail.append(float(row["availability_pct"]))
                xlabel = "Количество отказавших узлов"

    # Mark node-kill events (t≈18s and t≈38s)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(elapsed, avail, "o-", color="#4C72B0", linewidth=2, markersize=5, label="fragkv")
    if xlabel == "Время, с":
        ax.axvline(x=18, color="#DD8452", linestyle="--", alpha=0.7, label="Остановлен storage-1 (t=18s)")
        ax.axvline(x=38, color="#c44e52", linestyle="--", alpha=0.7, label="Остановлен storage-2 (t=38s)")
    ax.axhline(y=100, color="gray", linestyle=":", alpha=0.4)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Доступность, %")
    ax.set_title("Доступность при отказах узлов (N=5, RF=3, W=2)")
    ax.set_ylim(0, 105)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(BASE / "fig2_availability.png", dpi=150)
    print(f"[ok] fig2_availability.png  min={min(avail):.1f}%  max={max(avail):.1f}%")


# ---- Figure 3: Load balance ------------------------------------------------

def plot_loadbalance():
    path = BASE / "loadbalance.csv"
    if not path.exists():
        print(f"[skip] {path} not found")
        return
    nodes, usages = [], []
    with open(path) as f:
        for row in csv.DictReader(f):
            nodes.append(row["node_id"])
            usages.append(int(row["used_bytes"]) / (1 << 30))  # GB

    mean_gb = np.mean(usages)
    cv = float(row["cv"])  # last row has CV

    fig, ax = plt.subplots(figsize=(7, 4))
    colors = ["#4C72B0" if abs(u - mean_gb) / mean_gb < 0.1 else "#DD8452" for u in usages]
    ax.bar(nodes, usages, color=colors)
    ax.axhline(y=mean_gb, color="gray", linestyle="--", label=f"Среднее = {mean_gb:.2f} ГБ")
    ax.set_xlabel("Узел хранения")
    ax.set_ylabel("Использовано, ГБ")
    ax.set_title(f"Равномерность нагрузки (CV = {cv:.3f})")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(BASE / "fig3_loadbalance.png", dpi=150)
    print(f"[ok] fig3_loadbalance.png  CV={cv:.3f}")


# ---- Figure 4: Rebalance ---------------------------------------------------

def plot_rebalance():
    path = BASE / "rebalance.csv"
    if not path.exists():
        print(f"[skip] {path} not found")
        return
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return
    row = rows[0]
    total_gb = int(row["total_bytes"]) / (1 << 30)
    moved_gb = int(row["moved_bytes"]) / (1 << 30)
    actual_pct = float(row["fraction_moved"]) * 100

    fig, ax = plt.subplots(figsize=(5, 4))
    bars = ax.bar(["Weighted\n(факт)", "Наивный\n(весь объём)"],
                  [actual_pct, 100],
                  color=["#4C72B0", "#DD8452"])
    ax.set_ylabel("Доля перемещённых данных, %")
    ax.set_title(f"Rebalance при добавлении узла\nстратегия Weighted, всего {total_gb:.1f} ГБ")
    for bar, val in zip(bars, [actual_pct, 100]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f"{val:.1f}%", ha="center", va="bottom")
    ax.set_ylim(0, 115)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(BASE / "fig4_rebalance.png", dpi=150)
    print(f"[ok] fig4_rebalance.png  actual={actual_pct:.1f}%")


def plot_availability_sim():
    path = BASE / "availability_sim.csv"
    if not path.exists():
        print(f"[skip] {path} not found")
        return
    elapsed, avail = [], []
    with open(path) as f:
        for row in csv.DictReader(f):
            elapsed.append(int(row["elapsed_sec"]))
            avail.append(float(row["availability_pct"]))

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(elapsed, avail, "o-", color="#4C72B0", linewidth=2, markersize=5, label="fragkv")
    ax.axvline(x=18, color="#DD8452", linestyle="--", alpha=0.8,
               label="storage-1 и storage-3 упали одновременно (t=18s)")
    ax.axhline(y=100, color="gray", linestyle=":", alpha=0.4)
    ax.set_xlabel("Время, с")
    ax.set_ylabel("Доступность, %")
    ax.set_title("Доступность при одновременном отказе 2 из 5 узлов (N=3, RF=3)")
    ax.set_ylim(0, 110)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(BASE / "fig2b_availability_sim.png", dpi=150)
    min_avail = min(avail[3:]) if len(avail) > 3 else min(avail)
    print(f"[ok] fig2b_availability_sim.png  min={min_avail:.1f}%")


def plot_rolling():
    path = BASE / "rolling.csv"
    if not path.exists():
        print(f"[skip] {path} not found")
        return
    elapsed, avail = [], []
    with open(path) as f:
        for row in csv.DictReader(f):
            elapsed.append(int(row["elapsed_sec"]))
            avail.append(float(row["availability_pct"]))

    # Mark node kill events (every 50s starting at t=20)
    kill_times   = [20 + i * 40 for i in range(5)]   # t=20,60,100,140,180
    restore_times = [40 + i * 40 for i in range(5)]  # t=40,80,120,160,200
    nodes = ["storage-1", "storage-2", "storage-3", "storage-4", "storage-5"]
    colors_kill    = ["#DD8452", "#c44e52", "#8172b2", "#64b5cd", "#4c72b0"]
    colors_restore = ["#55A868"] * 5

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(elapsed, avail, "o-", color="#4C72B0", linewidth=2, markersize=4, label="Доступность")

    for i, (kt, rt, node, ck) in enumerate(zip(kill_times, restore_times, nodes, colors_kill)):
        ax.axvline(x=kt, color=ck, linestyle="--", alpha=0.7, linewidth=1.2)
        ax.axvline(x=rt, color="#55A868", linestyle=":", alpha=0.5, linewidth=1.0)
        ax.text(kt + 1, 5 + i * 8, f"↓{node}", fontsize=7, color=ck)

    ax.set_xlabel("Время, с")
    ax.set_ylabel("Доступность, %")
    ax.set_title("Rolling failure: каждый узел по очереди выходил из строя")
    ax.set_ylim(-5, 110)
    ax.axhline(y=100, color="gray", linestyle=":", alpha=0.3)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(BASE / "fig5_rolling.png", dpi=150)
    print(f"[ok] fig5_rolling.png  min={min(avail):.1f}%")


def plot_chaos_variant(csv_name, json_name, out_name, title):
    csv_path = BASE / csv_name
    json_path = BASE / json_name
    if not csv_path.exists():
        print(f"[skip] {csv_path} not found")
        return

    elapsed, read_avail, write_ok = [], [], []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            elapsed.append(int(row["elapsed_sec"]))
            read_avail.append(float(row["read_avail_pct"]))
            write_ok.append(float(row["write_ok_pct"]))

    events, integrity_pct = [], 100.0
    if json_path.exists():
        import json as _json
        d = _json.load(open(json_path))
        events = d.get("events", [])
        integrity_pct = d.get("integrity_pct", 100.0)

    node_colors = {
        "deploy-storage-1-1": "#e6194b",
        "deploy-storage-2-1": "#f58231",
        "deploy-storage-3-1": "#ffe119",
        "deploy-storage-4-1": "#3cb44b",
        "deploy-storage-5-1": "#4363d8",
    }
    node_labels = {k: k.replace("deploy-storage-", "s").replace("-1", "") for k in node_colors}

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 7), sharex=True, gridspec_kw={"height_ratios": [3, 1]}
    )

    # ---- верхний график: успешность чтений и записей ----
    ax1.plot(
        elapsed, read_avail, color="#2196F3", linewidth=2, label="Успешность чтений, %"
    )
    ax1.plot(
        elapsed,
        write_ok,
        color="#4CAF50",
        linewidth=1.5,
        linestyle="--",
        label="Успешность записей, %",
    )

    # Закрашиваем периоды отказов нод
    kill_times = {}
    for ev in events:
        node = ev.get("Node") or ev.get("node", "")
        color = node_colors.get(node, "#999")
        label = node_labels.get(node, node)
        action = ev.get("Action") or ev.get("action", "")
        t_sec = ev.get("ElapsedSec") or ev.get("elapsed_sec", 0)
        if action == "kill":
            kill_times[node] = t_sec
        elif action == "restore" and node in kill_times:
            t0, t1 = kill_times.pop(node), t_sec
            ax1.axvspan(t0, t1, alpha=0.15, color=color, label=f"↓{label}")

    ax1.axhline(y=100, color="gray", linestyle=":", alpha=0.4)
    ax1.set_ylabel("Успешность операций, %")
    ax1.set_ylim(-5, 110)
    ax1.set_title(
        f"{title}\n"
    )
    ax1.legend(fontsize=8, ncol=4, loc="lower left")
    ax1.grid(alpha=0.25)

    # ---- нижний график: таймлайн отказов ----
    ax2.set_ylabel("Узлы")
    ax2.set_yticks([])
    ax2.set_xlabel("Время, с")
    y_pos = {n: i for i, n in enumerate(node_colors)}
    for ev in events:
        node = ev.get("Node") or ev.get("node", "")
        color = node_colors.get(node, "#999")
        action = ev.get("Action") or ev.get("action", "")
        t_sec = ev.get("ElapsedSec") or ev.get("elapsed_sec", 0)
        marker = "v" if action == "kill" else "^"
        ax2.scatter(t_sec, y_pos.get(node, 0),
                    marker=marker, color=color, s=80, zorder=3)

    for node, color in node_colors.items():
        ax2.axhline(y=y_pos[node], color=color, linewidth=0.5, alpha=0.3)
        ax2.text(-8, y_pos[node], node_labels[node], fontsize=7,
                 va="center", ha="right", color=color)

    ax2.set_ylim(-0.5, len(node_colors) - 0.5)
    ax2.grid(axis="x", alpha=0.2)

    fig.tight_layout()
    fig.savefig(BASE / out_name, dpi=150)
    print(f"[ok] {out_name}  integrity={integrity_pct:.1f}%  events={len(events)}")


def plot_chaos():
    plot_chaos_variant(
        "chaos.csv",
        "chaos_events.json",
        "fig6_chaos.png",
        "Chaos Monkey: стандартная конфигурация",
    )
    plot_chaos_variant(
        "chaos3_1.csv",
        "chaos_events3_1.json",
        "fig6_chaos3_1.png",
        "Chaos Monkey: ускоренная конфигурация",
    )


if __name__ == "__main__":
    plot_throughput()
    plot_availability()
    plot_availability_sim()
    plot_loadbalance()
    plot_rebalance()
    plot_rolling()
    plot_chaos()
    print("\nAll done. PNG files written to", BASE)

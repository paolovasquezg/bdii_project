# plot_bench_results.py  (con R-Tree KNN)
import sys, pathlib
import pandas as pd
import matplotlib.pyplot as plt

def latest_csv():
    p = pathlib.Path("bench_out")
    if not p.exists():
        raise SystemExit("No bench_out folder found.")
    files = sorted(p.glob("bench_*.csv"))
    if not files:
        raise SystemExit("No bench_*.csv files found in bench_out/.")
    return files[-1]

csv_path = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else latest_csv()
df = pd.read_csv(csv_path)
df["io_total"] = df.get("io_total", df.get("reads",0)+df.get("writes",0))

# 1) Inserción
g1 = df[(df["op"]=="insert") & (df["target"]=="pk")]
if not g1.empty:
    g1a = g1.groupby("primary")[["io_total","time_ms_exec"]].mean()
    ax = g1a.plot(kind="bar", rot=0, title="Inserción (import) por organización primaria")
    ax.set_ylabel("Promedio (io / ms)")
    plt.tight_layout(); plt.savefig("bench_out/chart_insert_by_primary.png", dpi=160); plt.close()

# 2) PK =
g2 = df[(df["op"]=="search_eq") & (df["target"]=="pk")]
if not g2.empty:
    g2a = g2.groupby("primary")[["io_total","time_ms_exec"]].mean()
    ax = g2a.plot(kind="bar", rot=0, title="Búsqueda puntual por PK")
    ax.set_ylabel("Promedio (io / ms)")
    plt.tight_layout(); plt.savefig("bench_out/chart_search_eq_pk.png", dpi=160); plt.close()

# 3) PK rango
g3 = df[(df["op"]=="search_range") & (df["target"]=="pk")]
if not g3.empty:
    g3a = g3.groupby("primary")[["io_total","time_ms_exec"]].mean()
    ax = g3a.plot(kind="bar", rot=0, title="Búsqueda por rango (PK)")
    ax.set_ylabel("Promedio (io / ms)")
    plt.tight_layout(); plt.savefig("bench_out/chart_search_range_pk.png", dpi=160); plt.close()

# 4) name = igualdad (secundarios)
g4 = df[(df["target"]=="name") & (df["op"]=="search_eq")]
if not g4.empty:
    g4a = g4.groupby(["primary","secondary"])[["io_total","time_ms_exec"]].mean().unstack("secondary")
    ax = g4a["io_total"].plot(kind="bar", rot=0, title="Secundarios en name (igualdad): IO por método")
    ax.set_ylabel("IO total (promedio)")
    plt.tight_layout(); plt.savefig("bench_out/chart_sec_name_eq_io.png", dpi=160); plt.close()
    ax = g4a["time_ms_exec"].plot(kind="bar", rot=0, title="Secundarios en name (igualdad): Tiempo por método")
    ax.set_ylabel("Tiempo exec (ms, promedio)")
    plt.tight_layout(); plt.savefig("bench_out/chart_sec_name_eq_time.png", dpi=160); plt.close()

# 5) price = rango (secundarios)
g5 = df[(df["target"]=="price") & (df["op"]=="search_range")]
if not g5.empty:
    g5a = g5.groupby(["primary","secondary"])[["io_total","time_ms_exec"]].mean().unstack("secondary")
    ax = g5a["io_total"].plot(kind="bar", rot=0, title="Secundarios en price (rango): IO por método")
    ax.set_ylabel("IO total (promedio)")
    plt.tight_layout(); plt.savefig("bench_out/chart_sec_price_range_io.png", dpi=160); plt.close()
    ax = g5a["time_ms_exec"].plot(kind="bar", rot=0, title="Secundarios en price (rango): Tiempo por método")
    ax.set_ylabel("Tiempo exec (ms, promedio)")
    plt.tight_layout(); plt.savefig("bench_out/chart_sec_price_range_time.png", dpi=160); plt.close()

# 6) coords = KNN (R-Tree)
g6 = df[(df["target"]=="coords") & (df["op"]=="knn")]
if not g6.empty:
    g6a = g6.groupby("primary")[["io_total","time_ms_exec"]].mean()
    ax = g6a.plot(kind="bar", rot=0, title="Espacial (R-Tree): kNN sobre coords")
    ax.set_ylabel("Promedio (io / ms)")
    plt.tight_layout(); plt.savefig("bench_out/chart_rtree_knn.png", dpi=160); plt.close()

print("Charts saved into bench_out/*.png")
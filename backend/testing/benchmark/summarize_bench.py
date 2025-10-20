# summarize_bench_PLUS.py
# Informe descriptivo con separación IO de datos vs índices y uso del plan.
# Escribe: bench_out/bench_summary_PLUS.md

import sys, pathlib
import pandas as pd
from datetime import datetime

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

# Normaliza columnas
for c in ("reads","writes","time_ms_exec","io_total","io_data_reads","io_data_writes","io_index_reads","io_index_writes",
          "idx_used_primary","idx_used_secondary","index_usage"):
    if c not in df.columns: df[c] = 0

df["io_total"]      = df.get("io_total", df.get("reads",0)+df.get("writes",0))
df["io_data_total"] = df.get("io_data_reads",0) + df.get("io_data_writes",0)
df["io_index_total"]= df.get("io_index_reads",0) + df.get("io_index_writes",0)

def q90(s):
    try: return float(s.quantile(0.90))
    except: return float("nan")

def md_table(frame, cols):
    if frame.empty: return "_(sin datos)_"
    try:
        return frame[cols].to_markdown(index=False)
    except Exception:
        return "```\n" + frame[cols].to_string(index=False) + "\n```"

def agg(frame, by, sort_col):
    if frame.empty: return frame
    g = (frame.groupby(by, dropna=False)
        .agg(
            runs=("time_ms_exec","size"),
            time_mean=("time_ms_exec","mean"),
            time_median=("time_ms_exec","median"),
            time_p90=("time_ms_exec", q90),
            time_std=("time_ms_exec","std"),
            io_total=("io_total","mean"),
            io_data=("io_data_total","mean"),
            io_index=("io_index_total","mean"),
            used_secondary_ratio=("idx_used_secondary", "mean"),
        )
        .reset_index()
        .sort_values(sort_col, ascending=True))
    return g

lines=[]
lines.append(f"# Resumen PLUS de Benchmarks\n")
lines.append(f"- Archivo: `{csv_path.name}`")
lines.append(f"- Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
lines.append(f"- Filas: {len(df)}")

# 1) Inserción
s = df[(df["op"]=="insert") & (df["target"]=="pk")]
lines.append("\n\n## Inserción (import) por organización primaria")
if s.empty:
    lines.append("_(sin datos de inserción)_")
else:
    t = agg(s, ["primary"], "time_mean")
    lines.append(md_table(t, ["primary","runs","time_mean","io_total","io_data","io_index"]))
    if not s["index_usage"].eq(0).all():
        lines.append("\n**Ejemplos de index_usage (import)**")
        lines.append("```\n" + "\n".join(s["index_usage"].astype(str).head(5).tolist()) + "\n```")

# 2) PK = igualdad
s = df[(df["op"]=="search_eq") & (df["target"]=="pk")]
lines.append("\n\n## Búsqueda puntual por PK")
if s.empty:
    lines.append("_(sin datos)_")
else:
    t = agg(s, ["primary"], "time_mean")
    lines.append(md_table(t, ["primary","runs","time_mean","time_p90","io_total","io_data","io_index"]))
    lines.append("\n**Uso de índice secundario (debería ser 0 aquí):**")
    lines.append(md_table(agg(s, ["primary"], "used_secondary_ratio"), ["primary","used_secondary_ratio"]))

# 3) PK = rango
s = df[(df["op"]=="search_range") & (df["target"]=="pk")]
lines.append("\n\n## Búsqueda por rango (PK)")
if s.empty:
    lines.append("_(sin datos)_")
else:
    t = agg(s, ["primary"], "time_mean")
    lines.append(md_table(t, ["primary","runs","time_mean","time_p90","io_total","io_data","io_index"]))

# 4) name = igualdad (secundarios)
s = df[(df["target"]=="name") & (df["op"]=="search_eq")]
lines.append("\n\n## Secundarios en `name` (igualdad)")
if s.empty:
    lines.append("_(sin datos)_")
else:
    lines.append("**Por primary y método**")
    t = agg(s, ["primary","secondary"], "time_mean")
    lines.append(md_table(t, ["primary","secondary","runs","time_mean","io_total","io_data","io_index","used_secondary_ratio"]))
    lines.append("\n**Agregado por método (independiente del primario)**")
    t2 = agg(s, ["secondary"], "time_mean")
    lines.append(md_table(t2, ["secondary","runs","time_mean","time_p90","io_total","io_data","io_index","used_secondary_ratio"]))

# 5) price = rango (secundarios)
s = df[(df["target"]=="price") & (df["op"]=="search_range")]
lines.append("\n\n## Secundarios en `price` (rango)")
if s.empty:
    lines.append("_(sin datos)_")
else:
    lines.append("**Por primary y método**")
    t = agg(s, ["primary","secondary"], "time_mean")
    lines.append(md_table(t, ["primary","secondary","runs","time_mean","time_p90","io_total","io_data","io_index","used_secondary_ratio"]))
    lines.append("\n**Agregado por método**")
    t2 = agg(s, ["secondary"], "time_mean")
    lines.append(md_table(t2, ["secondary","runs","time_mean","time_p90","io_total","io_data","io_index","used_secondary_ratio"]))

# 6) Muestra de planes usados
ls = df.loc[(df["index_usage"] != 0) & (df["index_usage"].astype(str) != ""), "index_usage"].astype(str)
if not ls.empty:
    lines.append("\n\n## Muestra de `index_usage`")
    lines.append("```\n" + "\n".join(ls.head(12).tolist()) + "\n```")

# Guardar
outdir = pathlib.Path("bench_out"); outdir.mkdir(exist_ok=True)
md_path = outdir / "bench_summary_PLUS.md"
with open(md_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f"OK. Escribí: {md_path}")

"""Render comparison sigilos for the same fictional bank dataset."""

from __future__ import annotations

import sys
from pathlib import Path

from atlas.sigilo.builder import SigiloBuilder
from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.sigilo.style import get_style_params


def _render_variant(*, prefer_native: bool, layout: str) -> bytes:
    from render_fictional_bank_sigilo import build_result

    result = build_result()
    datamap = (
        DatamapSigiloBuilder(result)
        .set_style("network")
        .set_layout(layout)
    )
    nodes, edges, schema_names = datamap._collect()
    config = datamap._build_config(get_style_params(datamap._style), schema_names)
    builder = SigiloBuilder(result, config=config, prefer_native=prefer_native)
    return builder._render_from(
        nodes,
        edges,
        layout,
        {
            "force_iterations": datamap._force_iterations,
            "force_temperature": datamap._force_temperature,
            "force_cooling": datamap._force_cooling,
        },
    )


def _html(native_circular: str, native_force: str, fallback_circular: str, fallback_force: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Atlas Sigilo Comparison</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Courier New", monospace;
      background: #efe7d6;
      color: #1f2937;
    }}
    header {{
      padding: 18px 22px 12px;
      border-bottom: 1px solid #cbbda4;
      background: rgba(255, 252, 246, 0.9);
      position: sticky;
      top: 0;
      z-index: 2;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 20px;
    }}
    p {{
      margin: 0;
      font-size: 13px;
      line-height: 1.45;
      opacity: 0.85;
    }}
    main {{
      padding: 18px;
      display: grid;
      grid-template-columns: repeat(2, minmax(320px, 1fr));
      gap: 18px;
    }}
    section {{
      background: rgba(255, 252, 246, 0.88);
      border: 1px solid #cbbda4;
      border-radius: 10px;
      overflow: hidden;
      box-shadow: 0 10px 30px rgba(31, 41, 55, 0.08);
    }}
    .meta {{
      padding: 12px 14px;
      border-bottom: 1px solid #ddd2bd;
      background: #f8f2e6;
    }}
    .meta h2 {{
      margin: 0 0 4px;
      font-size: 15px;
    }}
    .meta div {{
      font-size: 12px;
      opacity: 0.72;
    }}
    object {{
      display: block;
      width: 100%;
      height: 880px;
      background: #f7f1e5;
    }}
    @media (max-width: 1100px) {{
      main {{ grid-template-columns: 1fr; }}
      object {{ height: 760px; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>Fictional Bank Sigilo Comparison</h1>
    <p>Same database, rendered with both engines and both layouts. This lets you compare the native C renderer against the Python fallback, and also compare circular versus force placement without changing the dataset.</p>
  </header>
  <main>
    <section>
      <div class="meta">
        <h2>Native C · Circular</h2>
        <div>{native_circular}</div>
      </div>
      <object data="{native_circular}" type="image/svg+xml"></object>
    </section>
    <section>
      <div class="meta">
        <h2>Native C · Force</h2>
        <div>{native_force}</div>
      </div>
      <object data="{native_force}" type="image/svg+xml"></object>
    </section>
    <section>
      <div class="meta">
        <h2>Python Fallback · Circular</h2>
        <div>{fallback_circular}</div>
      </div>
      <object data="{fallback_circular}" type="image/svg+xml"></object>
    </section>
    <section>
      <div class="meta">
        <h2>Python Fallback · Force</h2>
        <div>{fallback_force}</div>
      </div>
      <object data="{fallback_force}" type="image/svg+xml"></object>
    </section>
  </main>
</body>
</html>
"""


def render_all(output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    native_circular = output_dir / "fictional_bank.native.circular.system.svg"
    native_force = output_dir / "fictional_bank.native.force.system.svg"
    fallback_circular = output_dir / "fictional_bank.fallback.circular.system.svg"
    fallback_force = output_dir / "fictional_bank.fallback.force.system.svg"
    compare_html = output_dir / "fictional_bank.compare.html"

    native_circular.write_bytes(_render_variant(prefer_native=True, layout="circular"))
    native_force.write_bytes(_render_variant(prefer_native=True, layout="force"))
    fallback_circular.write_bytes(_render_variant(prefer_native=False, layout="circular"))
    fallback_force.write_bytes(_render_variant(prefer_native=False, layout="force"))

    compare_html.write_text(
        _html(
            native_circular.name,
            native_force.name,
            fallback_circular.name,
            fallback_force.name,
        ),
        encoding="utf-8",
    )
    return [
        native_circular,
        native_force,
        fallback_circular,
        fallback_force,
        compare_html,
    ]


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    target_dir = Path(args[0]) if args else Path(__file__).with_name("sigilo_compare")
    rendered = render_all(target_dir)
    for path in rendered:
        print(f"[atlas] wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import pandas as pd
from pathlib import Path
import json

analyzer = Path('docs/assessment/infa-analyzer-report-25072025_130600.xlsx')
summary = Path('docs/assessment/infa-summary-report-25072025_130618.xlsx')

out_dir = Path('docs/assessment/_parsed')
out_dir.mkdir(parents=True, exist_ok=True)

def dump_book(xlsx: Path, prefix: str):
    xls = pd.ExcelFile(xlsx)
    meta = {}
    for name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, name)
        except Exception:
            continue
        df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
        csv_path = out_dir / f"{prefix}_{name.replace('/', '_')}.csv"
        df.to_csv(csv_path, index=False)
        meta[name] = {
            'rows': int(df.shape[0]),
            'cols': int(df.shape[1]),
            'path': str(csv_path)
        }
    (out_dir / f'{prefix}__meta.json').write_text(json.dumps(meta, indent=2))

dump_book(analyzer, 'analyzer')
dump_book(summary, 'summary')
print('Parsed to', out_dir)

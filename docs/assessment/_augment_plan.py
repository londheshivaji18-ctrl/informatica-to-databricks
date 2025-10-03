import pandas as pd
from pathlib import Path
from collections import Counter

md_path = Path('docs/assessment/workflows_migration_plan.md')
text = md_path.read_text(encoding='utf-8')
# We will recompute categories using parsed metrics
p = Path('docs/assessment/_parsed')
wfd = pd.read_csv(p/'analyzer_Workflow Details.csv')
wt = pd.read_csv(p/'analyzer_Workflow Task Details.csv')
ws = pd.read_csv(p/'analyzer_Workflow Session Details.csv')
map_comp = pd.read_csv(p/'analyzer_Mapping Component Details.csv')

for df in [wfd, wt, ws, map_comp]:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

wf_sessions = ws[['Subject Area','Workflow Name','Session Name','Mapping Name']].dropna().drop_duplicates()
wf_mapnames = wf_sessions.groupby(['Subject Area','Workflow Name'])['Mapping Name'].apply(lambda s: sorted(set(s))).to_dict()
map_comp_by_map = {m: g.copy() for m, g in map_comp.groupby(['Subject Area','Mapping Name'])}

rows = []
for (sa,wf), maps in wf_mapnames.items():
    counts = Counter()
    for m in maps:
        dfm = map_comp_by_map.get((sa,m))
        if dfm is None:
            key_any = next((k for k in map_comp_by_map.keys() if k[1]==m), None)
            if key_any: dfm = map_comp_by_map[key_any]
        if dfm is None: continue
        ct = dfm['Component Type'].fillna('')
        counts['Unconnected Lookup'] += int(ct.str.contains('Unconnected Lookup', na=False).sum())
        counts['Connected Lookup'] += int(ct.str.contains('Connected Lookup', na=False).sum())
        counts['Java'] += int(ct.str.contains('Java', na=False).sum())
        counts['XML'] += int((ct.str.contains('XML', na=False) | dfm['Property Type'].astype(str).str.contains('XML', na=False)).sum())
        # Dynamic SQL
        dyn = dfm[(dfm['Component Type'].astype(str).str.contains('Source Qualifier', na=False)) & (dfm['Property Type'].isin(['Sql Query','User Defined Join','Source Filter']))]
        counts['Dynamic SQL'] += int((dyn['Property Value'].astype(str).str.len()>0).sum())
    manual = 0
    for k in ['Unconnected Lookup','Java','XML','Dynamic SQL']:
        if counts[k]>0: manual += 1
    category = 'Mostly Automatable' if manual==0 else ('Hybrid (some manual redesign)' if manual<=2 else 'Manual-leaning (significant redesign)')
    rows.append((sa, wf, category, counts['Unconnected Lookup'], counts['Java'], counts['XML'], counts['Dynamic SQL']))

# Build summary header
lines = ['# Workflow Migration Plan', 'Generated from analyzer/summary reports. Tags: #pattern #antipattern #tradeoff', '', '## Categorization Summary']
lines.append('- Category legend: Mostly Automatable; Hybrid (some manual redesign); Manual-leaning (significant redesign)')
for sa,wf,cat,ucl,jv,xml,dsql in rows:
    lines.append(f"- {wf} ({sa}) ? {cat} [Unconnected Lookup={ucl}, Java={jv}, XML={xml}, DynamicSQL={dsql}]")

# Append existing content after the summary
# Remove the existing header lines to avoid duplication
old = text.splitlines()
# Find first workflow section index
start_idx = 0
for i,l in enumerate(old):
    if l.startswith('## '):
        start_idx = i
        break
combined = '\n'.join(lines + [''] + old[start_idx:])
md_path.write_text(combined, encoding='utf-8')
print('Prepended categorization summary to', md_path)

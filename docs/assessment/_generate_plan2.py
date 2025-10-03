import pandas as pd
from pathlib import Path
from collections import Counter

p = Path('docs/assessment/_parsed')
wfd = pd.read_csv(p/'analyzer_Workflow Details.csv')
wt = pd.read_csv(p/'analyzer_Workflow Task Details.csv')
ws = pd.read_csv(p/'analyzer_Workflow Session Details.csv')
map_comp = pd.read_csv(p/'analyzer_Mapping Component Details.csv')
paramvars = pd.read_csv(p/'analyzer_Parameter and Variable Details.csv')

# Normalize
for df in [wfd, wt, ws, map_comp, paramvars]:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

wf_sessions = ws[['Subject Area','Workflow Name','Session Name','Mapping Name']].dropna().drop_duplicates()
wf_mapnames = wf_sessions.groupby(['Subject Area','Workflow Name'])['Mapping Name'].apply(lambda s: sorted(set(s))).to_dict()
wf_tasktypes = wt.groupby(['Subject Area','Workflow Name'])['TaskType'].apply(lambda s: sorted(set(s.dropna()))).to_dict()
wfd_pvt = wfd.pivot_table(index=['Subject Area','Workflow Name'], columns='Property Type', values='Property Value', aggfunc=lambda x: next((v for v in x if pd.notna(v) and str(v)!=''), ''))
wf_flags = wfd_pvt.reset_index()
map_comp_by_map = {m: g.copy() for m, g in map_comp.groupby(['Subject Area','Mapping Name'])}

FEATURES = ['Sorter','Expression','Joiner','Connected Lookup','Unconnected Lookup','Java','XML']

def assess_mapping(df):
    comp_types = df['Component Type'].fillna('')
    counts = Counter()
    for t in FEATURES:
        if t=='Java':
            counts['Java'] = int(comp_types.str.contains('Java', na=False).sum())
        elif t=='XML':
            counts['XML'] = int((comp_types.str.contains('XML', na=False) | df['Property Type'].str.contains('XML', na=False)).sum())
        else:
            counts[t] = int(comp_types.str.contains(t, na=False).sum())
    dyn = df[(df['Component Type'].str.contains('Source Qualifier', na=False)) & (df['Property Type'].isin(['Sql Query','User Defined Join','Source Filter']))]
    counts['Dynamic SQL'] = int((dyn['Property Value'].astype(str).str.len()>0).sum())
    return counts

map_param_counts = paramvars.groupby(['Entity Type','Entity Name']).size().to_dict() if not paramvars.empty else {}

assess = []
for (sa,wf), _ in wf_flags.set_index(['Subject Area','Workflow Name']).iterrows():
    maps = wf_mapnames.get((sa,wf), [])
    ttypes = wf_tasktypes.get((sa,wf), [])
    flagrow = wf_flags[(wf_flags['Subject Area']==sa) & (wf_flags['Workflow Name']==wf)]
    agg_counts = Counter()
    dyn_sql_maps = []
    param_count = 0
    for m in maps:
        dfm = map_comp_by_map.get((sa,m))
        if dfm is None:
            key_any = next((k for k in map_comp_by_map.keys() if k[1]==m), None)
            if key_any:
                dfm = map_comp_by_map[key_any]
        if dfm is not None:
            c = assess_mapping(dfm)
            agg_counts.update(c)
            if c.get('Dynamic SQL',0)>0:
                dyn_sql_maps.append(m)
        param_count += map_param_counts.get(('Mapping', m), 0)
    websvc = str(flagrow['Web Services'].values[0]) if 'Web Services' in flagrow.columns else ''
    assess.append({
        'Subject Area': sa,
        'Workflow Name': wf,
        'Task Types': ttypes,
        'Mappings': maps,
        'Counts': dict(agg_counts),
        'Dynamic SQL Maps': dyn_sql_maps,
        'Param Count': int(param_count),
        'Web Services': websvc,
        'Suspend On Error': str(flagrow['Suspend On Error'].values[0]) if 'Suspend On Error' in flagrow.columns else ''
    })

out = []
out.append('# Workflow Migration Plan')
out.append('Generated from analyzer/summary reports. Tags: #pattern #antipattern #tradeoff')
for a in assess:
    sa = a['Subject Area']; wf = a['Workflow Name']
    out.append(f"\n## {wf} ({sa})")
    out.append('### Overview')
    out.append(f"- Task Types: {', '.join(a['Task Types']) or 'n/a'}")
    out.append(f"- Sessions/Maps: {len(a['Mappings'])} ? {', '.join(a['Mappings']) or 'n/a'}")
    c = a['Counts']
    out.append('### Patterns #pattern')
    auto = []
    if c.get('Expression',0)>0: auto.append(f"Expressions: {c.get('Expression',0)}")
    if c.get('Joiner',0)>0: auto.append(f"Joins: {c.get('Joiner',0)}")
    if c.get('Sorter',0)>0: auto.append(f"Sorters: {c.get('Sorter',0)} (review necessity)")
    out.append('- Mostly automatable: ' + (', '.join(auto) if auto else 'General projections/filters/aggregations'))
    out.append('### Antipatterns #antipattern')
    anti = []
    if c.get('Unconnected Lookup',0)>0: anti.append(f"Unconnected Lookups: {c['Unconnected Lookup']}")
    if c.get('Connected Lookup',0)>0: anti.append(f"Connected Lookups: {c['Connected Lookup']} (optimize via broadcast, caching)")
    if c.get('Java',0)>0: anti.append(f"Java transformations: {c['Java']}")
    if c.get('XML',0)>0: anti.append(f"XML processing refs: {c['XML']}")
    if c.get('Dynamic SQL',0)>0: anti.append(f"Dynamic SQL in SQ (maps: {', '.join(a['Dynamic SQL Maps'])})")
    if a['Web Services']=='YES': anti.append('Workflow uses Web Services features')
    out.append('- ' + ('; '.join(anti) if anti else 'None observed from reports'))
    out.append('### Approach')
    bullets = []
    bullets.append('Ingestion: land sources to ADLS (Auto Loader for files; JDBC for RDBMS) and register Delta bronze tables.')
    bullets.append('Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.')
    if c.get('Connected Lookup',0)>0:
        bullets.append('Lookups: replace with joins; broadcast small dims; maintain surrogate keys via window functions.')
    if c.get('Unconnected Lookup',0)>0:
        bullets.append('Unconnected lookups: refactor to reusable join helpers or precomputed reference tables; avoid per-row UDF calls.')
    if c.get('Sorter',0)>0:
        bullets.append('Sorters: remove redundant sorts; use orderBy only when determinism is required.')
    if c.get('Java',0)>0:
        bullets.append('Java transforms: port to PySpark/Scala UDFs or external services; validate performance and vectorize.')
    if c.get('XML',0)>0:
        bullets.append('XML: ingest via spark-xml with explicit schema; avoid row-wise parsing; flatten to normalized columns.')
    if c.get('Dynamic SQL',0)>0:
        bullets.append('Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string concatenation.')
    if a['Param Count']>0:
        bullets.append('Parameterization: manage with Databricks Jobs params, widgets, and secret scopes; maintain per-env config files.')
    bullets.append('Storage: write curated datasets as Delta; partition by date/keys; enforce expectations (DQ checks).')
    bullets.append('Orchestration: Databricks Workflows/ADF; implement retries, SLAs, lineage capture.')
    for b in bullets:
        out.append(f"- {b}")
    out.append('### Performance #tradeoff')
    t = []
    if c.get('Joiner',0)>0 or c.get('Connected Lookup',0)>0:
        t.append('Joins: enable AQE, broadcast hints; check skew and salt where needed.')
    if c.get('Sorter',0)>0:
        t.append('Avoid full dataset sorts; leverage partitioning and window order.')
    t.append('Optimize Delta (OPTIMIZE/ZORDER); set Auto Optimize and compaction for small files.')
    out += [f"- {x}" for x in t]

md_path = Path('docs/assessment/workflows_migration_plan.md')
out = '\n'.join(out)
md_path.write_text(out, encoding='utf-8')
print('Wrote', md_path)

import pandas as pd
from pathlib import Path
from collections import defaultdict, Counter

p = Path('docs/assessment/_parsed')
# Load dfs
wfd = pd.read_csv(p/'analyzer_Workflow Details.csv')
wt = pd.read_csv(p/'analyzer_Workflow Task Details.csv')
ws = pd.read_csv(p/'analyzer_Workflow Session Details.csv')
map_comp = pd.read_csv(p/'analyzer_Mapping Component Details.csv')
paramvars = pd.read_csv(p/'analyzer_Parameter and Variable Details.csv')

# Normalize strings
for df in [wfd, wt, ws, map_comp, paramvars]:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

# Build mapping usage per workflow
wf_sessions = ws[['Subject Area','Workflow Name','Session Name','Mapping Name']].dropna().drop_duplicates()
wf_mapnames = wf_sessions.groupby(['Subject Area','Workflow Name'])['Mapping Name'].apply(lambda s: sorted(set(s))).to_dict()

# Task types per workflow
wf_tasktypes = wt.groupby(['Subject Area','Workflow Name'])['TaskType'].apply(lambda s: sorted(set(s.dropna()))).to_dict()

# Workflow-level flags
wfd_pvt = wfd.pivot_table(index=['Subject Area','Workflow Name'], columns='Property Type', values='Property Value', aggfunc=lambda x: next((v for v in x if pd.notna(v) and str(v)!=''), ''))
wf_flags = wfd_pvt.reset_index()

# Precompute mapping component index
# Map mapping name -> component rows
map_comp_by_map = {m: g.copy() for m, g in map_comp.groupby(['Subject Area','Mapping Name'])}

# Helper to assess mapping characteristics
FEATURES = ['Sorter','Expression','Joiner','Connected Lookup','Unconnected Lookup','Java','XML']

def assess_mapping(df):
    comp_types = df['Component Type'].fillna('')
    counts = Counter()
    for t in FEATURES:
        if t in ['Java','XML']:
            if t=='Java':
                counts['Java'] = int((comp_types.str.contains('Java', na=False)).sum())
            else:
                counts['XML'] = int((comp_types.str.contains('XML', na=False) | df['Property Type'].str.contains('XML', na=False)).sum())
        else:
            counts[t] = int((comp_types.str.contains(t, na=False)).sum())
    # Dynamic SQL indicators in Source Qualifier properties
    dyn = df[(df['Component Type'].str.contains('Source Qualifier', na=False)) & (df['Property Type'].isin(['Sql Query','User Defined Join','Source Filter']))]
    dyn_nonempty = int((dyn['Property Value'].astype(str).str.len()>0).sum())
    counts['Dynamic SQL'] = dyn_nonempty
    return counts

# Parameterization indicators per workflow
# Count parameters referenced that include $$ or $Param
param_usage = defaultdict(int)
if not paramvars.empty:
    for (sa,wf), g in paramvars.groupby(['Subject Area','Workflow Name']):
        # any reference in value/name columns
        txt = '\n'.join(g.apply(lambda r: ' '.join(map(str, r.values)), axis=1).astype(str))
        param_usage[(sa,wf)] = txt.count('$$') + txt.count('$PM') + txt.count('$$')

# Build per-workflow assessment structures
assess = []
for (sa,wf), _ in wf_flags.set_index(['Subject Area','Workflow Name']).iterrows():
    maps = wf_mapnames.get((sa,wf), [])
    ttypes = wf_tasktypes.get((sa,wf), [])
    flagrow = wf_flags[(wf_flags['Subject Area']==sa) & (wf_flags['Workflow Name']==wf)]
    ws_enabled = flagrow.get('Workflow Enabled')
    ws_websvc = flagrow.get('Web Services')
    suspend_err = flagrow.get('Suspend On Error')
    # Aggregate mapping metrics
    agg_counts = Counter()
    dyn_sql_maps = []
    for m in maps:
        dfm = map_comp_by_map.get((sa,m))
        if dfm is None:
            # mapping may appear without SA match; try any SA
            key_any = next((k for k in map_comp_by_map.keys() if k[1]==m), None)
            if key_any:
                dfm = map_comp_by_map[key_any]
        if dfm is None:
            continue
        c = assess_mapping(dfm)
        agg_counts.update(c)
        if c.get('Dynamic SQL',0)>0:
            dyn_sql_maps.append(m)
    assess.append({
        'Subject Area': sa,
        'Workflow Name': wf,
        'Task Types': ttypes,
        'Mappings': maps,
        'Counts': dict(agg_counts),
        'Dynamic SQL Maps': dyn_sql_maps,
        'Param Signals': param_usage.get((sa,wf),0),
        'Web Services': str(flagrow['Web Services'].values[0]) if 'Web Services' in flagrow.columns else '',
        'Suspend On Error': str(flagrow['Suspend On Error'].values[0]) if 'Suspend On Error' in flagrow.columns else ''
    })

# Render markdown
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
    out.append('- ' + ('; '.join(anti) if anti else 'None observed from reports'))
    out.append('### Approach')
    bullets = []
    bullets.append('Ingestion: land sources to ADLS (Auto Loader if files; JDBC for RDBMS) and register Delta bronze tables.')
    bullets.append('Transform: reimplement mappings in PySpark DataFrames or Spark SQL; modularize by business domains.')
    if c.get('Connected Lookup',0)>0:
        bullets.append('Lookups: replace with joins; broadcast small dims; maintain surrogate keys via window functions.')
    if c.get('Unconnected Lookup',0)>0:
        bullets.append('Unconnected lookups: refactor to reusable join UDFs or precomputed reference tables; avoid per-row UDF calls.')
    if c.get('Sorter',0)>0:
        bullets.append('Sorters: remove redundant sorts; use orderBy only when determinism is required.')
    if c.get('Java',0)>0:
        bullets.append('Java transforms: port to PySpark/Scala UDFs or external services; validate performance and vectorize where possible.')
    if c.get('XML',0)>0:
        bullets.append('XML: ingest via spark-xml with explicit schema; avoid row-wise parsing; flatten into normalized columns.')
    if c.get('Dynamic SQL',0)>0:
        bullets.append('Dynamic SQL: use parameterized Spark SQL with validated templates; avoid string-concatenation from untrusted inputs.')
    if a['Param Signals']>0:
        bullets.append('Parameterization: centralize in Databricks Jobs with task parameters; secrets in Azure Key Vault/Databricks scopes.')
    bullets.append('Storage: write curated datasets as Delta; partition on natural keys/dates; enforce constraints via expectations.')
    bullets.append('Orchestration: Databricks Workflows/ADF; implement retries, SLAs, and data quality checkpoints.')
    for b in bullets:
        out.append(f"- {b}")
    out.append('### Performance #tradeoff')
    t = []
    if c.get('Joiner',0)>0 or c.get('Connected Lookup',0)>0:
        t.append('Joins: enable AQE, broadcast hints; check skew via salting.')
    if c.get('Sorter',0)>0:
        t.append('Avoid full dataset sorts; rely on partitioning and window order as needed.')
    t.append('Optimize Delta (OPTIMIZE/ZORDER) and use Auto Optimize for small file problem.')
    out += [f"- {x}" for x in t]

md_path = Path('docs/assessment/workflows_migration_plan.md')
md_path.write_text('\n'.join(out), encoding='utf-8')
print('Wrote', md_path)

import pandas as pd
from pathlib import Path
p = Path('docs/assessment/_parsed')

wfd = pd.read_csv(p/'analyzer_Workflow Details.csv')
wt = pd.read_csv(p/'analyzer_Workflow Task Details.csv')
ws = pd.read_csv(p/'analyzer_Workflow Session Details.csv')
map_comp = pd.read_csv(p/'analyzer_Mapping Component Details.csv')
sqlc = pd.read_csv(p/'summary_SQL Constructs.csv')

print('WF count', wfd['Workflow Name'].nunique())
print('Workflows sample', wfd['Workflow Name'].dropna().unique()[:5])
print('\nTask types per WF (top 3):')
print(wt.groupby('Workflow Name')['TaskType'].apply(lambda s: sorted(set(s.dropna()))).head(3))
print('\nSessions per WF (first 3):')
ws2 = ws[['Workflow Name','Session Name','Mapping Name']].dropna().drop_duplicates()
print(ws2.groupby('Workflow Name').size().head(3))
print('\nComponent type counts (top 8):')
ct = map_comp['Component Type'].dropna().value_counts().head(8)
print(ct.to_string())

has_cmd = wt[wt['TaskType'].astype(str).str.contains('Command', na=False)]['Workflow Name'].unique()
has_assign = wt[wt['TaskType'].astype(str).str.contains('Assignment', na=False)]['Workflow Name'].unique()
has_decision = wt[wt['TaskType'].astype(str).str.contains('Decision', na=False)]['Workflow Name'].unique()
print('\nCounts: command', len(has_cmd),'assignment', len(has_assign),'decision', len(has_decision))

lk = map_comp[map_comp['Component Type'].astype(str).str.contains('Lookup', na=False)]
print('Lookup rows', len(lk))

java = map_comp[map_comp['Component Type'].astype(str).str.contains('Java', na=False)]
xml = map_comp[(map_comp['Property Type'].astype(str).str.contains('XML', na=False)) | (map_comp['Component Type'].astype(str).str.contains('XML', na=False))]
print('Java comps', len(java), 'XML refs', len(xml))

http = map_comp[map_comp['Property Values'].astype(str).str.contains('http', case=False, na=False)]
ws_flags = wfd[wfd['Property Type'].astype(str).str.contains('Web Services', na=False)]
print('HTTP refs', len(http), 'WebServices YES', int((ws_flags['Property Value'].astype(str)=='YES').sum()))

sorters = map_comp[map_comp['Component Type'].astype(str).str.contains('Sorter', na=False)]
exprs = map_comp[map_comp['Component Type'].astype(str).str.contains('Expression', na=False)]
print('Sorter rows', len(sorters), 'Expression comps', exprs['Component Name'].nunique())

print('\nSQL constructs nonzero rows:', (sqlc.applymap(lambda x: isinstance(x,(int,float)) and x!=0).any(axis=1)).sum())

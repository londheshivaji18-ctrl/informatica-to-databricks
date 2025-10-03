import pandas as pd
from pathlib import Path
p = Path('docs/assessment/_parsed')
for name in ['analyzer_Workflow Details.csv','analyzer_Workflow Task Details.csv','analyzer_Workflow Session Details.csv','summary_Workflows.csv','summary_Workflow Complexity.csv','summary_SQL Constructs.csv','analyzer_Pattern Details.csv','analyzer_Source Target Details (Session).csv']:
    print('\n===', name)
    df = pd.read_csv(p/name)
    print(df.head(5).to_string(index=False))

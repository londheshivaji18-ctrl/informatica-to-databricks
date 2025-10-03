import pandas as pd
from pathlib import Path
p = Path('docs/assessment/_parsed')
paramvars = pd.read_csv(p/'analyzer_Parameter and Variable Details.csv')
print(list(paramvars.columns))
print(paramvars.head(5).to_string(index=False))

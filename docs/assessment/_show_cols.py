import pandas as pd
from pathlib import Path
p = Path('docs/assessment/_parsed')
map_comp = pd.read_csv(p/'analyzer_Mapping Component Details.csv')
print(list(map_comp.columns))
print(map_comp.head(5).to_string(index=False))

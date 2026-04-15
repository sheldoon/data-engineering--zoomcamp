import sys
import pandas as pd

df = pd.DataFrame({
    'A': [1, 2, 3],
    'B': [4, 5, 6]
})
C = sys.argv[1]
df['C'] = C
print(df.head())
df.to_parquet(f'output_{C}.parquet')





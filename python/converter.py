import pandas as pd

df = pd.read_csv("input.txt",sep='\s+',header = None)
df.to_csv('input.csv',header=None)
from Functions.document_extraction import *

df = sql.select('applications')

regex_test_1 = df["name"].str.endswith("son")
print(df.loc[regex_test_1])

import json
import pandas as pd
# import xlwt

jsf =''
with open('jsfile.txt') as f:
    jsf = json.load(f)

df = pd.DataFrame(jsf)
print(f"DF length = {len(df.index)}")
new_df = df.drop_duplicates(['id'])
# df.to_excel("result.xls", index=False)
print(f"NEW DF length = {len(new_df.index)}")
with pd.ExcelWriter("path_to_file.xlsx") as writer:
    new_df.to_excel(writer)
print(len(jsf))



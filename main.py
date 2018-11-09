import csv
import numpy as np
import pandas as pd 
df = pd.DataFrame(np.genfromtxt("query_result.csv", dtype=str)[1:], columns=["user_id","timestamp","part","complete","answer", "metadata", "activity_type","url"])
keep_col = ["user_id","timestamp","part","complete","activity_type","url"]

print(df)

# new_f = f[keep_col]
# new_f.to_csv("query_resultNEW.csv", index=False)



# new_f = df[keep_col]
# new_f.to_csv("queryNEW.csv", index=False)


# # with open("query_result.csv", "rt",  encoding='utf8') as source:
#     rdr= csv.reader( source)
#     with open("result","wb") as result:
#         wtr= csv.writer( result )
#     # with open(source, 'rb') as f:
#     #     lines = [x.decode('utf8').strip() for x in f.readlines()]
#         for r in rdr:
#             wtr.writerow( (r[0], r[1], r[2], r[3],  r[6], r[7]) )

# import pandas as pd
# f=pd.read_csv("query_result.csv")
# keep_col = ["user_id","timestamp","part","complete","activity_type","url"]
# new_f = f[keep_col]
# new_f.to_csv("query_resultNEW.csv", index=False)
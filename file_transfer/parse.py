import sys
import pandas as pd

with open(sys.argv[1], "r") as f:
    sending = []
    for line in f.readlines():
        if "ssh" in line:
            continue
        elif "sending" in line:
            sending.append(line.strip().split(" ")[-1])

df = pd.DataFrame({'time': sending})
df = df.astype('float64')
print(df.mean()/30)

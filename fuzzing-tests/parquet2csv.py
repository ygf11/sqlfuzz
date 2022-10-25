import io
import os
import subprocess
from pathlib import Path
from wsgiref import headers

import numpy as np
import pandas as pd

for file in os.listdir("testdata"):
    if file.startswith("test"):
        file_name = Path(file).stem
        print("{} {} {}".format("testdata/" + file, file_name, "testdata/"+file_name+".csv"))
        df = pd.read_parquet('testdata/{}.parquet'.format(file_name), use_nullable_dtypes = True)
        print(df.dtypes)
        df.to_csv('testdata/{}.csv'.format(file_name), index = False)


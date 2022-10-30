import io
import os
import subprocess
from pathlib import Path
from wsgiref import headers

import numpy as np
import pandas as pd

df = pd.read_parquet('testdata/test0.parquet')
df.to_csv('testdata/test0.csv')
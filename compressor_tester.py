import pandas as pd
from compressor import DataCompress

pff_pass = pd.read_csv("/Users/cookedkaledev/Downloads/passing_summary.csv")

print(sum(pff_pass.memory_usage()))

DataCompress(pff_pass, category_percent = 0.35, verbose = 1,
             pickle_name = "pff_pass.pickle", zip_name = "pff_pass.zip")

print(sum(pff_pass.memory_usage()))

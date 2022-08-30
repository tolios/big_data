#pip install numpy
#pip install matplotlib

import numpy as np
import matplotlib.pyplot as plt

queries = ['Query 1','Query 2','Query 3', 'Query 4', 'Query 5', 'Query 6']
rdd = [61.6654, 64.7469, 66.9189, 82.1096, 126.111, 74.1933]
sql_csv = [77.3692, 89.0526, 111.7683, 131.7757, 187.0414, 187.3071]
sql_parquet = [19.8447, 31.0392, 26.843, 53.2964, 49.9423, 43.4202]

x_axis = np.arange(len(queries))

plt.bar(x_axis -0.3, rdd, width=0.3, label = 'rdd')
plt.bar(x_axis, sql_csv, width=0.3, label = 'sql_csv')
plt.bar(x_axis +0.3, sql_parquet, width=0.3, label = 'sql_parquet')

plt.xticks(x_axis, queries)
plt.ylabel("Time (s)")
plt.title("Time needed for each method, for each query to run")
plt.legend()

plt.show()

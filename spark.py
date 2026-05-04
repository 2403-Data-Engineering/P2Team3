import os
import sys
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = r"D:\hadoop-3.5.5-bin"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = f'"{sys.executable}"'
os.environ["PYSPARK_DRIVER_PYTHON"] = f'"{sys.executable}"'
from pyspark.sql.functions import explode, split





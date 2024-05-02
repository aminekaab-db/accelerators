# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
from faker import Faker
from collections import OrderedDict 
import uuid
import pandas as pd
import numpy as np
import random
import math
import requests
import json
from datetime import datetime, timedelta, time
fake = Faker()

# COMMAND ----------

dbutils_sp = dbutils.notebook.entry_point.getDbutils()
host = dbutils_sp.notebook().getContext().apiUrl().getOrElse(None)
databricks_token = dbutils_sp.notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------



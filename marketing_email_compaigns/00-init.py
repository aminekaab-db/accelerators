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
from datetime import datetime, timedelta, time
fake = Faker()

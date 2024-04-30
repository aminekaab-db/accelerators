# Databricks notebook source
# MAGIC %run ./00-init

# COMMAND ----------

dbutils.widgets.text("schema_path", "main.emailmarketing", "Schema Path")
schema_path = dbutils.widgets.get('schema_path')

# COMMAND ----------

def random_choice(choices):
  mean_index = len(choices) / 4
  std_dev = len(choices) / 4
  weights = [math.ceil(random.lognormvariate(mean_index, std_dev)) for _ in range(len(choices))]
  return random.choices(choices, weights)[0]

# COMMAND ----------

industries = [
    "Advertising", "Agriculture", "Automotive", "Banking", "Biotechnology", 
    "Chemical", "Construction", "Consulting", "Consumer Goods", "Education"
]

departments = [
    "Human Resources", "Finance", "Marketing", "Sales", "Operations", "Customer Service",
    "Information Technology", "Research and Development", "Product Management", "Legal"
]

devices = [
   "Mobile", "Desktop", "Tablet", "Smart TV", "Gaming Console", "Laptop", "E-reader"
]

countries = [fake.country_code() for _ in range(5)]
cities = [fake.city() for _ in range(5)]
postcodes = [fake.postcode() for _ in range(5)]


# COMMAND ----------

choices = industries
mean_index = len(choices)
std_dev = len(choices) / 2
weights = [random.lognormvariate(mean_index, std_dev) for _ in range(len(choices))]
weights

# COMMAND ----------

choices = ["referral", "web", "market", "event"]
mean_index = len(choices) / 4
std_dev = len(choices) / 4
weights = [math.floor(random.lognormvariate(mean_index, std_dev)) for _ in range(len(choices))]
weights

# COMMAND ----------

n_prospects = 50
rows = []

for i in range(n_prospects):
    employees = round(random.uniform(100, 100000))
    rows.append({
        'prospect_id': 3234 + i,
        'name': fake.company(),
        'annual_revenue': round(math.log(employees) * random.uniform(10, 500), 2),  # USD Millions
        'employees': employees,
        'industry': random_choice(industries),
        'country': random_choice(countries),
        'city': random_choice(cities),
        'postcode': random_choice(postcodes),
    })

prospects_df = spark.createDataFrame(pd.DataFrame(rows))
display(prospects_df)

# COMMAND ----------

prospects = [(row['prospect_id'], row['employees']) for row in prospects_df.select('prospect_id', 'employees').collect()]

rows = []
for prospect in prospects:
    prospect_id, employees = prospect
    n_contacts = round(math.log(employees) * 10)
    for i in range(n_contacts):
        rows.append({
            'contact_id': int(f"{prospect_id}{i}"),
            'prospect_id': prospect_id,
            'department': random_choice(departments),
            'job_title': fake.job(),
            'source': random_choice(["referral", "web", "market", "event"]),
            'device': random_choice(devices),
            'opted_out': False,
        })

contacts_df = spark.createDataFrame(pd.DataFrame(rows))
display(contacts_df)

# COMMAND ----------

contacts = [row['contact_id'] for row in contacts_df.select('contact_id').collect()]

n_compaigns = 100
rows = []

for i in range(n_compaigns):
    k = round(random.uniform(10, len(contacts)/4))
    start_date = fake.date_between(start_date='-2y', end_date='today')
    end_date = start_date + timedelta(days=random.randint(10, 90))
    rows.append({
        'compaign_id': 100 + i,
        'campaign_name': fake.sentence(nb_words=4),
        'compaign_description': fake.sentence(nb_words=50),
        'subject_line': fake.sentence(nb_words=10),
        'template': fake.sentence(nb_words=1),
        'cost': round(k * 4.2, 2),
        'start_date': start_date, 
        'end_date': end_date,
        'mailing_list': random.sample(contacts, k=k),
    })

compaigns_df = spark.createDataFrame(pd.DataFrame(rows))
display(compaigns_df)

# COMMAND ----------

from dataclasses import dataclass, field
from typing import List 

@dataclass
class Event:
    compaign_id: int
    contact_id: int
    start: datetime
    delta: timedelta
    event_type: str
    metadata: dict = field(default_factory=dict)
    
def add_event(events: List, e: Event):
    events.append({
        'event_id': str(uuid.uuid4()),
        'compaign_id': e.compaign_id,
        'contact_id': e.contact_id,
        'event_type': e.event_type,
        'event_date': datetime.combine(e.start, e.delta),
        'metadata': e.metadata
    })

# COMMAND ----------

def generate_events(compaigns_list):
    events = []
    for compaign in compaigns_list:
        click_targets = [fake.url() for _ in range(random.randint(5, 20))]
        current_date = compaign['start_date']
        end_date = compaign['end_date']
        compaign_id = compaign['compaign_id']
        while current_date <= end_date:
            for contact in compaign['mailing_list']:
                e = Event(compaign_id, contact, current_date, time(0, random.randint(0, 5)), 'sent')
                add_event(events, e)

                is_delivered = random.choices([True, False], weights=[80, 20])[0]
                if is_delivered:
                    e = Event(compaign_id, contact, current_date, time(0, random.randint(10, 15)), 'delivered')
                    add_event(events, e)

                    is_spam = random.choices([True, False], weights=[4, 96])[0]
                    if is_spam:
                        e = Event(compaign_id, contact, current_date, time(0, random.randint(15, 20)), 'spam')
                        add_event(events, e)
                    else:
                        opened = random.choices([True, False], weights=[80, 20])[0]
                        if opened:
                            e = Event(compaign_id, contact, current_date, time(0, random.randint(15, 40)), 'html_open')
                            add_event(events, e)
                            opted_out = random.choices([True, False], weights=[10, 90])[0]
                            if opted_out:
                                e = Event(compaign_id, contact, current_date, time(0, random.randint(15, 40)), 'optout_click')
                                add_event(events, e)
                                break
                            else:
                                clicked = random.choices([True, False], weights=[30, 70])[0]
                                if clicked:
                                    e = Event(compaign_id, contact, current_date, time(random.randint(1, 23), random.randint(0, 55)), 'click', {'target': random_choice(click_targets), 'cta': random_choice(['text', 'link', 'image', 'video', 'button'])})
                                    add_event(events, e)
            current_date += timedelta(days=1)
    return events


compaigns_list = [{'compaign_id':row['compaign_id'], 'mailing_list': row['mailing_list'], 'start_date': row['start_date'], 'end_date': row['end_date']} for row in compaigns_df.collect()]
events = generate_events(compaigns_list)
events_df = spark.createDataFrame(pd.DataFrame(events))
display(events_df)

# COMMAND ----------

def generate_feedback():
    params = {"temperature": 0.9, "max_tokens": 256}
    try:
        headers = {"Authorization": f"Bearer {databricks_token}"}
        url = f"{host}/serving-endpoints/databricks-dbrx-instruct/invocations"
        
        data = {
            "messages": [
                {
                    "role": "user",
                    "content": "generate one feedback about a product  or offering that is neutral, good, or bad. give only the feedback without the sentiment."
                }
            ],
            **params
        }
        
        response = requests.post(url, headers=headers, json=data)
        return json.loads(response.text)['choices'].pop()['message']['content']
    except:
        return ''

# COMMAND ----------

contacts = [row['contact_id'] for row in contacts_df.select('contact_id').collect()]

n_compaigns = 15
rows = []

for i in range(n_compaigns):
    rows.append({
        'compaign_id': 100 + i,
        'feedbacks': generate_feedback(),
        'contact_id': random.choice(contacts),
    })

feedbacks_df = spark.createDataFrame(pd.DataFrame(rows))
display(feedbacks_df)

# COMMAND ----------

contacts = [row['contact_id'] for row in contacts_df.select('contact_id').collect()]

n_compaigns = 12
rows = []

for i in range(n_compaigns):
    rows.append({
        'compaign_id': 100 + i,
        'complaint_type': random.choices(["CAN-SPAM Act", "GDPR"], [85, 15]),
        'contact_id': random.choice(contacts),
    })

issues_df = spark.createDataFrame(pd.DataFrame(rows))
display(issues_df)

# COMMAND ----------

compaigns_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{schema_path}.compaigns")
contacts_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{schema_path}.contacts")
prospects_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{schema_path}.prospects")
events_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{schema_path}.compaign_events")
feedbacks_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{schema_path}.feedbacks")
issues_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{schema_path}.issues")


# COMMAND ----------



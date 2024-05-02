# Databricks notebook source
# MAGIC %run ./00-init

# COMMAND ----------

dbutils.widgets.text("schema_path", "main.emailmarketing", "Schema Path")
schema_path = dbutils.widgets.get('schema_path')

# COMMAND ----------

def random_choice(choices):
  raw_weights = np.random.default_rng().standard_cauchy(len(choices))
  non_negative_weights = np.abs(raw_weights)
  # weights = non_negative_weights / np.sum(non_negative_weights)
  return random.choices(choices, non_negative_weights, k=1)[0]

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
        'template': random.choice([f'Template {i}' for i in range(20)]),
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

n_feedbacks = 15
rows = []

for i in range(n_feedbacks):
    rows.append({
        'compaign_id': 100 + i,
        'feedbacks': generate_feedback(),
        'contact_id': random.choice(contacts),
    })

feedbacks_df = spark.createDataFrame(pd.DataFrame(rows))
display(feedbacks_df)

# COMMAND ----------

contacts = [row['contact_id'] for row in contacts_df.select('contact_id').collect()]

n_issues = 12
rows = []

for i in range(n_issues):
    rows.append({
        'compaign_id': random_choice([100, 102, 105]),
        'complaint_type': random.choices(["CAN-SPAM Act", "GDPR", "Other"], [70, 15, 15]),
        'contact_id': random.choice(contacts),
    })

issues_df = spark.createDataFrame(pd.DataFrame(rows))
display(issues_df)

# COMMAND ----------

def persist_table(df, table_name, schema, comment):
  spark.sql(f"DROP TABLE IF EXISTS {table_name}")
  empty_df = spark.createDataFrame(df.toPandas(), schema=schema)
  empty_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
  spark.sql(f"COMMENT ON TABLE {table_name} IS '{comment}'")
  display(spark.sql(f"DESCRIBE TABLE {table_name}"))

# COMMAND ----------

table_name = f"{schema_path}.feedbacks"
table_comment = "Feedbacks collected during email campaigns"
schema= "compaign_id LONG COMMENT 'ID of the campaign', feedbacks STRING COMMENT 'Feedbacks received from the contact', contact_id LONG COMMENT 'ID of the contact submitting the feedback'"
persist_table(feedbacks_df, table_name, schema, table_comment)

# COMMAND ----------

table_name = f"{schema_path}.issues"
table_comment = "Contains issues reported during email campaigns"
schema= "compaign_id LONG COMMENT 'ID of the campaign', complaint_type ARRAY<STRING> COMMENT 'Complaint types (GDPR, CAN-SPAM Act, etc)', contact_id LONG COMMENT 'ID of the contact submitting the issue'"
persist_table(issues_df, table_name, schema, table_comment)

# COMMAND ----------

table_name = f"{schema_path}.compaigns"
table_comment = "Details of marketing campaigns including names, descriptions, and costs"
schema= "compaign_id LONG COMMENT 'Unique identifier for each campaign', campaign_name STRING COMMENT 'Name of the campaign', compaign_description STRING COMMENT 'Description of the campaign', subject_line STRING COMMENT 'Subject line used in the campaign emails', template STRING COMMENT 'Email template used for the campaign', cost DOUBLE COMMENT 'Total cost of the campaign', start_date DATE COMMENT 'Start date of the campaign', end_date DATE COMMENT 'End date of the campaign', mailing_list ARRAY<LONG> COMMENT 'List of contact IDs targeted in the campaign'"
persist_table(compaigns_df, table_name, schema, table_comment)

# COMMAND ----------

table_name = f"{schema_path}.contacts"
table_comment = "Contains detailed contact information for potential clients"
schema= "contact_id LONG COMMENT 'The unique ID for each contact', prospect_id LONG COMMENT 'The ID linking the contact to a specific prospect', department STRING COMMENT 'The department where the contact is employed', job_title STRING COMMENT 'The official job title of the contact', source STRING COMMENT 'The origin source of the contact information', device STRING COMMENT 'The primary device type used by the contact for communication',  opted_out BOOLEAN COMMENT 'Flag indicating if the contact has opted out of marketing communications'"
persist_table(contacts_df, table_name, schema, table_comment)

# COMMAND ----------

table_name = f"{schema_path}.prospects"
table_comment = "Stores information about potential business prospects"
schema= "prospect_id LONG COMMENT 'Unique identifier for each prospect', name STRING COMMENT 'Name of the prospect or company', annual_revenue DOUBLE COMMENT 'Reported annual revenue of the prospect in Million USD', employees LONG COMMENT 'Number of employees working for the prospect', industry STRING COMMENT 'Industry sector the prospect operates in', country STRING COMMENT 'Country where the prospect is located', city STRING COMMENT 'City where the prospect is located', postcode STRING COMMENT 'Postal code for the prospects location'"
persist_table(prospects_df, table_name, schema, table_comment)

# COMMAND ----------

table_name = f"{schema_path}.events"
table_comment = "Records all events related to marketing campaigns"
schema= "event_id STRING COMMENT 'Unique identifier for the event', compaign_id LONG COMMENT 'Identifier for the campaign associated with this event', contact_id LONG COMMENT 'Identifier for the contact involved in this event', event_type STRING COMMENT 'Type of event (e.g., html_open, click)', event_date TIMESTAMP COMMENT 'Timestamp when the event occurred', metadata MAP<STRING, STRING> COMMENT 'Additional information about the event in key-value pairs'"
persist_table(events_df, table_name, schema, table_comment)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW $schema_path.metrics_daily_rolling 
# MAGIC (date COMMENT 'The day of the event', unique_clicks COMMENT 'The number of unique clicks per campaign', total_delivered COMMENT 'The total number of emails successfully delivered', total_sent COMMENT 'The total number of emails sent', total_opens COMMENT 'The total number of emails opened', total_clicks COMMENT 'The total number of clicks', total_optouts COMMENT 'The total number of opt-outs', total_spam COMMENT 'The total number of emails marked as spam')
# MAGIC     COMMENT 'A view aggregating daily metrics for email campaigns, including unique clicks and various totals'
# MAGIC      AS
# MAGIC
# MAGIC WITH event_rankings AS (
# MAGIC     SELECT 
# MAGIC         e.*, 
# MAGIC         ROW_NUMBER() OVER (PARTITION BY event_type, compaign_id, contact_id ORDER BY e.event_date) AS contact_rn 
# MAGIC     FROM 
# MAGIC         $schema_path.events e
# MAGIC ),
# MAGIC event_with_metrics AS (
# MAGIC     SELECT 
# MAGIC         DATE(event_date) AS date,
# MAGIC         compaign_id,
# MAGIC         SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) AS total_sent,
# MAGIC         SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
# MAGIC         SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END) AS total_delivered,
# MAGIC         SUM(CASE WHEN event_type = 'spam' THEN 1 ELSE 0 END) AS total_spam,
# MAGIC         SUM(CASE WHEN event_type = 'html_open' THEN 1 ELSE 0 END) AS total_opens,
# MAGIC         SUM(CASE WHEN event_type = 'optout_click' THEN 1 ELSE 0 END) AS total_optouts,
# MAGIC         SUM(CASE WHEN event_type = 'click' AND contact_rn = 1 THEN 1 ELSE 0 END) AS unique_clicks
# MAGIC     FROM 
# MAGIC         event_rankings
# MAGIC     GROUP BY 
# MAGIC         date, 
# MAGIC         compaign_id
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     date,
# MAGIC     unique_clicks,
# MAGIC     total_delivered,
# MAGIC     total_sent,
# MAGIC     total_opens,
# MAGIC     total_clicks,
# MAGIC     total_optouts,
# MAGIC     total_spam
# MAGIC FROM event_with_metrics

# COMMAND ----------



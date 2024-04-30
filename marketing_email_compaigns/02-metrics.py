# Databricks notebook source
# MAGIC %run ./00-init

# COMMAND ----------

dbutils.widgets.text("schema_path", "main.emailmarketing", "Schema Path")
schema_path = dbutils.widgets.get('schema_path')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW $schema_path.daily_stats AS
# MAGIC SELECT 
# MAGIC     DATE(event_date) AS day,
# MAGIC     compaign_id,
# MAGIC     SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) AS total_sent,
# MAGIC     SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END) AS total_delivered,
# MAGIC     SUM(CASE WHEN event_type = 'spam' THEN 1 ELSE 0 END) AS total_spam,
# MAGIC     SUM(CASE WHEN event_type = 'html_open' THEN 1 ELSE 0 END) AS total_opens,
# MAGIC     SUM(CASE WHEN event_type = 'optout_click' THEN 1 ELSE 0 END) AS total_optouts,
# MAGIC     SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
# MAGIC     count(distinct case when event_type = 'click' then contact_id end) as unique_clicks,
# MAGIC     round(unique_clicks / total_delivered, 2) as daily_ctr,
# MAGIC     round(total_delivered / total_sent, 2) as daily_delivery_rate,
# MAGIC     round(total_optouts / total_delivered, 2) as daily_optouts_rate,
# MAGIC     round(total_spam / total_delivered, 2) as daily_spam_rate
# MAGIC FROM 
# MAGIC     $schema_path.compaign_events
# MAGIC GROUP BY 
# MAGIC     day,
# MAGIC     compaign_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW $schema_path.rolling_metrics_daily AS
# MAGIC
# MAGIC WITH event_rankings AS (
# MAGIC     SELECT 
# MAGIC         e.*, 
# MAGIC         ROW_NUMBER() OVER (PARTITION BY event_type, compaign_id, contact_id ORDER BY e.event_date) AS contact_rn 
# MAGIC     FROM 
# MAGIC         $schema_path.compaign_events e
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

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW $schema_path.agg_metrics as
# MAGIC SELECT 
# MAGIC     
# MAGIC     SUM(CASE WHEN event_type = 'sent' THEN 1 ELSE 0 END) AS total_sent,
# MAGIC     SUM(CASE WHEN event_type = 'delivered' THEN 1 ELSE 0 END) AS total_delivered,
# MAGIC     SUM(CASE WHEN event_type = 'spam' THEN 1 ELSE 0 END) AS total_spam,
# MAGIC     SUM(CASE WHEN event_type = 'html_open' THEN 1 ELSE 0 END) AS total_opens,
# MAGIC     SUM(CASE WHEN event_type = 'optout_click' THEN 1 ELSE 0 END) AS total_optouts,
# MAGIC     SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
# MAGIC     count(distinct case when event_type = 'click' then contact_id end) as unique_clicks,
# MAGIC     format_number(unique_clicks / total_delivered, '##.##%') as ctr,
# MAGIC     format_number(total_delivered / total_sent, '##.##%') as delivery_rate,
# MAGIC     format_number(total_optouts / total_delivered, '##.##%') as optouts_rate,
# MAGIC     format_number(total_spam / total_delivered, '##.##%') as spam_rate
# MAGIC FROM 
# MAGIC     $schema_path.compaign_events

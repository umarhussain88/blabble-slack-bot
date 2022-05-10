import slack_sdk as slack
import os 
from src import blabble
from time import sleep

client = slack.WebClient(token=os.environ['SLACK_TOKEN'])


sql_user = os.environ.get('sql_user')
sql_password = os.environ.get('sql_password')
sql_host = os.environ.get('sql_host')
sql_port = os.environ.get('sql_port')

lead = blabble.Lead(
    sql_user,sql_password,sql_host,sql_port)


with open('last_lead_id.csv', 'r') as f:
    last_lead_id = int(f.read())

lead_results = lead.return_leads(1207)


for result in lead_results:
    if result.property_value:
        property_value = f"£{result.property_value:,.2f}"
    if result.mortgage_amount:
        mortgage_amount = f"£{result.mortgage_amount:,.2f}"
    sleep(0.5)
    client.chat_postMessage(channel='#mortgage-leads', 
            text = f""" New Lead:* #{result.lead_id} - {result.name}* *Property Value: {property_value}* *Loan Amount: {mortgage_amount}*  \n\nMessage: {result.message}\n<https://docs.google.com/forms/d/e/1FAIpQLSc6Xp_l9rfTA_OhW9wlHcjWMXyuGejOVoSJQeoo8eLuQqn2kA/viewform|Purchase Lead Here>
                            """)
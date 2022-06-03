from unicodedata import name
import slack_sdk as slack
import os
from src import blabble, logger_util
from time import sleep


logger = logger_util(__name__)

client = slack.WebClient(token=os.environ["SLACK_TOKEN"])


sql_user = os.environ.get("sql_user")
sql_password = os.environ.get("sql_password")
sql_host = os.environ.get("sql_host")
sql_port = os.environ.get("sql_port")
sql_db = os.environ.get("sql_db")

lead = blabble.Lead(
    sql_user=sql_user,
    sql_password=sql_password,
    sql_host=sql_host,
    sql_port=sql_port,
    sql_db=sql_db,
)




if __name__ == '__main__':

    with open("last_lead_id.csv", "r") as f:
        last_lead_id = int(f.read())
        logger.info(f"Using  {last_lead_id} as the last lead_id from last_lead_id.csv")

    lead_results = lead.return_leads(last_lead_id)


    for result in lead_results:
        if result.property_value:
            property_value = f"£{result.property_value:,.2f}"
        if result.mortgage_amount:
            mortgage_amount = f"£{result.mortgage_amount:,.2f}"
        logger.info(f"Sending message to slack channel for lead_id: {result.lead_id}")
        sleep(1)
        client.chat_postMessage(
            channel="#mortgage-leads",
            text=f""" New Lead: *#{result.lead_id} - {result.name}*\n*Property Value: {property_value}* *Loan Amount: {mortgage_amount}*  \n\nMessage: {result.message}\n<https://docs.google.com/forms/d/e/1FAIpQLSc6Xp_l9rfTA_OhW9wlHcjWMXyuGejOVoSJQeoo8eLuQqn2kA/viewform|Purchase Lead Here>
                                """,
        )

from collections import namedtuple
from dataclasses import dataclass
from typing import List
import sys

import sqlalchemy
from sqlalchemy import create_engine

from src import logger_util


logger = logger_util(__name__)


@dataclass
class Engine:

    sql_user: str
    sql_password: str
    sql_host: str
    sql_port: str
    sql_db: str

    def __post_init__(self):
        engine = create_engine(
            f"postgresql://{self.sql_user}:{self.sql_password}@{self.sql_host}:{self.sql_port}/{self.sql_db}"
        )

        # should add a check to check for engine connectivity
        # and raise an exception if not connected
        logger.info(f"Connected to database: {engine.url}")

        object.__setattr__(self, "engine", engine)


@dataclass
class Lead(Engine):
    def __post_init__(self):
        super().__post_init__()

    lead_id: int = 0

    logger.info(f"Using  {lead_id} as the last lead_id")

    lead = namedtuple(
        "lead",
        "lead_id, property_value, mortgage_amount, form_submit_date, message, email, name, phone",
    )

    def fetch_leads(self, lead_id: int) -> namedtuple:

        sql_str = f"""
                    SELECT lead.id
                         , CAST(COALESCE(NULLIF(lead.property_amount,''), '0') AS DECIMAL) AS "Property Value"
                         , CAST(COALESCE(NULLIF(lead.mortgage_amount,''), '0') AS DECIMAL) AS "Loan Amount"
                         , lead.created                                         AS "form submit date"
                         , COALESCE(lead.message, '')
                         , COALESCE(cust.email, '')                             AS email
                         , CONCAT(cust.first_name, '', cust.last_name)          AS "Name"
                         , mobile_phone                                         AS "Phone"
                    FROM leads_lead            lead
                      LEFT JOIN users_customer cust
                        ON lead.customer_id = cust.id
                    WHERE
                      lead.id > 1296
                    AND lead.created >= '2022-04-04'
                    AND cust.email NOT LIKE '%%test%%'
                    AND lead.message NOT LIKE '%%test%%'
                    ORDER BY lead.id ASC
                    """
        res = self.engine.execute(sql_str)

        return res.fetchall()

    def parse_leads(self, leads: list) -> List[namedtuple]:

        return [self.lead(*row) for row in leads]

    def return_leads(self, lead_id: int) -> List[namedtuple]:
        leads = self.fetch_leads(lead_id)

        if not leads:
            logger.info(f"No leads found exiting")
            sys.exit(0)

        logger.info(f"Found {len(leads)} leads")
        # -1 = last record, 0 = lead_id.
        self.store_lead_id(leads[-1][0])

        return self.parse_leads(leads)

    # locally store the last lead_id
    def store_lead_id(self, lead_id: int) -> None:

        logger.info(f"Storing {lead_id} as the max lead_id")

        with open("last_lead_id.csv", "w") as f:
            f.write(str(lead_id))

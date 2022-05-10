from collections import namedtuple
from dataclasses import dataclass
from sqlalchemy import create_engine
import sqlalchemy

from typing import Optional, List


@dataclass
class Engine:

    sql_user: str
    sql_password: str
    sql_host: str
    sql_port: str

    def __post_init__(self):
        engine = create_engine(
            f"postgresql://{self.sql_user}:{self.sql_password}@{self.sql_host}:{self.sql_port}/defaultdb"
        )

        object.__setattr__(self, "engine", engine)


@dataclass
class Lead(Engine):
    def __post_init__(self):
        super().__post_init__()

    lead_id: int = 0

    lead = namedtuple(
        "lead",
        "lead_id, property_value, mortgage_amount, form_submit_date, message, email, name, phone"
    )

    def fetch_leads(self, lead_id: int) -> namedtuple:

        sql_str = f"""
                    SELECT cl.id
                        , COALESCE(cl.property_amount, 0)                      AS "Property Value"
                        , COALESCE(cl.mortgage_amount, 0)                      AS "Loan Amount"
                        , cl.create_date                                       AS "form submit date"
                        , COALESCE(cl.message, '')
                        , COALESCE(cc.email, '')                               AS email
                        , CONCAT(cc.first_name, '', cc.last_name)              AS "Name"
                        , mobile_phone                                         AS "Phone"
                    FROM core_lead            cl
                    LEFT JOIN core_customer cc
                        ON cl.customer_id = cc.id
                    WHERE cl.id > {lead_id}
                    and cl.create_date >= '2022-04-04'
                    and cc.email not like '%%test%%'
                    ORDER BY cl.id ASC
                    """
        res = self.engine.execute(sql_str)

        return res.fetchall()

    def parse_leads(self, leads : list) -> List[namedtuple]:

        return [self.lead(*row) for row in leads]

    def return_leads(self, lead_id: int) -> List[namedtuple]:
        leads = self.fetch_leads(lead_id)
        #-1 = last record, 0 = lead_id.
        self.store_lead_id(leads[-1][0])
        return self.parse_leads(leads)

    #locally store the last lead_id
    def store_lead_id(self, lead_id: int) -> None:
        
        with open('last_lead_id.csv', 'w') as f:
            f.write(str(lead_id))

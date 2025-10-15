#!/usr/bin/env python
from sqlite3 import connect

import pandas as pd
from crewai.flow import Flow, listen, start
from pydantic import BaseModel

from customer_support_ticket_flow.crews import (
    TextToSqlCrew,
)


class State(BaseModel):
    database_structure: str = ""
    user_prompt: str = ""
    answer: str = ""


class CustomerSupportTicketFlow(Flow[State]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table_name = "customer_support_tickets"
        self.csv_data_path = f"data/{self.table_name}.csv"
        self.database_path = f"data/{self.table_name}.db"

        self.conn = None
        self.cursor = None

    @start()
    def load_dataset(self):
        print(">>> Loading dataset")

        df = pd.read_csv(self.csv_data_path, na_filter=False)

        self.conn = connect(self.database_path)
        self.cursor = self.conn.cursor()

        df.to_sql(self.table_name, self.conn, if_exists="replace", index=False)

    @listen(load_dataset)
    def inspect_database_structure(self):
        print(">>> Inspecting database structure")
        self.cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{self.table_name}'"
        )
        ddl = self.cursor.fetchone()[0]
        self.state.database_structure = ddl

    @listen(inspect_database_structure)
    def answer_user_prompt(self):
        print(">>> Answering user prompt")
        result = (
            TextToSqlCrew()
            .crew()
            .kickoff(
                inputs={
                    "user_prompt": self.state.user_prompt,
                    "database_structure": self.state.database_structure,
                    "database_path": self.database_path,
                }
            )
        )
        self.state.answer = result.raw
        with open("executive_summary.md", "w", encoding="utf-8") as f:
            f.write(self.state.answer)

    @listen(answer_user_prompt)
    def close_connection(self):
        print(">>> Closing connection")
        self.conn.close()


def kickoff():
    CustomerSupportTicketFlow().kickoff(
        inputs={
            # "user_prompt": "Help me identify the gender distribution of customers who submitted support tickets"
            "user_prompt": "What is the most recurrent type of issue reported by customers that are still unsolved?"
        }
    )


def plot():
    CustomerSupportTicketFlow().plot()


if __name__ == "__main__":
    kickoff()

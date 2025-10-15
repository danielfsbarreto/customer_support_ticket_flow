from typing import List

from crewai import Agent, Crew, Process, Task
from crewai.agents.agent_builder.base_agent import BaseAgent
from crewai.project import CrewBase, agent, crew, task

from customer_support_ticket_flow.crews.text_to_sql_crew.tools.text_to_sql_tool import (
    SQLiteDatabaseTool,
)


@CrewBase
class TextToSqlCrew:
    """Poem Crew"""

    agents: List[BaseAgent]
    tasks: List[Task]

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"

    @agent
    def database_specialist(self) -> Agent:
        return Agent(
            config=self.agents_config["database_specialist"],
        )

    @task
    def perform_sql_query(self) -> Task:
        return Task(
            config=self.tasks_config["perform_sql_query"],
            tools=[SQLiteDatabaseTool()],
        )

    @crew
    def crew(self) -> Crew:
        """Creates the Research Crew"""

        return Crew(
            agents=self.agents,
            tasks=self.tasks,
            process=Process.sequential,
            verbose=True,
        )

"""
SQLite Database Query Tool for Customer Support Tickets

This tool provides a secure interface for executing SQL queries against the customer support tickets database.
It includes proper error handling, connection management, and query validation.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, field_validator


class SQLiteQueryInput(BaseModel):
    """Input schema for SQLite Database Query Tool."""

    query: str = Field(
        ...,
        description="SQL query to execute against the customer support tickets database. "
        "Only SELECT statements are allowed for security reasons.",
    )
    database_path: str = Field(
        default="data/customer_support_tickets.db",
        description="Path to the SQLite database file relative to project root",
    )

    @field_validator("query")
    @classmethod
    def validate_query_safety(cls, v):
        """Ensure only SELECT queries are allowed for security."""
        query_upper = v.strip().upper()
        if not query_upper.startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed for security reasons")

        # Block potentially dangerous SQL keywords
        dangerous_keywords = [
            "DROP",
            "DELETE",
            "INSERT",
            "UPDATE",
            "ALTER",
            "CREATE",
            "TRUNCATE",
        ]
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                raise ValueError(f"Query contains forbidden keyword: {keyword}")

        return v


class SQLiteDatabaseTool(BaseTool):
    """
    A secure SQLite database query tool for customer support ticket analysis.

    This tool allows agents to execute SELECT queries against the customer support
    tickets database while maintaining security through query validation and
    proper error handling.
    """

    name: str = "SQLite Database Query Tool"
    description: str = (
        "Execute SELECT queries against the customer support tickets SQLite database. "
        "This tool provides access to customer support data including ticket information, "
        "customer details, product data, and resolution metrics. Only read-only SELECT "
        "queries are permitted for security."
    )
    args_schema: Type[BaseModel] = SQLiteQueryInput

    def _run(
        self, query: str, database_path: str = "data/customer_support_tickets.db"
    ) -> str:
        """
        Execute a SQL query against the SQLite database and return formatted results.

        Args:
            query: SQL SELECT query to execute
            database_path: Path to the SQLite database file

        Returns:
            Formatted string containing query results or error message
        """
        try:
            # Resolve the database path relative to project root
            project_root = Path(__file__).parent.parent.parent.parent.parent.parent
            full_db_path = project_root / database_path

            # Verify database file exists
            if not full_db_path.exists():
                return f"Error: Database file not found at {full_db_path}"

            # Execute query with proper connection management
            with sqlite3.connect(str(full_db_path)) as conn:
                # Enable row factory for better result formatting
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                # Execute the query
                cursor.execute(query)
                rows = cursor.fetchall()

                # Format results
                if not rows:
                    return "Query executed successfully but returned no results."

                # Convert to list of dictionaries for better readability
                results = [dict(row) for row in rows]

                # Format output with summary
                result_summary = (
                    f"Query executed successfully. Retrieved {len(results)} row(s).\n\n"
                )

                # Add column headers
                if results:
                    columns = list(results[0].keys())
                    result_summary += "Columns: " + ", ".join(columns) + "\n\n"

                # Add first few rows as examples (limit to prevent overwhelming output)
                max_display_rows = min(20, len(results))
                result_summary += (
                    f"Sample results (showing first {max_display_rows} rows):\n"
                )

                for i, row in enumerate(results[:max_display_rows], 1):
                    result_summary += f"\nRow {i}:\n"
                    for key, value in row.items():
                        # Truncate long text fields for readability
                        display_value = str(value)
                        if len(display_value) > 100:
                            display_value = display_value[:97] + "..."
                        result_summary += f"  {key}: {display_value}\n"

                if len(results) > max_display_rows:
                    result_summary += (
                        f"\n... and {len(results) - max_display_rows} more rows."
                    )

                return result_summary

        except sqlite3.Error as e:
            error_msg = f"SQLite error: {str(e)}"
            logging.error(error_msg)
            return error_msg

        except Exception as e:
            error_msg = f"Unexpected error executing query: {str(e)}"
            logging.error(error_msg)
            return error_msg


# Create an instance of the tool for easy import
sqlite_database_tool = SQLiteDatabaseTool()

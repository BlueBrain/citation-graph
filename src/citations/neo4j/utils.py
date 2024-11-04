"""Utility functions for interacting with a Neo4j database."""

import logging

from neo4j import ManagedTransaction, Transaction
from neo4j.exceptions import ClientError

logging.basicConfig(level=logging.INFO)


def count_all_nodes(tx: ManagedTransaction) -> None:
    """
    Count all nodes in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    try:
        result = tx.run("MATCH (n) RETURN count(n) as num_nodes")
        record = result.single()
        if record is not None:
            logging.info(f"Number of nodes: {record['num_nodes']}")
    except Exception as e:
        logging.error(f"Error counting nodes: {e}")


def count_all_edges(tx: ManagedTransaction) -> None:
    """
    Count all edges in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    try:
        result = tx.run("MATCH ()-->() RETURN count(*) as num_edges")
        record = result.single()
        if record is not None:
            logging.info(f"Number of edges: {record['num_edges']}")
    except Exception as e:
        logging.error(f"Error counting edges: {e}")


def count_nodes_of_type(tx: Transaction, label: str) -> int:
    """
    Count nodes of a given type in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    label
        The label of the nodes to count

    Returns
    -------
        The number of nodes of the given type
    """
    try:
        result = tx.run(f"MATCH (n:{label}) RETURN count(n) as num_nodes")
        record = result.single()
        if record is not None:
            return int(record["num_nodes"])
        else:
            return 0
    except Exception as e:
        logging.error(f"Error counting nodes of type {label}: {e}")
        raise e


def count_edges_of_type(tx: Transaction, relationship: str) -> int | None:
    """
    Count edges of a given relationship type in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    relationship
        The type of relationship to count

    Returns
    -------
        The number of edges of the given type
    """
    result = tx.run(
        f"MATCH ()-[r:{relationship}]->() RETURN count(r) as num_edges"
    )
    record = result.single()
    if record is None:
        return None
    return int(record["num_edges"])


def remove_all_nodes_and_edges(tx: ManagedTransaction) -> None:
    """
    Remove all nodes and edges from the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    try:
        tx.run("MATCH (n) DETACH DELETE n")
    except Exception as e:
        logging.error(f"Error removing nodes and edges: {e}")


def remove_self_loops_of_type(tx: ManagedTransaction, rel_type: str) -> None:
    """
    Remove self loops of a given relationship type from the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    rel_type
        The type of relationship to remove self loops for
    """
    try:
        tx.run(f"MATCH (n)-[r:{rel_type}]->(n) DELETE r")
    except Exception as e:
        logging.error(f"Error removing self loops of type {rel_type}: {e}")


def remove_relationship_type(
    tx: ManagedTransaction, relationship: str
) -> None:
    """
    Remove a specific relationship type from the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    relationship
        The type of relationship to remove
    """
    tx.run(f"MATCH ()-[r:{relationship}]->() DELETE r")


def remove_node_type(tx: ManagedTransaction, label: str) -> None:
    """
    Remove a specific node type from the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    label
        The label of the nodes to remove
    """
    tx.run(f"MATCH (n:{label}) DETACH DELETE n")


def drop_all_constraints_and_indexes(tx):
    """Drop all constraints and indexes in the database."""
    # Drop all constraints
    constraints = tx.run("SHOW CONSTRAINTS")
    for record in constraints:
        constraint_name = record["name"]
        try:
            tx.run(f"DROP CONSTRAINT {constraint_name} IF EXISTS")
        except ClientError as e:
            print(f"Error dropping constraint {constraint_name}: {str(e)}")

    # Drop all indexes
    indexes = tx.run("SHOW INDEXES")
    for record in indexes:
        index_name = record["name"]
        try:
            tx.run(f"DROP INDEX {index_name} IF EXISTS")
        except ClientError as e:
            print(f"Error dropping index {index_name}: {str(e)}")
            # If the index is in a failed state, try to drop it using the name
            if "IndexNotFoundError" in str(e):
                try:
                    tx.run("DROP INDEX ON :Label(property)")
                    print(
                        f"Successfully dropped index {index_name} using"
                        " alternate method"
                    )
                except ClientError as inner_e:
                    print(
                        f"Error dropping index {index_name} using alternate"
                        f" method: {str(inner_e)}"
                    )


def print_constraints(tx: ManagedTransaction) -> None:
    """
    Print all constraints in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    result = tx.run("CALL db.constraints()")
    for record in result:
        logging.info(record)


def print_indexes(tx: ManagedTransaction) -> None:
    """
    Print all indexes in the Neo4j database.

    Parameters
    ----------
    tx
        A Neo4j transaction object
    """
    result = tx.run("CALL db.indexes()")
    for record in result:
        logging.info(record)

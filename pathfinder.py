# sql_graph_optimizer.py
import psycopg2
import time
import math
import heapq

# ----------------------------
# Database Schema Mapping
# ----------------------------
def create_db_schema_table(conn):
    """Create table to store database schema (tables, columns, keys)."""
    sql = """
    CREATE TABLE IF NOT EXISTS db_schema (
        schema_name TEXT,
        table_name TEXT,
        column_name TEXT,
        data_type TEXT,
        is_primary_key BOOLEAN,
        is_foreign_key BOOLEAN,
        references_table TEXT,
        references_column TEXT,
        PRIMARY KEY (schema_name, table_name, column_name)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def populate_db_schema(conn):
    """Populate db_schema from information_schema."""
    columns_sql = """
    SELECT table_schema, table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema NOT IN ('pg_catalog','information_schema');
    """
    pk_sql = """
    SELECT kcu.table_schema, kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY';
    """
    fk_sql = """
    SELECT kcu.table_schema, kcu.table_name, kcu.column_name,
           ccu.table_name AS references_table, ccu.column_name AS references_column
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY';
    """

    with conn.cursor() as cur:
        cur.execute(columns_sql)
        columns = cur.fetchall()
        cur.execute(pk_sql)
        pk_set = {(r[0], r[1], r[2]) for r in cur.fetchall()}
        cur.execute(fk_sql)
        fk_dict = {(r[0], r[1], r[2]): (r[3], r[4]) for r in cur.fetchall()}

        for schema, table, column, dtype in columns:
            is_pk = (schema, table, column) in pk_set
            is_fk = (schema, table, column) in fk_dict
            ref_table, ref_col = fk_dict.get((schema, table, column), (None, None))

            cur.execute("""
                INSERT INTO db_schema (schema_name, table_name, column_name, data_type,
                                       is_primary_key, is_foreign_key, references_table, references_column)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (schema_name, table_name, column_name) DO UPDATE
                  SET data_type=EXCLUDED.data_type,
                      is_primary_key=EXCLUDED.is_primary_key,
                      is_foreign_key=EXCLUDED.is_foreign_key,
                      references_table=EXCLUDED.references_table,
                      references_column=EXCLUDED.references_column;
            """, (schema, table, column, dtype, is_pk, is_fk, ref_table, ref_col))
    conn.commit()


# ----------------------------
# Join Metrics
# ----------------------------
def create_join_metrics_table(conn):
    """Create table to store real join metrics."""
    sql = """
    CREATE TABLE IF NOT EXISTS join_metrics (
        left_table TEXT NOT NULL,
        left_column TEXT NOT NULL,
        right_table TEXT NOT NULL,
        right_column TEXT NOT NULL,
        join_type TEXT DEFAULT 'INNER',
        actual_rows BIGINT,
        actual_time_ms FLOAT,
        left_is_pk BOOLEAN,
        right_is_pk BOOLEAN,
        left_is_fk BOOLEAN,
        right_is_fk BOOLEAN,
        last_seen TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (left_table, left_column, right_table, right_column)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def populate_join_metrics_real(conn):
    """Populate join_metrics with real join metrics."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, column_name, references_table, references_column, is_primary_key, is_foreign_key
            FROM db_schema
            WHERE is_foreign_key = TRUE;
        """)
        fk_rows = cur.fetchall()

        for left_table, left_col, right_table, right_col, left_is_pk, left_is_fk in fk_rows:
            cur.execute("""
                SELECT is_primary_key, is_foreign_key
                FROM db_schema
                WHERE table_name=%s AND column_name=%s
            """, (right_table, right_col))
            result = cur.fetchone()
            if not result:
                continue
            right_is_pk, right_is_fk = result

            join_sql = f"SELECT COUNT(*) FROM {left_table} l JOIN {right_table} r ON l.{left_col} = r.{right_col}"
            start_time = time.time()
            try:
                cur.execute(join_sql)
                row_count = cur.fetchone()[0]
                elapsed_ms = (time.time() - start_time) * 1000
            except Exception as e:
                conn.rollback()
                print(f"[WARN] Join failed for {left_table}-{right_table}: {e}")
                continue

            cur.execute("""
                INSERT INTO join_metrics (
                    left_table,left_column,right_table,right_column,join_type,
                    actual_rows,actual_time_ms,
                    left_is_pk,right_is_pk,left_is_fk,right_is_fk,last_seen
                )
                VALUES (%s,%s,%s,%s,'INNER',%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (left_table,left_column,right_table,right_column) DO UPDATE
                  SET actual_rows=EXCLUDED.actual_rows,
                      actual_time_ms=EXCLUDED.actual_time_ms,
                      left_is_pk=EXCLUDED.left_is_pk,
                      right_is_pk=EXCLUDED.right_is_pk,
                      left_is_fk=EXCLUDED.left_is_fk,
                      right_is_fk=EXCLUDED.right_is_fk,
                      last_seen=NOW();
            """, (
                left_table, left_col, right_table, right_col,
                row_count, elapsed_ms,
                left_is_pk, right_is_pk, left_is_fk, right_is_fk
            ))
    conn.commit()


# ----------------------------
# Join Weights
# ----------------------------
def create_join_weights_table(conn):
    """Create table to store join weights."""
    sql = """
    CREATE TABLE IF NOT EXISTS join_weights (
        left_table TEXT NOT NULL,
        left_column TEXT NOT NULL,
        right_table TEXT NOT NULL,
        right_column TEXT NOT NULL,
        weight FLOAT NOT NULL,
        last_update TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (left_table,left_column,right_table,right_column)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def update_join_weights(conn, alpha=0.6, beta=0.3, gamma=0.1):
    """Calculate weights based on real join metrics."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT left_table,left_column,right_table,right_column,
                   actual_rows,actual_time_ms,left_is_pk,right_is_pk
            FROM join_metrics
        """)
        metrics = cur.fetchall()

        for left_table, left_col, right_table, right_col, actual_rows, actual_time_ms, left_is_pk, right_is_pk in metrics:
            index_penalty = 0.5 if left_is_pk or right_is_pk else 1.0
            weight = alpha*actual_time_ms + beta*math.log1p(actual_rows) + gamma*index_penalty

            cur.execute("""
                INSERT INTO join_weights (left_table,left_column,right_table,right_column,weight,last_update)
                VALUES (%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (left_table,left_column,right_table,right_column) DO UPDATE
                  SET weight=EXCLUDED.weight,last_update=NOW();
            """, (left_table, left_col, right_table, right_col, weight))
    conn.commit()


# ----------------------------
# Dijkstra Shortest Path
# ----------------------------
def build_graph(conn):
    """Build weighted graph from join_weights."""
    graph = {}
    with conn.cursor() as cur:
        cur.execute("SELECT left_table,right_table,weight FROM join_weights")
        for left, right, weight in cur.fetchall():
            graph.setdefault(left, {})[right] = weight
            graph.setdefault(right, {})[left] = weight
    return graph


def dijkstra(graph, start, end):
    """Find shortest path from start to end in weighted graph."""
    queue = [(0, start, [])]
    visited = set()

    while queue:
        cost, node, path = heapq.heappop(queue)
        if node in visited:
            continue
        visited.add(node)
        path = path + [node]

        if node == end:
            return cost, path

        for neighbor, weight in graph.get(node, {}).items():
            if neighbor not in visited:
                heapq.heappush(queue, (cost + weight, neighbor, path))
    return float('inf'), []


# ----------------------------
# Query Builder (Post-Dijkstra)
# ----------------------------
def build_optimized_query(conn, start_table, end_table, select_columns=None):
    """
    Build an optimized SQL query based on the minimum join path.
    Uses db_schema relationships and weights computed via Dijkstra.
    """
    # 1. Build graph and find optimal path
    graph = build_graph(conn)
    cost, path = dijkstra(graph, start_table, end_table)

    if not path or cost == float('inf'):
        raise ValueError(f"No path found between {start_table} and {end_table}")

    # 2. Retrieve join relationships between adjacent tables
    join_conditions = []
    with conn.cursor() as cur:
        for i in range(len(path) - 1):
            left, right = path[i], path[i + 1]
            cur.execute("""
                SELECT column_name, references_table, references_column
                FROM db_schema
                WHERE (table_name = %s AND references_table = %s)
                   OR (table_name = %s AND references_table = %s)
            """, (left, right, right, left))
            rel = cur.fetchone()
            if not rel:
                raise ValueError(f"No join condition found between {left} and {right}")
            col, ref_table, ref_col = rel
            if ref_table == right:
                join_conditions.append(f"{left}.{col} = {right}.{ref_col}")
            else:
                join_conditions.append(f"{right}.{col} = {left}.{ref_col}")

    # 3. Construct final SQL query
    select_clause = "*"
    if select_columns:
        select_clause = ", ".join(select_columns)

    from_clause = path[0]
    for i in range(1, len(path)):
        from_clause += f"\nJOIN {path[i]} ON {join_conditions[i - 1]}"

    query = f"SELECT {select_clause}\nFROM {from_clause};"

    return {
        "cost": cost,
        "path": path,
        "sql": query
    }


# ----------------------------
# Author
# ----------------------------
# powered and created by Giuseppe D'Ambrosio at Noahdambrosio.com
# find us at https://noahdambrosio.com

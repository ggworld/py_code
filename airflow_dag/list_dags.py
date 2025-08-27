import os
import json
import logging
import sqlite3
import requests
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote  # added for URL-encoding dag_id

# Optional Ray import for parallelism
try:
    import ray  # type: ignore
    RAY_AVAILABLE = True
except Exception:
    ray = None  # type: ignore
    RAY_AVAILABLE = False


def get_airflow_credentials() -> Tuple[str, str]:
    username = os.environ.get("AIRFLOW_API_USERNAME")
    password = os.environ.get("AIRFLOW_API_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Missing credentials. Set AIRFLOW_API_USERNAME and AIRFLOW_API_PASSWORD environment variables."
        )
    return username, password


def fetch_all_dags_v1(auth: Tuple[str, str], base_url: str = "https://airflow.riskxint.com") -> List[Dict]:
    all_dags: List[Dict] = []
    limit = 100
    offset = 0
    total_entries: Optional[int] = None
    page_count = 0

    logging.info(f"Starting DAG fetch (API v1) with pagination (limit={limit}) from {base_url}")

    while True:
        page_count += 1
        url = f"{base_url}/api/v1/dags?limit={limit}&offset={offset}"
        try:
            logging.info(f"Fetching page {page_count}: offset={offset}, limit={limit}")
            response = requests.get(url, auth=auth, timeout=30)
            logging.info(f"HTTP {response.status_code} from {url}")
            response.raise_for_status()
            data = response.json()

            dags = data.get("dags", [])
            current_total = data.get("total_entries", 0)

            if total_entries is None:
                total_entries = current_total
                logging.info(f"Total DAGs to fetch: {total_entries}")

            logging.info(f"Page {page_count}: Retrieved {len(dags)} DAGs (offset={offset})")
            all_dags.extend(dags)

            if len(dags) < limit or len(all_dags) >= total_entries:
                logging.info(f"Pagination complete. Total DAGs fetched: {len(all_dags)}")
                break

            offset += limit

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from {url}: {e}")
            break
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON response from {url}: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error during fetch: {e}")
            break

    logging.info(f"Final DAG count: {len(all_dags)}")
    if len(all_dags) == 0:
        logging.warning("API returned 0 DAGs. Table will still be created with minimal schema.")
    return all_dags


def fetch_latest_dag_run_v1(auth: Tuple[str, str], base_url: str, dag_id: str) -> Optional[Dict]:
    # URL-encode dag_id to handle special characters (e.g., dots, slashes)
    encoded_dag_id = quote(dag_id, safe="")
    url = f"{base_url}/api/v1/dags/{encoded_dag_id}/dagRuns?order_by=-start_date&limit=1"
    try:
        resp = requests.get(url, auth=auth, timeout=30)
        logging.debug(f"Latest run request for {dag_id}: HTTP {resp.status_code}")
        resp.raise_for_status()
        payload = resp.json()
        runs = payload.get("dag_runs", [])
        return runs[0] if runs else None
    except Exception as e:
        logging.warning(f"Could not fetch latest run for {dag_id}: {e}")
        return None


# Ray remote for parallel latest-run fetch
if RAY_AVAILABLE:
    @ray.remote  # type: ignore
    def _fetch_latest_run_remote(base_url: str, username: str, password: str, dag_id: str) -> Dict:
        try:
            encoded_dag_id = quote(dag_id, safe="")
            url = f"{base_url}/api/v1/dags/{encoded_dag_id}/dagRuns?order_by=-start_date&limit=1"
            resp = requests.get(url, auth=(username, password), timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            runs = payload.get("dag_runs", [])
            latest = runs[0] if runs else None
            if latest:
                return {
                    "dag_id": dag_id,
                    "last_run_id": latest.get("dag_run_id"),
                    "last_run_state": latest.get("state"),
                    "last_run_execution_date": latest.get("execution_date"),
                    "last_run_start_date": latest.get("start_date"),
                    "last_run_end_date": latest.get("end_date"),
                    "last_run_conf": latest.get("conf"),
                }
            else:
                return {
                    "dag_id": dag_id,
                    "last_run_id": None,
                    "last_run_state": None,
                    "last_run_execution_date": None,
                    "last_run_start_date": None,
                    "last_run_end_date": None,
                    "last_run_conf": None,
                }
        except Exception as e:
            return {
                "dag_id": dag_id,
                "last_run_id": None,
                "last_run_state": None,
                "last_run_execution_date": None,
                "last_run_start_date": None,
                "last_run_end_date": None,
                "last_run_conf": None,
                "error": str(e),
            }


def enrich_dags_with_last_run(auth: Tuple[str, str], base_url: str, dags: List[Dict]) -> List[Dict]:
    if not dags:
        return dags

    username, password = auth

    if RAY_AVAILABLE:
        # Initialize Ray if needed
        if not ray.is_initialized():  # type: ignore
            ray.init(ignore_reinit_error=True, include_dashboard=False)  # type: ignore
        logging.info("Fetching latest run for each DAG in parallel with Ray")

        dag_ids = [d.get("dag_id") for d in dags if d.get("dag_id")]
        futures = [_fetch_latest_run_remote.remote(base_url, username, password, dag_id) for dag_id in dag_ids]  # type: ignore

        results: List[Dict] = []
        total = len(futures)
        for i in range(0, total, 10):
            # pull next chunk of up to 10 results and log progress
            chunk = futures[i:i+10]
            results.extend(ray.get(chunk))  # type: ignore
            last = results[-1]
            logging.info(
                f"Parallel fetched latest runs: {min(i+10, total)}/{total}. Last: dag_id={last.get('dag_id')}, "
                f"state={last.get('last_run_state')}, start={last.get('last_run_start_date')}, end={last.get('last_run_end_date')}"
            )

        # Merge results back into dags
        by_id = {r.get("dag_id"): r for r in results}
        for d in dags:
            dag_id = d.get("dag_id")
            if not dag_id:
                continue
            r = by_id.get(dag_id)
            if r:
                d["last_run_id"] = r.get("last_run_id")
                d["last_run_state"] = r.get("last_run_state")
                d["last_run_execution_date"] = r.get("last_run_execution_date")
                d["last_run_start_date"] = r.get("last_run_start_date")
                d["last_run_end_date"] = r.get("last_run_end_date")
                d["last_run_conf"] = r.get("last_run_conf")
        return dags

    # Fallback: sequential
    logging.info("Fetching latest run for each DAG to populate Last Run fields (sequential)")
    for idx, dag in enumerate(dags, start=1):
        dag_id = dag.get("dag_id")
        if not dag_id:
            continue
        latest = fetch_latest_dag_run_v1(auth=auth, base_url=base_url, dag_id=dag_id)
        if latest:
            dag["last_run_id"] = latest.get("dag_run_id")
            dag["last_run_state"] = latest.get("state")
            dag["last_run_execution_date"] = latest.get("execution_date")
            dag["last_run_start_date"] = latest.get("start_date")
            dag["last_run_end_date"] = latest.get("end_date")
            dag["last_run_conf"] = latest.get("conf")
        else:
            dag["last_run_id"] = None
            dag["last_run_state"] = None
            dag["last_run_execution_date"] = None
            dag["last_run_start_date"] = None
            dag["last_run_end_date"] = None
            dag["last_run_conf"] = None

        if idx % 10 == 0:
            logging.info(
                f"Fetched latest run for {idx}/{len(dags)} DAGs. Last: dag_id={dag_id}, "
                f"state={dag.get('last_run_state')}, start={dag.get('last_run_start_date')}, "
                f"end={dag.get('last_run_end_date')}"
            )
    return dags


def store_all_fields_in_sqlite(dags: List[Dict], db_name: str = "./airflow_dags.db") -> int:
    logging.info(f"Preparing to store DAGs in SQLite database: {db_name}")

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Collect all unique fields from all DAGs; ensure minimal schema if none
    all_fields = set()
    for dag in dags:
        all_fields.update(dag.keys())

    if not all_fields:
        all_fields = {"dag_id"}
        logging.info("No fields discovered from data; using minimal schema with 'dag_id' only.")

    all_fields = sorted(all_fields)
    logging.info(f"Using {len(all_fields)} fields for table creation: {all_fields}")

    # Create table with all fields as TEXT
    columns_sql = []
    for field in all_fields:
        if field == "dag_id":
            columns_sql.append(f"{field} TEXT PRIMARY KEY")
        else:
            columns_sql.append(f"{field} TEXT")

    create_table_sql = f"CREATE TABLE IF NOT EXISTS dags1 ({', '.join(columns_sql)})"
    cursor.execute(create_table_sql)
    logging.info('created tabele')    
    conn.commit()

    # Ensure schema has all columns (auto-migrate missing ones)
    try:
        cursor.execute("PRAGMA table_info(dags1)")
        existing_cols = {row[1] for row in cursor.fetchall()}  # second column is name
        missing = [c for c in all_fields if c not in existing_cols]
        for col in missing:
            cursor.execute(f"ALTER TABLE dags1 ADD COLUMN {col} TEXT")
            logging.info(f"Added missing column to dags1: {col}")
        if missing:
            conn.commit()
    except Exception as e:
        logging.warning(f"Could not verify/migrate schema: {e}")

    # Optional helpful indexes
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dags_is_active ON dags1(is_active)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dags_is_paused ON dags1(is_paused)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dags_updated_at ON dags1(updated_at)")
        conn.commit()
    except Exception as e:
        logging.warning(f"Could not create indexes: {e}")

    if not dags:
        logging.warning("No DAGs to insert. Table was created/ensured.")
        conn.close()
        return 0

    placeholders = ", ".join(["?"] * len(all_fields))
    insert_sql = f"INSERT OR REPLACE INTO dags1 ({', '.join(all_fields)}) VALUES ({placeholders})"

    stored_count = 0
    for dag in dags:
        try:
            values = []
            for field in all_fields:
                value = dag.get(field)
                if value is None:
                    values.append(None)
                elif isinstance(value, (list, dict)):
                    values.append(json.dumps(value, ensure_ascii=False))
                elif isinstance(value, bool):
                    values.append(str(value).lower())
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                else:
                    values.append(str(value))

            cursor.execute(insert_sql, values)
            stored_count += 1
            if stored_count % 10 == 0:
                logging.info(f"Stored {stored_count}/{len(dags)} DAGs")
        except Exception as e:
            dag_id = dag.get('dag_id', '<unknown>')
            logging.error(f"Error storing DAG {dag_id}: {e}")
            continue

    conn.commit()
    conn.close()

    logging.info(f"Successfully stored {stored_count}/{len(dags)} DAGs in database")
    return stored_count


def log_summary(dags_count: int, stored_count: int) -> None:
    logging.info("=" * 50)
    logging.info("DAG FETCH AND STORE SUMMARY")
    logging.info("=" * 50)
    logging.info(f"Total DAGs fetched from API: {dags_count}")
    logging.info(f"Total DAGs stored in database: {stored_count}")
    logging.info(f"Success rate: {(stored_count/dags_count)*100:.1f}%" if dags_count > 0 else "N/A")
    logging.info("=" * 50)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

    base_url = os.environ.get("AIRFLOW_BASE_URL", "https://airflow.riskxint.com")
    db_path = os.environ.get("SQLITE_DB_PATH", "./airflow_dags.db")

    auth = get_airflow_credentials()
    dags = fetch_all_dags_v1(auth=auth, base_url=base_url)
    dags = enrich_dags_with_last_run(auth=auth, base_url=base_url, dags=dags)
    stored_count = store_all_fields_in_sqlite(dags, db_name=db_path)
    log_summary(len(dags), stored_count)


if __name__ == "__main__":
    main()

import requests
import os
import psycopg2
import json
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

queues = ["Apex_Financial","ApplicationCenter","Fin_Up","First_Alliance","Fivepoint_Lending","Frontline_Partners"
          ,"Guardian_Peak","Inhouse_Social","Jumpstartloans","Lending_Que","LibertyFunds","LiftAI","LiftAIM","Mailer","Modern_Money_Direct","Newhorizonfunding",
          "Outbound","Patriot_Financial","Purl_Live_Transfer","Retention_ALV","Sales","Social_Live_Transfer","ThriveLoan","TriCountyLoans"]

authtoken = os.getenv("Token")
db_config = {
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT"),
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS")
}

def get_queue_monitor_reports(token,queue_name):
    url = f"https://api.1bluerock.com/v2/queue/{queue_name}/calls/hour/completed"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("response", {}).get("calls", [])
    else:
        raise Exception(f"Error fetching reports: {response.status_code} - {response.text}")

def insert_calls_to_db(calls):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    for call in calls:
        # Generate record_id
        called_on_str = call.get("datetime")
        called_on = datetime.strptime(called_on_str, "%Y-%m-%d %H:%M:%S")
        caller_id = call.get("callerid")
        record_id = f"{caller_id}_{called_on.strftime('%Y%m%d%H%M%S')}"

        # Mark completed based on exit_reason
        completed = "Yes" if call.get("exit_reason") == "Agent Hang up" else "No"

        cur.execute("""
            INSERT INTO bluerock (
                record_id, called_on, queue, trunk, caller_id,
                completed, agent, wait, call_time, position,
                orig_pos, exit_reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_id) DO NOTHING;
        """, (
            record_id,
            called_on,
            call.get("queue"),
            call.get("trunk"),
            caller_id,
            completed,
            call.get("agent"),
            int(call["holdtime"]) if call.get("holdtime") else None,
            int(call["calltime"]) if call.get("calltime") else None,
            int(call["exit_position"]) if call.get("exit_position") else None,
            int(call["enter_position"]) if call.get("enter_position") else None,
            call.get("exit_reason")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(calls)} records into 'bluerock'.")

# Run the job
for queue in queues:
    print(f"Fetching reports for queue: {queue}")
    try:
        calls = get_queue_monitor_reports(authtoken, queue)
        if calls:
            print(f"Found {len(calls)} calls for queue: {queue}")
            insert_calls_to_db(calls)
        else:
            print(f"No calls found for queue: {queue}")
    except Exception as e:
        print(f"Error processing queue {queue}: {e}")


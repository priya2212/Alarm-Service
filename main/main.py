#!/usr/bin/env python3
import sqlite3
import json
import logging
import multiprocessing as mp
import signal
import sys
from datetime import datetime, timezone
from queue import Empty, Full
from threading import Thread
import paho.mqtt.client as mqtt

# ---------------- CONFIG ----------------
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC_TELEMETRY = "telemetry/#"
MQTT_TOPIC_ALARM = "alarm"
DB_PATH = "/tmp/alarms.db"
WORKERS = 1
QUEUE_MAXSIZE = 1000
# ----------------------------------------


# ---------------- DB Initialize and connection ----------------
class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    @staticmethod
    def init(path):
        conn = sqlite3.connect(path)

        conn.executescript("""
        PRAGMA journal_mode=WAL;

        DROP TABLE IF EXISTS state;

        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER PRIMARY KEY,
            device_id TEXT,
            sensor_id TEXT,
            condition_type TEXT,
            operator TEXT,
            threshold_value REAL,
            duration_seconds INTEGER,
            secondary_device_id TEXT,
            secondary_sensor_id TEXT,
            secondary_operator TEXT,
            secondary_value REAL,
            active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS state (
            rule_id INTEGER,
            device_id TEXT,
            sensor_id TEXT,
            secondary_device_id TEXT DEFAULT '',
            secondary_sensor_id TEXT DEFAULT '',
            in_alarm INTEGER,
            breach_start TEXT,
            last_eval TEXT,
            last_value REAL,

            UNIQUE(rule_id, device_id, sensor_id, secondary_device_id, secondary_sensor_id)
        );

        CREATE TABLE IF NOT EXISTS latest_values (
            device_id TEXT,
            sensor_id TEXT,
            value REAL,
            ts TEXT,
            UNIQUE(device_id, sensor_id)
        );

        CREATE TABLE IF NOT EXISTS alarm_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER,
            device_id TEXT,
            sensor_id TEXT,
            triggered_at TEXT,
            cleared_at TEXT,
            value REAL,
            message TEXT
        );
        """)

        # seed rules if empty
        if conn.execute("SELECT COUNT(*) FROM rules").fetchone()[0] == 0:

            # threshold rule
            conn.execute("""
                INSERT INTO rules
                (device_id,sensor_id,condition_type,operator,threshold_value,duration_seconds)
                VALUES ('sensor1','temperature','threshold','>',24,10)
            """)

            # conditional rule
            conn.execute("""
                INSERT INTO rules
                (device_id,sensor_id,condition_type,operator,threshold_value,duration_seconds,
                 secondary_device_id,secondary_sensor_id,secondary_operator,secondary_value)
                VALUES ('sensor1','temperature','conditional','>',24,5,
                        'sensor2','current','>',0)
            """)

        conn.commit()
        conn.close()

    # ---------- RULES ----------
    def get_rules(self, device, sensor):
        return self.conn.execute(
            "SELECT * FROM rules WHERE device_id=? AND sensor_id=? AND active=1",
            (device, sensor)
        ).fetchall()

    # ---------- STATE ----------
    def get_state(self, rule):
        return self.conn.execute("""
            SELECT * FROM state
            WHERE rule_id=?
            AND device_id=?
            AND sensor_id=?
            AND secondary_device_id=?
            AND secondary_sensor_id=?
        """, (
            rule['id'],
            rule['device_id'],
            rule['sensor_id'],
            rule['secondary_device_id'] or '',
            rule['secondary_sensor_id'] or ''
        )).fetchone()

    def upsert_state(self, rule, new_state):

        self.conn.execute("""
        INSERT INTO state VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(rule_id,device_id,sensor_id,secondary_device_id,secondary_sensor_id)
        DO UPDATE SET
            in_alarm=excluded.in_alarm,
            breach_start=excluded.breach_start,
            last_eval=excluded.last_eval,
            last_value=excluded.last_value
        """, (
            rule['id'],
            rule['device_id'],
            rule['sensor_id'],
            rule['secondary_device_id'] or '',
            rule['secondary_sensor_id'] or '',
            new_state['in_alarm'],
            new_state['breach_start'],
            new_state['last_eval'],
            new_state['last_value']
        ))

        self.conn.commit()

    # ---------- LATEST ----------
    def update_latest(self, device, sensor, value, ts):
        self.conn.execute("""
        INSERT INTO latest_values VALUES(?,?,?,?)
        ON CONFLICT(device_id,sensor_id)
        DO UPDATE SET value=excluded.value, ts=excluded.ts
        """, (device, sensor, value, ts))
        self.conn.commit()

    def get_latest(self, device, sensor):
        row = self.conn.execute(
            "SELECT value FROM latest_values WHERE device_id=? AND sensor_id=?",
            (device, sensor)
        ).fetchone()
        return row['value'] if row else None

    # ---------- HISTORY ----------
    def save_history(self, alarm):
        self.conn.execute("""
        INSERT INTO alarm_history(rule_id,device_id,sensor_id,triggered_at,value,message)
        VALUES(?,?,?,?,?,?)
        """, (
            alarm['rule_id'],
            alarm['device_id'],
            alarm['sensor_id'],
            alarm['triggered_at'],
            alarm['value'],
            alarm['message']
        ))
        self.conn.commit()


# ---------------- MQTT PUBLISHER ----------------
class AlarmPublisher:
    def __init__(self):
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_start()

    def publish(self, alarm):
        topic = f"{MQTT_TOPIC_ALARM}/{alarm['device_id']}/{alarm['sensor_id']}"
        self.client.publish(topic, json.dumps(alarm), qos=1)


# ---------------- ENGINE ----------------
def compare(val, op, thresh):
    if op == '>': return val > thresh
    if op == '<': return val < thresh
    if op == '>=': return val >= thresh
    if op == '<=': return val <= thresh
    if op == '==': return val == thresh
    return False


def evaluate(rule, value, state, secondary_value, now):

    in_alarm = state['in_alarm'] if state else 0

    breach_start = (
        datetime.fromisoformat(state['breach_start'])
        if state and state['breach_start']
        else None
    )

    # conditional check
    # ---------- CONDITIONAL CHECK ----------
    if rule['condition_type'] == 'conditional':

        if secondary_value is None:
            # secondary sensor not yet reported → do NOT block rule
            pass

        else:
            if not compare(
                float(secondary_value),
                rule['secondary_operator'],
                float(rule['secondary_value'])
            ):
                return {
                    'in_alarm': 0,
                    'breach_start': None,
                    'last_eval': now.isoformat(),
                    'last_value': value
                }, None


    violated = compare(float(value), rule['operator'], float(rule['threshold_value']))
    alarm = None

    if violated:

        if breach_start is None:
            print("violation start")
            breach_start = now

        elif (now - breach_start).total_seconds() >= rule['duration_seconds'] and not in_alarm:
            print("alarm triggered")
            in_alarm = 1

            alarm = {
                'rule_id': rule['id'],
                'device_id': rule['device_id'],
                'sensor_id': rule['sensor_id'],
                'value': value,
                'triggered_at': now.isoformat(),
                'message': 'ALARM TRIGGERED'
            }

    else:
        breach_start = None
        in_alarm = 0

    return {
        'in_alarm': in_alarm,
        'breach_start': breach_start.isoformat() if breach_start else None,
        'last_eval': now.isoformat(),
        'last_value': value
    }, alarm


# ---------------- WORKER ----------------
def worker(_, queue):

    db = DB(DB_PATH)
    pub = AlarmPublisher()

    while True:
        try:
            msg = queue.get(timeout=1)
            if msg is None:
                break
        except Empty:
            continue

        device, sensor, value, ts = msg.values()

        db.update_latest(device, sensor, value, ts.isoformat())

        rules = db.get_rules(device, sensor)

        for rule in rules:

            state = db.get_state(rule)

            secondary_val = None
            if rule['condition_type'] == 'conditional':
                secondary_val = db.get_latest(
                    rule['secondary_device_id'],
                    rule['secondary_sensor_id']
                )

            new_state, alarm = evaluate(rule, value, state, secondary_val, ts)

            db.upsert_state(rule, new_state)

            if alarm:
                db.save_history(alarm)
                pub.publish(alarm)


# ---------------- MQTT LISTENER ----------------
def mqtt_listener(queue):

    def on_message(_, __, msg):
        try:
            payload = json.loads(msg.payload)

            device, sensor = msg.topic.split('/')[1:3]

            queue.put({
                'device': device,
                'sensor': sensor,
                'value': payload['value'],
                'ts': datetime.now(timezone.utc)
            }, timeout=1)

        except Full:
            logging.warning("Queue full")

        except Exception as e:
            logging.error(f"Bad MQTT payload: {e}")

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    print("mqtt connected", flush=True)

    client.subscribe(MQTT_TOPIC_TELEMETRY)
    client.loop_forever()


# ---------------- MAIN ----------------
def main():

    logging.basicConfig(level=logging.INFO)

    DB.init(DB_PATH)

    queue = mp.Queue(maxsize=QUEUE_MAXSIZE)

    workers = [mp.Process(target=worker, args=(i, queue)) for i in range(WORKERS)]
    for w in workers:
        w.start()

    Thread(target=mqtt_listener, args=(queue,), daemon=True).start()

    def shutdown(*_):
        for _ in workers:
            queue.put(None)
        for w in workers:
            w.join()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    signal.pause()


if __name__ == "__main__":
    main()

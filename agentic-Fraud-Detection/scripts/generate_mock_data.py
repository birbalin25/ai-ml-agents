"""
Mock Banking Dataset Generator for Agentic Fraud Triage
Generates: transactions, login_logs, user_profiles, known_fraud_signatures
Embeds realistic fraud patterns: impossible travel, MFA abuse, velocity anomaly, synthetic identity
"""

import csv
import json
import random
import uuid
import math
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

# --- Constants ---
N_USERS = 5000
N_TRANSACTIONS = 100000
N_LOGIN_LOGS = 200000
N_FRAUD_SIGNATURES = 200
FRAUD_RATE = 0.03  # 3% of users are fraud actors

CITIES = {
    "New York": (40.7128, -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298),
    "Houston": (29.7604, -95.3698),
    "Phoenix": (33.4484, -112.0740),
    "Philadelphia": (39.9526, -75.1652),
    "San Antonio": (29.4241, -98.4936),
    "San Diego": (32.7157, -117.1611),
    "Dallas": (32.7767, -96.7970),
    "Miami": (25.7617, -80.1918),
    "London": (51.5074, -0.1278),
    "Lagos": (6.5244, 3.3792),
    "Moscow": (55.7558, 37.6173),
    "Mumbai": (19.0760, 72.8777),
    "Shanghai": (31.2304, 121.4737),
}

US_CITIES = [c for c in CITIES if c not in ["London", "Lagos", "Moscow", "Mumbai", "Shanghai"]]
FOREIGN_CITIES = ["London", "Lagos", "Moscow", "Mumbai", "Shanghai"]

MERCHANTS = [
    ("MCH-001", "Amazon", "retail"), ("MCH-002", "Walmart", "retail"),
    ("MCH-003", "Shell Gas", "fuel"), ("MCH-004", "Starbucks", "food"),
    ("MCH-005", "Delta Airlines", "travel"), ("MCH-006", "Hilton Hotels", "travel"),
    ("MCH-007", "Best Buy", "electronics"), ("MCH-008", "Target", "retail"),
    ("MCH-009", "CVS Pharmacy", "health"), ("MCH-010", "Uber", "transport"),
    ("MCH-011", "Netflix", "subscription"), ("MCH-012", "Apple Store", "electronics"),
    ("MCH-013", "Whole Foods", "grocery"), ("MCH-014", "Home Depot", "home"),
    ("MCH-015", "Costco", "retail"), ("MCH-016", "Wire Transfer Svc", "wire"),
    ("MCH-017", "ACH Payment Svc", "ach"), ("MCH-018", "Zelle Transfer", "p2p"),
    ("MCH-019", "Crypto Exchange", "crypto"), ("MCH-020", "Foreign Exchange", "forex"),
]

TXN_TYPES = ["card_purchase", "wire_transfer", "ach_transfer", "atm_withdrawal", "p2p_transfer"]
CHANNELS = ["mobile_app", "web_browser", "branch", "atm", "phone_banking"]
DEVICES = ["iPhone 15", "Samsung S24", "Pixel 8", "iPad Pro", "MacBook", "Windows PC", "Android Tablet"]
BROWSERS = ["Chrome/120", "Safari/17", "Firefox/121", "Edge/120"]


def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3959
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def random_ip():
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def jitter_location(lat, lon, miles=20):
    delta = miles / 69.0
    return (lat + random.uniform(-delta, delta), lon + random.uniform(-delta, delta))


# --- Generate User Profiles ---
def generate_user_profiles():
    print("Generating user profiles...")
    users = []
    fraud_user_ids = set()

    for i in range(N_USERS):
        user_id = f"USR-{i+1:06d}"
        home_city = random.choice(US_CITIES)
        home_lat, home_lon = CITIES[home_city]
        account_age = random.randint(30, 3650)
        avg_monthly_txn = round(random.lognormvariate(7, 1.2), 2)  # median ~$1100
        is_fraud_actor = random.random() < FRAUD_RATE

        if is_fraud_actor:
            fraud_user_ids.add(user_id)
            risk_tier = random.choice(["high", "critical"])
        else:
            risk_tier = random.choices(["low", "medium", "high"], weights=[70, 25, 5])[0]

        users.append({
            "user_id": user_id,
            "account_age_days": account_age,
            "avg_monthly_txn": avg_monthly_txn,
            "home_city": home_city,
            "home_lat": round(home_lat, 6),
            "home_lon": round(home_lon, 6),
            "risk_tier": risk_tier,
            "card_number": f"4{random.randint(100,999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}",
            "email": f"user{i+1}@{'gmail' if random.random() > 0.3 else 'yahoo'}.com",
            "phone": f"+1{random.randint(200,999)}{random.randint(1000000,9999999)}",
            "created_at": (datetime.now() - timedelta(days=account_age)).strftime("%Y-%m-%d"),
        })

    with open(OUTPUT_DIR / "user_profiles.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=users[0].keys())
        writer.writeheader()
        writer.writerows(users)

    print(f"  -> {len(users)} users, {len(fraud_user_ids)} fraud actors")
    return users, fraud_user_ids


# --- Generate Login Logs ---
def generate_login_logs(users, fraud_user_ids):
    print("Generating login logs...")
    logs = []
    base_time = datetime.now() - timedelta(days=30)

    user_map = {u["user_id"]: u for u in users}

    for _ in range(N_LOGIN_LOGS):
        user = random.choice(users)
        uid = user["user_id"]
        is_fraud = uid in fraud_user_ids
        ts = base_time + timedelta(seconds=random.randint(0, 30 * 86400))

        home_lat, home_lon = user["home_lat"], user["home_lon"]

        if is_fraud and random.random() < 0.3:
            # Impossible travel: login from foreign city
            foreign_city = random.choice(FOREIGN_CITIES)
            lat, lon = CITIES[foreign_city]
            lat, lon = jitter_location(lat, lon, 5)
            ip = random_ip()
            mfa_change = random.random() < 0.4
            typing_cadence = round(random.uniform(0.1, 0.45), 3)  # abnormal
            device = random.choice(DEVICES)
            is_bot_signature = random.random() < 0.25
        else:
            lat, lon = jitter_location(home_lat, home_lon, 15)
            ip = random_ip()
            mfa_change = random.random() < 0.02 if not is_fraud else random.random() < 0.2
            typing_cadence = round(random.uniform(0.5, 1.0), 3)  # normal range
            device = random.choice(DEVICES[:4])
            is_bot_signature = False

        logs.append({
            "session_id": str(uuid.uuid4())[:12],
            "user_id": uid,
            "ip_address": ip,
            "geo_lat": round(lat, 6),
            "geo_lon": round(lon, 6),
            "device_fingerprint": device,
            "browser_agent": random.choice(BROWSERS),
            "mfa_change_flag": mfa_change,
            "mfa_change_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S") if mfa_change else "",
            "typing_cadence_score": typing_cadence,
            "is_bot_signature": is_bot_signature,
            "login_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "login_status": "success" if random.random() > 0.05 else "failed",
            "channel": random.choice(CHANNELS[:3]),
        })

    # Inject specific impossible travel sequences
    for uid in list(fraud_user_ids)[:50]:
        user = user_map[uid]
        ts1 = base_time + timedelta(days=random.randint(1, 28), hours=random.randint(8, 20))
        ts2 = ts1 + timedelta(minutes=random.randint(3, 8))  # < 10 min apart

        home_lat, home_lon = user["home_lat"], user["home_lon"]
        foreign_city = random.choice(FOREIGN_CITIES)
        f_lat, f_lon = CITIES[foreign_city]

        # Login 1: from home
        logs.append({
            "session_id": str(uuid.uuid4())[:12],
            "user_id": uid,
            "ip_address": random_ip(),
            "geo_lat": round(home_lat, 6),
            "geo_lon": round(home_lon, 6),
            "device_fingerprint": "iPhone 15",
            "browser_agent": "Safari/17",
            "mfa_change_flag": False,
            "mfa_change_timestamp": "",
            "typing_cadence_score": round(random.uniform(0.6, 0.9), 3),
            "is_bot_signature": False,
            "login_timestamp": ts1.strftime("%Y-%m-%d %H:%M:%S"),
            "login_status": "success",
            "channel": "mobile_app",
        })
        # Login 2: from foreign city minutes later
        logs.append({
            "session_id": str(uuid.uuid4())[:12],
            "user_id": uid,
            "ip_address": random_ip(),
            "geo_lat": round(f_lat + random.uniform(-0.05, 0.05), 6),
            "geo_lon": round(f_lon + random.uniform(-0.05, 0.05), 6),
            "device_fingerprint": "Windows PC",
            "browser_agent": "Chrome/120",
            "mfa_change_flag": True,
            "mfa_change_timestamp": ts2.strftime("%Y-%m-%d %H:%M:%S"),
            "typing_cadence_score": round(random.uniform(0.15, 0.4), 3),
            "is_bot_signature": True,
            "login_timestamp": ts2.strftime("%Y-%m-%d %H:%M:%S"),
            "login_status": "success",
            "channel": "web_browser",
        })

    logs.sort(key=lambda x: x["login_timestamp"])

    with open(OUTPUT_DIR / "login_logs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=logs[0].keys())
        writer.writeheader()
        writer.writerows(logs)

    print(f"  -> {len(logs)} login records")
    return logs


# --- Generate Transactions ---
def generate_transactions(users, fraud_user_ids, login_logs):
    print("Generating transactions...")
    txns = []
    base_time = datetime.now() - timedelta(days=30)
    user_map = {u["user_id"]: u for u in users}

    # Build login index for cross-referencing
    user_logins = {}
    for log in login_logs:
        uid = log["user_id"]
        if uid not in user_logins:
            user_logins[uid] = []
        user_logins[uid].append(log)

    for i in range(N_TRANSACTIONS):
        user = random.choice(users)
        uid = user["user_id"]
        is_fraud = uid in fraud_user_ids
        ts = base_time + timedelta(seconds=random.randint(0, 30 * 86400))

        if is_fraud and random.random() < 0.25:
            # Fraud: high-value wire/ach after MFA change
            txn_type = random.choice(["wire_transfer", "ach_transfer"])
            amount = round(random.uniform(10000, 95000), 2)
            merchant = random.choice([m for m in MERCHANTS if m[2] in ("wire", "ach", "crypto", "forex")])
            channel = "web_browser"
            is_fraud_txn = True
            fraud_pattern = random.choice([
                "mfa_change_high_value_wire",
                "impossible_travel_wire",
                "velocity_anomaly",
                "synthetic_identity",
            ])
        elif is_fraud and random.random() < 0.3:
            # Fraud: velocity attack - many small transactions
            txn_type = "card_purchase"
            amount = round(random.uniform(50, 500), 2)
            merchant = random.choice(MERCHANTS[:15])
            channel = random.choice(["mobile_app", "web_browser"])
            is_fraud_txn = True
            fraud_pattern = "velocity_anomaly"
        else:
            # Normal transaction
            txn_type = random.choices(TXN_TYPES, weights=[60, 5, 10, 15, 10])[0]
            if txn_type == "wire_transfer":
                amount = round(random.lognormvariate(8, 1.5), 2)
            elif txn_type == "atm_withdrawal":
                amount = round(random.choice([20, 40, 60, 80, 100, 200, 300, 500]), 2)
            else:
                amount = round(random.lognormvariate(3.5, 1.3), 2)
            amount = min(amount, 100000)
            merchant = random.choice(MERCHANTS)
            channel = random.choice(CHANNELS)
            is_fraud_txn = False
            fraud_pattern = ""

        txns.append({
            "transaction_id": f"TXN-{uuid.uuid4().hex[:10].upper()}",
            "user_id": uid,
            "amount": amount,
            "currency": "USD",
            "txn_type": txn_type,
            "merchant_id": merchant[0],
            "merchant_name": merchant[1],
            "merchant_category": merchant[2],
            "channel": channel,
            "txn_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "card_number_masked": f"****-****-****-{user['card_number'][-4:]}",
            "is_international": random.random() < (0.3 if is_fraud else 0.05),
            "is_fraud": is_fraud_txn,
            "fraud_pattern": fraud_pattern,
        })

    # Inject velocity bursts for fraud users
    for uid in list(fraud_user_ids)[:30]:
        user = user_map[uid]
        burst_time = base_time + timedelta(days=random.randint(1, 28), hours=random.randint(10, 22))
        for j in range(random.randint(6, 15)):
            t = burst_time + timedelta(seconds=random.randint(0, 300))  # within 5 min
            txns.append({
                "transaction_id": f"TXN-{uuid.uuid4().hex[:10].upper()}",
                "user_id": uid,
                "amount": round(random.uniform(100, 999), 2),
                "currency": "USD",
                "txn_type": "card_purchase",
                "merchant_id": random.choice(MERCHANTS[:8])[0],
                "merchant_name": random.choice(MERCHANTS[:8])[1],
                "merchant_category": random.choice(MERCHANTS[:8])[2],
                "channel": "web_browser",
                "txn_timestamp": t.strftime("%Y-%m-%d %H:%M:%S"),
                "card_number_masked": f"****-****-****-{user['card_number'][-4:]}",
                "is_international": False,
                "is_fraud": True,
                "fraud_pattern": "velocity_burst",
            })

    # Inject high-value wires after MFA change for fraud users
    for uid in list(fraud_user_ids)[:40]:
        user = user_map[uid]
        logins = user_logins.get(uid, [])
        mfa_logins = [l for l in logins if l["mfa_change_flag"]]
        if mfa_logins:
            mfa_log = random.choice(mfa_logins)
            mfa_ts = datetime.strptime(mfa_log["login_timestamp"], "%Y-%m-%d %H:%M:%S")
            wire_ts = mfa_ts + timedelta(hours=random.uniform(0.5, 20))
            txns.append({
                "transaction_id": f"TXN-{uuid.uuid4().hex[:10].upper()}",
                "user_id": uid,
                "amount": round(random.uniform(15000, 85000), 2),
                "currency": "USD",
                "txn_type": "wire_transfer",
                "merchant_id": "MCH-016",
                "merchant_name": "Wire Transfer Svc",
                "merchant_category": "wire",
                "channel": "web_browser",
                "txn_timestamp": wire_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "card_number_masked": f"****-****-****-{user['card_number'][-4:]}",
                "is_international": random.random() < 0.6,
                "is_fraud": True,
                "fraud_pattern": "mfa_change_high_value_wire",
            })

    txns.sort(key=lambda x: x["txn_timestamp"])

    with open(OUTPUT_DIR / "transactions.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=txns[0].keys())
        writer.writeheader()
        writer.writerows(txns)

    fraud_count = sum(1 for t in txns if t["is_fraud"])
    print(f"  -> {len(txns)} transactions, {fraud_count} fraudulent ({fraud_count/len(txns)*100:.1f}%)")
    return txns


# --- Generate Known Fraud Signatures ---
def generate_fraud_signatures():
    print("Generating known fraud signatures...")
    patterns = [
        ("impossible_travel", "User login from geographically distant locations within impossible timeframe indicating compromised credentials"),
        ("mfa_change_high_value", "MFA settings changed shortly before high-value wire transfer initiated, typical account takeover pattern"),
        ("velocity_burst", "Rapid succession of card transactions within minutes suggesting automated fraud bot"),
        ("synthetic_identity", "New account with thin credit file initiating high-value international transfers"),
        ("device_switching", "Rapid switching between multiple devices and browsers suggesting credential sharing"),
        ("ip_hopping", "Login attempts from multiple IP addresses across different countries in short timeframe"),
        ("bot_typing_pattern", "Abnormally consistent typing cadence suggesting automated input rather than human"),
        ("late_night_wire", "High-value wire transfers initiated during unusual hours for the account holder timezone"),
        ("dormant_account_spike", "Long-dormant account suddenly showing high transaction volume"),
        ("round_amount_pattern", "Multiple transactions at exact round dollar amounts suggesting test transactions"),
    ]

    signatures = []
    for i in range(N_FRAUD_SIGNATURES):
        pattern_type, base_desc = random.choice(patterns)
        severity = random.choices(["critical", "high", "medium"], weights=[20, 40, 40])[0]

        # Generate a mock feature vector (in production this would be a real embedding)
        feature_vector = [round(random.gauss(0, 1), 4) for _ in range(64)]

        signatures.append({
            "pattern_id": f"SIG-{i+1:04d}",
            "pattern_type": pattern_type,
            "description": f"{base_desc}. Variant {i+1}: detected in {random.choice(['retail', 'wire', 'p2p', 'atm', 'international'])} channel with {random.choice(['high', 'medium'])} confidence.",
            "severity": severity,
            "feature_vector": feature_vector,
            "detection_count": random.randint(10, 5000),
            "last_seen": (datetime.now() - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
            "created_at": (datetime.now() - timedelta(days=random.randint(90, 730))).strftime("%Y-%m-%d"),
        })

    with open(OUTPUT_DIR / "known_fraud_signatures.json", "w") as f:
        json.dump(signatures, f, indent=2)

    # Also write a CSV version without the vector for easier Delta loading
    csv_sigs = []
    for s in signatures:
        row = {k: v for k, v in s.items() if k != "feature_vector"}
        row["feature_vector"] = json.dumps(s["feature_vector"])
        csv_sigs.append(row)

    with open(OUTPUT_DIR / "known_fraud_signatures.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_sigs[0].keys())
        writer.writeheader()
        writer.writerows(csv_sigs)

    print(f"  -> {len(signatures)} fraud signatures")
    return signatures


# --- Main ---
if __name__ == "__main__":
    print("=" * 60)
    print("Fraud Triage Agent - Mock Data Generator")
    print("=" * 60)

    users, fraud_ids = generate_user_profiles()
    login_logs = generate_login_logs(users, fraud_ids)
    transactions = generate_transactions(users, fraud_ids, login_logs)
    signatures = generate_fraud_signatures()

    print("=" * 60)
    print("Data generation complete! Files saved to:", OUTPUT_DIR)
    for f in OUTPUT_DIR.iterdir():
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name}: {size_mb:.1f} MB")
    print("=" * 60)

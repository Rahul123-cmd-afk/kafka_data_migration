"""
Kafka Bulk Producer — Generate banking transactions
Usage inside a Kafka-reachable pod:
  python producer.py --bootstrap-server <host>:9093 --count 10000
"""
import json
import random
import argparse
import sys
import time
import os
from datetime import datetime, timedelta

# ============================================================
# Data pools
# ============================================================
BRANCHES = ["BR001", "BR002", "BR003", "BR004", "BR005"]
CHANNELS = ["NEFT", "RTGS", "UPI", "IMPS", "ATM", "POS", "CASH", "INTERNET_BANKING"]
STATUSES = ["SUCCESS"] * 8 + ["FLAGGED", "FAILED"]
TXN_TYPES = ["CREDIT", "DEBIT"]
CUSTOMER_IDS = [f"CUST{str(i).zfill(6)}" for i in range(1, 501)]
ACCOUNT_IDS = [f"ACCT{str(i).zfill(6)}" for i in range(1, 501)]

NARRATIONS_CREDIT = [
    "SALARY {month} 2026 - {company}", "NEFT CREDIT FROM {name}",
    "RENTAL INCOME", "FD MATURITY CREDIT", "MUTUAL FUND REDEMPTION",
    "BUSINESS INCOME", "GST REFUND", "INSURANCE CLAIM SETTLEMENT",
    "PENSION CREDIT", "DIVIDEND INCOME",
]
NARRATIONS_DEBIT = [
    "EMI PAYMENT - {loan_type}", "RENT PAYMENT", "ELECTRICITY BILL - {provider}",
    "UPI TRANSFER TO {name}", "ATM CASH WITHDRAWAL", "INSURANCE PREMIUM - {company}",
    "CREDIT CARD BILL PAYMENT", "SIP INVESTMENT", "GST PAYMENT",
    "SUPPLIER PAYMENT", "SCHOOL FEE", "MEDICAL BILL",
    "GROCERY PURCHASE", "FUEL STATION",
]
COMPANIES = ["INFOSYS", "TCS", "WIPRO", "HCL", "COGNIZANT", "ACCENTURE",
             "MINDTREE", "TECH MAHINDRA", "MPHASIS", "L&T INFOTECH"]
NAMES = ["Rajesh", "Priya", "Arun", "Kavitha", "Suresh", "Deepa",
         "Venkatesh", "Lakshmi", "Karthik", "Anjali", "Mohammed", "Pooja"]
PROVIDERS = ["BESCOM", "TANGEDCO", "TSSPDCL", "CESC", "BSES", "MSEDCL"]
LOAN_TYPES = ["HOME LOAN", "PERSONAL LOAN", "CAR LOAN", "BUSINESS LOAN"]
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


# ============================================================
# Generators
# ============================================================
def generate_amount(channel):
    ranges = {
        "ATM": (500, 25000), "UPI": (10, 100000), "POS": (100, 50000),
        "IMPS": (100, 200000), "NEFT": (1000, 1000000),
        "RTGS": (200000, 10000000), "CASH": (1000, 500000),
        "INTERNET_BANKING": (100, 500000),
    }
    low, high = ranges.get(channel, (100, 100000))
    amount = round(random.uniform(low, high), 2)
    if random.random() < 0.02:
        amount = round(random.uniform(900000, 5000000), 2)
    return amount


def generate_narration(txn_type):
    template = random.choice(NARRATIONS_CREDIT if txn_type == "CREDIT" else NARRATIONS_DEBIT)
    return template.format(
        month=random.choice(MONTHS), company=random.choice(COMPANIES),
        name=random.choice(NAMES), provider=random.choice(PROVIDERS),
        loan_type=random.choice(LOAN_TYPES),
    )


def generate_transaction(seq_num, base_date):
    txn_type = random.choice(TXN_TYPES)
    channel = random.choice(CHANNELS)
    idx = random.randint(0, len(CUSTOMER_IDS) - 1)
    offset = random.randint(0, 86400)
    txn_time = base_date + timedelta(seconds=offset)
    amount = generate_amount(channel)
    balance = round(random.uniform(10000, 5000000), 2)
    status = random.choice(STATUSES)
    if channel == "CASH" and amount > 500000:
        status = "FLAGGED"
    return {
        "txn_id": f"TXN{base_date.strftime('%Y%m%d')}{str(seq_num).zfill(6)}",
        "account_id": ACCOUNT_IDS[idx],
        "customer_id": CUSTOMER_IDS[idx],
        "txn_type": txn_type,
        "amount": amount,
        "currency": "INR",
        "txn_date": txn_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "value_date": txn_time.strftime("%Y-%m-%d"),
        "branch_code": random.choice(BRANCHES),
        "channel": channel,
        "narration": generate_narration(txn_type),
        "balance_after": balance,
        "status": status,
        "ref_number": f"{channel}{base_date.strftime('%Y%m%d')}{str(seq_num).zfill(6)}",
    }


def generate_aml_alert(txn):
    alert_types = [
        ("STRUCTURING", "RULE_CTR_SPLIT", "Multiple transactions structured"),
        ("HIGH_VALUE", "RULE_HIGH_VALUE", f"High value {txn['channel']} {txn['amount']}"),
        ("VELOCITY", "RULE_HIGH_VELOCITY", "Unusual velocity"),
        ("UNUSUAL_PATTERN", "RULE_PATTERN_BREAK", "Pattern deviation"),
    ]
    alert_type, rule, desc = random.choice(alert_types)
    return {
        "alert_id": f"AML{txn['txn_date'][:10].replace('-', '')}{random.randint(10000, 99999)}",
        "customer_id": txn["customer_id"],
        "alert_date": txn["txn_date"],
        "alert_type": alert_type,
        "risk_score": random.randint(60, 99),
        "rule_triggered": rule,
        "transaction_ids": txn["txn_id"],
        "total_amount": txn["amount"],
        "currency": "INR",
        "description": desc,
        "status": random.choice(["OPEN", "UNDER_REVIEW"]),
        "priority": random.choice(["CRITICAL", "HIGH", "MEDIUM"]),
    }


# ============================================================
# DNS check — fail fast if Kafka not reachable
# ============================================================
def check_dns(bootstrap_server):
    """Verify we can resolve the Kafka hostname before attempting connection."""
    import socket
    host = bootstrap_server.split(":")[0]
    try:
        ip = socket.gethostbyname(host)
        print(f"✓ DNS OK: {host} → {ip}")
        return True
    except socket.gaierror as e:
        print(f"✗ DNS FAILED: cannot resolve '{host}'")
        print(f"  Reason: {e}")
        print(f"  Are you running INSIDE the OpenShift cluster?")
        print(f"  If you're on a laptop, this won't work — use 'oc exec' instead.")
        return False


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Kafka Bulk Producer")
    parser.add_argument("--bootstrap-server", default=None,
                        help="Kafka bootstrap server (host:port)")
    parser.add_argument("--username", default=None,
                        help="SASL username")
    parser.add_argument("--password", default=None,
                        help="SASL password")
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--save-fallback", action="store_true",
                        help="If Kafka fails, save JSON files (default: fail loudly)")
    args = parser.parse_args()

    # ✅ FIX 1: Resolve config from args → env → default
    bootstrap = args.bootstrap_server or os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9093  ")
    username = args.username or os.environ.get("KAFKA_USERNAME", "app-user")
    password = args.password or os.environ.get(
        "KAFKA_PASSWORD", "bwDqbYGrgC2AKOMuthoUu7Ckkj8tNjtB")

    print("=" * 60)
    print("Kafka Bulk Producer")
    print(f"  Bootstrap:  {bootstrap}")
    print(f"  Username:   {username}")
    print(f"  Password:   {'*' * len(password)}")
    print(f"  Count:      {args.count}")
    print(f"  Days:       {args.days}")
    print(f"  Dry-run:    {args.dry_run}")
    print("=" * 60)

    # ✅ FIX 2: DNS check BEFORE generating data
    if not args.dry_run:
        if not check_dns(bootstrap):
            print("\nAborting — DNS resolution failed.")
            print("Fix: Run this script INSIDE a Kafka-reachable pod")
            print("  oc exec -it kafka-test-client -n simplelogic -- python ...")
            return 2

    # ---- Generate data ----
    base_date = datetime(2026, 3, 15)
    transactions = []
    aml_alerts = []

    print(f"\nGenerating {args.count} transactions across {args.days} days...")
    for i in range(1, args.count + 1):
        day_offset = random.randint(0, args.days - 1)
        txn_date = base_date + timedelta(days=day_offset)
        txn = generate_transaction(i, txn_date)
        transactions.append(txn)
        if txn["status"] == "FLAGGED":
            aml_alerts.append(generate_aml_alert(txn))
        if i % 2000 == 0:
            print(f"  ...{i} generated")

    print(f"Generated {len(transactions)} transactions, {len(aml_alerts)} AML alerts")

    # ---- Dry-run: print samples ----
    if args.dry_run:
        print("\nDRY RUN — first 3 transactions:")
        for txn in transactions[:3]:
            print(json.dumps(txn, indent=2))
        print(f"... and {len(transactions) - 3} more")
        return 0

    # ---- Import kafka-python ----
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
    except ImportError:
        print("\n✗ ERROR: kafka-python not installed")
        print("  Install: pip install kafka-python==2.0.2")
        return 3

    # ---- Create producer ----
    print("\nConnecting to Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,             # ✅ from args/env, not hardcoded
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            ssl_cafile="/etc/kafka/ca.crt",
            sasl_plain_username=username,            # ✅ from args/env
            sasl_plain_password=password,            # ✅ from args/env
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
            batch_size=32768,
            linger_ms=10,
            api_version_auto_timeout_ms=30000,
            request_timeout_ms=60000,
            compression_type="gzip",
        )
    except Exception as e:
        print(f"✗ Failed to create KafkaProducer: {e}")
        return 4

    # ---- Publish ----
    start = time.time()
    try:
        print(f"Publishing {len(transactions)} txns to 'finacle-transactions'...")
        for txn in transactions:
            producer.send("finacle-transactions", key=txn["txn_id"], value=txn)

        if aml_alerts:
            print(f"Publishing {len(aml_alerts)} alerts to 'aml-alerts'...")
            for alert in aml_alerts:
                producer.send("aml-alerts", key=alert["alert_id"], value=alert)

        print("Flushing (waiting for acks)...")
        producer.flush(timeout=120)
        producer.close()

    except KafkaError as e:
        print(f"\n✗ Kafka error: {e}")
        if args.save_fallback:
            _save_fallback(transactions, aml_alerts)
        return 5
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        if args.save_fallback:
            _save_fallback(transactions, aml_alerts)
        return 6

    elapsed = time.time() - start
    throughput = len(transactions) / elapsed if elapsed > 0 else 0

    print()
    print("=" * 60)
    print("✓ Published successfully")
    print(f"  Transactions: {len(transactions)} → finacle-transactions")
    print(f"  AML Alerts:   {len(aml_alerts)} → aml-alerts")
    print(f"  Time:         {elapsed:.2f}s")
    print(f"  Throughput:   {throughput:.0f} msg/s")
    print("=" * 60)
    return 0


def _save_fallback(transactions, aml_alerts):
    """Fallback: save to /tmp as JSON lines."""
    import tempfile
    tmpdir = tempfile.gettempdir()
    txn_path = os.path.join(tmpdir, "transactions.json")
    alert_path = os.path.join(tmpdir, "aml_alerts.json")

    with open(txn_path, "w") as f:
        for txn in transactions:
            f.write(json.dumps(txn) + "\n")
    with open(alert_path, "w") as f:
        for alert in aml_alerts:
            f.write(json.dumps(alert) + "\n")

    # ✅ FIX 4: use os.path for proper separator (no hardcoded \\)
    print(f"Fallback written to:")
    print(f"  {txn_path}")
    print(f"  {alert_path}")


if __name__ == "__main__":
    sys.exit(main())

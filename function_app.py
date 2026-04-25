import os
import json
import decimal
import pyodbc
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def json_response(payload, status_code=200):
    return func.HttpResponse(
        json.dumps(payload, default=serialize_value),
        mimetype="application/json",
        status_code=status_code,
    )


def serialize_value(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    return str(value)


def get_connection():
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    missing = [
        name for name, value in {
            "SQL_SERVER": server,
            "SQL_DATABASE": database,
            "SQL_USERNAME": username,
            "SQL_PASSWORD": password,
        }.items() if not value
    ]
    if missing:
        raise ValueError(f"Missing required app settings: {', '.join(missing)}")

    safe_password = password.replace("}", "}}")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{server},1433;"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={{{safe_password}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "HostNameInCertificate=*.database.windows.net;"
        "Login Timeout=30;"
    )
    return pyodbc.connect(conn_str)


@app.route(route="people", methods=["GET"])
def people(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT person_id, display_name, created_at, updated_at
            FROM people
            ORDER BY display_name
        """)

        rows = cursor.fetchall()
        results = [
            {
                "person_id": row.person_id,
                "display_name": row.display_name,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in rows
        ]

        cursor.close()
        conn.close()
        return json_response(results)

    except Exception as e:
        return json_response({"error": str(e)}, status_code=500)


@app.route(route="balances", methods=["GET"])
def balances(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                d.person_id AS debtor_person_id,
                d.display_name AS debtor_name,
                c.person_id AS creditor_person_id,
                c.display_name AS creditor_name,
                COALESCE(SUM(l.amount), 0) AS amount,
                l.currency
            FROM ledger l
            JOIN people d
                ON l.debtor_person_id = d.person_id
            JOIN people c
                ON l.creditor_person_id = c.person_id
            GROUP BY
                d.person_id,
                d.display_name,
                c.person_id,
                c.display_name,
                l.currency
            ORDER BY
                debtor_name,
                creditor_name
        """)

        rows = cursor.fetchall()
        results = [
            {
                "debtor_person_id": row.debtor_person_id,
                "debtor_name": row.debtor_name,
                "creditor_person_id": row.creditor_person_id,
                "creditor_name": row.creditor_name,
                "amount": row.amount,
                "currency": row.currency,
            }
            for row in rows
        ]

        cursor.close()
        conn.close()
        return json_response(results)

    except Exception as e:
        return json_response({"error": str(e)}, status_code=500)


@app.route(route="ledger", methods=["POST"])
def ledger(req: func.HttpRequest) -> func.HttpResponse:
    conn = None
    cursor = None

    try:
        body = req.get_json()

        debtor_name = body.get("debtor_name")
        creditor_name = body.get("creditor_name")
        amount = body.get("amount")
        memo = body.get("memo", "")
        currency = body.get("currency", "USD")

        if not debtor_name or not creditor_name:
            return json_response(
                {"error": "debtor_name and creditor_name are required"},
                status_code=400,
            )

        if amount is None:
            return json_response({"error": "amount is required"}, status_code=400)

        try:
            amount = decimal.Decimal(str(amount))
        except Exception:
            return json_response({"error": "amount must be numeric"}, status_code=400)

        if amount <= 0:
            return json_response({"error": "amount must be positive"}, status_code=400)

        if currency != "USD":
            return json_response({"error": "currency must be USD"}, status_code=400)

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT person_id, display_name FROM people WHERE display_name = ?",
            debtor_name,
        )
        debtor = cursor.fetchone()
        if not debtor:
            return json_response(
                {"error": f"Debtor not found in people: {debtor_name}"},
                status_code=400,
            )

        cursor.execute(
            "SELECT person_id, display_name FROM people WHERE display_name = ?",
            creditor_name,
        )
        creditor = cursor.fetchone()
        if not creditor:
            return json_response(
                {"error": f"Creditor not found in people: {creditor_name}"},
                status_code=400,
            )

        cursor.execute("""
            INSERT INTO ledger (
                entry_date,
                debtor_person_id,
                creditor_person_id,
                amount,
                currency,
                memo,
                created_at
            )
            OUTPUT INSERTED.entry_id
            VALUES (
                CAST(GETUTCDATE() AS date),
                ?,
                ?,
                ?,
                ?,
                ?,
                GETUTCDATE()
            )
        """, debtor.person_id, creditor.person_id, amount, currency, memo)

        inserted = cursor.fetchone()
        conn.commit()

        return json_response(
            {
                "success": True,
                "entry_id": inserted.entry_id,
                "debtor": {
                    "person_id": debtor.person_id,
                    "display_name": debtor.display_name,
                },
                "creditor": {
                    "person_id": creditor.person_id,
                    "display_name": creditor.display_name,
                },
                "amount": amount,
                "currency": currency,
                "memo": memo,
            },
            status_code=201,
        )

    except ValueError as e:
        return json_response({"error": str(e)}, status_code=500)

    except Exception as e:
        if conn:
            conn.rollback()
        return json_response({"error": str(e)}, status_code=500)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
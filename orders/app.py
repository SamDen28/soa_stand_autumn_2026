"""Orders — наивная реализация.

Оформление заказа: зарезервировать товар в Inventory, списать деньги в
Payment, записать заказ в базу. Пока Payment и Inventory работают без отказов, всё работает.

Задание 2 (необязательное): об оплаченном заказе сервис сообщает в шину
событий (order.paid), а за подтверждённую доставку начисляет покупателю
бонусы. Пока шина работает и Orders не падает, тоже всё работает.
"""

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import random

import psycopg

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
# Шина событий; пусто — события не публикуются.
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    items           JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_sent      BOOLEAN NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS bonus_events (
    id       TEXT PRIMARY KEY,
    user_id  TEXT NOT NULL
);
"""


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def init_db():
    # База может подниматься дольше сервиса — ждём её.
    for attempt in range(60):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as e:
            log.info("база недоступна (%s), жду", e)
            time.sleep(1)
    raise SystemExit("не дождался базы")


def call(method, url, body=None, headers=None, timeout=0.5):
    """HTTP-вызов Payment или Inventory. Возвращает (код, тело)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, None


def reserve(order_id, items):
    code, _ = call("POST", f"{INVENTORY_URL}/reservations", {
        "order_id": order_id,
        "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
    })
    if code not in (200, 201):
        raise RuntimeError(f"inventory: {code}")


def release(order_id):
    code, _ = call("DELETE", f"{INVENTORY_URL}/reservations/{order_id}")
    if code != 204:
        raise RuntimeError(f"inventory release: {code}")


_pay_lock = threading.Lock()
_pay = {"failures": 0, "open_until": 0.0, "probe_until": 0.0}


def payment_allowed():
    with _pay_lock:
        if _pay["failures"] == 0:
            return True
        now = time.monotonic()
        if now < _pay["open_until"] or now < _pay["probe_until"]:
            return False
        _pay["probe_until"] = now + CHARGE_TIMEOUT + 1
        return True


def payment_result(ok):
    with _pay_lock:
        _pay["probe_until"] = 0.0
        if ok:
            _pay["failures"] = 0
            _pay["open_until"] = 0.0
        else:
            _pay["failures"] += 1
            pause = min(3.0, 0.5 * 2 ** (_pay["failures"] - 1))
            _pay["open_until"] = time.monotonic() + pause


CHARGE_TIMEOUT = 3.0

def charge_once(order_id, amount):
    try:
        code, _ = call("POST", f"{PAYMENT_URL}/payments", {
            "order_id": order_id,
            "amount_cents": amount,
            "currency": "RUB",
        }, headers={"Idempotency-Key": order_id}, timeout=CHARGE_TIMEOUT)
    except Exception as e:
        log.warning("order %s: payment недоступен (%s)", order_id, e)
        payment_result(False)
        return None
    payment_result(True)
    if code in (200, 201):
        return "paid"
    if code == 402:
        return "declined"
    return None


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        if not isinstance(i.get("sku"), str) or not i["sku"]:
            return False
        for k, minimum in (("qty", 1), ("price_cents", 0)):
            v = i.get(k)
            if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
                return False
    return True


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])

    with db() as conn:
        conn.execute(
            "INSERT INTO orders (id, user_id, status, amount_cents, items, attempt, next_attempt_at) "
            "VALUES (%s, %s, %s, %s, %s, 0, now())",
            (order_id, body["user_id"], "pending", amount, json.dumps(body["items"])),
        )

    log.info("order %s: pending", order_id)
    return {"id": order_id, "status": "pending"}


def payment_wait():
    with _pay_lock:
        return max(0.1, _pay["open_until"] - time.monotonic())


def _defer(order_id, secs):
    with db() as conn:
        conn.execute(
            "UPDATE orders SET status = 'pending', "
            "next_attempt_at = now() + make_interval(secs => %s) WHERE id = %s",
            (secs, order_id))


def _backoff(order_id):
    with db() as conn:
        attempt = conn.execute(
            "UPDATE orders SET attempt = attempt + 1 WHERE id = %s RETURNING attempt",
            (order_id,)).fetchone()[0]
    _defer(order_id, random.uniform(0.5, 1.0) * min(3.0, 0.2 * 2 ** min(attempt, 5)))


def process_order(order_id, items, amount):
    try:
        try:
            reserve(order_id, items)
        except (RuntimeError, OSError) as e:
            log.warning("order %s: inventory (%s)", order_id, e)
            return _backoff(order_id)

        if not payment_allowed():
            return _defer(order_id, payment_wait())

        result = charge_once(order_id, amount)
        if result is None:
            return _backoff(order_id)

        if result == "declined":
            try:
                release(order_id)
            except (RuntimeError, OSError) as e:
                log.warning("order %s: release (%s)", order_id, e)
                return _backoff(order_id)
            status = "rejected"
        else:
            status = "paid"

        with db() as conn:
            conn.execute(
                "UPDATE orders SET status = %s, event_sent = (status = 'paid') WHERE id = %s",
                (status, order_id),
            )
        log.info("order %s: %s", order_id, status)
    except Exception:
        log.exception("process_order %s", order_id)
        _backoff(order_id)


def worker_loop():
    while True:
        try:
            with db() as conn:
                row = conn.execute(
                    "UPDATE orders SET status = 'processing', next_attempt_at = now() + interval '10 seconds' "
                    "WHERE id = ("
                    "  SELECT id FROM orders "
                    "  WHERE status IN ('pending', 'processing') AND next_attempt_at <= now() "
                    "  ORDER BY next_attempt_at ASC LIMIT 1 "
                    "  FOR UPDATE SKIP LOCKED"
                    ") "
                    "RETURNING id, items, amount_cents"
                ).fetchone()
            if row is None:
                time.sleep(0.5)
                continue
            order_id, items, amount = row[0], row[1], row[2]
            process_order(order_id, items, amount)
        except Exception:
            log.exception("worker")
            time.sleep(1)


def publish_paid(order_id):
    """Сообщает в шину, что заказ оплачен."""
    if not EVENTS_URL:
        return True
    try:
        code, _ = call("POST", f"{EVENTS_URL}/events", {
            "id": order_id,
            "type": "order.paid",
            "order_id": order_id,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
    except OSError as e:
        log.warning("order %s: событие не опубликовано (%s)", order_id, e)
        return False
    if code == 202:
        return True
    log.warning("order %s: событие не опубликовано (код %s)", order_id, code)
    return False


def delivery_confirmed(event):
    """Начисляет бонусы за подтверждённую доставку."""
    with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bonus_events (id, user_id) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                (event["id"], event["user_id"]),
            )
            if cur.rowcount == 0:
                conn.rollback()
                return
            cur.execute(
                "INSERT INTO bonuses (user_id, points) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points",
                (event["user_id"], event["bonus_points"]),
            )
        conn.commit()

def publisher_loop():
    while True:
        try:
            with db() as conn:
                rows = conn.execute(
                    "SELECT id FROM orders WHERE status = 'paid' AND event_sent = false LIMIT 20"
                ).fetchall()
            if not rows:
                time.sleep(1)
                continue
            for (order_id,) in rows:
                if publish_paid(order_id):
                    with db() as conn:
                        conn.execute(
                            "UPDATE orders SET event_sent = true WHERE id = %s", (order_id,)
                        )
        except Exception:
            log.exception("publisher")
            time.sleep(1)


def get_bonus(user_id):
    with db() as conn:
        row = conn.execute("SELECT points FROM bonuses WHERE user_id = %s", (user_id,)).fetchone()
    return {"points": row[0] if row else 0}


def get_order(order_id):
    with db() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None:
        return None
    status = "pending" if row[1] == "processing" else row[1]
    return {"id": row[0], "status": status, "amount_cents": row[2]}


ORDER_PATH = re.compile(r"^/orders/([^/]+)$")
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self.send_json(200, {"status": "ok"})
        m = ORDER_PATH.match(self.path)
        if m:
            order = get_order(m.group(1))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        m = BONUS_PATH.match(self.path)
        if m:
            return self.send_json(200, get_bonus(urllib.parse.unquote(m.group(1))))
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if self.path == "/webhooks/delivery":
            return self.webhook(body)
        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def webhook(self, body):
        if not isinstance(body, dict) or not isinstance(body.get("id"), str) \
                or not isinstance(body.get("user_id"), str) \
                or not isinstance(body.get("bonus_points"), int) or body["bonus_points"] <= 0:
            return self.send_json(400, {"error": "bad_request"})
        try:
            delivery_confirmed(body)
        except Exception as e:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal", "message": str(e)})
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    for _ in range(8):
        threading.Thread(target=worker_loop, daemon=True).start()
    threading.Thread(target=publisher_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()

"""End-to-end tests with real FastAPI apps (in-process ASGI transport) and
separate SQLite databases - no network sockets or Docker required.

Run: PYTHONPATH=/workspace:/workspace/.pylibs \
     python3 -m unittest tests.test_integration -v
"""
import copy
import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import httpx
from fastapi.testclient import TestClient

from app.analyzer.app import create_app
from app.analyzer import db as analyzer_db
from app.webapp import db as web_db
from app.webapp import main as web_main
from app.webapp.analyzer_client import AnalyzerClient, RemoteError
from app.webapp.services import StaleCorrection


V1 = {
    "type": "record", "name": "InstrumentEvent", "namespace": "lab.warehouse",
    "fields": [
        {"name": "sample_id", "type": "string"},
        {"name": "project", "type": "string"},
        {"name": "measurement", "type": "double"},
    ],
}


def writer_v2():
    s = copy.deepcopy(V1)
    # added optional field with default: old readers stay compatible; numeric
    # promotion int->long inside an array exercises arrays + promotion.
    s["fields"].append({"name": "instrument_id", "type": "string",
                        "default": "UNKNOWN"})
    s["fields"].append({"name": "tags",
                        "type": {"type": "array", "items": "int"},
                        "default": []})
    return s


def bad_writer_v2():
    s = copy.deepcopy(V1)
    # string -> int has no resolution path: must be blocked
    s["fields"][0] = {"name": "sample_id", "type": "int"}
    return s


def reader_v2_with_default():
    s = copy.deepcopy(V1)
    s["fields"].append({"name": "temperature", "type": "double",
                        "default": 0.0})
    s["fields"].append({"name": "quality_flag",
                        "type": ["null", "string"], "default": None})
    return s


def bad_reader_drops_required():
    s = copy.deepcopy(V1)
    # reader no longer contains 'measurement' but retained v1 events have it;
    # field removal on the reader side against old writers is fine in Avro,
    # so instead add a required field with no default to force failure.
    s["fields"].append({"name": "new_required", "type": "string"})
    return s


class _TestClientTransport:
    """Transport adapter exposing the synchronous httpx transport surface on
    top of Starlette's TestClient portal (httpx 0.28's ASGITransport is
    async-only)."""

    def __init__(self, asgi_app):
        self._tc = TestClient(asgi_app)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        tc = self._tc
        method = request.method
        url = str(request.url).split(request.url.scheme + "://"
                                     + request.url.host)[-1]
        if not url:
            url = "/"
        content = request.content if request.content else None
        headers = dict(request.headers)
        r = tc.request(method, url, content=content, headers=headers)
        return httpx.Response(r.status_code, headers=dict(r.headers),
                              content=r.content, request=request)


class ASGIAnalyzerClient(AnalyzerClient):
    """HTTP client talking to an ASGI app over an in-process transport."""

    def __init__(self, asgi_app):
        super().__init__("http://asgi-local")
        self._transport = _TestClientTransport(asgi_app)

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, transport=self._transport,
                            timeout=10)


class PlatformHarness:
    """Spin up webapp + two analyzers with isolated temp databases."""

    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="avroplat-")
        self.web_db_path = os.path.join(self.tmp, "webapp.db")
        self.a_db = os.path.join(self.tmp, "a.db")
        self.b_db = os.path.join(self.tmp, "b.db")

        # Two fully independent analyzer apps, each its own SQLite database.
        analyzer_db.init(self.a_db)
        analyzer_db.init(self.b_db)
        self.app_a = create_app("analyzer-a", self.a_db)
        self.app_b = create_app("analyzer-b", self.b_db)

        # Webapp
        os.environ["WEBAPP_DB"] = self.web_db_path
        web_db.init(self.web_db_path)
        self.service = web_main.SwitchService(
            self.web_db_path,
            {"analyzer-a": ASGIAnalyzerClient(self.app_a),
             "analyzer-b": ASGIAnalyzerClient(self.app_b)},
        )
        web_main.service = self.service
        self.web = TestClient(web_main.app)


class WebappTestCase(unittest.TestCase):
    """Shared harness lifecycle for webapp end-to-end tests."""

    def setUp(self):
        self.h = PlatformHarness()
        self.web = self.h.web
        # Keep the background reconciler quiet; tests drive reconcile manually.
        web_main.RECONCILE_INTERVAL = 3600
        web_main._bg_stop.clear()
        self._ctx = self.web
        self._ctx.__enter__()

    def tearDown(self):
        self._ctx.__exit__(None, None, None)
        web_main._bg_stop.set()

    # -- helpers ------------------------------------------------------------
    def ingest(self, payload, **kw):
        body = {"payload": payload}
        body.update(kw)
        r = self.web.post("/api/events", json=body).json()
        self.assertTrue(r["ok"], r)
        return r

    def effective_fp(self):
        return self.web.get("/api/status").json()["effective_writer"]["fingerprint"]

    def correct(self, entity_id, seen_revision, seen_writer_fp, payload,
                reason="测值录错", **kw):
        body = {
            "entity_id": entity_id,
            "seen_revision": seen_revision,
            "seen_writer_fp": seen_writer_fp,
            "payload": payload,
            "reason": reason,
        }
        body.update(kw)
        return self.web.post("/api/corrections", json=body)

    def chain(self, entity_id):
        r = self.web.get(f"/api/entities/{entity_id}/revisions")
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["chain"]

    def event_blob(self, event_id):
        conn = self.h.service._conn()
        try:
            return web_db.get_event(conn, event_id)["blob_b64"]
        finally:
            conn.close()


class TestFullFlow(WebappTestCase):

    # ------------------------------------------------------------------
    def test_01_initial_state_and_basic_ingest_decode(self):
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 1)
        for a in ("analyzer-a", "analyzer-b"):
            self.assertTrue(st["analyzers"][a]["reachable"])
            self.assertEqual(st["analyzers"][a]["effective_version"], 1)

        r = self.web.post("/api/events", json={
            "payload": {"sample_id": "S-1", "project": "P1", "measurement": 2.5},
            "decode_on": ["analyzer-a", "analyzer-b"],
        }).json()
        self.assertTrue(r["ok"])
        self.assertEqual(r["writer_version"], 1)
        self.assertEqual(len(r["decodes"]), 2)
        for d in r["decodes"]:
            self.assertTrue(d["decoded_ok"], d)
            self.assertEqual(d["datum"]["sample_id"], "S-1")

    def test_02_incompatible_writer_switch_blocked_with_field_path(self):
        r = self.web.post("/api/switches/writer",
                          json={"schema": bad_writer_v2()})
        self.assertEqual(r.status_code, 409)
        self.assertIn("sample_id", r.json()["error"])
        # writer must remain v1
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 1)

    def test_03_compatible_writer_switch_confirmed(self):
        r = self.web.post("/api/switches/writer",
                          json={"schema": writer_v2()})
        self.assertEqual(r.status_code, 200, r.text)
        sw = r.json()["switch"]
        self.assertEqual(sw["status"], "confirmed")
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 2)

        # New event is encoded with v2; old v1 readers still decode via
        # schema resolution with the defaulted new field.
        r = self.web.post("/api/events", json={
            "payload": {"sample_id": "S-2", "project": "P2", "measurement": 4.0,
                        "instrument_id": "GC-7", "tags": [1, 2, 3]},
            "decode_on": ["analyzer-a"],
        }).json()
        self.assertTrue(r["ok"])
        d = r["decodes"][0]
        self.assertTrue(d["decoded_ok"], d)
        # v1 reader projects away unknown fields and keeps known ones
        self.assertNotIn("instrument_id", d["datum"])
        self.assertEqual(d["datum"]["project"], "P2")

    def test_04_reader_switch_guard_covers_all_retained_events(self):
        # make one v1 event first
        self.web.post("/api/events", json={
            "payload": {"sample_id": "S-old", "project": "P",
                        "measurement": 1.0}})
        # advance writer to v2
        self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        # and one v2 event
        self.web.post("/api/events", json={
            "payload": {"sample_id": "S-new", "project": "P", "measurement": 2.0,
                        "instrument_id": "X", "tags": []}})

        # bad reader: required new field -> cannot read retained old writers
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": bad_reader_drops_required()})
        self.assertEqual(r.status_code, 409)
        err = r.json()["error"]
        self.assertIn("new_required", err)
        self.assertIn("current writer", err)
        # analyzer stays at v1
        st = self.web.get("/api/status").json()
        self.assertEqual(st["analyzers"]["analyzer-a"]["effective_version"], 1)

        # good reader with defaults reads both v1 and v2 retained events
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a",
            "schema": reader_v2_with_default()})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["switch"]["status"], "confirmed")
        st = self.web.get("/api/status").json()
        self.assertEqual(st["analyzers"]["analyzer-a"]["effective_version"], 2)

        # It can actually decode both retained events
        events = st["events"]
        v1_event = next(e for e in events if e["writer_version"] == 1)
        r = self.web.post("/api/events/decode", json={
            "event_id": v1_event["id"],
            "analyzer_id": "analyzer-a"}).json()
        self.assertTrue(r["result"]["decoded_ok"])
        self.assertEqual(r["result"]["datum"]["temperature"], 0.0)

    def test_05_reader_switch_idempotent_replay(self):
        client = self.h.service.clients["analyzer-b"]
        schema = reader_v2_with_default()
        first = client.reader_switch(
            switch_id="fixed-id-1", version=2, base_version=1,
            reader_schema=schema, writer_schema=V1)
        self.assertEqual(first["status"], "applied")
        second = client.reader_switch(
            switch_id="fixed-id-1", version=2, base_version=1,
            reader_schema=schema, writer_schema=V1)
        self.assertEqual(second["status"], "already_applied")

    def test_06_late_switch_fenced(self):
        client = self.h.service.clients["analyzer-b"]
        schema = reader_v2_with_default()
        # base v1 -> v2 succeeds
        client.reader_switch(switch_id="s1", version=2, base_version=1,
                             reader_schema=schema, writer_schema=V1)
        # a late duplicate based on the stale v1 base must be rejected
        late = client.reader_switch(switch_id="s2", version=99, base_version=1,
                                    reader_schema=schema, writer_schema=V1)
        self.assertEqual(late["status"], "stale")
        # and must not have changed effective state
        state = client.reader_state()
        self.assertEqual(state["effective_version"], 2)

    def test_07_lost_reader_receipt_then_reconcile_confirms(self):
        # Arm a one-shot dropped response inside analyzer A.
        a_db_path = self.h.a_db
        analyzer_db.set_sim(a_db_path, 0, True, False)

        schema = reader_v2_with_default()
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": schema})
        # Platform must not have reported success.
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["switch"]["status"], "pending")

        # Crucially the durable remote mutation DID happen before the drop:
        # the analyzer's own state must already be at v2.
        state = self.h.service.clients["analyzer-a"].reader_state()
        self.assertEqual(state["effective_version"], 2)

        # Reconciliation observes real state and confirms.
        rec = self.web.post("/api/reconcile").json()["result"]["reader"]
        outcomes = [x["outcome"] for x in rec]
        self.assertIn("confirmed_from_remote_state", outcomes)
        st = self.web.get("/api/status").json()
        pending = [s for s in st["reader_switches"] if s["status"] == "pending"]
        self.assertEqual(pending, [])

    def test_08_lost_reader_receipt_without_remote_apply_retries(self):
        # Simulate at the *webapp* side: the request never reaches the
        # analyzer, so no durable mutation exists; reconcile must retry.
        client = self.h.service.clients["analyzer-b"]
        original = client.reader_switch
        calls = {"n": 0}

        def flaky(*a, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RemoteError("simulated connection reset")
            return original(*a, **kw)

        with mock.patch.object(client, "reader_switch", side_effect=flaky):
            r = self.web.post("/api/switches/reader", json={
                "analyzer_id": "analyzer-b",
                "schema": reader_v2_with_default()})
        self.assertEqual(r.json()["switch"]["status"], "pending")
        self.assertEqual(calls["n"], 1)

        # Reconcile retries the same idempotent switch successfully.
        rec = self.web.post("/api/reconcile").json()["result"]["reader"]
        self.assertTrue(any(x["outcome"] == "confirmed_on_retry" for x in rec))
        self.assertEqual(
            client.reader_state()["effective_version"], 2)

    def test_09_lost_writer_receipt_blocks_success_and_reconciles(self):
        conn = self.h.service._conn()
        web_db.set_sim(conn, 0, True, False)  # arm local writer receipt drop
        conn.commit()
        conn.close()

        r = self.web.post("/api/switches/writer",
                          json={"schema": writer_v2()})
        sw = r.json()["switch"]
        self.assertEqual(sw["status"], "pending")  # never reported confirmed

        # New events must still be ingestible with the *confirmed* v1 writer;
        # interleaving must not be blocked or lost.
        ev = self.web.post("/api/events", json={
            "payload": {"sample_id": "S-while-pending", "project": "P",
                        "measurement": 9.0}}).json()
        self.assertTrue(ev["ok"])
        self.assertEqual(ev["writer_version"], 1)

        # Reconcile resumes the switch and confirms.
        rec = self.web.post("/api/reconcile").json()["result"]["writer"]
        self.assertTrue(rec)
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 2)
        self.assertTrue(all(s["status"] == "confirmed"
                            for s in st["writer_switches"]))
        # and the interleaved v1 event is still retained and decodable
        ids = {e["id"]: e for e in st["events"]}
        self.assertIn(ev["event_id"], ids)
        self.assertEqual(ids[ev["event_id"]]["writer_version"], 1)

    def test_10_pending_writer_switch_blocks_new_proposal(self):
        conn = self.h.service._conn()
        web_db.set_sim(conn, 0, True, False)
        conn.commit()
        conn.close()
        self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        r = self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        self.assertEqual(r.status_code, 409)
        self.assertIn("still unconfirmed", r.json()["error"])
        # clear and reconcile
        self.web.post("/api/reconcile")

    def test_11_rejected_switch_never_changes_format(self):
        self.web.post("/api/switches/writer",
                      json={"schema": bad_writer_v2()})
        st = self.web.get("/api/status").json()
        self.assertEqual(st["effective_writer"]["version"], 1)
        # no pending switch row should have been created for a guard failure
        self.assertEqual(st["writer_switches"], [])

    def test_12_encoding_error_surfaces(self):
        r = self.web.post("/api/events", json={
            "payload": {"sample_id": 123, "project": "P", "measurement": 1.0}})
        self.assertEqual(r.status_code, 422)
        self.assertIn("Avro encoding failed", r.json()["error"])

    def test_13_numeric_promotion_and_array_round_trip(self):
        # Writer v2 carries int array + string default; promote reader view.
        self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        promoted = reader_v2_with_default()
        # reader asks for tags as long[] and instrument_id stays string
        promoted["fields"].append({
            "name": "tags",
            "type": {"type": "array", "items": "long"},
            "default": []})
        r = self.web.post("/api/switches/reader", json={
            "analyzer_id": "analyzer-a", "schema": promoted})
        self.assertEqual(r.status_code, 200, r.text)
        ev = self.web.post("/api/events", json={
            "payload": {"sample_id": "S", "project": "P", "measurement": 1.0,
                        "instrument_id": "GC", "tags": [1, 2]},
            "decode_on": ["analyzer-a"]}).json()
        d = ev["decodes"][0]
        self.assertTrue(d["decoded_ok"], d)
        self.assertEqual(d["datum"]["tags"], [1, 2])
        self.assertIsInstance(d["datum"]["tags"][0], int)


class TestCorrections(WebappTestCase):
    """事件更正：修订链、原子生效、过期拒绝、历史保留。"""

    P = {"sample_id": "S-1", "project": "重金属普查", "measurement": 3.14}

    def test_01_correction_happy_path_and_chain(self):
        ev = self.ingest(self.P)
        entity, old_event = ev["entity_id"], ev["event_id"]
        old_blob = self.event_blob(old_event)
        fp = self.effective_fp()
        # 分析组先解码过一次原始事件 -> 产生历史解码记录
        d0 = self.web.post("/api/events/decode", json={
            "event_id": old_event, "analyzer_id": "analyzer-a"}).json()["result"]
        self.assertTrue(d0["decoded_ok"])

        r = self.correct(entity, 1, fp,
                         {**self.P, "measurement": 3.15},
                         reason="测值录错，3.14 应为 3.15",
                         decode_on=["analyzer-a", "analyzer-b"])
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["revision"], 2)
        self.assertEqual(body["old_event_id"], old_event)
        self.assertNotEqual(body["new_event_id"], old_event)
        self.assertEqual(body["writer_version"], 1)
        for d in body["decodes"]:
            self.assertTrue(d["decoded_ok"], d)
            self.assertEqual(d["datum"]["measurement"], 3.15)

        # 新事件、修订关系、当前指向一并生效
        ch = self.chain(entity)
        self.assertEqual(ch["current_revision"], 2)
        self.assertEqual(ch["current_event_id"], body["new_event_id"])
        self.assertEqual([e["revision"] for e in ch["revisions"]], [1, 2])
        self.assertEqual(len(ch["corrections"]), 1)
        cor = ch["corrections"][0]
        self.assertEqual(cor["old_event_id"], old_event)
        self.assertEqual(cor["new_event_id"], body["new_event_id"])
        self.assertEqual(cor["reason"], "测值录错，3.14 应为 3.15")

        # 现行事件查询只返回最新修订
        cur = self.web.get("/api/events/current").json()["current_events"]
        self.assertEqual(len(cur), 1)
        self.assertEqual(cur[0]["id"], body["new_event_id"])
        self.assertEqual(cur[0]["payload"]["measurement"], 3.15)

        # 旧字节、原格式、历史解码记录不变；旧事件仍可按原格式交 A/B 解码
        self.assertEqual(self.event_blob(old_event), old_blob)
        old = next(e for e in ch["revisions"] if e["revision"] == 1)
        self.assertEqual(old["payload"]["measurement"], 3.14)
        self.assertFalse(old["is_current"])
        for a in ("analyzer-a", "analyzer-b"):
            d = self.web.post("/api/events/decode", json={
                "event_id": old_event, "analyzer_id": a}).json()["result"]
            self.assertTrue(d["decoded_ok"], d)
            self.assertEqual(d["datum"]["measurement"], 3.14)
        decodes = self.web.get("/api/status").json()["decode_results"]
        self.assertTrue(any(d["event_id"] == old_event for d in decodes))

    def test_02_stale_seen_revision_rejected(self):
        ev = self.ingest(self.P)
        entity = ev["entity_id"]
        fp = self.effective_fp()
        r1 = self.correct(entity, 1, fp, {**self.P, "measurement": 3.15})
        self.assertEqual(r1.status_code, 200, r1.text)
        # 另一笔基于同一所见修订的更正必须被拒绝
        r2 = self.correct(entity, 1, fp, {**self.P, "measurement": 9.99})
        self.assertEqual(r2.status_code, 409)
        self.assertIn("所见修订", r2.json()["error"])
        # 当前指向仍是第一笔更正的结果
        ch = self.chain(entity)
        self.assertEqual(ch["current_revision"], 2)
        self.assertEqual(len(ch["revisions"]), 2)
        self.assertEqual(len(ch["corrections"]), 1)
        cur = self.web.get("/api/events/current").json()["current_events"]
        self.assertEqual(cur[0]["payload"]["measurement"], 3.15)

    def test_03_stale_write_format_rejected_then_fresh_correction_ok(self):
        ev = self.ingest(self.P)
        entity, old_event = ev["entity_id"], ev["event_id"]
        old_blob = self.event_blob(old_event)
        fp_v1 = self.effective_fp()
        # 表单载入后发生写格式切换
        r = self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        self.assertEqual(r.json()["switch"]["status"], "confirmed")
        # 基于过期写格式的更正被拒绝
        r = self.correct(entity, 1, fp_v1, {**self.P, "measurement": 3.15})
        self.assertEqual(r.status_code, 409)
        self.assertIn("写格式", r.json()["error"])
        # 失败后仍能读取原当前事件（指针未动、原事件可解码）
        cur = self.web.get("/api/events/current").json()["current_events"]
        self.assertEqual(len(cur), 1)
        self.assertEqual(cur[0]["id"], old_event)
        self.assertEqual(cur[0]["payload"]["measurement"], 3.14)
        d = self.web.post("/api/events/decode", json={
            "event_id": old_event, "analyzer_id": "analyzer-b"}).json()["result"]
        self.assertTrue(d["decoded_ok"])
        # 基于新生效写格式重新提交 -> 按 v2 编码成功
        fp_v2 = self.effective_fp()
        r = self.correct(entity, 1, fp_v2,
                         {**self.P, "measurement": 3.15,
                          "instrument_id": "GC-7", "tags": [1, 2]},
                         decode_on=["analyzer-a"])
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["writer_version"], 2)
        self.assertEqual(r.json()["revision"], 2)
        self.assertTrue(r.json()["decodes"][0]["decoded_ok"])
        # 跨格式的修订链：v1 原字节保留，v2 为当前
        ch = self.chain(entity)
        by_rev = {e["revision"]: e for e in ch["revisions"]}
        self.assertEqual(by_rev[1]["writer_version"], 1)
        self.assertEqual(by_rev[2]["writer_version"], 2)
        self.assertEqual(self.event_blob(old_event), old_blob)

    def test_04_encode_failure_rolls_back_everything(self):
        ev = self.ingest(self.P)
        entity, old_event = ev["entity_id"], ev["event_id"]
        fp = self.effective_fp()
        # sample_id 类型错误 -> 编码失败
        r = self.correct(entity, 1, fp,
                         {"sample_id": 123, "project": "P", "measurement": 3.15})
        self.assertEqual(r.status_code, 422)
        self.assertIn("Avro encoding failed", r.json()["error"])
        # 新事件、修订关系、当前指向均未产生；原当前事件可读
        ch = self.chain(entity)
        self.assertEqual(ch["current_revision"], 1)
        self.assertEqual(ch["current_event_id"], old_event)
        self.assertEqual(len(ch["revisions"]), 1)
        self.assertEqual(ch["corrections"], [])
        d = self.web.post("/api/events/decode", json={
            "event_id": old_event, "analyzer_id": "analyzer-a"}).json()["result"]
        self.assertTrue(d["decoded_ok"])
        self.assertEqual(d["datum"]["measurement"], 3.14)

    def test_05_correction_during_pending_writer_switch_uses_confirmed_format(self):
        ev = self.ingest(self.P)
        entity = ev["entity_id"]
        fp_v1 = self.effective_fp()
        # 丢回执使写格式切换停在 pending；生效写格式仍是 v1
        conn = self.h.service._conn()
        web_db.set_sim(conn, 0, True, False)
        conn.commit()
        conn.close()
        r = self.web.post("/api/switches/writer", json={"schema": writer_v2()})
        self.assertEqual(r.json()["switch"]["status"], "pending")
        # pending 不阻塞更正：按已确认的 v1 生效格式编码
        r = self.correct(entity, 1, fp_v1, {**self.P, "measurement": 3.15})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["writer_version"], 1)
        # 对账后写格式推进到 v2，旧所见写格式立即过期
        self.web.post("/api/reconcile")
        self.assertEqual(self.web.get("/api/status").json()
                         ["effective_writer"]["version"], 2)
        r = self.correct(entity, 2, fp_v1, {**self.P, "measurement": 3.16})
        self.assertEqual(r.status_code, 409)
        self.assertIn("写格式", r.json()["error"])

    def test_06_concurrent_corrections_exactly_one_winner(self):
        ev = self.ingest(self.P)
        entity = ev["entity_id"]
        fp = self.effective_fp()
        outcomes = []

        def worker(i):
            try:
                r = self.h.service.correct_event(
                    entity_id=entity, seen_revision=1, seen_writer_fp=fp,
                    payload={**self.P, "measurement": 10.0 + i},
                    reason=f"并发更正 {i}")
                outcomes.append(("ok", r))
            except StaleCorrection as exc:
                outcomes.append(("stale", exc))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        oks = [r for k, r in outcomes if k == "ok"]
        stales = [r for k, r in outcomes if k == "stale"]
        self.assertEqual(len(oks), 1, outcomes)
        self.assertEqual(len(stales), 1, outcomes)
        # 链上恰好两修订、一条更正记录，当前指向唯一
        ch = self.chain(entity)
        self.assertEqual(ch["current_revision"], 2)
        self.assertEqual(len(ch["revisions"]), 2)
        self.assertEqual(len(ch["corrections"]), 1)
        self.assertEqual(ch["corrections"][0]["new_event_id"],
                         ch["current_event_id"])

    def test_07_current_query_only_latest_per_entity(self):
        e1 = self.ingest(self.P)
        e2 = self.ingest({"sample_id": "S-2", "project": "P2",
                          "measurement": 1.0})
        fp = self.effective_fp()
        self.correct(e1["entity_id"], 1, fp, {**self.P, "measurement": 3.15})
        cur = self.web.get("/api/events/current").json()["current_events"]
        self.assertEqual(len(cur), 2)
        by_entity = {e["entity_id"]: e for e in cur}
        self.assertEqual(by_entity[e1["entity_id"]]["revision"], 2)
        self.assertEqual(by_entity[e1["entity_id"]]["payload"]["measurement"],
                         3.15)
        self.assertEqual(by_entity[e2["entity_id"]]["revision"], 1)
        # 仓内仍留存全部 3 条历史事件
        st = self.web.get("/api/status").json()
        self.assertEqual(len(st["events"]), 3)
        current_ids = {e["id"] for e in st["events"] if e["is_current"]}
        self.assertEqual(len(current_ids), 2)

    def test_08_unknown_entity_and_empty_reason_rejected(self):
        fp = self.effective_fp()
        r = self.correct("ent_nonexistent", 1, fp, self.P)
        self.assertEqual(r.status_code, 404)
        ev = self.ingest(self.P)
        r = self.correct(ev["entity_id"], 1, fp, self.P, reason="  ")
        self.assertEqual(r.status_code, 422)
        # 原事件未受影响
        ch = self.chain(ev["entity_id"])
        self.assertEqual(ch["current_revision"], 1)

    def test_09_legacy_db_migration_backfills_entities(self):
        tmp = tempfile.mkdtemp(prefix="avrolegacy-")
        path = os.path.join(tmp, "legacy.db")
        conn = sqlite3.connect(path)
        conn.executescript(
            "CREATE TABLE events ("
            "  id TEXT PRIMARY KEY, writer_fp TEXT NOT NULL,"
            "  writer_version INTEGER NOT NULL, payload_json TEXT NOT NULL,"
            "  blob_b64 TEXT NOT NULL, ingested_at TEXT NOT NULL);")
        conn.execute("INSERT INTO events VALUES ('evt_old1', 'fp1', 1, "
                     "'{\"sample_id\": \"S-old\"}', 'AA==', '2026-01-01T00:00:00Z')")
        conn.commit()
        conn.close()
        web_db.init(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT entity_id, revision FROM events WHERE id = 'evt_old1'"
        ).fetchone()
        self.assertEqual(row["entity_id"], "evt_old1")
        self.assertEqual(row["revision"], 1)
        cur = conn.execute(
            "SELECT * FROM event_current WHERE entity_id = 'evt_old1'").fetchone()
        self.assertEqual(cur["event_id"], "evt_old1")
        self.assertEqual(cur["revision"], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

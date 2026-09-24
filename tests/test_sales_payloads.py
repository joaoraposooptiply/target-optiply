import csv
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
from typing import Any, Tuple
from unittest.mock import Mock, patch


def _module(name: str, **attributes: object) -> ModuleType:
    module = ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


def _load_sink_classes() -> Tuple[Any, Any, Any]:
    singer_sdk = _module("singer_sdk")
    setattr(singer_sdk, "__path__", [])
    exceptions = _module(
        "singer_sdk.exceptions",
        FatalAPIError=type("FatalAPIError", (Exception,), {}),
        RetriableAPIError=type("RetriableAPIError", (Exception,), {}),
    )
    plugin_base = _module("singer_sdk.plugin_base", PluginBase=object)
    setattr(singer_sdk, "exceptions", exceptions)
    setattr(singer_sdk, "plugin_base", plugin_base)

    target_hotglue = _module("target_hotglue")
    setattr(target_hotglue, "__path__", [])
    target_hotglue_client = _module("target_hotglue.client", HotglueSink=object)
    setattr(target_hotglue, "client", target_hotglue_client)

    target_optiply = _module("target_optiply")
    setattr(target_optiply, "__path__", [])
    target_optiply_client = _module(
        "target_optiply.client", OptiplySink=object
    )
    target_optiply_auth = _module(
        "target_optiply.auth", OptiplyAuthenticator=object
    )
    setattr(target_optiply, "client", target_optiply_client)
    setattr(target_optiply, "auth", target_optiply_auth)

    fake_modules = {
        "backoff": _module("backoff"),
        "requests": _module("requests"),
        "singer_sdk": singer_sdk,
        "singer_sdk.exceptions": exceptions,
        "singer_sdk.plugin_base": plugin_base,
        "target_hotglue": target_hotglue,
        "target_hotglue.client": target_hotglue_client,
        "target_optiply": target_optiply,
        "target_optiply.client": target_optiply_client,
        "target_optiply.auth": target_optiply_auth,
    }
    sink_path = Path(__file__).resolve().parents[1] / "target_optiply" / "sinks.py"
    spec = importlib.util.spec_from_file_location("_test_target_optiply_sinks", sink_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {sink_path}")

    with patch.dict(sys.modules, fake_modules, clear=False):
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return getattr(module, "SellOrderLineSink"), getattr(module, "SellOrderSink"), module


SellOrderLineSink, SellOrderSink, _sinks = _load_sink_classes()


def _sink(sink_class: Any) -> Any:
    sink = object.__new__(sink_class)
    sink.stream_name = sink_class.endpoint
    sink.endpoint = sink_class.endpoint
    sink.logger = logging.getLogger(sink_class.__name__)
    sink._stashed_external_id = None
    return sink


class _Response:
    status_code = 201
    text = ""

    def json(self):
        return {"data": {"id": "created-order"}}


class _FailedResponse:
    status_code = 400
    text = "rejected"


CREATED = ("created-order", True, {})
FAILED = (None, False, {})
UNKNOWN = ("unknown", True, {})
UPDATED = ("existing-order", True, {})


def test_nested_sell_order_payload_and_post_without_target_id():
    sink = _sink(SellOrderSink)
    record = {
        "order_id": "fe-order-1",
        "placed": "2025-01-01T00:00:00Z",
        "completed": "2025-01-02T00:00:00Z",
        "totalValue": "99.99",
        "line_items": json.dumps(
            [
                {
                    "quantity": 2,
                    "subtotalValue": "12.50",
                    "productId": 7,
                    "placed": "2025-01-01T01:00:00Z",
                },
                {"quantity": 1, "subtotalValue": 0, "productId": 8, "placed": " "},
            ]
        ),
    }

    payload = sink.preprocess_record(record, {})
    attributes = payload["data"]["attributes"]
    assert "id" not in payload["data"]
    assert attributes["completed"] == "2025-01-02T00:00:00Z"
    assert attributes["totalValue"] == "99.99"
    assert attributes["orderLines"] == [
        {
            "quantity": 2,
            "subtotalValue": "12.5",
            "productId": 7,
            "placed": "2025-01-01T01:00:00Z",
        },
        {
            "quantity": 1,
            "subtotalValue": "0.0",
            "productId": 8,
        },
    ]

    for completed in (" ", None):
        optional_completed = sink.preprocess_record(
            {**record, "completed": completed}, {}
        )
        assert "completed" not in optional_completed["data"]["attributes"]

    missing_completed = sink.preprocess_record(
        {key: value for key, value in record.items() if key != "completed"}, {}
    )
    assert "completed" not in missing_completed["data"]["attributes"]

    request_api = Mock(return_value=_Response())
    with patch.object(sink, "request_api", new=request_api, create=True):
        result = sink.upsert_record(payload, {})
    assert result == CREATED
    request_api.assert_called_once_with(
        http_method="POST",
        endpoint="sellOrders",
        request_data=payload,
    )


def test_sell_order_remote_id_is_sdk_external_id_and_stays_in_optiply_payload():
    sink = _sink(SellOrderSink)
    payload = sink.preprocess_record(
        {"remoteId": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
    )
    assert payload["externalId"] == "source-order"
    assert payload["data"]["attributes"]["remoteId"] == "source-order"

    # HotglueSink removes externalId before handing the record to upsert_record.
    sdk_payload = dict(payload)
    assert sdk_payload.pop("externalId", None) == "source-order"
    request_api = Mock(return_value=_Response())
    with patch.object(sink, "request_api", new=request_api, create=True):
        assert sink.upsert_record(sdk_payload, {}) == CREATED
    request_api.assert_called_once_with(
        http_method="POST",
        endpoint="sellOrders",
        request_data=sdk_payload,
    )
    assert sdk_payload["data"]["attributes"]["remoteId"] == "source-order"

    missing_remote_id = sink.preprocess_record(
        {"order_id": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
    )
    assert "externalId" not in missing_remote_id


def test_sell_order_total_fallback_and_standalone_line_placed():
    order_sink = _sink(SellOrderSink)
    payload = order_sink.preprocess_record(
        {
            "placed": "2025-01-01T00:00:00Z",
            "line_items": json.dumps(
                [
                    {"quantity": 1, "subtotalValue": "1.25", "productId": 7},
                    {"quantity": 2, "subtotalValue": "2", "productId": 8},
                ]
            ),
        },
        {},
    )
    assert payload["data"]["attributes"]["totalValue"] == "3.25"

    line_sink = _sink(SellOrderLineSink)
    line_record = {
        "quantity": 1,
        "subtotalValue": "4.00",
        "productId": 7,
        "sellOrderId": 3,
        "placed": "2025-01-01T01:00:00Z",
    }
    line = line_sink.preprocess_record(line_record, {})
    assert line["data"]["attributes"]["placed"] == "2025-01-01T01:00:00Z"

    for placed in ("", None):
        optional_line = line_sink.preprocess_record(
            {**line_record, "placed": placed}, {}
        )
        assert "placed" not in optional_line["data"]["attributes"]

    missing_line = line_sink.preprocess_record(
        {key: value for key, value in line_record.items() if key != "placed"}, {}
    )
    assert "placed" not in missing_line["data"]["attributes"]


def test_sell_order_snapshot_only_after_success_and_remote_id_present():
    sink = _sink(SellOrderSink)
    payload = sink.preprocess_record(
        {"remoteId": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
    )

    with TemporaryDirectory() as snapshots, patch.dict(
        os.environ, {"SNAPSHOT_DIR": snapshots}, clear=False
    ):
        request_api = Mock(return_value=_Response())
        with patch.object(sink, "request_api", new=request_api, create=True):
            assert sink.upsert_record(payload, {}) == CREATED
        snapshot = Path(snapshots) / "export_optiply_sell_orders.snapshot.csv"
        with snapshot.open(newline="", encoding="utf-8") as file:
            assert list(csv.reader(file)) == [
                ["InputId", "RemoteId"],
                ["source-order", "created-order"],
            ]

        failed_sink = _sink(SellOrderSink)
        failed_request = Mock(return_value=_FailedResponse())
        with patch.object(failed_sink, "request_api", new=failed_request, create=True):
            assert failed_sink.upsert_record(payload, {}) == FAILED
        failed_request.assert_called_once()
        assert snapshot.read_text(encoding="utf-8").count("source-order") == 1

        missing_remote_id_sink = _sink(SellOrderSink)
        missing_payload = missing_remote_id_sink.preprocess_record(
            {"order_id": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
        )
        with patch.object(
            missing_remote_id_sink,
            "request_api",
            new=Mock(return_value=_Response()),
            create=True,
        ):
            assert missing_remote_id_sink.upsert_record(missing_payload, {}) == CREATED
        assert snapshot.read_text(encoding="utf-8").count("source-order") == 1


def test_parallel_sell_order_successes_write_one_snapshot_header_and_both_rows():
    sinks_and_payloads = []
    for remote_id in ("source-order-1", "source-order-2"):
        sink = _sink(SellOrderSink)
        payload = sink.preprocess_record(
            {"remoteId": remote_id, "placed": "2025-01-01", "totalValue": 12}, {}
        )
        sinks_and_payloads.append((sink, payload))

    with TemporaryDirectory() as snapshots, patch.dict(
        os.environ, {"SNAPSHOT_DIR": snapshots}, clear=False
    ):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda item: _upsert_success(*item),
                    sinks_and_payloads,
                )
            )

        assert results == [CREATED] * 2
        snapshot = Path(snapshots) / "export_optiply_sell_orders.snapshot.csv"
        with snapshot.open(newline="", encoding="utf-8") as file:
            rows = list(csv.reader(file))
        assert rows[0] == ["InputId", "RemoteId"]
        assert sorted(row[0] for row in rows[1:]) == [
            "source-order-1",
            "source-order-2",
        ]


def test_configured_snapshot_dir_and_all_routed_entities():
    endpoints = (
        ("ProductsSink", "products"),
        ("SupplierSink", "suppliers"),
        ("SupplierProductSink", "supplier_products"),
        ("BuyOrderSink", "buy_orders"),
        ("BuyOrderLineSink", "buy_order_lines"),
        ("SellOrderLineSink", "sell_order_lines"),
        ("PromotionSink", "promotions"),
        ("PromotionProductSink", "promotion_products"),
    )
    with TemporaryDirectory() as snapshots, TemporaryDirectory() as unused, patch.dict(
        os.environ, {"SNAPSHOT_DIR": unused}, clear=False
    ):
        legacy = Path(snapshots) / "export_optiply_products.snapshot.csv"
        legacy.write_text("id,updated_at\nprior,2025-01-01\n", encoding="utf-8")
        for class_name, filename in endpoints:
            sink = _sink(getattr(_sinks, class_name))
            sink._target = SimpleNamespace(_config={"snapshot_dir": snapshots})
            record = {"data": {"attributes": {}}}
            with patch.object(sink, "get_mandatory_fields", return_value=[]), patch.object(
                sink, "request_api", return_value=_Response(), create=True
            ):
                assert sink.upsert_record(record, {}) == CREATED
            path = Path(snapshots) / f"target_optiply_{filename}.snapshot.csv"
            with path.open(newline="", encoding="utf-8") as file:
                assert list(csv.reader(file)) == [["id"], ["created-order"]]

            with patch.object(sink, "get_mandatory_fields", return_value=[]), patch.object(
                sink, "request_api", return_value=_FailedResponse(), create=True
            ):
                assert sink.upsert_record(record, {}) == FAILED
            assert path.read_text(encoding="utf-8").count("created-order") == 1
            unknown_response = Mock(status_code=201, text="", json=Mock(return_value={"data": {}}))
            with patch.object(sink, "get_mandatory_fields", return_value=[]), patch.object(
                sink, "request_api", return_value=unknown_response, create=True
            ):
                assert sink.upsert_record(record, {}) == UNKNOWN
            assert path.read_text(encoding="utf-8").count("created-order") == 1
        assert legacy.read_text(encoding="utf-8") == "id,updated_at\nprior,2025-01-01\n"
        fallback = _sink(_sinks.BaseOptiplySink)
        fallback.endpoint = "products"
        with patch.object(fallback, "get_mandatory_fields", return_value=[]), patch.object(
            fallback, "request_api", return_value=_Response(), create=True
        ):
            assert fallback.upsert_record({"data": {"attributes": {}}}, {}) == CREATED
        assert (Path(snapshots) / "target_optiply_products.snapshot.csv").read_text(
            encoding="utf-8"
        ).count("created-order") == 1
        assert list(Path(unused).iterdir()) == []


def test_sell_order_configured_snapshot_requires_both_ids():
    sink = _sink(SellOrderSink)
    with TemporaryDirectory() as snapshots, patch.dict(
        os.environ, {"SNAPSHOT_DIR": ""}, clear=False
    ):
        sink._target = SimpleNamespace(_config={"snapshot_dir": snapshots})
        payload = sink.preprocess_record(
            {"remoteId": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
        )
        with patch.object(sink, "request_api", return_value=_Response(), create=True):
            assert sink.upsert_record(payload, {}) == CREATED
        path = Path(snapshots) / "export_optiply_sell_orders.snapshot.csv"
        with path.open(newline="", encoding="utf-8") as file:
            assert list(csv.reader(file)) == [
                ["InputId", "RemoteId"], ["source-order", "created-order"]
            ]

        unknown_response = Mock(status_code=201, text="", json=Mock(return_value={"data": {}}))
        with patch.object(sink, "request_api", return_value=unknown_response, create=True):
            assert sink.upsert_record(payload, {}) == UNKNOWN
        assert path.read_text(encoding="utf-8").count("source-order") == 1

        update = {**payload, "data": {**payload["data"], "id": "existing-order"}}
        with patch.object(sink, "request_api", return_value=unknown_response, create=True):
            assert sink.upsert_record(update, {}) == UPDATED
        assert path.read_text(encoding="utf-8").count("source-order") == 2


def _upsert_success(sink: Any, payload: dict) -> tuple:
    with patch.object(sink, "request_api", new=Mock(return_value=_Response()), create=True):
        return sink.upsert_record(payload, {})


def test_sell_order_snapshot_io_error_does_not_report_api_failure():
    sink = _sink(SellOrderSink)
    payload = sink.preprocess_record(
        {"remoteId": "source-order", "placed": "2025-01-01", "totalValue": 12}, {}
    )

    with TemporaryDirectory() as snapshots, patch.dict(
        os.environ, {"SNAPSHOT_DIR": snapshots}, clear=False
    ):
        # A directory at the requested filename forces the CSV open to fail.
        (Path(snapshots) / "export_optiply_sell_orders.snapshot.csv").mkdir()
        request_api = Mock(return_value=_Response())
        with patch.object(sink, "request_api", new=request_api, create=True):
            assert sink.upsert_record(payload, {}) == CREATED
        request_api.assert_called_once()

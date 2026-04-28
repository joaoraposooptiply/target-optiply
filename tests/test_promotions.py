from target_optiply import sinks
from target_optiply.sinks import BaseOptiplySink, PromotionProductSink, PromotionSink


def test_promotion_success_populates_run_cache(monkeypatch):
    sink = PromotionSink.__new__(PromotionSink)
    sink._stashed_external_id = "remote-promo-1"

    def fake_upsert(self, record, context):
        return "98765", True, {"_action": "upsert"}

    monkeypatch.setattr(BaseOptiplySink, "upsert_record", fake_upsert)
    sinks._promotions_id_cache.pop("remote-promo-1", None)

    try:
        record_id, success, state = sink.upsert_record({}, {})

        assert record_id == "98765"
        assert success is True
        assert state == {"_action": "upsert"}
        assert sinks._promotions_id_cache["remote-promo-1"] == "98765"
    finally:
        sinks._promotions_id_cache.pop("remote-promo-1", None)


def test_promotion_product_resolves_promotion_id_from_run_cache():
    sink = PromotionProductSink.__new__(PromotionProductSink)
    attributes = {
        "productId": "123",
        "specificUpliftIncrease": "2.5",
        "specificUpliftType": "relative",
    }
    sinks._promotions_id_cache["remote-promo-1"] = "98765"

    try:
        sink._add_additional_attributes(
            {"Remote_promotionId": "remote-promo-1", "specificUpliftType": "relative"},
            attributes,
        )

        assert attributes["productId"] == 123
        assert attributes["promotionId"] == 98765
        assert attributes["specificUpliftIncrease"] == 2.5
        assert attributes["specificUpliftType"] == "relative"
    finally:
        sinks._promotions_id_cache.pop("remote-promo-1", None)


def test_promotion_product_falls_back_to_explicit_promotion_id():
    sink = PromotionProductSink.__new__(PromotionProductSink)
    attributes = {"productId": "123", "promotionId": "456"}

    sink._add_additional_attributes({"Remote_promotionId": "missing"}, attributes)

    assert attributes["productId"] == 123
    assert attributes["promotionId"] == 456

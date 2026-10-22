use serde::Deserialize;

use crate::book::{DepthEvent, Snapshot};
use crate::fixed::{Px, Qty};

#[derive(Deserialize)]
pub(crate) struct Envelope<'a> {
    pub(crate) stream: &'a str,
    #[serde(borrow)]
    pub(crate) data: DepthUpdate<'a>,
}

/// A diff event. Sizes are absolute, and a size of `"0"` deletes the level.
#[derive(Deserialize)]
pub(crate) struct DepthUpdate<'a> {
    #[serde(rename = "e")]
    pub(crate) event_type: &'a str,
    #[serde(rename = "E")]
    pub(crate) event_time_ms: u64,
    #[serde(rename = "s")]
    pub(crate) symbol: &'a str,
    #[serde(rename = "U")]
    pub(crate) first_update_id: u64,
    #[serde(rename = "u")]
    pub(crate) final_update_id: u64,
    #[serde(rename = "pu")]
    pub(crate) prev_final_update_id: u64,
    #[serde(rename = "b")]
    pub(crate) bids: Vec<(Px, Qty)>,
    #[serde(rename = "a")]
    pub(crate) asks: Vec<(Px, Qty)>,
}

#[derive(Deserialize)]
pub(crate) struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub(crate) last_update_id: u64,
    pub(crate) bids: Vec<(Px, Qty)>,
    pub(crate) asks: Vec<(Px, Qty)>,
}

impl From<DepthUpdate<'_>> for DepthEvent {
    fn from(update: DepthUpdate<'_>) -> Self {
        DepthEvent {
            first_update_id: update.first_update_id,
            final_update_id: update.final_update_id,
            prev_final_update_id: update.prev_final_update_id,
            bids: update.bids,
            asks: update.asks,
        }
    }
}

impl From<DepthSnapshot> for Snapshot {
    fn from(snapshot: DepthSnapshot) -> Self {
        Snapshot {
            last_update_id: snapshot.last_update_id,
            bids: snapshot.bids,
            asks: snapshot.asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FRAME: &str = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1792668731538,"T":1792668731535,"s":"BTCUSDT","U":11890853741879,"u":11890853741890,"pu":11890853741878,"b":[["81866.00","0.251"],["81865.80","0"]],"a":[["81866.10","27.503"]]}}"#;

    #[test]
    fn combined_frame_parses_into_a_depth_event() {
        let envelope: Envelope =
            serde_json::from_str(FRAME).expect("well-formed frame");
        assert_eq!(envelope.stream, "btcusdt@depth@100ms");
        assert_eq!(envelope.data.event_type, "depthUpdate");
        assert_eq!(envelope.data.symbol, "BTCUSDT");
        let event = DepthEvent::from(envelope.data);
        assert_eq!(event.first_update_id, 11_890_853_741_879);
        assert_eq!(event.final_update_id, 11_890_853_741_890);
        assert_eq!(event.prev_final_update_id, 11_890_853_741_878);
        assert_eq!(event.bids.len(), 2);
        assert!(event.bids[1].1.is_zero());
        assert_eq!(event.asks[0].0.to_string(), "81866.1");
    }

    #[test]
    fn snapshot_body_parses_into_a_snapshot() {
        let body = r#"{"lastUpdateId":615120220303,"E":1792668732019,"T":1792668732000,"bids":[["81858.5","0.678"]],"asks":[["81858.6","0.347"],["81858.7","0.772"]]}"#;
        let wire: DepthSnapshot =
            serde_json::from_str(body).expect("well-formed body");
        let snapshot = Snapshot::from(wire);
        assert_eq!(snapshot.last_update_id, 615_120_220_303);
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.asks.len(), 2);
    }

    #[test]
    fn bad_decimal_in_a_level_fails_the_frame() {
        let frame = FRAME.replace("27.503", "27.5x");
        let error = serde_json::from_str::<Envelope>(&frame)
            .err()
            .expect("a bad decimal must fail the frame");
        assert!(
            error.to_string().contains("unexpected character 'x'"),
            "{error}"
        );
    }
}

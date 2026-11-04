from __future__ import annotations

import json
from typing import Any, cast

import pytest

from phoenix_channels_python_client.codec import decode_message, encode_message
from phoenix_channels_python_client.messages import SystemEvent
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.utils import make_message
from tests.support import EVENT, TOPIC

V1 = ProtocolVersion.V1
V2 = ProtocolVersion.V2
OPAQUE = b"\x00\xff\x80"


@pytest.mark.parametrize("protocol", [V1, V2])
@pytest.mark.parametrize("ref", [None, "r1", 1])
def test_text_frames_preserve_fields_and_normalize_numeric_refs(
    protocol: ProtocolVersion, ref: str | int | None
) -> None:
    raw = (
        json.dumps([ref, ref, TOPIC, EVENT, {"x": 1}])
        if protocol == V2
        else json.dumps(
            {
                "topic": TOPIC,
                "event": EVENT,
                "ref": ref,
                "join_ref": ref,
                "payload": {"x": 1},
            }
        )
    )
    decoded = decode_message(raw, protocol)
    message = decoded.message
    normalized_ref = str(ref) if ref is not None else None
    assert (
        message.topic,
        message.event,
        message.ref,
        message.join_ref,
        message.payload,
        decoded.binary_reply_status,
    ) == (TOPIC, EVENT, normalized_ref, normalized_ref, {"x": 1}, None)


@pytest.mark.parametrize(
    ("protocol", "raw", "expected_exception"),
    [
        (V2, '{"bad": "shape"}', TypeError),
        (V2, "[1, 2, 3]", ValueError),
        (V2, json.dumps([None, None, "", EVENT, {}]), TypeError),
        (V2, json.dumps([None, None, TOPIC, "", {}]), TypeError),
        (V1, "[1, 2, 3]", TypeError),
        (V1, '{"topic": "", "event": "x", "payload": {}}', TypeError),
        (V1, '{"topic": "t", "event": "", "payload": {}}', TypeError),
        (V1, "{", ValueError),
        (V2, "{", ValueError),
    ],
)
def test_malformed_text_frames_are_rejected(
    protocol: ProtocolVersion,
    raw: str,
    expected_exception: type[Exception],
) -> None:
    with pytest.raises(expected_exception):
        decode_message(raw, protocol)


@pytest.mark.parametrize(
    ("protocol", "raw"),
    [
        (V2, json.dumps([None, None, TOPIC, EVENT, None])),
        (V1, json.dumps({"topic": TOPIC, "event": EVENT, "payload": "not-a-dict"})),
    ],
)
def test_text_frames_without_an_object_payload_normalize_to_an_empty_map(
    protocol: ProtocolVersion, raw: str
) -> None:
    assert decode_message(raw, protocol).message.payload == {}


@pytest.mark.parametrize(
    ("topic", "subtopic"),
    [("room:lobby", "lobby"), ("room:a:b", "a:b"), ("lobby", None)],
)
def test_message_subtopic_is_what_follows_the_first_colon(
    topic: str, subtopic: str | None
) -> None:
    message = decode_message(json.dumps([None, None, topic, EVENT, {}]), V2).message
    assert message.subtopic == subtopic


@pytest.mark.parametrize("protocol", [V1, V2])
def test_dict_payload_encoding_retains_the_protocol_text_shape(
    protocol: ProtocolVersion,
) -> None:
    message = make_message(
        event=EVENT, topic=TOPIC, join_ref="jr", ref="r1", payload={"x": 1}
    )
    encoded = encode_message(message, protocol)
    assert isinstance(encoded, str)
    expected = (
        ["jr", "r1", TOPIC, EVENT, {"x": 1}]
        if protocol == V2
        else {"topic": TOPIC, "event": EVENT, "ref": "r1", "payload": {"x": 1}}
    )
    assert json.loads(encoded) == expected


def test_unserializable_json_payload_raises_type_error_with_its_cause() -> None:
    message = make_message(event=EVENT, topic=TOPIC, payload={"bad": {1}})
    with pytest.raises(TypeError) as failure:
        encode_message(message, V2)
    assert isinstance(failure.value.__cause__, TypeError)


def test_client_binary_push_uses_four_lengths_including_its_ref() -> None:
    message = make_message(
        event="new", topic="room", join_ref="jr", ref="r1", payload=OPAQUE
    )
    assert encode_message(message, V2) == b"\x00\x02\x02\x04\x03jrr1roomnew" + OPAQUE


def test_client_binary_headers_count_utf8_bytes() -> None:
    message = make_message(event="év", topic="rém", join_ref="ñ", ref="界", payload=b"")
    assert encode_message(message, V2) == (
        b"\x00\x02\x03\x04\x03\xc3\xb1\xe7\x95\x8cr\xc3\xa9m\xc3\xa9v"
    )


@pytest.mark.parametrize("ref", [None, ""])
def test_client_binary_missing_refs_are_zero_length_fields(ref: str | None) -> None:
    message = make_message(
        event="new", topic="room", join_ref=ref, ref=ref, payload=b""
    )
    assert encode_message(message, V2) == b"\x00\x00\x00\x04\x03roomnew"


@pytest.mark.parametrize("field", ["join_ref", "ref", "topic", "event"])
def test_outbound_binary_header_accepts_255_encoded_bytes(field: str) -> None:
    value = "é" * 127 + "a"
    fields = {"join_ref": "jr", "ref": "r1", "topic": "room", "event": "new"}
    fields[field] = value
    message = make_message(**fields, payload=OPAQUE)
    encoded = encode_message(message, V2)
    expected_fields = [
        fields[name].encode() for name in ("join_ref", "ref", "topic", "event")
    ]
    assert (
        encoded
        == bytes((0, *(len(part) for part in expected_fields)))
        + b"".join(expected_fields)
        + OPAQUE
    )


@pytest.mark.parametrize("field", ["join_ref", "ref", "topic", "event"])
def test_outbound_binary_header_rejects_256_encoded_bytes(field: str) -> None:
    fields = {"join_ref": "jr", "ref": "r1", "topic": "room", "event": "new"}
    fields[field] = "é" * 128
    message = make_message(**fields, payload=OPAQUE)
    with pytest.raises(ValueError, match="255 UTF-8 bytes"):
        encode_message(message, V2)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"\x00\x02\x04\x03jrroomnew", ("jr", None, "room", "new", None)),
        (
            b"\x01\x02\x02\x04\x02jrr1roomok",
            ("jr", "r1", "room", SystemEvent.reply, "ok"),
        ),
        (b"\x02\x04\x03roomnew", (None, None, "room", "new", None)),
        (b"\x00\x00\x04\x03roomnew", (None, None, "room", "new", None)),
        (b"\x01\x00\x00\x04\x00room", (None, None, "room", SystemEvent.reply, "")),
    ],
)
@pytest.mark.parametrize("payload", [b"", OPAQUE])
def test_server_binary_layouts_preserve_opaque_payload_and_reply_metadata(
    raw: bytes,
    expected: tuple[str | None, str | None, str, str, str | None],
    payload: bytes,
) -> None:
    decoded = decode_message(raw + payload, V2)
    message = decoded.message
    assert (
        message.join_ref,
        message.ref,
        message.topic,
        message.event,
        decoded.binary_reply_status,
    ) == expected
    assert message.payload == payload


def test_server_binary_headers_decode_utf8_and_leave_payload_opaque() -> None:
    decoded = decode_message(
        b"\x00\x02\x04\x03\xc3\xb1r\xc3\xa9m\xc3\xa9v" + OPAQUE, V2
    )
    assert (
        decoded.message.join_ref,
        decoded.message.topic,
        decoded.message.event,
        decoded.message.payload,
    ) == ("ñ", "rém", "év", OPAQUE)


@pytest.mark.parametrize(
    "raw",
    [
        frame[:end]
        for frame in (
            b"\x00\x02\x04\x03jrroomnew",
            b"\x01\x02\x02\x04\x02jrr1roomok",
            b"\x02\x04\x03roomnew",
        )
        for end in range(len(frame))
    ],
)
def test_every_truncated_server_binary_header_is_rejected(raw: bytes) -> None:
    with pytest.raises(ValueError, match=r"Empty|Truncated"):
        decode_message(raw, V2)


@pytest.mark.parametrize("kind", [3, 255])
def test_unknown_binary_frame_kind_is_rejected(kind: int) -> None:
    with pytest.raises(ValueError, match="not a valid"):
        decode_message(bytes((kind,)) + OPAQUE, V2)


@pytest.mark.parametrize(
    "raw",
    [
        b"\x00\x01\x01\x01\xffte",
        b"\x00\x01\x01\x01j\xffe",
        b"\x00\x01\x01\x01jt\xff",
        b"\x01\x01\x01\x01\x01\xffrts",
        b"\x01\x01\x01\x01\x01j\xffts",
        b"\x01\x01\x01\x01\x01jr\xffs",
        b"\x01\x01\x01\x01\x01jrt\xff",
        b"\x02\x01\x01\xffe",
        b"\x02\x01\x01t\xff",
    ],
)
def test_invalid_utf8_in_any_server_header_field_is_rejected(raw: bytes) -> None:
    with pytest.raises(UnicodeDecodeError):
        decode_message(raw, V2)


def test_v1_rejects_outbound_binary_payloads() -> None:
    message = make_message(event=EVENT, topic=TOPIC, payload=OPAQUE)
    with pytest.raises(TypeError, match="V2"):
        encode_message(message, V1)


@pytest.mark.parametrize("raw", [OPAQUE, b'{"topic":"room","event":"new"}'])
def test_v1_rejects_all_binary_frames_including_utf8_json(raw: bytes) -> None:
    with pytest.raises(TypeError, match="V2"):
        decode_message(raw, V1)


@pytest.mark.parametrize(
    ("topic", "event"),
    [(TOPIC, SystemEvent.leave), ("phoenix", "heartbeat")],
)
def test_binary_payload_is_rejected_for_json_control_frames(
    topic: str, event: str
) -> None:
    message = make_message(event=event, topic=topic, payload=OPAQUE)
    with pytest.raises(TypeError, match="Leave and heartbeat"):
        encode_message(message, V2)


def test_binary_join_encodes_as_a_v2_binary_push() -> None:
    message = make_message(
        event=SystemEvent.join, topic="room", join_ref="1", ref="1", payload=OPAQUE
    )
    assert (
        encode_message(message, V2)
        == b"\x00\x01\x01\x04\x08" + b"1" + b"1" + b"room" + b"phx_join" + OPAQUE
    )


def test_binary_join_requires_v2() -> None:
    message = make_message(event=SystemEvent.join, topic=TOPIC, payload=OPAQUE)
    with pytest.raises(TypeError, match="V2"):
        encode_message(message, V1)


def test_heartbeat_named_application_event_can_have_a_binary_payload() -> None:
    message = make_message(event="heartbeat", topic="room", payload=OPAQUE)
    assert encode_message(message, V2) == b"\x00\x00\x00\x04\x09roomheartbeat" + OPAQUE


@pytest.mark.parametrize("payload", ["text", bytearray(b"x"), memoryview(b"x")])
def test_message_payload_does_not_coerce_non_payload_types(payload: object) -> None:
    with pytest.raises(TypeError):
        make_message(event=EVENT, topic=TOPIC, payload=cast(Any, payload))


@pytest.mark.parametrize("raw", [bytearray(b"{}"), memoryview(b"{}"), 1])
def test_decoder_rejects_values_outside_its_frame_types(raw: object) -> None:
    with pytest.raises(TypeError, match="Frames must be str or bytes"):
        decode_message(cast(Any, raw), V2)

#pylint: skip-file
import io
import struct

import pytest

import kafka.errors as Errors
from kafka.protocol.api import RequestHeader
from kafka.protocol.commit import GroupCoordinatorRequest
from kafka.protocol.fetch import FetchRequest, FetchResponse
from kafka.protocol.message import Message, MessageSet, PartialMessage
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.protocol.parser import KafkaProtocol
from kafka.protocol.types import Int16, Int32, Int64, String


def test_create_message():
    payload = b'test'
    key = b'key'
    msg = Message(payload, key=key)
    assert msg.magic == 0
    assert msg.attributes == 0
    assert msg.key == key
    assert msg.value == payload


def test_encode_message_v0():
    message = Message(b'test', key=b'key')
    encoded = message.encode()
    expect = b''.join([
        struct.pack('>i', -1427009701), # CRC
        struct.pack('>bb', 0, 0),       # Magic, flags
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    assert encoded == expect


def test_encode_message_v1():
    message = Message(b'test', key=b'key', magic=1, timestamp=1234)
    encoded = message.encode()
    expect = b''.join([
        struct.pack('>i', 1331087195),  # CRC
        struct.pack('>bb', 1, 0),       # Magic, flags
        struct.pack('>q', 1234),        # Timestamp
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    assert encoded == expect


def test_decode_message():
    encoded = b''.join([
        struct.pack('>i', -1427009701), # CRC
        struct.pack('>bb', 0, 0),       # Magic, flags
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    decoded_message = Message.decode(encoded)
    msg = Message(b'test', key=b'key')
    msg.encode() # crc is recalculated during encoding
    assert decoded_message == msg


def test_decode_message_validate_crc():
    encoded = b''.join([
        struct.pack('>i', -1427009701), # CRC
        struct.pack('>bb', 0, 0),       # Magic, flags
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    decoded_message = Message.decode(encoded)
    assert decoded_message.validate_crc() is True

    encoded = b''.join([
        struct.pack('>i', 1234),           # Incorrect CRC
        struct.pack('>bb', 0, 0),       # Magic, flags
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    decoded_message = Message.decode(encoded)
    assert decoded_message.validate_crc() is False


def test_encode_message_set():
    messages = [
        Message(b'v1', key=b'k1'),
        Message(b'v2', key=b'k2')
    ]
    encoded = MessageSet.encode([(0, msg.encode())
                                 for msg in messages])
    expect = b''.join([
        struct.pack('>q', 0),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                          # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                          # Value

        struct.pack('>q', 0),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                          # Key
        struct.pack('>i', 2),          # Length of value
        b'v2',                          # Value
    ])
    expect = struct.pack('>i', len(expect)) + expect
    assert encoded == expect


def test_decode_message_set():
    encoded = b''.join([
        struct.pack('>q', 0),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        struct.pack('>q', 1),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v2',                         # Value
    ])

    msgs = MessageSet.decode(encoded, bytes_to_read=len(encoded))
    assert len(msgs) == 2
    msg1, msg2 = msgs

    returned_offset1, message1_size, decoded_message1 = msg1
    returned_offset2, message2_size, decoded_message2 = msg2

    assert returned_offset1 == 0
    message1 = Message(b'v1', key=b'k1')
    message1.encode()
    assert decoded_message1 == message1

    assert returned_offset2 == 1
    message2 = Message(b'v2', key=b'k2')
    message2.encode()
    assert decoded_message2 == message2


def test_encode_message_header():
    expect = b''.join([
        struct.pack('>h', 10),             # API Key
        struct.pack('>h', 0),              # API Version
        struct.pack('>i', 4),              # Correlation Id
        struct.pack('>h', len('client3')), # Length of clientId
        b'client3',                        # ClientId
    ])

    req = GroupCoordinatorRequest[0]('foo')
    header = RequestHeader(req, correlation_id=4, client_id='client3')
    assert header.encode() == expect


def test_decode_message_set_partial():
    encoded = b''.join([
        struct.pack('>q', 0),          # Msg Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        struct.pack('>q', 1),          # Msg Offset
        struct.pack('>i', 24),         # Msg Size (larger than remaining MsgSet size)
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 8),          # Length of value
        b'ar',                         # Value (truncated)
    ])

    msgs = MessageSet.decode(encoded, bytes_to_read=len(encoded))
    assert len(msgs) == 2
    msg1, msg2 = msgs

    returned_offset1, message1_size, decoded_message1 = msg1
    returned_offset2, message2_size, decoded_message2 = msg2

    assert returned_offset1 == 0
    message1 = Message(b'v1', key=b'k1')
    message1.encode()
    assert decoded_message1 == message1

    assert returned_offset2 is None
    assert message2_size is None
    assert decoded_message2 == PartialMessage()


def test_decode_fetch_response_partial():
    encoded = b''.join([
        Int32.encode(1),               # Num Topics (Array)
        String('utf-8').encode('foobar'),
        Int32.encode(2),               # Num Partitions (Array)
        Int32.encode(0),               # Partition id
        Int16.encode(0),               # Error Code
        Int64.encode(1234),            # Highwater offset
        Int32.encode(52),              # MessageSet size
        Int64.encode(0),               # Msg Offset
        Int32.encode(18),              # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        Int64.encode(1),               # Msg Offset
        struct.pack('>i', 24),         # Msg Size (larger than remaining MsgSet size)
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 8),          # Length of value
        b'ar',                         # Value (truncated)
        Int32.encode(1),
        Int16.encode(0),
        Int64.encode(2345),
        Int32.encode(52),              # MessageSet size
        Int64.encode(0),               # Msg Offset
        Int32.encode(18),              # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        Int64.encode(1),               # Msg Offset
        struct.pack('>i', 24),         # Msg Size (larger than remaining MsgSet size)
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 8),          # Length of value
        b'ar',                         # Value (truncated)
    ])
    resp = FetchResponse[0].decode(io.BytesIO(encoded))
    assert len(resp.topics) == 1
    topic, partitions = resp.topics[0]
    assert topic == 'foobar'
    assert len(partitions) == 2

    m1 = MessageSet.decode(
        partitions[0][3], bytes_to_read=len(partitions[0][3]))
    assert len(m1) == 2
    assert m1[1] == (None, None, PartialMessage())


def test_struct_unrecognized_kwargs():
    try:
        mr = MetadataRequest[0](topicz='foo')
        assert False, 'Structs should not allow unrecognized kwargs'
    except ValueError:
        pass


def test_struct_missing_kwargs():
    fr = FetchRequest[0](max_wait_time=100)
    assert fr.min_bytes is None


def _encode_frame(payload, size=None):
    """Prefix payload with a 4-byte big-endian frame size header.

    An explicit `size` overrides the real payload length, which is how a
    malicious/corrupt broker advertises a frame it never intends to send.
    """
    if size is None:
        size = len(payload)
    return Int32.encode(size) + payload


def _in_flight_metadata_protocol(**kwargs):
    protocol = KafkaProtocol(client_id='client3', **kwargs)
    correlation_id = protocol.send_request(MetadataRequest[0]([]))
    protocol.send_bytes()
    return protocol, correlation_id


def test_protocol_frame_size_over_max_frame_size():
    # An oversized frame header must be rejected before the receive buffer
    # for it is allocated -- otherwise a single 4-byte header can make the
    # client allocate an arbitrary amount of memory (OOM).
    protocol, _ = _in_flight_metadata_protocol(max_frame_size=1000)
    with pytest.raises(Errors.InvalidReceiveError):
        protocol.receive_bytes(_encode_frame(b'', size=1001))
    assert protocol._rbuffer is None


def test_protocol_frame_size_max_int32_with_default_client_limit():
    # The worst case: Int32 max advertises a ~2GB allocation. The default
    # BrokerConnection limit (receive_message_max_bytes) must reject it.
    protocol, _ = _in_flight_metadata_protocol(max_frame_size=1000000)
    with pytest.raises(Errors.InvalidReceiveError):
        protocol.receive_bytes(_encode_frame(b'', size=2**31 - 1))
    assert protocol._rbuffer is None


@pytest.mark.parametrize("nbytes", [-1, -2**31])
def test_protocol_frame_size_negative(nbytes):
    # Int32 is signed, so a header with the high bit set decodes negative.
    protocol, _ = _in_flight_metadata_protocol(max_frame_size=1000)
    with pytest.raises(Errors.InvalidReceiveError):
        protocol.receive_bytes(_encode_frame(b'', size=nbytes))
    assert protocol._rbuffer is None


def test_protocol_frame_size_error_is_kafka_protocol_error():
    # Existing `except KafkaProtocolError` handlers must keep working.
    assert issubclass(Errors.InvalidReceiveError, Errors.KafkaProtocolError)


def test_protocol_frame_size_within_limit_still_parses():
    # Regression: a well-formed, in-bounds frame is unaffected by the check.
    protocol, correlation_id = _in_flight_metadata_protocol(max_frame_size=1000)
    # keep a strong ref -- Struct.encode is a WeakMethod bound to the instance
    expected = MetadataResponse[0](brokers=[(0, 'host1', 9092)], topics=[])
    payload = Int32.encode(correlation_id) + expected.encode()
    frame = _encode_frame(payload)
    assert len(frame) - 4 <= 1000

    responses = protocol.receive_bytes(frame)
    assert len(responses) == 1
    recv_correlation_id, response = responses[0]
    assert recv_correlation_id == correlation_id
    assert response.brokers == [(0, 'host1', 9092)]


def test_protocol_frame_size_default_is_100mb():
    # Default keeps a generous bound for anyone constructing KafkaProtocol
    # directly, but Int32 max is still rejected.
    protocol, _ = _in_flight_metadata_protocol()
    assert protocol._max_frame_size == 100000000
    with pytest.raises(Errors.InvalidReceiveError):
        protocol.receive_bytes(_encode_frame(b'', size=100000001))

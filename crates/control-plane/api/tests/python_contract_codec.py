#!/usr/bin/env python3

import json
import sys


def variant(value):
    if isinstance(value, str):
        return value, None
    if isinstance(value, dict) and len(value) == 1:
        return next(iter(value.items()))
    raise ValueError(f"invalid enum encoding: {value!r}")


def encode_varint(value):
    out = []
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            byte |= 0x80
        out.append(byte)
        if not value:
            return out


def decode_varint(data, offset=0):
    shift = 0
    value = 0
    while offset < len(data):
        byte = data[offset]
        value |= (byte & 0x7F) << shift
        offset += 1
        if not (byte & 0x80):
            return value, offset
        shift += 7
        if shift >= 64:
            raise ValueError("varint exceeds 64 bits")
    raise ValueError("unexpected end of varint")


def encode_value(schema, root, value):
    tag, payload = variant(root)
    value_tag, value_payload = variant(value)
    if tag == "Bool":
        if value_tag != "Bool":
            raise ValueError(f"expected bool, got {value}")
        return [1 if value_payload else 0]
    if tag == "U64":
        if value_tag != "U64":
            raise ValueError(f"expected u64, got {value}")
        return encode_varint(value_payload)
    if tag == "String":
        if value_tag != "String":
            raise ValueError(f"expected string, got {value}")
        return list(value_payload.encode())
    if tag == "Bytes":
        if value_tag != "Bytes":
            raise ValueError(f"expected bytes, got {value}")
        return value_payload
    if tag == "List":
        if value_tag != "List":
            raise ValueError(f"expected list, got {value}")
        item_ty = payload
        out = encode_varint(len(value_payload))
        for item in value_payload:
            item_bytes = encode_value(schema, item_ty, item)
            out.extend(encode_varint(len(item_bytes)))
            out.extend(item_bytes)
        return out
    if tag == "Map":
        if value_tag != "Map":
            raise ValueError(f"expected map, got {value}")
        key_ty, value_ty = payload
        entries = [(encode_value(schema, key_ty, k), encode_value(schema, value_ty, v)) for k, v in value_payload]
        entries.sort(key=lambda pair: pair[0])
        for left, right in zip(entries, entries[1:]):
            if left[0] == right[0]:
                raise ValueError("duplicate map key")
        out = encode_varint(len(entries))
        for key_bytes, value_bytes in entries:
            out.extend(encode_varint(len(key_bytes)))
            out.extend(key_bytes)
            out.extend(encode_varint(len(value_bytes)))
            out.extend(value_bytes)
        return out
    if tag != "Named":
        raise ValueError(f"unknown type ref {root}")
    typedef = schema["types"][payload]
    type_tag, type_payload = variant(typedef)
    if type_tag == "Record":
        if value_tag != "Record":
            raise ValueError(f"expected record, got {value}")
        fields = type_payload["fields"]
        record = value_payload
        encoded_fields = []
        for field in fields:
            field_name = field["name"]
            presence_tag, presence_payload = variant(field["presence"])
            if field_name not in record:
                if presence_tag == "Required":
                    raise ValueError(f"missing required field `{field_name}`")
                continue
            field_value = record[field_name]
            if presence_tag == "Defaulted" and field_value == presence_payload:
                continue
            encoded_fields.append((field["id"], encode_value(schema, field["ty"], field_value)))
        unknown = sorted(set(record.keys()) - {field["name"] for field in fields})
        if unknown:
            raise ValueError(f"unknown field `{unknown[0]}`")
        encoded_fields.sort(key=lambda field: field[0])
        out = encode_varint(len(encoded_fields))
        for field_id, field_bytes in encoded_fields:
            out.extend(encode_varint(field_id))
            out.extend(encode_varint(len(field_bytes)))
            out.extend(field_bytes)
        return out
    if type_tag == "Enum":
        if value_tag != "Enum":
            raise ValueError(f"expected enum, got {value}")
        selected = next((candidate for candidate in type_payload["variants"] if candidate["name"] == value_payload["variant"]), None)
        if selected is None:
            raise ValueError(f"unknown variant `{value_payload['variant']}`")
        payload_ty = selected["payload"]
        enum_value = value_payload["value"]
        if payload_ty is None:
            if enum_value is not None:
                raise ValueError(f"variant `{selected['name']}` must not carry payload")
            payload_bytes = []
        else:
            if enum_value is None:
                raise ValueError(f"variant `{selected['name']}` requires payload")
            payload_bytes = encode_value(schema, payload_ty, enum_value)
        return encode_varint(selected["id"]) + encode_varint(len(payload_bytes)) + payload_bytes
    raise ValueError(f"unknown type definition {typedef}")


def decode_value(schema, root, data):
    tag, payload = variant(root)
    if tag == "Bool":
        if data == [0]:
            return {"Bool": False}
        if data == [1]:
            return {"Bool": True}
        raise ValueError("bool payload must be a single 0x00 or 0x01 byte")
    if tag == "U64":
        value, offset = decode_varint(data)
        if offset != len(data):
            raise ValueError("u64 payload has trailing bytes")
        return {"U64": value}
    if tag == "String":
        return {"String": bytes(data).decode()}
    if tag == "Bytes":
        return {"Bytes": data}
    if tag == "List":
        item_ty = payload
        count, offset = decode_varint(data)
        items = []
        for _ in range(count):
            item_len, offset = decode_varint(data, offset)
            item = data[offset : offset + item_len]
            if len(item) != item_len:
                raise ValueError("list item length exceeds payload")
            items.append(decode_value(schema, item_ty, item))
            offset += item_len
        if offset != len(data):
            raise ValueError("list payload has trailing bytes")
        return {"List": items}
    if tag == "Map":
        key_ty, value_ty = payload
        count, offset = decode_varint(data)
        entries = []
        for _ in range(count):
            key_len, offset = decode_varint(data, offset)
            key = data[offset : offset + key_len]
            if len(key) != key_len:
                raise ValueError("map key length exceeds payload")
            offset += key_len
            value_len, offset = decode_varint(data, offset)
            item = data[offset : offset + value_len]
            if len(item) != value_len:
                raise ValueError("map value length exceeds payload")
            offset += value_len
            entries.append((decode_value(schema, key_ty, key), decode_value(schema, value_ty, item)))
        if offset != len(data):
            raise ValueError("map payload has trailing bytes")
        entries.sort(key=lambda pair: encode_value(schema, key_ty, pair[0]))
        return {"Map": entries}
    typedef = schema["types"][payload]
    type_tag, type_payload = variant(typedef)
    if type_tag == "Record":
        count, offset = decode_varint(data)
        fields = {field["id"]: field for field in type_payload["fields"]}
        seen = set()
        values = {}
        for _ in range(count):
            field_id, offset = decode_varint(data, offset)
            field_len, offset = decode_varint(data, offset)
            field_bytes = data[offset : offset + field_len]
            if len(field_bytes) != field_len:
                raise ValueError("field length exceeds payload")
            offset += field_len
            if field_id in seen:
                raise ValueError(f"duplicate field id {field_id}")
            seen.add(field_id)
            field = fields.get(field_id)
            if field is not None:
                values[field["name"]] = decode_value(schema, field["ty"], field_bytes)
        if offset != len(data):
            raise ValueError("record payload has trailing bytes")
        for field in type_payload["fields"]:
            if field["name"] in values:
                continue
            presence_tag, presence_payload = variant(field["presence"])
            if presence_tag == "Required":
                raise ValueError(f"missing required field `{field['name']}`")
            if presence_tag == "Defaulted":
                values[field["name"]] = presence_payload
        return {"Record": values}
    if type_tag == "Enum":
        variant_id, offset = decode_varint(data)
        payload_len, offset = decode_varint(data, offset)
        payload_bytes = data[offset : offset + payload_len]
        if len(payload_bytes) != payload_len:
            raise ValueError("enum payload length exceeds payload")
        if offset + payload_len != len(data):
            raise ValueError("enum payload has trailing bytes")
        selected = next((candidate for candidate in type_payload["variants"] if candidate["id"] == variant_id), None)
        if selected is None:
            raise ValueError(f"unknown variant id {variant_id}")
        if selected["payload"] is None:
            if payload_bytes:
                raise ValueError(f"variant `{selected['name']}` must not carry payload")
            enum_value = None
        else:
            enum_value = decode_value(schema, selected["payload"], payload_bytes)
        return {"Enum": {"variant": selected["name"], "value": enum_value}}
    raise ValueError(f"unknown type definition {typedef}")


def main():
    request = json.load(sys.stdin)
    try:
        if request["action"] == "encode":
            print(json.dumps({"ok": True, "bytes": encode_value(request["schema"], request["root"], request["value"])}))
            return
        if request["action"] == "decode":
            print(json.dumps({"ok": True, "value": decode_value(request["schema"], request["root"], request["bytes"])}))
            return
        raise ValueError(f"unknown action {request['action']}")
    except Exception as err:
        print(json.dumps({"ok": False, "error": str(err)}))


if __name__ == "__main__":
    main()
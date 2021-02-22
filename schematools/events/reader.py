import json


def read_messages_from_file(messages_path):
    """ Helper function, primarily used for testing """

    with open(messages_path) as ef:
        for line in ef:
            if line.strip():
                source_id, header_str, body_str = line.split("|", maxsplit=2)
                message_headers = json.loads(header_str)
                message_body = json.loads(body_str)
                yield source_id, message_headers, message_body

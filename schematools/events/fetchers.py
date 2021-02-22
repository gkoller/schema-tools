def fetch_insert_data(event_data):
    # remove dict values from the event. We only handle scalar datatypes.
    # Json blobs in the events are containing irrelevant GOB-only data.
    return {k: v for k, v in event_data["entity"].items() if not isinstance(v, dict)}


def fetch_update_data(event_data):
    # Update data has a different structure in the events. We convert it
    # into a single dict (like for insert data), so we can handle
    # the data in the exact same way.
    update_data = {}
    for modification in event_data["modifications"]:
        # XXX skip geometrie for now, has geojson format (should be wkt)
        if modification["key"] == "geometrie":
            continue
        update_data[modification["key"]] = modification["new_value"]
    return update_data

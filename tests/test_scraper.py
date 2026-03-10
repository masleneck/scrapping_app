from app.models import Direction, FlightStatus
from app.scraper import (
    parse_flightaware_airport_regex,
    parse_flights_html,
    parse_flights_html_generic_table,
    parse_kupibilet_embedded_schedule,
    parse_svo_bitrix_payload,
)


def test_parse_flights_html_extracts_rows() -> None:
    html = """
    <table id='flights'><tbody>
      <tr>
        <td>SU100</td><td>ARR</td><td>2026-03-01 10:15</td><td>2026-03-01 10:22</td>
        <td></td><td>A320</td><td>B</td><td>DELAYED</td>
      </tr>
      <tr>
        <td>SU245</td><td>DEP</td><td>2026-03-01 11:05</td><td></td>
        <td></td><td>B738</td><td>C</td><td>BOARDING</td>
      </tr>
    </tbody></table>
    """

    snapshots = parse_flights_html(html=html, source="test")

    assert len(snapshots) == 2
    assert snapshots[0].direction == Direction.ARR
    assert snapshots[0].status == FlightStatus.DELAYED
    assert snapshots[1].direction == Direction.DEP
    assert snapshots[1].status == FlightStatus.BOARDING


def test_parse_flights_html_generic_table_headers() -> None:
    html = """
    <table>
      <thead>
        <tr>
          <th>Flight</th><th>Direction</th><th>Scheduled</th><th>Status</th><th>Terminal</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>AF123</td><td>ARRIVAL</td><td>2026-03-01 12:00</td><td>LANDED</td><td>D</td>
        </tr>
      </tbody>
    </table>
    """

    snapshots = parse_flights_html_generic_table(html=html, source="real_test")

    assert len(snapshots) == 1
    assert snapshots[0].flight_number == "AF123"
    assert snapshots[0].direction == Direction.ARR
    assert snapshots[0].status == FlightStatus.LANDED
    assert snapshots[0].terminal == "D"


def test_parse_svo_bitrix_payload_maps_times_and_statuses() -> None:
    payload = {
        "items": [
            {
                "co": {"code": "SU"},
                "flt": "100",
                "ad": "D",
                "t_st": "2026-03-01T12:00:00+03:00",
                "t_et": "2026-03-01T12:45:00+03:00",
                "t_at": None,
                "term": "B",
                "aircraft_type_name": "Airbus A320",
                "vip_status": "unknown",
            }
        ]
    }

    snapshots = parse_svo_bitrix_payload(payload=payload, source="svo_official")

    assert len(snapshots) == 1
    assert snapshots[0].flight_number == "SU100"
    assert snapshots[0].direction == Direction.DEP
    assert snapshots[0].status == FlightStatus.DELAYED
    assert snapshots[0].terminal == "B"


def test_parse_kupibilet_embedded_schedule_extracts_records() -> None:
    schedule_blob = (
        '<script>self.__next_f.push([1,"15:{\\"6tpibioa|ff.fetchScheduleQuery.$data\\":['
        '{\\"departure_airport\\":\\"SVO\\",\\"arrival_airport\\":\\"LED\\",'
        '\\"departure_date\\":\\"2026-03-01\\",\\"departure_time\\":\\"15:10\\",'
        '\\"departure_estimated_time\\":\\"15:25\\",\\"departure_actual_time\\":null,'
        '\\"departure_timezone\\":\\"Europe/Moscow\\",\\"departure_terminal\\":\\"B\\",'
        '\\"equipment\\":\\"Airbus A320\\",\\"last_status\\":\\"active\\",'
        '\\"flights\\":[{\\"flight_number\\":\\"SU 100\\"}]}],'
        '\\"online-table/schedule\\":\\"$15\\"}"]);</script>'
    )
    html = schedule_blob

    snapshots = parse_kupibilet_embedded_schedule(html=html, source="kupibilet_aggregator")

    assert len(snapshots) == 1
    assert snapshots[0].flight_number == "SU100"
    assert snapshots[0].direction == Direction.DEP
    assert snapshots[0].scheduled_time is not None
    assert snapshots[0].status == FlightStatus.SCHEDULED


def test_parse_flightaware_airport_regex_extracts_flights() -> None:
    html = """
    <a href="/live/flight/AFL1975/history/20260301/1530Z/UZTT/UUEE">AFL1975</a>
    <a href="/live/flight/SDM5956/history/20260301/1345Z/HESH/UUEE">SDM5956</a>
    """
    snapshots = parse_flightaware_airport_regex(
        html=html,
        source="flightaware_airport_regex",
        provider="flightaware_airport",
        strategy="flightaware_airport_regex",
        source_priority=50,
    )
    assert len(snapshots) == 2
    assert snapshots[0].direction == Direction.ARR
    assert snapshots[0].normalized_flight_number == "SU1975"

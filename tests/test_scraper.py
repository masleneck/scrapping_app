from app.models import Direction, FlightStatus
from app.scraper import parse_flights_html, parse_flights_html_generic_table


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

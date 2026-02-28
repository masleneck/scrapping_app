from app.models import Direction, FlightStatus
from app.scraper import parse_flights_html


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

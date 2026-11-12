"""Parse the versioned synthetic monitoring bulletin table."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser


class BulletinTable(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.active = False
        self.cell = None
        self.row = []
        self.rows = []
        self.matches = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table" and dict(attrs).get("id") == "observations":
            self.active = True
            self.matches += 1
        if self.active and tag == "tr":
            self.row = []
        if self.active and tag in ("td", "th"):
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if self.active and tag in ("td", "th") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        if self.active and tag == "tr" and self.row:
            self.rows.append(self.row)
        if tag == "table":
            self.active = False


HEADERS = ["station", "observed_at", "pollutant", "value", "unit", "qualifier"]


def parse_bulletin(html):
    parser = BulletinTable()
    parser.feed(html)
    if parser.matches != 1 or not parser.rows or parser.rows[0] != HEADERS:
        raise ValueError("expected one observations table with contract headers")
    results = []
    for line, cells in enumerate(parser.rows[1:], 2):
        try:
            if len(cells) != len(HEADERS):
                raise ValueError("wrong cell count")
            station, timestamp, pollutant, value, unit, qualifier = cells
            if not station or pollutant not in {"PM2.5", "PM10", "NO2"}:
                raise ValueError("unknown station/pollutant")
            instant = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if instant.tzinfo is None:
                raise ValueError("timezone is required")
            instant = instant.astimezone(timezone.utc).isoformat()
            if unit not in {"ug/m3", "mg/m3"} or qualifier not in {"measured", "ND"}:
                raise ValueError("unknown unit/qualifier")
            if qualifier == "ND":
                if value:
                    raise ValueError("ND must not contain a numeric value")
                normalized = None
            else:
                concentration = Decimal(value)
                if (not concentration.is_finite() or concentration < 0
                        or (concentration != 0 and concentration < Decimal("1e-12"))
                        or concentration > (Decimal("1e6") if unit == "mg/m3" else Decimal("1e9"))):
                    raise ValueError("concentration is outside the finite nonnegative v1 range")
                normalized = format(concentration * (1000 if unit == "mg/m3" else 1), "f")
            results.append((station, instant, pollutant, normalized, qualifier))
        except (ValueError, InvalidOperation) as exc:
            raise ValueError(f"bulletin row {line}: {exc}") from exc
    return results

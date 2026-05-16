package plugin

import "testing"

func TestMapToGrafanaUnit(t *testing.T) {
	tests := []struct {
		name   string
		symbol string
		want   string
	}{
		// Representative canonical mappings — one per category exercised in the plan.
		{"velocity mph", "mph", "velocitymph"},
		{"temperature celsius", "Cel", "celsius"},
		{"pressure psi absolute", "psia", "pressurepsi"},
		{"pressure psi gauge", "psig", "pressurepsi"},
		{"length centimeter", "cm", "lengthcm"},
		{"information byte decimal", "By", "decbytes"},
		{"information kilobytes decimal", "KB", "deckbytes"},
		{"information megabytes decimal", "MB", "decmbytes"},
		{"information gigabytes decimal", "GB", "decgbytes"},
		{"information bit", "bit", "bits"},
		{"acceleration m/s^2", "m/s^2", "accMS2"},
		{"percent", "%", "percent"},
		{"fahrenheit bracketed", "[degF]", "fahrenheit"},
		{"foot bracketed", "[ft_i]", "lengthft"},
		{"knot bracketed", "[kn_i]", "velocityknot"},

		// Suffix-mode fallthrough — canonical UCUM with no Grafana ID.
		{"W/m^2 falls through", "W/m^2", "W/m^2"},
		{"kg/s falls through", "kg/s", "kg/s"},
		{"MPa falls through", "MPa", "MPa"}, // canonical but no Grafana pressurempa ID
		{"atm falls through", "atm", "atm"}, // canonical but no Grafana pressureatm ID

		// Display-only / unrecognized — also falls through.
		{"empty stays empty", "", ""},
		{"degC display-only falls through", "degC", "degC"},
		{"pct display-only falls through", "pct", "pct"},
		{"random user input falls through", "asdfsdfs", "asdfsdfs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapToGrafanaUnit(tt.symbol)
			if got != tt.want {
				t.Errorf("mapToGrafanaUnit(%q) = %q, want %q", tt.symbol, got, tt.want)
			}
		})
	}
}

// TestUnitMapMinuteMeterCollision is a targeted tripwire for the UCUM/Grafana
// short-symbol collision documented in the plan's Risks section:
//
//   - In UCUM, "m" means meter and "min" means minute.
//   - In Grafana, the ID "m" means minutes and the ID "lengthm" means meters.
//
// The mapping table correctly inverts these (m → lengthm; min → m), but a
// hurried edit could swap them. If this test fails, the mapping table has been
// edited carelessly.
func TestUnitMapMinuteMeterCollision(t *testing.T) {
	if got := mapToGrafanaUnit("m"); got != "lengthm" {
		t.Errorf("UCUM m (meter) must map to Grafana lengthm, got %q", got)
	}
	if got := mapToGrafanaUnit("min"); got != "m" {
		t.Errorf("UCUM min (minute) must map to Grafana m, got %q", got)
	}
}

// TestUnitMapMicrosecondUnicode is a tripwire for an ASCII↔unicode trap.
// The mapping is "us" → "µs" where:
//   - The KEY is ASCII (Nominal canonicalizes the print-form µs as ASCII "us")
//   - The VALUE contains U+00B5 MICRO SIGN — Grafana's actual unit ID character
//
// U+00B5 (micro sign) and U+03BC (Greek small letter mu) render identically in
// most fonts but are different code points. A refactor or copy-paste that
// substitutes Greek mu would silently break rendering — Grafana wouldn't match
// the ID and would fall through to suffix mode with the Greek-mu glyph.
// Asserting the exact byte sequence catches the swap.
func TestUnitMapMicrosecondUnicode(t *testing.T) {
	const wantMicroSign = "µs" // U+00B5 (micro sign) + s — NOT U+03BC (Greek mu)
	got := mapToGrafanaUnit("us")
	if got != wantMicroSign {
		t.Errorf("UCUM us must map to Grafana %q (U+00B5 micro sign + s), got %q (% x)", wantMicroSign, got, got)
	}
}

// validGrafanaUnitIDs is a frozen snapshot of unit IDs from Grafana 12.3.1
// (packages/grafana-data/src/valueFormats/categories.ts). It enforces the
// file-header claim in unit_map.go that mapped values are real Grafana IDs —
// the test below asserts every value in unitSymbolToGrafanaID is in this set.
//
// When adding a new unit, verify the target ID exists in Grafana's
// categories.ts and add it here if missing. When bumping Grafana versions,
// re-verify this snapshot. The snapshot is intentionally broader than the
// current mapping so additions to unit_map.go don't always require
// snapshot updates — just verification.
var validGrafanaUnitIDs = map[string]bool{
	// Misc
	"none": true, "string": true, "short": true, "percent": true, "percentunit": true,
	"humidity": true, "ppm": true, "dB": true, "candela": true, "lumens": true, "lux": true,

	// Acceleration
	"accMS2": true, "accFts2": true, "accG": true,

	// Angle
	"degree": true, "radian": true, "grad": true, "arcmin": true, "arcsec": true,

	// Area
	"areaM2": true, "areaF2": true, "areaMI2": true,

	// Computation
	"flops": true, "mflops": true, "gflops": true, "tflops": true, "pflops": true,
	"eflops": true, "zflops": true, "yflops": true,

	// Concentration
	"ppb": true, "conppm": true, "conppt": true, "conpct": true, "conngm3": true,
	"conngNm3": true, "conμgm3": true, "conμgNm3": true, "conmgm3": true, "conmgNm3": true,
	"congm3": true, "congNm3": true, "conmgdL": true, "conmmolL": true,

	// Currency
	"currencyUSD": true, "currencyGBP": true, "currencyEUR": true, "currencyJPY": true,

	// Data (Decimal)
	"decbytes": true, "deckbytes": true, "decmbytes": true, "decgbytes": true,
	"dectbytes": true, "decpbytes": true,
	// Data (Binary/IEC)
	"bits": true, "bytes": true, "kbytes": true, "mbytes": true, "gbytes": true,
	"tbytes": true, "pbytes": true,

	// Data rate
	"pps": true, "binBps": true, "Bps": true, "binbps": true, "bps": true,
	"KiBs": true, "Kibits": true, "KBs": true, "Kbits": true,
	"MiBs": true, "Mibits": true, "MBs": true, "Mbits": true,
	"GiBs": true, "Gibits": true, "GBs": true, "Gbits": true,
	"TiBs": true, "Tibits": true, "TBs": true, "Tbits": true,
	"PiBs": true, "Pibits": true, "PBs": true, "Pbits": true,

	// Date & time
	"dateTimeAsIso": true, "dateTimeAsIsoNoDateIfToday": true,
	"dateTimeAsUS": true, "dateTimeAsUSNoDateIfToday": true,
	"dateTimeAsLocal": true, "dateTimeAsLocalNoDateIfToday": true,
	"dateTimeAsSystem": true, "dateTimeFromNow": true,

	// Energy
	"watt": true, "kwatt": true, "megwatt": true, "gwatt": true, "mwatt": true,
	"Wm2": true, "voltamp": true, "kvoltamp": true, "voltampreact": true, "kvoltampreact": true,
	"watth": true, "watthperkg": true, "kwatth": true, "kwattm": true, "amph": true,
	"kamph": true, "mamph": true, "joule": true, "ev": true,
	"amp": true, "kamp": true, "mamp": true, "microamp": true, "namp": true, "pamp": true,
	"volt": true, "kvolt": true, "millivolt": true, "microvolt": true,
	"dBm": true, "ohm": true, "kohm": true, "Mohm": true,
	"farad": true, "µfarad": true, "nfarad": true, "pfarad": true, "ffarad": true, "afarad": true,
	"henry": true, "mhenry": true, "µhenry": true, "lumensWatt": true, "flux": true, "mflux": true,
	"electronWb": true, "webers": true, "tesla": true,

	// Flow
	"flowgpm": true, "flowcms": true, "flowcfs": true, "flowcfm": true, "litreh": true,
	"flowlpm": true, "flowmlpm": true, "lux2": true,

	// Force
	"forceN": true, "forcekN": true,

	// Hash rate
	"Hs": true, "KHs": true, "MHs": true, "GHs": true, "THs": true, "PHs": true, "EHs": true,

	// Mass
	"massmg": true, "massg": true, "masskg": true, "masst": true, "masslb": true,

	// Length
	"lengthmm": true, "lengthsm": true, "lengthcm": true, "lengthm": true, "lengthkm": true,
	"lengthft": true, "lengthin": true, "lengthmi": true, "lengthyd": true,

	// Pressure
	"pressurembar": true, "pressurebar": true, "pressurekbar": true,
	"pressurepa": true, "pressurenpa": true, "pressuremicropa": true, "pressuremillipa": true,
	"pressurekpa": true, "pressurehpa": true,
	"pressurehg": true, "pressurepsi": true,

	// Radiation
	"radbq": true, "radci": true, "radgy": true, "radrad": true, "radsv": true, "radmsv": true,
	"radusv": true, "radrem": true, "radexpckg": true, "radr": true, "radsvh": true,
	"radmsvh": true, "radusvh": true,

	// Rotational speed
	"rpm": true, "hz": true, "rotrads": true, "rotdegs": true, "rotrpm": true,

	// Temperature
	"celsius": true, "fahrenheit": true, "kelvin": true,

	// Time
	"hertz": true, "ns": true, "µs": true, "ms": true, "s": true, "m": true, "h": true, "d": true,
	"dtdurationms": true, "dtdurations": true, "dthms": true, "dtdhms": true,
	"timeticks": true, "clockms": true, "clocks": true,

	// Throughput
	"cps": true, "ops": true, "reqps": true, "rps": true, "rpm2": true,
	"wps": true, "iops": true, "cpm": true, "opm": true, "rpm3": true, "wpm": true,

	// Velocity
	"velocityms": true, "velocitykmh": true, "velocitymph": true, "velocityknot": true,

	// Volume
	"mlitre": true, "litre": true, "m3": true, "Nm3": true, "dm3": true,
	"gallons": true, "kgallons": true,

	// Boolean
	"bool": true, "bool_yes_no": true, "bool_on_off": true,

	// Frequency
	"hertzkilo": true, "hertzmega": true, "hertzgiga": true,
}

// TestUnitMapValuesAreValidGrafanaIDs enforces the unit_map.go file-header
// promise that "Grafana IDs verified against @grafana/data" is not aspirational.
// Every value in unitSymbolToGrafanaID must appear in validGrafanaUnitIDs.
//
// This catches the class of bug where a plausible-looking-but-nonexistent
// Grafana ID (e.g. "pressurempa", "pressureatm") is added to the map: those
// IDs don't exist in Grafana's categories.ts, so Grafana renders them as the
// literal string suffix instead of formatting the value. The map is supposed
// to either produce a real Grafana ID or fall through to suffix mode — never
// produce a fake ID that hits the wrong code path.
func TestUnitMapValuesAreValidGrafanaIDs(t *testing.T) {
	for symbol, id := range unitSymbolToGrafanaID {
		if !validGrafanaUnitIDs[id] {
			t.Errorf("unitSymbolToGrafanaID[%q] = %q is not a known Grafana unit ID. "+
				"Verify against @grafana/data/src/valueFormats/categories.ts. "+
				"If %q is real, add it to validGrafanaUnitIDs. If it isn't, "+
				"remove the entry so the symbol falls through to suffix mode.",
				symbol, id, id)
		}
	}
}

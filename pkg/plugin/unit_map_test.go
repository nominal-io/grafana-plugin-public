package plugin

import "testing"

func TestMapToGrafanaUnit(t *testing.T) {
	tests := []struct {
		name   string
		symbol string
		want   string
	}{
		// Representative canonical mappings — one per UnitProperty category.
		{"velocity mph", "mph", "velocitymph"},
		{"temperature celsius", "Cel", "celsius"},
		{"pressure psi absolute", "psia", "pressurepsi"},
		{"pressure psi gauge", "psig", "pressurepsi"},
		{"rotational rpm", "rpm", "rotrpm"}, // NOT Grafana 'rpm' (= reads/min)
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
		{"W/m^2 falls through", "W/m^2", "suffix:W/m^2"},
		{"kg/s falls through", "kg/s", "suffix:kg/s"},
		{"MPa falls through", "MPa", "suffix:MPa"}, // canonical but no Grafana pressurempa ID
		{"atm falls through", "atm", "suffix:atm"}, // canonical but no Grafana pressureatm ID
		{"cm falls through (no Grafana 12.x ID)", "cm", "suffix:cm"}, // lengthcm lands in 13.1+

		// Micro-prefix mappings (probed 2026-05-18 — see file header).
		{"microF micro-farad spelled-out", "microF", "µfarad"},
		{"microH micro-henry spelled-out", "microH", "µhenry"},

		// Micro-prefix dead-ends — Nominal rejects these symbols, so they can
		// never appear on a real channel. If a future Nominal release ever
		// canonicalizes one of them, SearchChannels would start emitting the
		// canonical form (microF/microH) anyway, and these still fall through.
		{"uF rejected by Nominal — falls through", "uF", "suffix:uF"},
		{"µF non-canonical (server emits microF) — falls through", "µF", "suffix:µF"},
		{"hPa canonicalized to mbar — bare hPa never reaches map", "hPa", "suffix:hPa"},

		// Display-only / unrecognized — also falls through.
		{"empty stays empty", "", ""},
		{"degC display-only falls through", "degC", "suffix:degC"},
		{"pct display-only falls through", "pct", "suffix:pct"},
		{"random user input falls through", "asdfsdfs", "suffix:asdfsdfs"},
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

// TestUnitMapMinuteMeterCollision guards a UCUM/Grafana short-symbol swap:
// UCUM "m" = meter, "min" = minute; Grafana "m" = minutes, "lengthm" = meters.
func TestUnitMapMinuteMeterCollision(t *testing.T) {
	if got := mapToGrafanaUnit("m"); got != "lengthm" {
		t.Errorf("UCUM m (meter) must map to Grafana lengthm, got %q", got)
	}
	if got := mapToGrafanaUnit("min"); got != "m" {
		t.Errorf("UCUM min (minute) must map to Grafana m, got %q", got)
	}
}

// TestUnitMapMicrosecondUnicode guards the "us" → "µs" mapping: the value must
// be U+00B5 MICRO SIGN, not U+03BC GREEK SMALL LETTER MU. Glyphs are identical
// in most fonts but Grafana matches by code point.
func TestUnitMapMicrosecondUnicode(t *testing.T) {
	const wantMicroSign = "µs" // U+00B5 (micro sign) + s — NOT U+03BC (Greek mu)
	got := mapToGrafanaUnit("us")
	if got != wantMicroSign {
		t.Errorf("UCUM us must map to Grafana %q (U+00B5 micro sign + s), got %q (% x)", wantMicroSign, got, got)
	}
}

// validGrafanaUnitIDs is a frozen snapshot of unit IDs from Grafana 12.1.0
// (https://github.com/grafana/grafana/blob/v12.1.0/packages/grafana-data/src/valueFormats/categories.ts).
//
// v12.1.0 is the minimum supported Grafana version (see plugin.json grafanaDependency
// >=12.1.0). The unit registry is invariant across v12.1.0–v12.4.x (verified by
// diffing categories.ts between tags), so this snapshot is also valid for the
// latest 12.x patch. Pinning to the floor catches mappings that would break on the
// oldest supported runtime — a value that's valid in 12.4 but absent in 12.1 would
// silently render with no unit for users still on 12.1.
//
// TestUnitMapValuesAreValidGrafanaIDs uses this snapshot to assert every value in
// unitSymbolToGrafanaID is a real Grafana ID.
var validGrafanaUnitIDs = map[string]bool{
	// Misc
	"none": true, "string": true, "short": true, "sishort": true, "percent": true,
	"percentunit": true, "humidity": true, "dB": true, "candela": true, "hex0x": true,
	"hex": true, "sci": true, "locale": true, "pixel": true,

	// Acceleration
	"accMS2": true, "accFS2": true, "accG": true,

	// Angle
	"degree": true, "radian": true, "grad": true, "arcmin": true, "arcsec": true,

	// Area
	"areaM2": true, "areaF2": true, "areaMI2": true, "acres": true, "hectares": true,

	// Computation
	"flops": true, "mflops": true, "gflops": true, "tflops": true, "pflops": true,
	"eflops": true, "zflops": true, "yflops": true,

	// Concentration
	"ppm": true, "conppb": true, "conngm3": true, "conngNm3": true, "conμgm3": true,
	"conμgNm3": true, "conmgm3": true, "conmgNm3": true, "congm3": true, "congNm3": true,
	"conmgdL": true, "conmmolL": true,

	// Currency
	"currencyUSD": true, "currencyGBP": true, "currencyEUR": true, "currencyJPY": true,
	"currencyRUB": true, "currencyUAH": true, "currencyBRL": true, "currencyDKK": true,
	"currencyISK": true, "currencyNOK": true, "currencySEK": true, "currencyCZK": true,
	"currencyCHF": true, "currencyPLN": true, "currencyBTC": true, "currencymBTC": true,
	"currencyμBTC": true, "currencyZAR": true, "currencyINR": true, "currencyKRW": true,
	"currencyIDR": true, "currencyPHP": true, "currencyVND": true, "currencyTRY": true,
	"currencyMYR": true, "currencyXPF": true, "currencyBGN": true, "currencyPYG": true,
	"currencyUYU": true, "currencyILS": true,

	// Data
	"bytes": true, "decbytes": true, "bits": true, "decbits": true, "kbytes": true,
	"deckbytes": true, "mbytes": true, "decmbytes": true, "gbytes": true,
	"decgbytes": true, "tbytes": true, "dectbytes": true, "pbytes": true,
	"decpbytes": true,

	// Data rate
	"pps": true, "binBps": true, "Bps": true, "binbps": true, "bps": true, "KiBs": true,
	"Kibits": true, "KBs": true, "Kbits": true, "MiBs": true, "Mibits": true, "MBs": true,
	"Mbits": true, "GiBs": true, "Gibits": true, "GBs": true, "Gbits": true, "TiBs": true,
	"Tibits": true, "TBs": true, "Tbits": true, "PiBs": true, "Pibits": true, "PBs": true,
	"Pbits": true,

	// Date & time
	"dateTimeAsIso": true, "dateTimeAsIsoNoDateIfToday": true, "dateTimeAsUS": true,
	"dateTimeAsUSNoDateIfToday": true, "dateTimeAsLocal": true,
	"dateTimeAsLocalNoDateIfToday": true, "dateTimeAsSystem": true,
	"dateTimeFromNow": true,

	// Energy
	"watt": true, "kwatt": true, "megwatt": true, "gwatt": true, "mwatt": true,
	"Wm2": true, "voltamp": true, "kvoltamp": true, "voltampreact": true,
	"kvoltampreact": true, "watth": true, "watthperkg": true, "kwatth": true,
	"kwattm": true, "mwatth": true, "amph": true, "kamph": true, "mamph": true,
	"joule": true, "ev": true, "amp": true, "kamp": true, "mamp": true, "volt": true,
	"kvolt": true, "mvolt": true, "dBm": true, "mohm": true, "ohm": true, "kohm": true,
	"Mohm": true, "farad": true, "µfarad": true, "nfarad": true, "pfarad": true,
	"ffarad": true, "henry": true, "mhenry": true, "µhenry": true, "lumens": true,

	// Flow
	"flowgpm": true, "flowcms": true, "flowcfs": true, "flowcfm": true, "litreh": true,
	"flowlpm": true, "flowmlpm": true, "lux": true,

	// Force
	"forceNm": true, "forcekNm": true, "forceN": true, "forcekN": true,

	// Hash rate
	"Hs": true, "KHs": true, "MHs": true, "GHs": true, "THs": true, "PHs": true,
	"EHs": true,

	// Mass
	"massmg": true, "massg": true, "masslb": true, "masskg": true, "masst": true,

	// Length
	"lengthmm": true, "lengthin": true, "lengthft": true, "lengthm": true,
	"lengthkm": true, "lengthmi": true,

	// Pressure
	"pressurembar": true, "pressurebar": true, "pressurekbar": true, "pressurepa": true,
	"pressurehpa": true, "pressurekpa": true, "pressurehg": true, "pressurepsi": true,

	// Radiation
	"radbq": true, "radci": true, "radgy": true, "radrad": true, "radsv": true,
	"radmsv": true, "radusv": true, "radrem": true, "radexpckg": true, "radr": true,
	"radsvh": true, "radmsvh": true, "radusvh": true,

	// Rotational Speed
	"rotrpm": true, "rothz": true, "rotkhz": true, "rotmhz": true, "rotghz": true,
	"rotrads": true, "rotdegs": true,

	// Temperature
	"celsius": true, "fahrenheit": true, "kelvin": true,

	// Time
	"hertz": true, "ns": true, "µs": true, "ms": true, "s": true, "m": true, "h": true,
	"d": true, "dtdurationms": true, "dtdurations": true, "dthms": true, "dtdhms": true,
	"timeticks": true, "clockms": true, "clocks": true,

	// Throughput
	"cps": true, "ops": true, "reqps": true, "rps": true, "wps": true, "iops": true,
	"eps": true, "mps": true, "recps": true, "rowsps": true, "cpm": true, "opm": true,
	"reqpm": true, "rpm": true, "wpm": true, "epm": true, "mpm": true, "recpm": true,
	"rowspm": true,

	// Velocity
	"velocityms": true, "velocitykmh": true, "velocitymph": true, "velocityknot": true,

	// Volume
	"mlitre": true, "litre": true, "m3": true, "Nm3": true, "dm3": true, "gallons": true,

	// Boolean
	"bool": true, "bool_yes_no": true, "bool_on_off": true,
}

// TestUnitMapValuesAreValidGrafanaIDs asserts every value in unitSymbolToGrafanaID
// is a real Grafana ID. Catches plausible-but-fake IDs (e.g. "pressurempa") that
// would hit Grafana's registry-miss path instead of our suffix-mode fallthrough.
func TestUnitMapValuesAreValidGrafanaIDs(t *testing.T) {
	for symbol, id := range unitSymbolToGrafanaID {
		if !validGrafanaUnitIDs[id] {
			t.Errorf("unitSymbolToGrafanaID[%q] = %q is not in validGrafanaUnitIDs; "+
				"either add the ID to the snapshot or remove the mapping", symbol, id)
		}
	}
}

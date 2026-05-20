package plugin

import (
	"os"
	"regexp"
	"testing"
)

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

// grafanaCategoriesFixture is the upstream Grafana 12.1.0 unit registry source,
// pinned to the plugin's minimum supported version (plugin.json grafanaDependency
// >=12.1.0). The fixture is the raw categories.ts file — no hand-transcription —
// and loadValidGrafanaUnitIDs parses every `id: '…'` from it.
//
// v12.1.0 is the floor because a value valid in a later patch but absent in 12.1
// would silently render with no unit for users still on 12.1. The unit registry
// is invariant across v12.1.0–v12.4.x (verified by diffing categories.ts between
// tags), so pinning the floor is sufficient.
//
// To refresh against a newer minimum version: bump the v12.1.0 tag in the
// go:generate directive below (and the filename if you change it), then run
// `go generate ./pkg/plugin/...`.
//
//go:generate curl -sS -o testdata/grafana_categories_v12_1_0.ts https://raw.githubusercontent.com/grafana/grafana/v12.1.0/packages/grafana-data/src/valueFormats/categories.ts
const grafanaCategoriesFixture = "testdata/grafana_categories_v12_1_0.ts"

// grafanaIDPattern matches every `id: '<id>'` in categories.ts. The upstream file
// uses this single form on every format entry (272 matches in v12.1.0; verified
// no `id:` lines deviate from the pattern).
var grafanaIDPattern = regexp.MustCompile(`id: '([^']+)'`)

// loadValidGrafanaUnitIDs parses the pinned categories.ts fixture and returns the
// set of every Grafana unit ID it declares. TestUnitMapValuesAreValidGrafanaIDs
// uses this to assert every value in unitSymbolToGrafanaID is a real Grafana ID.
func loadValidGrafanaUnitIDs(t *testing.T) map[string]bool {
	t.Helper()
	src, err := os.ReadFile(grafanaCategoriesFixture)
	if err != nil {
		t.Fatalf("read %s: %v", grafanaCategoriesFixture, err)
	}
	matches := grafanaIDPattern.FindAllSubmatch(src, -1)
	if len(matches) == 0 {
		t.Fatalf("no Grafana unit IDs found in %s — fixture likely corrupt or format changed", grafanaCategoriesFixture)
	}
	ids := make(map[string]bool, len(matches))
	for _, m := range matches {
		ids[string(m[1])] = true
	}
	return ids
}

// TestUnitMapValuesAreValidGrafanaIDs asserts every value in unitSymbolToGrafanaID
// is a real Grafana ID. Catches plausible-but-fake IDs (e.g. "pressurempa") that
// would hit Grafana's registry-miss path instead of our suffix-mode fallthrough.
func TestUnitMapValuesAreValidGrafanaIDs(t *testing.T) {
	validIDs := loadValidGrafanaUnitIDs(t)
	for symbol, id := range unitSymbolToGrafanaID {
		if !validIDs[id] {
			t.Errorf("unitSymbolToGrafanaID[%q] = %q is not a valid Grafana unit ID in %s; "+
				"either remove the mapping or refresh the fixture against a Grafana version that defines it",
				symbol, id, grafanaCategoriesFixture)
		}
	}
}

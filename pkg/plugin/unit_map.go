// Package plugin — unit_map.go
//
// Maps Nominal-canonical channel unit symbols to Grafana unit IDs.
//
// Keys are Nominal canonical (typically UCUM: Cel not °C, [ft_i] not ft),
// but Nominal canonicalizes a few non-UCUM-pure shorthands (mph, psia).
// Source of truth: Nominal's `GET /units/v1/units` (GetAllUnits) — the canonical
// catalog grouped by UnitProperty. Symbols not in this table fall through to
// suffix mode via mapToGrafanaUnit.
//
// Values are enforced against a Grafana ID snapshot in unit_map_test.go.
package plugin

// unitSymbolToGrafanaID maps Nominal canonical UCUM symbols to Grafana unit IDs.
// See file header for keying convention and fallthrough behavior.
var unitSymbolToGrafanaID = map[string]string{
	// Temperature
	"Cel":    "celsius",    // UCUM uses Cel, not °C
	"[degF]": "fahrenheit", // bracketed; UCUM uses [degF], not °F
	"K":      "kelvin",

	// Pressure
	// Note: MPa and atm are Nominal canonical but have no Grafana unit ID — they
	// fall through to suffix mode (rendered verbatim, e.g. "23.4 MPa") rather
	// than producing an invalid Grafana ID that renders as the literal string.
	"Pa":   "pressurepa",
	"kPa":  "pressurekpa",
	"mbar": "pressurembar",
	"bar":  "pressurebar",
	"psia": "pressurepsi", // UCUM absolute psi; Grafana has no abs/gauge distinction
	"psig": "pressurepsi", // UCUM gauge psi; same target. Bare "psi" is NOT Nominal canonical

	// Length
	"mm":     "lengthmm",
	"cm":     "lengthcm",
	"m":      "lengthm", // UCUM m=meter (UCUM min=minute, mapped separately)
	"km":     "lengthkm",
	"[ft_i]": "lengthft", // UCUM international foot — NOT "ft"
	"[in_i]": "lengthin", // UCUM international inch — NOT "in"
	"[mi_i]": "lengthmi", // UCUM international mile

	// Mass
	"mg":      "massmg",
	"g":       "massg", // UCUM g=gram; gravity is [g] (bracketed). No ambiguity.
	"kg":      "masskg",
	"t":       "masst",  // metric tonne
	"[lb_av]": "masslb", // UCUM avoirdupois pound — NOT "lb"

	// Velocity
	"m/s":    "velocityms",
	"km/h":   "velocitykmh",
	"mph":    "velocitymph", // Nominal canonical; UCUM-pure [mi_i]/h is NOT in Nominal catalog
	"[kn_i]": "velocityknot",

	// Acceleration
	"m/s^2": "accMS2", // Nominal canonical uses caret form; "m/s2" is NOT canonical
	"[g]":   "accG",   // bracketed = standard gravity (distinct from gram "g")

	// Frequency
	"Hz": "hertz",

	// Electrical
	"V":   "volt",
	"A":   "amp",
	"mA":  "mamp",  // only canonical SI-prefixed ampere in Nominal
	"Ohm": "ohm",   // UCUM uses Ohm, not Ω. Only base form is canonical.
	"F":   "farad", // UCUM F=farad (capacitance); fahrenheit is [degF], no ambiguity

	// Energy / power
	"J":  "joule",
	"eV": "ev",
	"W":  "watt", // only base watt is canonical — no mW/kW/MW/GW

	// Force
	"N": "forceN",

	// Information — Nominal's canonical forms are SI/decimal (kilobyte,
	// megabyte, gigabyte), not IEC binary, so values map to Grafana's "dec*" family.
	"By":  "decbytes", // UCUM atomic byte (UCUM `B`=bel, hence `By`); decimal-family lineage
	"KB":  "deckbytes",
	"MB":  "decmbytes",
	"GB":  "decgbytes",
	"bit": "bits",

	// Rotation
	"rpm":   "rpm", // Nominal canonical
	"rad/s": "rotrads",
	"deg/s": "rotdegs", // Nominal canonical

	// Time
	"s":   "s",
	"ms":  "ms",
	"us":  "µs", // Nominal canonical is ASCII "us"; print-form µs / Greek μs are NOT canonical
	"ns":  "ns",
	"min": "m", // UCUM min=minute; Grafana ID m=minutes
	"h":   "h", // hour
	"d":   "d", // day

	// Misc
	"%":            "percent",
	"deg":          "degree", // UCUM uses deg, not °
	"rad":          "radian",
	"lx":           "lux",     // UCUM lux
	"[gal_us]/min": "flowgpm", // US gallons per minute
}

// mapToGrafanaUnit resolves a Nominal canonical unit symbol to a Grafana unit ID,
// returning "suffix:<symbol>" when unmapped so Grafana renders the symbol as an
// explicit literal suffix. Case-sensitive: UCUM is case-significant
// (Cel ≠ cel, m=meter vs M=mega-).
func mapToGrafanaUnit(symbol string) string {
	if symbol == "" {
		return ""
	}
	if id, ok := unitSymbolToGrafanaID[symbol]; ok {
		return id
	}
	return "suffix:" + symbol
}

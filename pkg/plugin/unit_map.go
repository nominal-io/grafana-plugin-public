// Package plugin — unit_map.go
//
// Maps channel unit symbols (ChannelMetadata.Unit.Symbol) to Grafana unit IDs.
//
// Keying:
//   - Keys are UCUM-based and case-significant (Cel ≠ cel, m=meter vs M=mega-).
//   - SI-prefixed forms (mg, kW, mV, kN, nF, …) are valid keys even when not
//     in the published browse catalog — the upstream parser accepts standard
//     SI-prefix composition on metric atoms, and real channels arrive with
//     these forms. Include them.
//   - A few non-UCUM shorthands (mph, psia, psig, rpm) are canonical at the
//     source and included as-is.
//
// Values are Grafana unit IDs pinned to the 12.1.0 floor (plugin.json's
// grafanaDependency) and enforced by a snapshot test in unit_map_test.go.
// Unmapped symbols fall through to "suffix:<symbol>" (rendered verbatim,
// e.g. "12.3 MPa") — always safe, just less rich.
//
// Editing this table:
//   - Verify candidate symbols are actually emitted by the source API before
//     adding rows; an entry for a symbol the API never produces is dead code.
//   - Micro-prefix trap: "us" is the ONLY ASCII-u canonical form. Everywhere
//     else the canonical form is spelled-out "micro<atom>" (microF, microH,
//     …); both "u<X>" and Unicode "µ<X>" are non-canonical and never reach
//     the map. Don't generalize the "us" row.
package plugin

// unitSymbolToGrafanaID maps source-canonical UCUM symbols to Grafana unit IDs.
// See file header for keying convention and fall-through behavior.
var unitSymbolToGrafanaID = map[string]string{
	// Temperature
	"Cel":    "celsius",    // UCUM uses Cel, not °C
	"[degF]": "fahrenheit", // bracketed; UCUM uses [degF], not °F
	"K":      "kelvin",

	// Pressure — MPa and atm are canonical but Grafana 12.x has no ID for them,
	// so they fall through to suffix mode. hPa is server-canonicalized to mbar
	// (1 hPa = 1 mbar) and never reaches the map.
	"Pa":   "pressurepa",
	"kPa":  "pressurekpa",
	"mbar": "pressurembar",
	"bar":  "pressurebar",
	"psia": "pressurepsi", // UCUM absolute psi; Grafana has no abs/gauge distinction
	"psig": "pressurepsi", // UCUM gauge psi; same target. Bare "psi" is NOT canonical.

	// Length — cm falls through on Grafana 12.x (lengthcm lands in 13.1+,
	// grafana#122489). Re-add "cm": "lengthcm" once the floor reaches 13.1.
	"mm":     "lengthmm",
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
	"mph":    "velocitymph", // canonical shorthand; UCUM-pure [mi_i]/h is not canonical
	"[kn_i]": "velocityknot",

	// Acceleration
	"m/s^2": "accMS2", // canonical caret form; "m/s2" is not canonical
	"[g]":   "accG",   // bracketed = standard gravity (distinct from gram "g")

	// Frequency
	"Hz": "hertz",

	// Electrical — prefixed Ohm forms (kOhm/MOhm/mOhm) are intentionally omitted
	// because UCUM treats "Ohm" as a multi-letter atom and SI-prefix handling on
	// multi-letter atoms isn't verified for this deployment.
	"V":      "volt",
	"mV":     "mvolt",
	"kV":     "kvolt",
	"A":      "amp",
	"mA":     "mamp",
	"kA":     "kamp",
	"Ohm":    "ohm",    // UCUM uses Ohm, not Ω. Base form only.
	"F":      "farad",  // UCUM F=farad (capacitance); fahrenheit is [degF], no ambiguity
	"microF": "µfarad", // spelled-out micro-prefix; see header
	"nF":     "nfarad",
	"pF":     "pfarad",
	"microH": "µhenry", // spelled-out micro-prefix; see header

	// Energy / power — J and eV have no prefixed Grafana IDs in 12.x, so only
	// the base forms are mapped.
	"J":  "joule",
	"eV": "ev",
	"W":  "watt",
	"mW": "mwatt",
	"kW": "kwatt",
	"MW": "megwatt",
	"GW": "gwatt",

	// Force
	"N":  "forceN",
	"kN": "forcekN",

	// Information — canonical forms are SI/decimal (kilobyte, megabyte, gigabyte),
	// not IEC binary, so values map to Grafana's "dec*" family.
	"By":  "decbytes", // UCUM atomic byte (UCUM `B`=bel, hence `By`)
	"KB":  "deckbytes",
	"MB":  "decmbytes",
	"GB":  "decgbytes",
	"bit": "bits",

	// Rotation — Grafana 'rpm' is reads/min (throughput); 'rotrpm' is the
	// rotational-speed ID we actually want.
	"rpm":   "rotrpm",
	"rad/s": "rotrads",
	"deg/s": "rotdegs",

	// Time — "us" is the only category where ASCII "u"-prefix is canonical;
	// elsewhere the canonical form is spelled-out "micro<atom>" (see header).
	// Do not generalize this row.
	"s":   "s",
	"ms":  "ms",
	"us":  "µs",
	"ns":  "ns",
	"min": "m", // UCUM min=minute; Grafana ID m=minutes
	"h":   "h",
	"d":   "d",

	// Misc
	"%":            "percent",
	"deg":          "degree", // UCUM uses deg, not °
	"rad":          "radian",
	"lx":           "lux",
	"[gal_us]/min": "flowgpm",
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

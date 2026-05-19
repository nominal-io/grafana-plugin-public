// Package plugin — unit_map.go
//
// Maps Nominal channel unit symbols to Grafana unit IDs.
//
// What counts as a key:
//
//	A channel's Unit.Symbol is whatever Nominal's UCUM-based parser accepted
//	and round-tripped at upload time. That set is strictly larger than what
//	the public browse endpoint GET /units/v1/units enumerates — in particular,
//	SI-prefixed forms (mg, kW, mV, kN, nF, …) are parseable via standard
//	UCUM SI-prefix composition on metric atoms (g, W, V, N, F, …) even when
//	they are not listed in the browse catalog. Keys here intentionally include
//	those SI-prefixed forms alongside the catalog symbols themselves.
//
//	UCUM is case-significant (Cel ≠ cel, m=meter vs M=mega-). Keys also include
//	the few non-UCUM-pure shorthands Nominal canonicalizes (mph, psia, psig, rpm).
//
// Values are enforced against a Grafana 12.1.0 ID snapshot in unit_map_test.go.
// Anything not in this table falls through to suffix mode via mapToGrafanaUnit
// (rendered verbatim as "12.3 <symbol>") — always safe, just less rich.
//
// Verification:
//
//	Browse catalog audited via cmd/unitprobe --list-units on 2026-05-18
//	(282 enumerated symbols across 59 properties). Entries here that are
//	absent from that audit (mg, kW, mV, kN, nF, microF, …) are SI-prefix
//	composites the parser accepts; round-trip behavior verified via
//	cmd/unitprobe --round-trip (POST /units/v1/units/get-batch-units) on
//	2026-05-18 — every key in this table either round-trips identically
//	or is the canonical form the server resolves common variants to.
//
//	Micro-prefix rule (surfaced by the round-trip probe): "us" is the ONLY
//	canonical ASCII-u form; for every other property the canonical form is
//	spelled-out "micro<atom>" (microF, microH, microPa, microm, microg, …),
//	with both "u<X>" and the Unicode "µ<X>" rejected or rewritten. The
//	"us" entry below has a guardrail comment; don't generalize from it.
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
	// Note: hPa is parser-accepted but the server canonicalizes it to mbar
	// (1 hPa = 1 mbar; verified via /units/v1/units/get-batch-units, 2026-05-18).
	// Real channels always arrive as "mbar", so no "hPa" row is needed.
	"Pa":   "pressurepa",
	"kPa":  "pressurekpa",
	"mbar": "pressurembar",
	"bar":  "pressurebar",
	"psia": "pressurepsi", // UCUM absolute psi; Grafana has no abs/gauge distinction
	"psig": "pressurepsi", // UCUM gauge psi; same target. Bare "psi" is NOT Nominal canonical

	// Length
	// Note: cm is Nominal canonical but has no Grafana unit ID in 12.x — Grafana
	// added `lengthcm` post-12.4 (PR #122489, milestone 13.1.x), so on supported
	// versions it falls through to suffix mode (rendered "12.3 cm"). Re-add the
	// "cm": "lengthcm" mapping once the minimum supported Grafana is 13.1+.
	"mm":     "lengthmm",
	"m":      "lengthm", // UCUM m=meter (UCUM min=minute, mapped separately)
	"km":     "lengthkm",
	"[ft_i]": "lengthft", // UCUM international foot — NOT "ft"
	"[in_i]": "lengthin", // UCUM international inch — NOT "in"
	"[mi_i]": "lengthmi", // UCUM international mile

	// Mass
	"mg":      "massmg", // SI-prefix composite; parser-accepted (see header)
	"g":       "massg",  // UCUM g=gram; gravity is [g] (bracketed). No ambiguity.
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
	// SI-prefixed entries here are parser-accepted composites (see header).
	// Prefixed Ohm forms (kOhm/MOhm/mOhm) are deliberately omitted — UCUM treats
	// "Ohm" as a multi-letter atom and JSR-385 SI-prefix handling on multi-letter
	// atoms isn't verified for this deployment; add once empirically confirmed.
	"V":   "volt",
	"mV":  "mvolt",
	"kV":  "kvolt",
	"A":   "amp",
	"mA":  "mamp",
	"kA":  "kamp",
	"Ohm": "ohm",    // UCUM uses Ohm, not Ω. Base form only.
	"F":      "farad",  // UCUM F=farad (capacitance); fahrenheit is [degF], no ambiguity
	"microF": "µfarad", // spelled-out micro-prefix. Nominal rejects "uF" and canonicalizes "µF" → "microF" (probed 2026-05-18). Time is the only category where ASCII "u" is canonical (us); for everything else micro<atom> is the canonical form.
	"nF":     "nfarad",
	"pF":     "pfarad",
	"microH": "µhenry", // inductance; same spelled-out micro rule as microF

	// Energy / power
	// W prefixes are parser-accepted composites (see header); J/eV have no
	// prefixed Grafana IDs in 12.1, so we map only the base forms.
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

	// Information — Nominal's canonical forms are SI/decimal (kilobyte,
	// megabyte, gigabyte), not IEC binary, so values map to Grafana's "dec*" family.
	"By":  "decbytes", // UCUM atomic byte (UCUM `B`=bel, hence `By`); decimal-family lineage
	"KB":  "deckbytes",
	"MB":  "decmbytes",
	"GB":  "decgbytes",
	"bit": "bits",

	// Rotation
	"rpm":   "rotrpm", // Nominal canonical; Grafana 'rpm' is reads/min (throughput), 'rotrpm' is revolutions/min
	"rad/s": "rotrads",
	"deg/s": "rotdegs", // Nominal canonical

	// Time
	"s":   "s",
	"ms":  "ms",
	"us":  "µs", // Time is the ONLY category where ASCII "u"-prefix is canonical. Nominal canonicalizes "µs"/"μs" → "us" but everywhere else (µF→microF, µA→microA, µH→microH, etc.) it spells out the micro- prefix. Don't generalize this row to other units.
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

package locodedb

// Continent is an enumeration of Earth's continent.
type Continent uint8

const (
	// ContinentUnknown is an undefined Continent value.
	ContinentUnknown = iota

	// ContinentEurope corresponds to Europe.
	ContinentEurope

	// ContinentAfrica corresponds to Africa.
	ContinentAfrica

	// ContinentNorthAmerica corresponds to North America.
	ContinentNorthAmerica

	// ContinentSouthAmerica corresponds to South America.
	ContinentSouthAmerica

	// ContinentAsia corresponds to Asia.
	ContinentAsia

	// ContinentAntarctica corresponds to Antarctica.
	ContinentAntarctica

	// ContinentOceania corresponds to Oceania.
	ContinentOceania
)

// Is checks if c is the same continent as c2.
func (c *Continent) Is(c2 Continent) bool {
	return *c == c2
}

func (c Continent) String() string {
	switch c {
	case ContinentUnknown:
		fallthrough
	default:
		return "Unknown"
	case ContinentEurope:
		return "Europe"
	case ContinentAfrica:
		return "Africa"
	case ContinentNorthAmerica:
		return "North America"
	case ContinentSouthAmerica:
		return "South America"
	case ContinentAsia:
		return "Asia"
	case ContinentAntarctica:
		return "Antarctica"
	case ContinentOceania:
		return "Oceania"
	}
}

// ContinentFromString returns Continent value
// corresponding to passed string representation.
func ContinentFromString(str string) Continent {
	switch str {
	default:
		return ContinentUnknown
	case "Europe":
		return ContinentEurope
	case "Africa":
		return ContinentAfrica
	case "North America":
		return ContinentNorthAmerica
	case "South America":
		return ContinentSouthAmerica
	case "Asia":
		return ContinentAsia
	case "Antarctica":
		return ContinentAntarctica
	case "Oceania":
		return ContinentOceania
	}
}

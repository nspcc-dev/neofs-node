package locode

type attrDescriptor struct {
	optional  bool
	converter func(Record) string
}

func countryCodeValue(r Record) (val string) {
	return r.CountryCode().String()
}

func countryValue(r Record) string {
	return r.CountryName()
}

func locationValue(r Record) string {
	return r.LocationName()
}

func subDivCodeValue(r Record) string {
	return r.SubDivCode()
}

func subDivValue(r Record) string {
	return r.SubDivName()
}

func continentValue(r Record) string {
	return r.Continent().String()
}

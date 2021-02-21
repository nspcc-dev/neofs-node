package innerring

type glagoliticLetter int8

const (
	_ glagoliticLetter = iota - 1

	az
	buky
	vedi
	glagoli
	dobro
	yest
	zhivete
	dzelo
	zemlja
	izhe
	iZhei
	gerv
	kako
	ljudi
	mislete
	nash
	on
	pokoj
	rtsi
	slovo
	tverdo
	uk
	fert
	kher
	oht
	shta
	tsi
	cherv
	sha
	yer
	yeri
	yerj
	yat
	jo
	yu
	smallYus
	smallIotatedYus
	bigYus
	bigIotatedYus
	fita
	izhitsa

	lastLetterNum
)

// returns string in config-compatible format
func (l glagoliticLetter) configString() string {
	switch l {
	default:
		return "unknown"
	case az:
		return "az"
	case buky:
		return "buky"
	case vedi:
		return "vedi"
	case glagoli:
		return "glagoli"
	case dobro:
		return "dobro"
	case yest:
		return "yest"
	case zhivete:
		return "zhivete"
	case dzelo:
		return "dzelo"
	case zemlja:
		return "zemlja"
	case izhe:
		return "izhe"
	case iZhei:
		return "izhei"
	case gerv:
		return "gerv"
	case kako:
		return "kako"
	case ljudi:
		return "ljudi"
	case mislete:
		return "mislete"
	case nash:
		return "nash"
	case on:
		return "on"
	case pokoj:
		return "pokoj"
	case rtsi:
		return "rtsi"
	case slovo:
		return "slovo"
	case tverdo:
		return "tverdo"
	case uk:
		return "uk"
	case fert:
		return "fert"
	case kher:
		return "kher"
	case oht:
		return "oht"
	case shta:
		return "shta"
	case tsi:
		return "tsi"
	case cherv:
		return "cherv"
	case sha:
		return "sha"
	case yer:
		return "yer"
	case yeri:
		return "yeri"
	case yerj:
		return "yerj"
	case yat:
		return "yat"
	case jo:
		return "jo"
	case yu:
		return "yu"
	case smallYus:
		return "small.yus"
	case smallIotatedYus:
		return "small.iotated.yus"
	case bigYus:
		return "big.yus"
	case bigIotatedYus:
		return "big.iotated.yus"
	case fita:
		return "fita"
	case izhitsa:
		return "izhitsa"
	}
}

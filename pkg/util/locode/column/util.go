package locodecolumn

func isDigit(sym uint8) bool {
	return sym >= '0' && sym <= '9'
}

func isUpperAlpha(sym uint8) bool {
	return sym >= 'A' && sym <= 'Z'
}

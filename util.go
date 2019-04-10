package cworm

import (
	"strings"
)

func pluralizeString(str string) string {
	if strings.HasSuffix(str, "y") {
		str = str[:len(str)-1] + "ie"
	}
	return str + "s"
}

func snakeCase(name string) string {
	str := make([]rune, 0)
	first := true

	for _, chr := range name {
		if isUpper := 'A' <= chr && chr <= 'Z'; isUpper {
			if first == true {
				first = false
			} else {
				str = append(str, '_')
			}
			chr -= ('A' - 'a')
		}
		str = append(str, chr)
	}

	return string(str)
}

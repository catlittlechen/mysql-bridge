// Author: chenkai@youmi.net

package main

import "strings"

func Split(query string) (value []string) {
	value = strings.Split(query, " ")
	index := 0
	for i := 0; i < len(value); i++ {
		if value[i] == "" {
			continue
		}
		value[index] = strings.Trim(value[i], "'\"`\t")
		index += 1
	}
	return value[:index]
}

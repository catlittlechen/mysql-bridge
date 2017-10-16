// Author: chenkai@youmi.net

package global

import (
	"io/ioutil"
	"strings"

	"gopkg.in/yaml.v2"
)

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

func ParseYamlFile(filepath string, data interface{}) error {
	if confile, err := ioutil.ReadFile(filepath); nil == err {
		if err = yaml.Unmarshal(confile, data); nil != err {
			return err
		}
	} else {
		return err
	}

	return nil
}

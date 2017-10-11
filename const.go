// Author: chenkai@youmi.net
package main

import "errors"

var (
	ErrorSQLSyntax    = errors.New("You have an error in your SQL syntax;")
	ErrorNotSupported = errors.New("not supported now")
)

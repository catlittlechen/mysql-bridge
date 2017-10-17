// Author: chenkai@youmi.net
package global

import "errors"

const (
	// MaxSeqID = 9000000000000000000
	MaxSeqID = 10000
	MinSeqID = 1
)

var (
	ErrorSQLSyntax    = errors.New("You have an error in your SQL syntax;")
	ErrorNotSupported = errors.New("not supported now")
)

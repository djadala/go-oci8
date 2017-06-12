// +build go1.9

package oci8

import "database/sql/driver"

//#include <oci.h>
import "C"

// Actually not tested with go 1.9
func (s *OCI8Stmt) CheckNamedValue(v interface{}) (driver.Value, error) {
	var kind C.ub2
	switch v.(type) {
	case *string:
		kind = C.SQLT_STR
	}
	s.pbind = append(s.pbind, oci8bind{out: v,kind:kind})
	return driver.DefaultParameterConverter.ConvertValue(v)
}

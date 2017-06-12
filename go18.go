// +build go1.8 !go1.9

package oci8

import "database/sql/driver"

//#include <oci.h>
import "C"

func (s *OCI8Stmt) ConvertValue(v interface{}) (driver.Value, error) {
	var kind C.ub2
	switch v.(type) {
	case *string:
		kind = C.SQLT_STR
	}
	s.pbind = append(s.pbind, oci8bind{out: v,kind:kind})
	return driver.DefaultParameterConverter.ConvertValue(v)
}

func (s *OCI8Stmt) ColumnConverter(i int) driver.ValueConverter {
	return s
}

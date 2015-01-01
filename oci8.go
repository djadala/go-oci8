package oci8

/*
#include <oci.h>
#include <stdlib.h>
#include <string.h>

#cgo pkg-config: oci8

//void getDate(  void *buf, sb2 *y, ub1 *m, ub1 *d, ub1 *hh, ub1 *mm, ub1 *ss) {
	
//	OCIDateGetDate( buf, 
//	   y, m, d
	 
//	 )
	 
//	OCIDateGetTime( buf,
//	   hh, mm, ss
	 
//	 )
//}
 
*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
	"math"
	"sync"
)

type DSN struct {
	Host     string
	Port     int
	Username string
	Password string
	SID      string
	Location *time.Location
}

func init() {
	sql.Register("oci8", &OCI8Driver{})
}

type OCI8Driver struct {
}

type OCI8Conn struct {
	svc      unsafe.Pointer
	env      unsafe.Pointer
	err      unsafe.Pointer
	attrs    Values
	location *time.Location
	inTransaction bool
}

type OCI8Tx struct {
	c *OCI8Conn
}

type Values map[string]interface{}

func (vs Values) Set(k string, v interface{}) {
	vs[k] = v
}

func (vs Values) Get(k string) (v interface{}) {
	v, _ = vs[k]
	return
}

//ParseDSN parses a DSN used to connect to Oracle
//It expects to receive a string in the form:
//user:password@host:port/sid?param1=value1&param2=value2
//
//Currently the only parameter supported is 'loc' which
//sets the timezone to read times in as and to marshal to when writing times to
//Oracle
func ParseDSN(dsnString string) (dsn *DSN, err error) {
	rs := []byte(dsnString)
break_loop:
	for i, r := range rs {
		if r == '/' {
			rs[i] = ':'
			dsnString = string(rs)
			break break_loop
		}
		if r == '@' {
			break break_loop
		}
	}

	if !strings.HasPrefix(dsnString, "oracle://") {
		dsnString = "oracle://" + dsnString
	}
	
	u, err := url.Parse(dsnString)
	if err != nil {
		return nil, err
	}
	dsn = &DSN{Location: time.Local}

	if u.User != nil {
		dsn.Username = u.User.Username()
		password, ok := u.User.Password()
		if ok {
			dsn.Password = password
		} else {
			if tok := strings.SplitN(dsn.Username, "/", 2); len(tok) >= 2 {
				dsn.Username = tok[0]
				dsn.Password = tok[1]
			}
		}
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		if err.Error() == "missing port in address" {
			return nil, fmt.Errorf("Invalid DSN: %v", err)
		}
		port = "0"
	}
	dsn.Host = host
	nport, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("Invalid DSN: %v", err)
	}
	dsn.Port = nport
	dsn.SID = strings.Trim(u.Path, "/")

	for k, v := range u.Query() {
		if k == "loc" && len(v) > 0 {
			if dsn.Location, err = time.LoadLocation(v[0]); err != nil {
				return nil, fmt.Errorf("Invalid DSN: %v", err)
			}
		}
	}
	return dsn, nil
}

func (tx *OCI8Tx) Commit() error {
	tx.c.inTransaction = false
	rv := C.OCITransCommit(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0)
	if rv == C.OCI_ERROR {
		return ociGetError(tx.c.err)
	}
	return nil
}

func (tx *OCI8Tx) Rollback() error {
	tx.c.inTransaction = false
	rv := C.OCITransRollback(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0)
	if rv == C.OCI_ERROR {
		return ociGetError(tx.c.err)
	}
	return nil
}

func (c *OCI8Conn) exec(cmd string) error {
	stmt, err := c.Prepare(cmd)
	if err == nil {
		defer stmt.Close()
		_, err = stmt.Exec(nil)
	}
	return err
}

func (c *OCI8Conn) Begin() (driver.Tx, error) {
	rv := C.OCITransStart(
		(*C.OCISvcCtx)(c.svc),
		(*C.OCIError)(c.err),
		60,
		C.OCI_TRANS_READWRITE)  //C.OCI_TRANS_NEW
	if rv == C.OCI_ERROR {
		return nil, ociGetError(c.err)
	}
	c.inTransaction = true
	return &OCI8Tx{c}, nil
}

var (
	once sync.Once
	rvInit C.sword
	)

func (d *OCI8Driver) Open(dsnString string) (connection driver.Conn, err error) {
	var (
		conn OCI8Conn
		dsn  *DSN
	)

	if dsn, err = ParseDSN(dsnString); err != nil {
		return nil, err
	}

	// set safe defaults
	conn.attrs = make(Values)
	conn.attrs.Set("prefetch_rows", 10)
	conn.attrs.Set("prefetch_memory", int64(0))

	for k, v := range parseEnviron(os.Environ()) {
		conn.attrs.Set(k, v)
	}

	once.Do(
		func() {
			rvInit = C.OCIInitialize(
				C.OCI_DEFAULT|C.OCI_THREADED /*|C.OCI_OBJECT*/,
				nil,
				nil,
				nil,
				nil)
		})
		
		//OCINlsCharSetNameToId()
		//OCIEnvNlsCreate()

	if rvInit == C.OCI_ERROR {
		return nil, ociGetError(conn.err)
	}

	rv := C.OCIEnvInit(
		(**C.OCIEnv)(unsafe.Pointer(&conn.env)),
		C.OCI_DEFAULT,
		0,
		nil)

	rv = C.OCIHandleAlloc(
		conn.env,
		&conn.err,
		C.OCI_HTYPE_ERROR,
		0,
		nil)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(conn.err)
	}

	var phost *C.char
	if dsn.Host != "" && dsn.SID != "" {
		phost = C.CString(fmt.Sprintf("%s:%d/%s", dsn.Host, dsn.Port, dsn.SID))
	} else {
		phost = C.CString(dsn.SID)
	}
	defer C.free(unsafe.Pointer(phost))
	phostlen := C.strlen(phost)
	puser := C.CString(dsn.Username)
	defer C.free(unsafe.Pointer(puser))
	ppass := C.CString(dsn.Password)
	defer C.free(unsafe.Pointer(ppass))

	rv = C.OCILogon(
		(*C.OCIEnv)(conn.env),
		(*C.OCIError)(conn.err),
		(**C.OCISvcCtx)(unsafe.Pointer(&conn.svc)),
		(*C.OraText)(unsafe.Pointer(puser)),
		C.ub4(C.strlen(puser)),
		(*C.OraText)(unsafe.Pointer(ppass)),
		C.ub4(C.strlen(ppass)),
		(*C.OraText)(unsafe.Pointer(phost)),
		C.ub4(phostlen))
	if rv == C.OCI_ERROR {
		return nil, ociGetError(conn.err)
	}

	conn.location = dsn.Location

	return &conn, nil
}

func (c *OCI8Conn) Close() error {
	rv := C.OCILogoff(
		(*C.OCISvcCtx)(c.svc),
		(*C.OCIError)(c.err))
	if rv == C.OCI_ERROR {
		return ociGetError(c.err)
	}

	C.OCIHandleFree(
		c.env,
		C.OCI_HTYPE_ENV)

	c.svc = nil
	c.env = nil
	c.err = nil
	return nil
}

type OCI8Stmt struct {
	c      *OCI8Conn
	s      unsafe.Pointer
	closed bool
}

func (c *OCI8Conn) Prepare(query string) (driver.Stmt, error) {
	pquery := C.CString(query)
	defer C.free(unsafe.Pointer(pquery))
	var s unsafe.Pointer

	rv := C.OCIHandleAlloc(
		c.env,
		&s,
		C.OCI_HTYPE_STMT,
		0,
		nil)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(c.err)
	}

	rv = C.OCIStmtPrepare(
		(*C.OCIStmt)(s),
		(*C.OCIError)(c.err),
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT))
	if rv == C.OCI_ERROR {
		return nil, ociGetError(c.err)
	}

	return &OCI8Stmt{c: c, s: s}, nil
}

func (s *OCI8Stmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	C.OCIHandleFree(
		s.s,
		C.OCI_HTYPE_STMT)
	s.s = nil

	return nil
}

func (s *OCI8Stmt) NumInput() int {
	var num C.int
	C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&num),
		nil,
		C.OCI_ATTR_BIND_COUNT,
		(*C.OCIError)(s.c.err))
	return int(num)
}

func (s *OCI8Stmt) bind(args []driver.Value) (freeBoundParameters func(), err error) {
	if args == nil {
		return func() {}, nil
	}

	var (
		bp              *C.OCIBind
		dty             C.ub2
		data            []byte
		cdata           *C.char
		boundParameters []oci8bind
	)

	freeBoundParameters = func() {
		for _, col := range boundParameters {
			if col.pbuf != nil {
				switch  col.kind {
				case C.SQLT_CLOB, C.SQLT_BLOB:
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_LOB)
				case C.SQLT_TIMESTAMP_TZ:
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_TIMESTAMP_TZ)
				case C.SQLT_TIMESTAMP:
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_TIMESTAMP)
				case C.SQLT_TIMESTAMP_LTZ:
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_TIMESTAMP_LTZ)
				default:
					C.free(col.pbuf)
				}
			}
		}
	}

	for i, v := range args {
		data = []byte{}

		switch  v.(type) {
		case nil:
			dty = C.SQLT_STR
			boundParameters = append(boundParameters, oci8bind{dty, nil})
			rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				nil,
				0,
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
		case []byte:
		    if v := v.([]byte); len(v) < 2000 {
				dty = C.SQLT_BIN
				data = v//.([]byte)
				xdata := unsafe.Pointer(&data[0])
				//boundParameters = append(boundParameters, oci8bind{dty, xdata})
				rv := C.OCIBindByPos(
					(*C.OCIStmt)(s.s),
					&bp,
					(*C.OCIError)(s.c.err),
					C.ub4(i+1),
					xdata,
					C.sb4(len(data)),
					dty,
					nil,
					nil,
					nil,
					0,
					nil,
					C.OCI_DEFAULT)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}
			} else {
				// FIXME: Currently, CLOB not supported
				dty = C.SQLT_BLOB
				data = v//.([]byte)
				var bamt C.ub4
				var pbuf unsafe.Pointer
				rv := C.OCIDescriptorAlloc(
					s.c.env,
					&pbuf,
					C.OCI_DTYPE_LOB,
					0,
					nil)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}

				rv = C.OCILobCreateTemporary(
					(*C.OCISvcCtx)(s.c.svc),
					(*C.OCIError)(s.c.err),
					(*C.OCILobLocator)(pbuf),
					0,
					C.SQLCS_IMPLICIT,
					C.OCI_TEMP_BLOB,
					C.OCI_ATTR_NOCACHE,
					C.OCI_DURATION_SESSION)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}

				bamt = C.ub4(len(data))
				rv = C.OCILobWrite(
					(*C.OCISvcCtx)(s.c.svc),
					(*C.OCIError)(s.c.err),
					(*C.OCILobLocator)(pbuf),
					&bamt,
					1,
					unsafe.Pointer(&data[0]),
					C.ub4(len(data)),
					C.OCI_ONE_PIECE,
					nil,
					nil,
					0,
					C.SQLCS_IMPLICIT)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}
				boundParameters = append(boundParameters, oci8bind{dty, pbuf})
				rv = C.OCIBindByPos(
					(*C.OCIStmt)(s.s),
					&bp,
					(*C.OCIError)(s.c.err),
					C.ub4(i+1),
					unsafe.Pointer(&pbuf),
					0,
					dty,
					nil,
					nil,
					nil,
					0,
					nil,
					C.OCI_DEFAULT)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}
			}
 
//		case int64:
		case float64:
			fb := math.Float64bits( v.(float64))
			if fb & 0x8000000000000000 != 0 {
				fb ^= 0xffffffffffffffff
			} else {
				fb |= 0x8000000000000000
			}
             dty = C.SQLT_IBDOUBLE
             data = []byte{ byte(fb>>56), byte(fb>>48), byte(fb>>40), byte(fb>>32), byte(fb>>24), byte(fb>>16), byte(fb>>8), byte(fb) }

			cdata = C.CString(string(data))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(cdata),
				C.sb4(len(data)),
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}

//		case bool:
//		case string:
        /*
		case time.Time:
			dty = C.SQLT_DAT
			now := v.(time.Time).In(s.c.location)
			//TODO Handle BCE dates (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#438305)
			//TODO Handle timezones (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#443601)
			data = []byte{
				byte(now.Year()/100 + 100),
				byte(now.Year()%100 + 100),
				byte(now.Month()),
				byte(now.Day()),
				byte(now.Hour() + 1),
				byte(now.Minute() + 1),
				byte(now.Second() + 1),
			}

			cdata = C.CString(string(data))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(cdata),
				C.sb4(len(data)),
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
			*/
			 
		case time.Time:
		var pt unsafe.Pointer
			dty = C.SQLT_TIMESTAMP_TZ
            	rv := C.OCIDescriptorAlloc(
				s.c.env,
				&pt,
				C.OCI_DTYPE_TIMESTAMP_TZ,
				0,
				nil)
			if rv == C.OCI_ERROR {
				return nil, ociGetError(s.c.err)
			}
            
            now := v.(time.Time)
            zone, _ := now.Zone()
            zp := C.CString(zone)
            //zp := C.CString("+00:00")
            rv = C.OCIDateTimeConstruct(
				s.c.env,
				(*C.OCIError)(s.c.err),
				(*C.OCIDateTime)(pt),
				C.sb2(now.Year()),
				C.ub1(now.Month()),
				C.ub1(now.Day()),
				C.ub1(now.Hour()),
				C.ub1(now.Minute()),
				C.ub1(now.Second()),
				C.ub4(now.Nanosecond()),
				(*C.OraText)(unsafe.Pointer(zp)),
				C.strlen( zp),//C.size_t(len(zone)),
            )
            //C.free( unsafe.Pointer(zp))
			if rv == C.OCI_ERROR {
				return nil, ociGetError(s.c.err)
			}
            
			boundParameters = append(boundParameters, oci8bind{dty, pt})
			rv = C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer( &pt),
				C.sb4( unsafe.Sizeof( pt)),
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
			
            var ( 
				lll C.ub4
				bbuf  [500]C.OraText
				)
			lll = 500	
			rv = C.OCIDateTimeToText(
				s.c.env,
				(*C.OCIError)(s.c.err),
				(*C.OCIDateTime)(pt),
				nil,
				0,
				9,
				nil,
				0,
				&lll,
				&bbuf[0],
			)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
			fmt.Println( int(rv),  C.GoString( (*C.char)(unsafe.Pointer(&bbuf[0]))))
		case string:
		if v := v.(string); len(v) >= 4000 {
				dty = C.SQLT_BLOB
				data = []byte(v)
				var bamt C.ub4
				var pbuf unsafe.Pointer
				rv := C.OCIDescriptorAlloc(
					s.c.env,
					&pbuf,
					C.OCI_DTYPE_LOB,
					0,
					nil)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}

				rv = C.OCILobCreateTemporary(
					(*C.OCISvcCtx)(s.c.svc),
					(*C.OCIError)(s.c.err),
					(*C.OCILobLocator)(pbuf),
					0,
					C.SQLCS_IMPLICIT,
					C.OCI_TEMP_BLOB,
					C.OCI_ATTR_NOCACHE,
					C.OCI_DURATION_SESSION)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}

				bamt = C.ub4(len(data))
				rv = C.OCILobWrite(
					(*C.OCISvcCtx)(s.c.svc),
					(*C.OCIError)(s.c.err),
					(*C.OCILobLocator)(pbuf),
					&bamt,
					1,
					unsafe.Pointer(&data[0]),
					C.ub4(len(data)),
					C.OCI_ONE_PIECE,
					nil,
					nil,
					0,
					C.SQLCS_IMPLICIT)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}
				boundParameters = append(boundParameters, oci8bind{dty, pbuf})
				rv = C.OCIBindByPos(
					(*C.OCIStmt)(s.s),
					&bp,
					(*C.OCIError)(s.c.err),
					C.ub4(i+1),
					unsafe.Pointer(&pbuf),
					0,
					dty,
					nil,
					nil,
					nil,
					0,
					nil,
					C.OCI_DEFAULT)
				if rv == C.OCI_ERROR {
					defer freeBoundParameters()
					return nil, ociGetError(s.c.err)
				}
		} else {
			dty = C.SQLT_STR
			data = []byte( v)
			data = append(data, 0)

			cdata = C.CString(string(data))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(cdata),
				C.sb4(len(data)),
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
		}
	    //fallthrough
		default:
			dty = C.SQLT_STR
			data = []byte(fmt.Sprintf("%v", v))
			data = append(data, 0)

			cdata = C.CString(string(data))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			rv := C.OCIBindByPos(
				(*C.OCIStmt)(s.s),
				&bp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(cdata),
				C.sb4(len(data)),
				dty,
				nil,
				nil,
				nil,
				0,
				nil,
				C.OCI_DEFAULT)
			if rv == C.OCI_ERROR {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
		}
	}
	return freeBoundParameters, nil
}

func (s *OCI8Stmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	var (
		freeBoundParameters func()
	)

	if freeBoundParameters, err = s.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters()

	var t C.int
	C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_STMT_TYPE,
		(*C.OCIError)(s.c.err))
	iter := C.ub4(1)
	if t == C.OCI_STMT_SELECT {
		iter = 0
	}

	// set the row prefetch.  Only one extra row per fetch will be returned unless this is set.
	prefetch_size := C.ub4(s.c.attrs.Get("prefetch_rows").(int))
	C.OCIAttrSet(s.s, C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetch_size), 0, C.OCI_ATTR_PREFETCH_ROWS, (*C.OCIError)(s.c.err))

	// if non-zero, oci will fetch rows until the memory limit or row prefetch limit is hit.
	// useful for memory constrained systems
	prefetch_memory := C.ub4(s.c.attrs.Get("prefetch_memory").(int64))
	C.OCIAttrSet(s.s, C.OCI_HTYPE_STMT, unsafe.Pointer(&prefetch_memory), 0, C.OCI_ATTR_PREFETCH_MEMORY, (*C.OCIError)(s.c.err))
    
    mode := C.ub4(C.OCI_DEFAULT)
    if s.c.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}
	rv := C.OCIStmtExecute(
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		iter,
		0,
		nil,
		nil,
		mode) //C.OCI_DEFAULT)// OCI_COMMIT_ON_SUCCESS
	if rv == C.OCI_ERROR {
		return nil, ociGetError(s.c.err)
	}

	var rc C.ub2
	C.OCIAttrGet(
		s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&rc),
		nil,
		C.OCI_ATTR_PARAM_COUNT,
		(*C.OCIError)(s.c.err))

	oci8cols := make([]oci8col, int(rc))
	for i := 0; i < int(rc); i++ {
		var p unsafe.Pointer
		var np *C.char
		var ns C.ub4
		var tp C.ub2
		var lp C.ub2
		C.OCIParamGet(
			s.s,
			C.OCI_HTYPE_STMT,
			(*C.OCIError)(s.c.err),
			(*unsafe.Pointer)(unsafe.Pointer(&p)),
			C.ub4(i+1))
		C.OCIAttrGet(
			p,
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&tp),
			nil,
			C.OCI_ATTR_DATA_TYPE,
			(*C.OCIError)(s.c.err))
		C.OCIAttrGet(
			p,
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&np),
			&ns,
			C.OCI_ATTR_NAME,
			(*C.OCIError)(s.c.err))
		C.OCIAttrGet(
			p,
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&lp),
			nil,
			C.OCI_ATTR_DATA_SIZE,
			(*C.OCIError)(s.c.err))

		switch tp {
		case C.SQLT_NUM:
			oci8cols[i].kind = C.SQLT_CHR
		case C.SQLT_CHR, C.SQLT_AFC:  // SQLT_VCS SQLT_AFC SQLT_CLOB SQLT_AVC
			lp *=4  //utf8 enc
			oci8cols[i].kind = tp
			
        case C.SQLT_DAT:
			 oci8cols[i].kind = C.SQLT_TIMESTAMP
			 tp = C.SQLT_TIMESTAMP

		default:
		    fmt.Println(  "KIND=", int(tp), "size=", int(lp))
			oci8cols[i].kind = tp
		}
		oci8cols[i].name = string((*[1 << 30]byte)(unsafe.Pointer(np))[0:int(ns)])
		oci8cols[i].size = int(lp)

		var defp *C.OCIDefine
		switch tp {
		case C.SQLT_CLOB, C.SQLT_BLOB:
			rv = C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_LOB,
				0,
				nil)
			if rv == C.OCI_ERROR {
				return nil, ociGetError(s.c.err)
			}
			rv = C.OCIDefineByPos(
				(*C.OCIStmt)(s.s),
				&defp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(&oci8cols[i].pbuf),
				-1,
				oci8cols[i].kind,
				unsafe.Pointer(&oci8cols[i].ind),
				&oci8cols[i].rlen,
				nil,
				C.OCI_DEFAULT)
		case C.SQLT_TIMESTAMP:
			rv = C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_TIMESTAMP,
				0,
				nil)
			if rv == C.OCI_ERROR {
				return nil, ociGetError(s.c.err)
			}
            lp = C.ub2(unsafe.Sizeof( unsafe.Pointer(nil)))
			rv = C.OCIDefineByPos(
				(*C.OCIStmt)(s.s),
				&defp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(&oci8cols[i].pbuf),
				C.sb4(lp),
				C.SQLT_TIMESTAMP,//oci8cols[i].kind,
				unsafe.Pointer(&oci8cols[i].ind),
				&oci8cols[i].rlen,
				nil,
				C.OCI_DEFAULT)
		case C.SQLT_TIMESTAMP_TZ:
			rv = C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_TIMESTAMP_TZ,
				0,
				nil)
			if rv == C.OCI_ERROR {
				return nil, ociGetError(s.c.err)
			}
            lp = C.ub2(unsafe.Sizeof( unsafe.Pointer(nil)))
			rv = C.OCIDefineByPos(
				(*C.OCIStmt)(s.s),
				&defp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(&oci8cols[i].pbuf),
				C.sb4(lp),
				C.SQLT_TIMESTAMP_TZ,//oci8cols[i].kind,
				unsafe.Pointer(&oci8cols[i].ind),
				&oci8cols[i].rlen,
				nil,
				C.OCI_DEFAULT)

		case C.SQLT_INTERVAL_DS:
			rv = C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_INTERVAL_DS,
				0,
				nil)
			if rv == C.OCI_ERROR {
				return nil, ociGetError(s.c.err)
			}
            lp = C.ub2(unsafe.Sizeof( unsafe.Pointer(nil)))
			rv = C.OCIDefineByPos(
				(*C.OCIStmt)(s.s),
				&defp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				unsafe.Pointer(&oci8cols[i].pbuf),
				C.sb4(lp),
				C.SQLT_INTERVAL_DS,//oci8cols[i].kind,
				unsafe.Pointer(&oci8cols[i].ind),
				&oci8cols[i].rlen,
				nil,
				C.OCI_DEFAULT)

		default:
			oci8cols[i].pbuf = C.malloc(C.size_t(lp) + 1)
			rv = C.OCIDefineByPos(
				(*C.OCIStmt)(s.s),
				&defp,
				(*C.OCIError)(s.c.err),
				C.ub4(i+1),
				oci8cols[i].pbuf,
				C.sb4(lp+1),
				oci8cols[i].kind,
				unsafe.Pointer(&oci8cols[i].ind),
				&oci8cols[i].rlen,
				nil,
				C.OCI_DEFAULT)
		}

		if rv == C.OCI_ERROR {
			return nil, ociGetError(s.c.err)
		}
	}
	return &OCI8Rows{s, oci8cols, false}, nil
}

type OCI8Result struct {
	s *OCI8Stmt
}

func (r *OCI8Result) LastInsertId() (int64, error) {
	var t C.ub4
	rv := C.OCIAttrGet(
		r.s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_ROWID,
		(*C.OCIError)(r.s.c.err))
	if rv == C.OCI_ERROR {
		return 0, ociGetError(r.s.c.err)
	}
	return int64(t), nil
}

func (r *OCI8Result) RowsAffected() (int64, error) {
	var t C.ub4
	rv := C.OCIAttrGet(
		r.s.s,
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&t),
		nil,
		C.OCI_ATTR_ROW_COUNT,
		(*C.OCIError)(r.s.c.err))
	if rv == C.OCI_ERROR {
		return 0, ociGetError(r.s.c.err)
	}
	return int64(t), nil
}

func (s *OCI8Stmt) Exec(args []driver.Value) (r driver.Result, err error) {
	var (
		freeBoundParameters func()
	)

	if freeBoundParameters, err = s.bind(args); err != nil {
		return nil, err
	}

	defer freeBoundParameters()
	
	//return nil, errors.New("***********************************")

    mode := C.ub4(C.OCI_DEFAULT)
    if s.c.inTransaction == false {
		mode = mode | C.OCI_COMMIT_ON_SUCCESS
	}

	rv := C.OCIStmtExecute(
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		1,
		0,
		nil,
		nil,
		mode) //C.OCI_DEFAULT)      //OCI_COMMIT_ON_SUCCESS
	if rv == C.OCI_ERROR {
		return nil, ociGetError(s.c.err)
	}
	if rv != C.OCI_SUCCESS {
		fmt.Println( "OCI_SUCCESS!=", rv)
        fmt.Println( ociGetError(s.c.err) )
	}
	return &OCI8Result{s}, nil
}

type oci8col struct {
	name string
	kind C.ub2
	size int
	ind  C.sb2
	rlen C.ub2
	pbuf unsafe.Pointer
}

type oci8bind struct {
	kind C.ub2
	pbuf unsafe.Pointer
}

type OCI8Rows struct {
	s    *OCI8Stmt
	cols []oci8col
	e    bool
}

func (rc *OCI8Rows) Close() error {
	for _, col := range rc.cols {
		switch col.kind {
		case C.SQLT_CLOB, C.SQLT_BLOB:
			C.OCIDescriptorFree(
				col.pbuf,
				C.OCI_DTYPE_LOB)
		case C.SQLT_TIMESTAMP:
			C.OCIDescriptorFree(
				col.pbuf,
				C.OCI_DTYPE_TIMESTAMP)
		case C.SQLT_TIMESTAMP_TZ:
			C.OCIDescriptorFree(
				col.pbuf,
				C.OCI_DTYPE_TIMESTAMP_TZ)
		case C.SQLT_INTERVAL_DS:	
			C.OCIDescriptorFree(
				col.pbuf,
				C.OCI_DTYPE_INTERVAL_DS)
		default:
			C.free(col.pbuf)
		}
	}
	return rc.s.Close()
}

func (rc *OCI8Rows) Columns() []string {
	cols := make([]string, len(rc.cols))
	for i, col := range rc.cols {
		cols[i] = col.name
	}
	return cols
}

func (rc *OCI8Rows) Next(dest []driver.Value) error {
	rv := C.OCIStmtFetch(
		(*C.OCIStmt)(rc.s.s),
		(*C.OCIError)(rc.s.c.err),
		1,
		C.OCI_FETCH_NEXT,
		C.OCI_DEFAULT)

	if rv == C.OCI_ERROR {
		err := ociGetError(rc.s.c.err)
		if err.Error()[:9] != "ORA-01405" {
			return err
		}
	}

	if rv == C.OCI_NO_DATA {
		return io.EOF
	}
	if rv != C.OCI_SUCCESS {
		fmt.Println( "OCI_SUCCESS!=", rv)
		fmt.Println( ociGetError(rc.s.c.err) )
	}



	for i := range dest {
		if rc.cols[i].ind == -1 { //Null
			dest[i] = nil
			continue
		}

		switch rc.cols[i].kind {
		case C.SQLT_DAT:
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:rc.cols[i].rlen]
		/*	
        var (
        y C.sb2
        m, d C.ub1
        hh, mm, ss C.ub1
        )
			C.getDate( &buf[0], 
               &y, &m, &d,
               &hh, &mm, &ss,
			 
			 )
				dest[i] = time.Date(
				int(y),
				time.Month(m),
				int(d),
				int(hh)-1,
				int(mm)-1,
				int(ss)-1,
				0,
				rc.s.c.location)		 
			*/ 
			
			//TODO Handle BCE dates (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#438305)
			//TODO Handle timezones (http://docs.oracle.com/cd/B12037_01/appdev.101/b10779/oci03typ.htm#443601)
			dest[i] = time.Date(
				(int(buf[0])-100)*100+(int(buf[1])-100),
				time.Month(int(buf[2])),
				int(buf[3]),
				int(buf[4])-1,
				int(buf[5])-1,
				int(buf[6])-1,
				0,
				rc.s.c.location)
				 
		case C.SQLT_BLOB, C.SQLT_CLOB:
			const bufSize = 4000
		    total :=  0
			var bamt C.ub4
			b := make([]byte, bufSize)
			again:
			rv = C.OCILobRead(
				(*C.OCISvcCtx)(rc.s.c.svc),
				(*C.OCIError)(rc.s.c.err),
				(*C.OCILobLocator)(rc.cols[i].pbuf),
				&bamt,
				1,
				unsafe.Pointer(&b[ total ]),
				C.ub4( bufSize),
				nil,
				nil,
				0,
				C.SQLCS_IMPLICIT)
            if rv == C.OCI_NEED_DATA { 
				b = append( b, b...)
                total += bufSize
				goto again
			}
			if rv == C.OCI_ERROR {
				return ociGetError(rc.s.c.err)
			}
			dest[i] = b[:total+int(bamt)]
		case C.SQLT_CHR, C.SQLT_AFC, C.SQLT_AVC:
			fmt.Println("SQLT_CHR")
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:rc.cols[i].rlen]
			switch {
			case rc.cols[i].ind == 0: //Normal
				dest[i] = string(buf)
			case rc.cols[i].ind == -2 || //Field longer than type (truncated)
				rc.cols[i].ind > 0: //Field longer than type (truncated). Value is original length.
				dest[i] = string(buf)
			default:
				return errors.New(fmt.Sprintf("Unknown column indicator: %d", rc.cols[i].ind))
			}

		case C.SQLT_LVB:  // LONG VARRAW 
			fmt.Println("LONG VARRAW column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
			dest[i] = nil
		case C.SQLT_BIN:  // RAW
		    buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:rc.cols[i].rlen]
			fmt.Println("RAW column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
			dest[i] = buf
		case C.SQLT_LNG:  // LONG
		    buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:rc.cols[i].rlen]
		    fmt.Println("LONG  column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
			dest[i] = buf
        

        case C.SQLT_IBFLOAT:
			fmt.Print("SQLT_IBFLOAT")
			fallthrough
        case C.SQLT_IBDOUBLE:
			fmt.Println("SQLT_IBDOUBLE")
        colsize := rc.cols[i].size
        buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:colsize]
        if  colsize == 4 {
			v := uint32( buf[3])
			v|= uint32( buf[2])<<8
			v|= uint32( buf[1])<<16
			v|= uint32( buf[0])<<24
			
			//SOME ORACLE SHIT ?
            if buf[0] & 0x80 == 0 {
				v ^= 0xffffffff
			} else {
				v &= 0x7fffffff
			}
			
			
			//fmt.Printf("%x %x %x %x\n", buf[0], buf[1], buf[2], buf[3])
			  
			dest[i] = math.Float32frombits( v)
		} else if colsize == 8 {
			v := uint64( buf[7])
			v|= uint64( buf[6])<<8
			v|= uint64( buf[5])<<16
			v|= uint64( buf[4])<<24
			v|= uint64( buf[3])<<32
			v|= uint64( buf[2])<<40
			v|= uint64( buf[1])<<48
			v|= uint64( buf[0])<<56

			//SOME ORACLE SHIT ?
            if buf[0] & 0x80 == 0 {
				v ^= 0xffffffffffffffff
			} else {
				v &= 0x7fffffffffffffff
			}

			dest[i] = math.Float64frombits(v)
		} else {
			return errors.New(fmt.Sprintf("Unhandled binary float size: %d", colsize))
		}
        case C.SQLT_TIMESTAMP:
        fmt.Println("SQLT_TIMESTAMP")
        fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
        /*
        var uu C.ub4
        fmt.Printf( "%p, %v\n", rc.cols[i].pbuf, C.OCIDateTimeCheck( 
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				(*C.OCIDateTime)(rc.cols[i].pbuf),
				&uu,
        ))
        */      
        var (
        y C.sb2
        m, d, hh, mm, ss C.ub1
        ff C.ub4
        )   
        rv = C.OCIDateTimeGetDate(
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				(*C.OCIDateTime)(rc.cols[i].pbuf),
				&y, 
				&m, 
				&d,
        )
			if rv == C.OCI_ERROR {
				return ociGetError(rc.s.c.err)
			}
        rv = C.OCIDateTimeGetTime(
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				(*C.OCIDateTime)(rc.cols[i].pbuf),
				&hh, 
				&mm, 
				&ss,
				&ff,
        )
			if rv == C.OCI_ERROR {
				return ociGetError(rc.s.c.err)
			}



        
			dest[i] = time.Date(
				int(y),
				time.Month(m),
				int(d),
				int( hh),//-1,
				int(mm),//-1
				int(ss),//int(buf[6])-1,
				int(ff),
				rc.s.c.location)




        case C.SQLT_TIMESTAMP_TZ:
        fmt.Println("SQLT_TIMESTAMP_TZ")
        fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
        var (
        y C.sb2
        m, d, hh, mm, ss C.ub1
        ff C.ub4
        zone [512]C.ub1
        zlen C.ub4
        )
        zlen = 512   
        rv = C.OCIDateTimeGetDate(
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				(*C.OCIDateTime)(rc.cols[i].pbuf),
				&y, 
				&m, 
				&d,
        )
			if rv == C.OCI_ERROR {
				return ociGetError(rc.s.c.err)
			}
        rv = C.OCIDateTimeGetTime(
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				(*C.OCIDateTime)(rc.cols[i].pbuf),
				&hh, 
				&mm, 
				&ss,
				&ff,
        )
			if rv == C.OCI_ERROR {
				return ociGetError(rc.s.c.err)
			}


         rv = C.OCIDateTimeGetTimeZoneName(
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				(*C.OCIDateTime)(rc.cols[i].pbuf),
				&zone[0],
				&zlen,
				)
			if rv == C.OCI_ERROR {
				return ociGetError(rc.s.c.err)
			}
            //zone[zlen]=0
            nnn :=  C.GoStringN( (*C.char)((unsafe.Pointer)(&zone[0])), C.int(zlen))
            fmt.Println( nnn)

            loc,err := time.LoadLocation( nnn)
            if err != nil {

                 var (
					h, m C.sb1
                 )
                 
				 rv = C.OCIDateTimeGetTimeZoneOffset(
						rc.s.c.env,
						(*C.OCIError)(rc.s.c.err),
						(*C.OCIDateTime)(rc.cols[i].pbuf),
						&h,
						&m,
						)
					if rv == C.OCI_ERROR {
						return ociGetError(rc.s.c.err)
					}
					//TODO reuse locations
                   fmt.Println( nnn, int( h)*60*60 + int(m)*60)
                  loc = time.FixedZone( nnn, int( h)*60*60 + int(m)*60 )

			} 
			dest[i] = time.Date(
				int(y),
				time.Month(m),
				int(d),
				int( hh),//-1,
				int(mm),//-1
				int(ss),//int(buf[6])-1,
				int(ff),
				loc)
			

        case C.SQLT_TIMESTAMP_LTZ:
        fmt.Println("SQLT_TIMESTAMP_LTZ")
        fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
        dest[i] = nil

        case C.SQLT_INTERVAL_DS:
        fmt.Println("SQLT_INTERVAL_DS")
        fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
        
                var (
        
        d, hh, mm, ss, ff C.sb4
        )   
        rv = C.OCIIntervalGetDaySecond(
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				&d, 
				&hh, 
				&mm,
				&ss,
				&ff,
				(*C.OCIInterval)(rc.cols[i].pbuf),
        )
			if rv != C.OCI_SUCCESS {
				return ociGetError(rc.s.c.err)
			}
			
        dest[i] = time.Duration( d) * time.Hour * 24 +  time.Duration( hh) * time.Hour +  time.Duration( mm) * time.Minute  +  time.Duration( ss) * time.Second +  time.Duration( ff) 

        case C.SQLT_INTERVAL_YM:
        fmt.Println("SQLT_INTERVAL_YM")
        fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
        dest[i] = nil
        
        
		default:
			return errors.New(fmt.Sprintf("Unhandled column type: %d", rc.cols[i].kind))
		}
	}

	return nil
}

func ociGetError(err unsafe.Pointer) error {
	var errcode C.sb4
	var errbuff [512]C.char
	C.OCIErrorGet(
		err,
		1,
		nil,
		&errcode,
		(*C.OraText)(unsafe.Pointer(&errbuff[0])),
		512,
		C.OCI_HTYPE_ERROR)
	s := C.GoString(&errbuff[0])
	return errors.New(s)
}

func parseEnviron(env []string) (out map[string]interface{}) {
	out = make(map[string]interface{})

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		// Better to have a type error here than later during query execution
		switch parts[0] {
		case "PREFETCH_ROWS":
			out["prefetch_rows"], _ = strconv.Atoi(parts[1])
		case "PREFETCH_MEMORY":
			out["prefetch_memory"], _ = strconv.ParseInt(parts[1], 10, 64)
		}
	}
	return out
}

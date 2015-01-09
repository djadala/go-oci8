package oci8

/*
#include <oci.h>
#include <stdlib.h>
#include <string.h>

#cgo pkg-config: oci8

typedef struct {
	int num;
	sword rv;
} retInt;
retInt OCIAttrGetInt( dvoid *ss, ub4 hType, ub4 aType, OCIError *err) {
	retInt vvv = {0,0};
	vvv.rv = OCIAttrGet(
		ss,
		hType,
		&vvv.num,
		NULL,
		aType,
		err);
	
	return vvv;
}

typedef struct {
	ub2 num;
	sword rv;
} retUb2;
retUb2 OCIAttrGetUb2( dvoid *ss, ub4 hType, ub4 aType, OCIError *err) {
	retUb2 vvv = {0,0};
	vvv.rv = OCIAttrGet(
		ss,
		hType,
		&vvv.num,
		NULL,
		aType,
		err);
	
	return vvv;
}

typedef struct {
	ub4 num;
	sword rv;
} retUb4;
retUb4 OCIAttrGetUb4( dvoid *ss, ub4 hType, ub4 aType, OCIError *err) {
	retUb4 vvv = {0,0};
	vvv.rv = OCIAttrGet(
		ss,
		hType,
		&vvv.num,
		NULL,
		aType,
		err);
	
	return vvv;
}

*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
	"log"
	//"sync"
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
	svc           unsafe.Pointer
	env           unsafe.Pointer
	err           unsafe.Pointer
	attrs         Values
	location      *time.Location
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
	if rv := C.OCITransCommit(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0); rv != C.OCI_SUCCESS {
		return ociGetError(tx.c.err)
	}
	return nil
}

func (tx *OCI8Tx) Rollback() error {
	tx.c.inTransaction = false
	if rv := C.OCITransRollback(
		(*C.OCISvcCtx)(tx.c.svc),
		(*C.OCIError)(tx.c.err),
		0); rv != C.OCI_SUCCESS {
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
	if rv := C.OCITransStart(
		(*C.OCISvcCtx)(c.svc),
		(*C.OCIError)(c.err),
		60,
		C.OCI_TRANS_READWRITE); //C.OCI_TRANS_NEW
	rv != C.OCI_SUCCESS {
		return nil, ociGetError(c.err)
	}
	c.inTransaction = true
	return &OCI8Tx{c}, nil
}

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

	if rv := C.OCIEnvCreate(
		(**C.OCIEnv)(unsafe.Pointer(&conn.env)),
		C.OCI_DEFAULT|C.OCI_THREADED,
		nil,
		nil,
		nil,
		nil,
		0,
		nil); rv != C.OCI_SUCCESS {
		return nil, ociGetError(conn.err)
	}


	if rv := C.OCIHandleAlloc(
		conn.env,
		&conn.err,
		C.OCI_HTYPE_ERROR,
		0,
		nil); rv != C.OCI_SUCCESS {
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

	if rv := C.OCILogon(
		(*C.OCIEnv)(conn.env),
		(*C.OCIError)(conn.err),
		(**C.OCISvcCtx)(unsafe.Pointer(&conn.svc)),
		(*C.OraText)(unsafe.Pointer(puser)),
		C.ub4(C.strlen(puser)),
		(*C.OraText)(unsafe.Pointer(ppass)),
		C.ub4(C.strlen(ppass)),
		(*C.OraText)(unsafe.Pointer(phost)),
		C.ub4(phostlen)); rv != C.OCI_SUCCESS {
		return nil, ociGetError(conn.err)
	}

	conn.location = dsn.Location

	return &conn, nil
}

func (c *OCI8Conn) Close() error {
	var err error
	
	if rv := C.OCILogoff(
		(*C.OCISvcCtx)(c.svc),
		(*C.OCIError)(c.err)); rv != C.OCI_SUCCESS {
		err = ociGetError(c.err)
	} else {
		err = nil
	}

	C.OCIHandleFree(
		c.env,
		C.OCI_HTYPE_ENV)

	c.svc = nil
	c.env = nil
	c.err = nil

	return err
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

	if rv := C.OCIHandleAlloc(
		c.env,
		&s,
		C.OCI_HTYPE_STMT,
		0,
		nil); rv != C.OCI_SUCCESS {
		return nil, ociGetError(c.err)
	}

	if rv := C.OCIStmtPrepare(
		(*C.OCIStmt)(s),
		(*C.OCIError)(c.err),
		(*C.OraText)(unsafe.Pointer(pquery)),
		C.ub4(C.strlen(pquery)),
		C.ub4(C.OCI_NTV_SYNTAX),
		C.ub4(C.OCI_DEFAULT)); rv != C.OCI_SUCCESS {
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
	r := C.OCIAttrGetInt( s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_BIND_COUNT, (*C.OCIError)(s.c.err))
	if r.rv != C.OCI_SUCCESS {
		log.Println( "NumInput:", ociGetError(s.c.err))
		return -1
	}
	return int(  r.num)
}

func (s *OCI8Stmt) bind(args []driver.Value) (freeBoundParameters func(), err error) {
	if args == nil {
		return func() {}, nil
	}

	var (
		bp              *C.OCIBind
		dty             C.ub2
		cdata           *C.char
		clen            C.sb4
		boundParameters []oci8bind
	)

	freeBoundParameters = func() {
		for _, col := range boundParameters {
			if col.pbuf != nil {
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
				case C.SQLT_TIMESTAMP_LTZ:
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_TIMESTAMP_LTZ)
				case C.SQLT_INTERVAL_DS:
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_INTERVAL_DS)
				case C.SQLT_INTERVAL_YM:
					C.OCIDescriptorFree(
						col.pbuf,
						C.OCI_DTYPE_INTERVAL_YM)
				default:
					C.free(col.pbuf)
				}
			}
		}
	}

	for i, v := range args {

		switch v.(type) {
		case nil:
			dty = C.SQLT_STR
			cdata = nil
			clen = 0
		case []byte:
			v := v.([]byte)
			dty = C.SQLT_BIN
			cdata = CByte(v)
			clen = C.sb4(len(v))
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})

		case float64:
			fb := math.Float64bits(v.(float64))
			if fb&0x8000000000000000 != 0 {
				fb ^= 0xffffffffffffffff
			} else {
				fb |= 0x8000000000000000
			}
			dty = C.SQLT_IBDOUBLE
			cdata = CByte([]byte{byte(fb >> 56), byte(fb >> 48), byte(fb >> 40), byte(fb >> 32), byte(fb >> 24), byte(fb >> 16), byte(fb >> 8), byte(fb)})
			clen = 8
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})

		case time.Time:

			var pt unsafe.Pointer

			clen = C.sb4(unsafe.Sizeof(pt))
			pt = C.malloc(C.size_t(clen))
			boundParameters = append(boundParameters, oci8bind{C.SQLT_CHR, pt})

			dty = C.SQLT_TIMESTAMP_TZ
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				(*unsafe.Pointer)(pt),
				C.OCI_DTYPE_TIMESTAMP_TZ,
				0,
				nil); rv != C.OCI_SUCCESS {
				defer freeBoundParameters()
				return nil, ociGetError(s.c.err)
			}
			boundParameters = append(boundParameters, oci8bind{dty, *(*unsafe.Pointer)(pt)})

			now := v.(time.Time)
			zone, offset := now.Zone()
			zp := C.CString(zone)

			for first := true; ; first = false {
				rv := C.OCIDateTimeConstruct(
					s.c.env,
					(*C.OCIError)(s.c.err),
					(*C.OCIDateTime)(*(*unsafe.Pointer)(pt)), //(*C.OCIDateTime)(pt),
					C.sb2(now.Year()),
					C.ub1(now.Month()),
					C.ub1(now.Day()),
					C.ub1(now.Hour()),
					C.ub1(now.Minute()),
					C.ub1(now.Second()),
					C.ub4(now.Nanosecond()),
					(*C.OraText)(unsafe.Pointer(zp)),
					C.size_t(len(zone)+1), //C.strlen( zp),//C.size_t(len(zone)),
				)
				C.free(unsafe.Pointer(zp))
				if rv != C.OCI_SUCCESS {
					if !first {
						defer freeBoundParameters()
						return nil, ociGetError(s.c.err)
					}
					sign := '+'
					if offset < 0 {
						offset = -offset
						offset /= 60
						sign = '-'
					} else {
						offset /= 60
					}
					zp = C.CString(fmt.Sprintf("%c%02d:%02d", sign, offset/60, offset%60))
				} else {
					break
				}
			}
			cdata = (*C.char)(pt)

		case string:
			v := v.(string)
			dty = C.SQLT_STR
			cdata = C.CString(v)
			clen = C.sb4(len(v) + 1)
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
			//		case int64:
			//		case bool:
			//fallthrough
		default:
			//fmt.Printf( "%T\n", v)
			dty = C.SQLT_STR
			d := fmt.Sprintf("%v", v)
			clen = C.sb4(len(d) + 1) // + terminating 00  ??????????
			cdata = C.CString(d)
			boundParameters = append(boundParameters, oci8bind{dty, unsafe.Pointer(cdata)})
		}

		if rv := C.OCIBindByPos(
			(*C.OCIStmt)(s.s),
			&bp,
			(*C.OCIError)(s.c.err),
			C.ub4(i+1),
			unsafe.Pointer(cdata),
			clen,
			dty,
			nil,
			nil,
			nil,
			0,
			nil,
			C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
			defer freeBoundParameters()
			return nil, ociGetError(s.c.err)
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

	iter := C.ub4(1)
	if retUb2 := C.OCIAttrGetUb2( s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_STMT_TYPE, (*C.OCIError)(s.c.err));
		retUb2.rv != C.OCI_SUCCESS {
		return nil, ociGetError(s.c.err)
	} else if retUb2.num == C.OCI_STMT_SELECT {
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
	if rv := C.OCIStmtExecute(
		(*C.OCISvcCtx)(s.c.svc),
		(*C.OCIStmt)(s.s),
		(*C.OCIError)(s.c.err),
		iter,
		0,
		nil,
		nil,
		mode); rv != C.OCI_SUCCESS {
		return nil, ociGetError(s.c.err)
	}
    
    var rc int
	if retUb2 := C.OCIAttrGetUb2( s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_PARAM_COUNT, (*C.OCIError)(s.c.err));
	retUb2.rv != C.OCI_SUCCESS {
		return nil, ociGetError(s.c.err)
	} else {
		rc = int(retUb2.num)
	}



	oci8cols := make([]oci8col, rc)
	for i := 0; i < rc; i++ {
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
		case C.SQLT_CHR, C.SQLT_AFC: // SQLT_VCS SQLT_AFC SQLT_CLOB SQLT_AVC
			lp *= 4 //utf8 enc
			oci8cols[i].kind = tp

		case C.SQLT_DAT:
			oci8cols[i].kind = C.SQLT_TIMESTAMP
			tp = C.SQLT_TIMESTAMP

		default:
			fmt.Println("KIND=", int(tp), "size=", int(lp))
			oci8cols[i].kind = tp
		}
		oci8cols[i].name = string((*[1 << 30]byte)(unsafe.Pointer(np))[0:int(ns)])
		oci8cols[i].size = int(lp)

		var (
			defp *C.OCIDefine
			valueP unsafe.Pointer
			valueSz C.sb4
			dty C.ub2
			)
		switch tp {
		case C.SQLT_CLOB, C.SQLT_BLOB:
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_LOB,
				0,
				nil);  rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
			}
			valueP = unsafe.Pointer(&oci8cols[i].pbuf)
			valueSz = C.sb4(unsafe.Sizeof(unsafe.Pointer(nil))) 
			dty = C.SQLT_TIMESTAMP

		case C.SQLT_TIMESTAMP:
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_TIMESTAMP,
				0,
				nil);  rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
			}
			
			valueP = unsafe.Pointer(&oci8cols[i].pbuf)
			valueSz = C.sb4(unsafe.Sizeof(unsafe.Pointer(nil))) 
			dty = C.SQLT_TIMESTAMP

		case C.SQLT_TIMESTAMP_TZ:
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_TIMESTAMP_TZ,
				0,
				nil);  rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
			}
			valueP = unsafe.Pointer(&oci8cols[i].pbuf)
			valueSz = C.sb4(unsafe.Sizeof(unsafe.Pointer(nil))) 
			dty = C.SQLT_TIMESTAMP_TZ
			
		case C.SQLT_INTERVAL_DS:
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_INTERVAL_DS,
				0,
				nil);  rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
			}
			
			valueP = unsafe.Pointer(&oci8cols[i].pbuf)
			valueSz = C.sb4(unsafe.Sizeof(unsafe.Pointer(nil))) 
			dty = C.SQLT_INTERVAL_DS//oci8cols[i].kind


		case C.SQLT_INTERVAL_YM:
			if rv := C.OCIDescriptorAlloc(
				s.c.env,
				&oci8cols[i].pbuf,
				C.OCI_DTYPE_INTERVAL_YM,
				0,
				nil);  rv != C.OCI_SUCCESS {
					return nil, ociGetError(s.c.err)
			}
			
			valueP = unsafe.Pointer(&oci8cols[i].pbuf)
			valueSz = C.sb4(unsafe.Sizeof(unsafe.Pointer(nil))) 
			dty = C.SQLT_INTERVAL_YM//oci8cols[i].kind

		default:
			oci8cols[i].pbuf = C.malloc(C.size_t(lp) + 1)
			valueP = oci8cols[i].pbuf
			valueSz = C.sb4(lp+1) 
			dty = oci8cols[i].kind
		}

		if rv := C.OCIDefineByPos(
			(*C.OCIStmt)(s.s),
			&defp,
			(*C.OCIError)(s.c.err),
			C.ub4(i+1),
			valueP,
			valueSz,
			dty,
			unsafe.Pointer(&oci8cols[i].ind),
			&oci8cols[i].rlen,
			nil,
			C.OCI_DEFAULT); rv != C.OCI_SUCCESS {
				return nil, ociGetError(s.c.err)
		}
	}
	return &OCI8Rows{s, oci8cols, false}, nil
}

type OCI8Result struct {
	s *OCI8Stmt
}

func (r *OCI8Result) LastInsertId() (int64, error) {
	retUb4 := C.OCIAttrGetUb4( r.s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_ROWID, (*C.OCIError)(r.s.c.err));
	if retUb4.rv != C.OCI_SUCCESS {
		return 0, ociGetError(r.s.c.err)
	}
	return int64(retUb4.num), nil
}

func (r *OCI8Result) RowsAffected() (int64, error) {
 	retUb4 := C.OCIAttrGetUb4( r.s.s, C.OCI_HTYPE_STMT, C.OCI_ATTR_ROW_COUNT, (*C.OCIError)(r.s.c.err));
	if retUb4.rv != C.OCI_SUCCESS {
		return 0, ociGetError(r.s.c.err)
	}
	return int64(retUb4.num), nil
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
		mode)
	if rv == C.OCI_ERROR {
		return nil, ociGetError(s.c.err)
	}
	if rv != C.OCI_SUCCESS {
		fmt.Println("OCI_SUCCESS!=", rv)
		fmt.Println(ociGetError(s.c.err))
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
		case C.SQLT_INTERVAL_YM:
			C.OCIDescriptorFree(
				col.pbuf,
				C.OCI_DTYPE_INTERVAL_YM)
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
		fmt.Println("OCI_SUCCESS!=", rv)
		fmt.Println(ociGetError(rc.s.c.err))
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
			total := 0
			var bamt C.ub4
			b := make([]byte, bufSize)
		again:
			rv = C.OCILobRead(
				(*C.OCISvcCtx)(rc.s.c.svc),
				(*C.OCIError)(rc.s.c.err),
				(*C.OCILobLocator)(rc.cols[i].pbuf),
				&bamt,
				1,
				unsafe.Pointer(&b[total]),
				C.ub4(bufSize),
				nil,
				nil,
				0,
				C.SQLCS_IMPLICIT)
			if rv == C.OCI_NEED_DATA {
				b = append(b, b...)
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

		case C.SQLT_LVB: // LONG VARRAW
			fmt.Println("LONG VARRAW column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
			dest[i] = nil
		case C.SQLT_BIN: // RAW
			buf := (*[1 << 30]byte)(unsafe.Pointer(rc.cols[i].pbuf))[0:rc.cols[i].rlen]
			fmt.Println("RAW column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
			dest[i] = buf
		case C.SQLT_LNG: // LONG
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
			if colsize == 4 {
				v := uint32(buf[3])
				v |= uint32(buf[2]) << 8
				v |= uint32(buf[1]) << 16
				v |= uint32(buf[0]) << 24

				//SOME ORACLE SHIT ?
				if buf[0]&0x80 == 0 {
					v ^= 0xffffffff
				} else {
					v &= 0x7fffffff
				}

				//fmt.Printf("%x %x %x %x\n", buf[0], buf[1], buf[2], buf[3])

				dest[i] = math.Float32frombits(v)
			} else if colsize == 8 {
				v := uint64(buf[7])
				v |= uint64(buf[6]) << 8
				v |= uint64(buf[5]) << 16
				v |= uint64(buf[4]) << 24
				v |= uint64(buf[3]) << 32
				v |= uint64(buf[2]) << 40
				v |= uint64(buf[1]) << 48
				v |= uint64(buf[0]) << 56

				//SOME ORACLE SHIT ?
				if buf[0]&0x80 == 0 {
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
				y                C.sb2
				m, d, hh, mm, ss C.ub1
				ff               C.ub4
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
				int(hh), //-1,
				int(mm), //-1
				int(ss), //int(buf[6])-1,
				int(ff),
				rc.s.c.location)

		case C.SQLT_TIMESTAMP_TZ, C.SQLT_TIMESTAMP_LTZ:
			fmt.Println("SQLT_TIMESTAMP_TZ")
			fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
			var (
				y                C.sb2
				m, d, hh, mm, ss C.ub1
				ff               C.ub4
				zone             [512]C.ub1
				zlen             C.ub4
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
			nnn := C.GoStringN((*C.char)((unsafe.Pointer)(&zone[0])), C.int(zlen))
			fmt.Println(nnn)

			loc, err := time.LoadLocation(nnn)
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
				fmt.Println(nnn, int(h)*60*60+int(m)*60)
				loc = time.FixedZone(nnn, int(h)*60*60+int(m)*60)

			}
			dest[i] = time.Date(
				int(y),
				time.Month(m),
				int(d),
				int(hh), //-1,
				int(mm), //-1
				int(ss), //int(buf[6])-1,
				int(ff),
				loc)

			// case C.SQLT_TIMESTAMP_LTZ:
			// fmt.Println("SQLT_TIMESTAMP_LTZ")
			// fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)
			// dest[i] = nil

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

			dest[i] = time.Duration(d)*time.Hour*24 + time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second + time.Duration(ff)

		case C.SQLT_INTERVAL_YM:
			fmt.Println("SQLT_INTERVAL_YM")
			fmt.Println("column size: ", rc.cols[i].size, "rlen =", rc.cols[i].rlen)

			var (
				y, m C.sb4
			)
			rv = C.OCIIntervalGetYearMonth(
				rc.s.c.env,
				(*C.OCIError)(rc.s.c.err),
				&y,
				&m,
				(*C.OCIInterval)(rc.cols[i].pbuf),
			)
			if rv != C.OCI_SUCCESS {
				return ociGetError(rc.s.c.err)
			}

			dest[i] = int64(y)*12 + int64(m)

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

func CByte(b []byte) *C.char {
	p := C.malloc(C.size_t(len(b)))
	pp := (*[1 << 30]byte)(p)
	copy(pp[:], b)
	return (*C.char)(p)
}

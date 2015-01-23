package oci8_test

// ( . oracle.sh ;DSN='user:pass@:0/(description=(address_list=(address=(protocol=tcp)(host=192.168.1.1)(port=1521)))(connect_data=(sid=SID)))'  go test )

import (
	"database/sql"
	_ "local/mattn/go-oci8"
	"fmt"
    "jambo/sqlrows"
	"os"
	"time"
	"testing"
	"bytes"
	"strings"
)

var db *sql.DB

func init() {
	//os.Setenv("NLS_LANG", ".AL32UTF8")
	os.Setenv("NLS_LANG", "BULGARIAN_BULGARIA.AL32UTF8")
	//os.Setenv("NLS_LANG", "BULGARIAN_BULGARIA.CL8MSWIN1251")

    var err error
    dsn:= os.Getenv( "DSN")

	db, err = sql.Open("oci8", dsn)
	if err != nil {
		panic( err)
	}
	err = db.Ping()
	if err != nil {
		panic( err)
	}
}

func sqlstest(t *testing.T, sql string, p ...interface{} ) map[string]interface{} {
	
	rows, err := sqlrows.New( db.Query( sql, p...))
	if err != nil {
		t.Fatal( err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatal( "no row returned:", rows.Err())
	}
    err = rows.Scan() 
	if err != nil {
		rows.Close()
		t.Fatal( err)
	}
	res := rows.Map()
	//res := rows.Row()
	rows.Print()
	err = rows.Close()
	if err != nil {
		rows.Close()
		t.Fatal( err)
	}
	return res
}

func sqlstestv(t *testing.T, sql string, p ...interface{} ) []interface{} {
	
	rows, err := sqlrows.New( db.Query( sql, p...))
	if err != nil {
		t.Fatal( err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatal( "no row returned:", rows.Err())
	}
    err = rows.Scan() 
	if err != nil {
		rows.Close()
		t.Fatal( err)
	}
	//res := rows.Map()
	res := rows.Row()
	rows.Print()
	err = rows.Close()
	if err != nil {
		rows.Close()
		t.Fatal( err)
	}
	return res
}


func TestSelect1(t *testing.T) {


	//rows, err := db.Query("select :1 as AA, :2 as BB, NUMTODSINTERVAL( :3, 'SECOND') as DD, NUMTOYMINTERVAL( :4, 'MONTH') as FF, :4 as nil from dual", time.Now(), 3.14, 3.004, 55, nil)
	//rows, err := db.Query("select :1 as AA, :2 as BB, :3 as CC from dual", time.Now(), time.Now().Add( 300000000000000000), time.Now().Add( 100000000100000000))
	//rows, err := db.Query("select sysdate from dual")
	
	fmt.Println("bind all go types:")
	
	sqlstest( t, 
	"select :0 as nil, :1 as true, :2 as false, :3 as int64, :4 as time, :5 as string, :6 as bytes, :7 as float64 from dual", 
	nil, true, false, 1234567890123456789, time.Now(), "bee     ", []byte{ 61,62,63,64,65,66,67,68}, 3.14)
}


func TestInterval1(t *testing.T) {
	
	fmt.Println("test interval1:")
	n := time.Duration( 1234567898123456789)
	r := sqlstest( t, "select NUMTODSINTERVAL( :0 / 1000000000, 'SECOND') as intervalds from dual", int64(n))
	if n != r["INTERVALDS"].(time.Duration) {
		t.Fatal( r,"!=", n)
	}
}

func TestInterval2(t *testing.T) {
	
	fmt.Println("test interval2:")
	n := time.Duration( -1234567898123456789)
	r := sqlstest( t, "select NUMTODSINTERVAL( :0 / 1000000000, 'SECOND') as intervalds from dual", int64(n))
	if n != r["INTERVALDS"].(time.Duration) {
		t.Fatal( r,"!=", n)
	}
}

func TestInterval3(t *testing.T) {
	
	fmt.Println("test interval3:")
	n := int64(1234567890)
	r := sqlstest( t, "select NUMTOYMINTERVAL( :0, 'MONTH') as intervalym from dual", n)
	if n != r["INTERVALYM"].(int64) {
		t.Fatal( r,"!=", n)
	}
}

func TestInterval4(t *testing.T) {
	
	fmt.Println("test interval4:")
	n := int64(-1234567890)
	r := sqlstest( t, "select NUMTOYMINTERVAL( :0, 'MONTH') as intervalym from dual", n)
	if n != r["INTERVALYM"].(int64) {
		t.Fatal( r,"!=", n)
	}
}

func TestIntervals5(t *testing.T) {
	
	fmt.Println("test interval5:")

    
	n1 := time.Duration( 987)
	n2 := time.Duration( -65)
	n3 := int64(4332)
	n4 := int64(-1239872)
	r := sqlstest( t, "select NUMTODSINTERVAL( :0 / 1000000000, 'SECOND') as i1, NUMTODSINTERVAL( :1 / 1000000000, 'SECOND') as i2, NUMTOYMINTERVAL( :2, 'MONTH') as i3, NUMTOYMINTERVAL( :3, 'MONTH') as i4 from dual", n1,n2,n3,n4)
	if n1 != r["I1"].(time.Duration) {
		t.Fatal( r["I1"],"!=", n1)
	}
	if n2 != r["I2"].(time.Duration) {
		t.Fatal( r["I2"],"!=", n2)
	}
	if n3 != r["I3"].(int64) {
		t.Fatal( r["I3"],"!=", n3)
	}
	if n4 != r["I4"].(int64) {
		t.Fatal( r["I4"],"!=", n4)
	}
}

func TestTime1(t *testing.T) {
	
	fmt.Println("test time1:")
	n := time.Now()
	r := sqlstest( t, "select :0 as time from dual", n)
	if !n.Equal(r["TIME"].(time.Time) ) {
		t.Fatal( r,"!=", n)
	}
}

func TestTime2(t *testing.T) {
	fmt.Println("test time 2:")
	
	const f = "2006-01-02 15:04:05.999999999 -07:00"
	in := []time.Time{}
	
	tm, err:= time.Parse( f, "2015-01-23 12:34:56.123456789 +09:05")
	if err != nil {
		t.Fatal( err)
	}
	in = append( in, tm)
	
	tm, err= time.Parse( f, "1014-10-14 21:43:50.987654321 -08:50")
	if err != nil {
		t.Fatal( err)
	}
	in = append( in, tm)

    tm = time.Date(-4123,time.Month(12),1,2,3,4,0,time.UTC)
    in = append( in, tm)

    tm = time.Date(9321,time.Month(11),2,3,4,5,0,time.UTC)
    in = append( in, tm)
	
	r := sqlstestv( t, "select :0, :1, :2, :3  from dual", in[0], in[1], in[2], in[3])
	for i, v  := range r {
		vt := v.(time.Time)
		if !vt.Equal( in[i] ) {
			t.Fatal( vt,"!=", in[i])
		}
	}
}

func TestTime3(t *testing.T) {
	
	fmt.Println("test sysdate:")
	sqlstest( t, "select sysdate - 365*6500 from dual")
}



func TestBytes1(t *testing.T) {
	
	fmt.Println("test bytes1:")
	n := bytes.Repeat( []byte{ 'A'}, 4000)
	r := sqlstest( t, "select :0 as bytes from dual", n)
	if !bytes.Equal(n, r["BYTES"].([]byte) ) {
		t.Fatal( r["BYTES"],"!=", n)
	}
}

func TestString1(t *testing.T) {
	
	fmt.Println("test string1:")
	n := strings.Repeat( "1234567890", 400)
	r := sqlstest( t, "select :0 as str from dual", n)
	if n != r["STR"].(string)  {
		t.Fatal( r["STR"],"!=", n)
	}
}




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
	//"bytes"
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
	r := sqlstest( t, "select NUMTODSINTERVAL( :0, 'SECOND') as intervalds from dual", "1234567898.123456789")
	if n != r["INTERVALDS"].(time.Duration) {
		t.Fatal( r,"!=", n)
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
	t1, err:= time.Parse( f, "2015-01-23 12:34:56.123456789 +09:05")
	if err != nil {
		t.Fatal( err)
	}
	t2, err:= time.Parse( f, "1014-10-14 21:43:50.987654321 -08:50")
	if err != nil {
		t.Fatal( err)
	}
	//fmt.Println( t1.Zone())
	
	r := sqlstest( t, "select :0 as time1, :1 as time2 from dual", t1, t2)
	rt := r["TIME1"].(time.Time)
	if !t1.Equal( rt ) {
		t.Fatal( t1,"!=", rt)
	}
	rt = r["TIME2"].(time.Time)
	if !t2.Equal( rt ) {
		t.Fatal( t2,"!=", rt)
	}
}

func TestTime3(t *testing.T) {
	
	fmt.Println("test sysdate:")
	sqlstest( t, "select sysdate from dual")
}



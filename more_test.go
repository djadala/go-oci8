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
}

func TestSelect1(t *testing.T) {


	//rows, err := db.Query("select :1 as AA, :2 as BB, NUMTODSINTERVAL( :3, 'SECOND') as DD, NUMTOYMINTERVAL( :4, 'MONTH') as FF, :4 as nil from dual", time.Now(), 3.14, 3.004, 55, nil)
	//rows, err := db.Query("select :1 as AA, :2 as BB, :3 as CC from dual", time.Now(), time.Now().Add( 300000000000000000), time.Now().Add( 100000000100000000))
	//rows, err := db.Query("select sysdate from dual")
	
	fmt.Println("bind all go types:")
	rows, err := sqlrows.New( db.Query("select :0 as nil, :1 as true, :2 as false, :3 as int64, :4 as time, :5 as string, :6 as bytes, :7 as float64 from dual", nil, true, false, 1234567890123456789, time.Now(), "bee     ", []byte{ 61,62,63,64,65,66,67,68}, 3.14))
	if err != nil {
		t.Fatal( err)
	}
	if !rows.Next() {
		t.Fatal( "no row returned")
	}
    err = rows.Scan() 
	if err != nil {
		t.Fatal( err)
	}
	rows.Print()
	err = rows.Close()
	if err != nil {
		t.Fatal( err)
	}
}


package tests

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"

	_ "embed"
)

func subTestAOF(g *testGroup) {
	g.regSubTest("loading", aof_loading_test)
	g.regSubTest("migrate", aof_migrate_test)
	g.regSubTest("AOF", aof_AOF_test)
	g.regSubTest("AOFMD5", aof_AOFMD5_test)
	g.regSubTest("AOFSHRINK", aof_AOFSHRINK_test)
	g.regSubTest("READONLY", aof_READONLY_test)
	g.regSubTest("get live reload", aof_get_live_reload_test)
}

// aof_get_live_reload_test verifies that a channel using `GET key id LIVE`
// survives an AOF reload and still resolves its boundary at match time using
// the reloaded (moved) boundary object. See issue #813.
func aof_get_live_reload_test(mc *mockServer) error {
	// Use BOUNDS (no quotes) so the inline AOF tokenizer is happy. Box A and
	// box B are disjoint. minlat minlon maxlat maxlon.
	boundsA := "SET zones zone1 BOUNDS 33.4 -112.2 33.6 -112.0"
	boundsB := "SET zones zone1 BOUNDS 33.4 -110.2 33.6 -110.0"

	// AOF: create the boundary, the live channel, then move the boundary to B.
	aof := boundsA + "\r\n" +
		"SETCHAN mychan WITHIN fleet FENCE DETECT inside,outside GET zones zone1 LIVE\r\n" +
		boundsB + "\r\n"

	rmc, err := loadAOF(aof)
	if err != nil {
		return err
	}
	defer rmc.Close()

	// Confirm the channel was reloaded.
	if _, err := rmc.Do("CHANS", "*"); err != nil {
		return err
	}

	finalErr := make(chan error, 1)
	var ready atomic.Bool
	go func() {
		sc, err := redis.Dial("tcp", fmt.Sprintf(":%d", rmc.port))
		if err != nil {
			finalErr <- err
			return
		}
		defer sc.Close()
		psc := redis.PubSubConn{Conn: sc}
		if err := psc.PSubscribe("*"); err != nil {
			finalErr <- err
			return
		}
		for sc.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Message:
				if v.Channel == "status" && string(v.Data) == "ready" {
					ready.Store(true)
					continue
				}
				if v.Channel != "mychan" {
					continue
				}
				// A truck inside B must match the reloaded boundary (B), proving
				// the live GET reference was preserved across reload.
				if d := gjson.Get(string(v.Data), "detect").String(); d != "inside" {
					finalErr <- fmt.Errorf("expected detect 'inside', got '%s'", d)
					return
				}
				finalErr <- nil
				return
			case error:
				finalErr <- v
				return
			}
		}
	}()

	for !ready.Load() {
		if _, err := rmc.Do("PUBLISH", "status", "ready"); err != nil {
			return err
		}
	}
	if _, err := rmc.Do("SET", "fleet", "truck1", "POINT", "33.5", "-110.1"); err != nil {
		return err
	}
	return <-finalErr
}

func loadAOFAndClose(aof any) error {
	mc, err := loadAOF(aof)
	if mc != nil {
		mc.Close()
	}
	return err
}

func loadAOF(aof any) (*mockServer, error) {
	var aofb []byte
	switch aof := aof.(type) {
	case []byte:
		aofb = []byte(aof)
	case string:
		aofb = []byte(aof)
	default:
		return nil, errors.New("aof is not string or bytes")
	}
	return mockOpenServer(MockServerOptions{
		Silent:  true,
		Metrics: false,
		AOFData: aofb,
	})
}

func aof_loading_test(mc *mockServer) error {

	var err error
	// invalid command
	err = loadAOFAndClose("asdfasdf\r\n")
	if err == nil || err.Error() != "unknown command 'asdfasdf'" {
		return fmt.Errorf("expected '%v', got '%v'",
			"unknown command 'asdfasdf'", err)
	}

	// incomplete command
	err = loadAOFAndClose("set fleet truck point 10 10\r\nasdfasdf")
	if err != nil {
		return err
	}

	// big aof file
	var aof string
	for i := 0; i < 10000; i++ {
		aof += fmt.Sprintf("SET fleet truck%d POINT 10 10\r\n", i)
	}
	err = loadAOFAndClose(aof)
	if err != nil {
		return err
	}

	// extra zeros at various places
	aof = ""
	for i := 0; i < 1000; i++ {
		if i%10 == 0 {
			aof += string(bytes.Repeat([]byte{0}, 100))
		}
		aof += fmt.Sprintf("SET fleet truck%d POINT 10 10\r\n", i)
	}
	aof += string(bytes.Repeat([]byte{0}, 5000))
	err = loadAOFAndClose(aof)
	if err != nil {
		return err
	}

	// bad protocol
	aof = "*2\r\n$1\r\nh\r\n+OK\r\n"
	err = loadAOFAndClose(aof)
	if fmt.Sprintf("%v", err) != "Protocol error: expected '$', got '+'" {
		return fmt.Errorf("expected '%v', got '%v'",
			"Protocol error: expected '$', got '+'", err)
	}
	return nil
}

func aof_AOFMD5_test(mc *mockServer) error {
	for i := 0; i < 10000; i++ {
		_, err := mc.Do("SET", "fleet", rand.Int(),
			"POINT", rand.Float64()*180-90, rand.Float64()*360-180)
		if err != nil {
			return err
		}
	}
	aof, err := mc.readAOF()
	if err != nil {
		return err
	}
	check := func(start, size int) func(s string) error {
		return func(s string) error {
			sum := md5.Sum(aof[start : start+size])
			val := hex.EncodeToString(sum[:])
			if s != val {
				return fmt.Errorf("expected '%s', got '%s'", val, s)
			}
			return nil
		}
	}
	return mc.DoBatch(
		Do("AOFMD5").Err("wrong number of arguments for 'aofmd5' command"),
		Do("AOFMD5", 0).Err("wrong number of arguments for 'aofmd5' command"),
		Do("AOFMD5", 0, 0, 1).Err("wrong number of arguments for 'aofmd5' command"),
		Do("AOFMD5", -1, 0).Err("invalid argument '-1'"),
		Do("AOFMD5", 1, -1).Err("invalid argument '-1'"),
		Do("AOFMD5", 0, 100000000000).Err("EOF"),
		Do("AOFMD5", 0, 0).Str("d41d8cd98f00b204e9800998ecf8427e"),
		Do("AOFMD5", 0, 0).JSON().Str(`{"ok":true,"md5":"d41d8cd98f00b204e9800998ecf8427e"}`),
		Do("AOFMD5", 0, 0).Func(check(0, 0)),
		Do("AOFMD5", 0, 1).Func(check(0, 1)),
		Do("AOFMD5", 0, 100).Func(check(0, 100)),
		Do("AOFMD5", 1002, 4321).Func(check(1002, 4321)),
	)
}

func openFollower(mc *mockServer) (conn redis.Conn, err error) {
	conn, err = redis.Dial("tcp", fmt.Sprintf(":%d", mc.port),
		redis.DialReadTimeout(time.Second))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			conn.Close()
			conn = nil
		}
	}()
	if err := conn.Send("AOF", 0); err != nil {
		return nil, err
	}
	if err := conn.Flush(); err != nil {
		return nil, err
	}
	str, err := redis.String(conn.Receive())
	if err != nil {
		return nil, err
	}
	if str != "OK" {
		return nil, fmt.Errorf("expected '%s', got '%s'", "OK", str)
	}
	return conn, nil
}

func aof_AOF_test(mc *mockServer) error {
	var argss [][]interface{}
	for i := 0; i < 10000; i++ {
		args := []interface{}{"SET", "fleet", fmt.Sprint(rand.Int()),
			"POINT", fmt.Sprint(rand.Float64()*180 - 90),
			fmt.Sprint(rand.Float64()*360 - 180)}
		argss = append(argss, args)
		_, err := mc.Do(fmt.Sprint(args[0]), args[1:]...)
		if err != nil {
			return err
		}
	}
	readAll := func() (conn redis.Conn, err error) {
		conn, err = openFollower(mc)
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				conn.Close()
				conn = nil
			}
		}()
		var t bool
		for i := 0; i < len(argss); i++ {
			args, err := redis.Values(conn.Receive())
			if err != nil {
				return nil, err
			}
			if t || (len(args) == len(argss[0]) &&
				fmt.Sprintf("%s", args[2]) == fmt.Sprintf("%s", argss[0][2])) {
				t = true
				if fmt.Sprintf("%s", args[2]) !=
					fmt.Sprintf("%s", argss[i][2]) {
					return nil, fmt.Errorf("expected '%s', got '%s'",
						argss[i][2], args[2])
				}
			} else {
				i--
			}
		}
		return conn, nil
	}

	conn, err := readAll()
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("fancy") // non-existent error
	if err == nil || err.Error() != "EOF" {
		return fmt.Errorf("expected '%v', got '%v'", "EOF", err)
	}

	conn, err = readAll()
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("quit")
	if err == nil || err.Error() != "EOF" {
		return fmt.Errorf("expected '%v', got '%v'", "EOF", err)
	}

	return mc.DoBatch(
		Do("AOF").Err("wrong number of arguments for 'aof' command"),
		Do("AOF", 0, 0).Err("wrong number of arguments for 'aof' command"),
		Do("AOF", -1).Err("invalid argument '-1'"),
		Do("AOF", 1000000000000).Err("pos is too big, must be less that the aof_size of leader"),
	)
}

func aof_AOFSHRINK_test(mc *mockServer) error {
	var err error
	haddr := fmt.Sprintf("localhost:%d", getNextPort())
	ln, err := net.Listen("tcp", haddr)
	if err != nil {
		return err
	}
	defer ln.Close()
	var msgs atomic.Int32
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			msgs.Add(1)
			// println(r.URL.Path)
		})
		http.Serve(ln, mux)
	}()
	err = mc.DoBatch(
		Do("SETCHAN", "mychan", "INTERSECTS", "mi:0", "BOUNDS", -10, -10, 10, 10).Str("1"),
		Do("SETHOOK", "myhook", "http://"+haddr, "INTERSECTS", "mi:0", "BOUNDS", -10, -10, 10, 10).Str("1"),
		Do("MASSINSERT", 5, 10000).OK(),
	)
	if err != nil {
		return err
	}
	err = mc.DoBatch(
		Do("AOFSHRINK").OK(),
		Do("MASSINSERT", 5, 10000).OK(),
	)
	if err != nil {
		return err
	}
	nmsgs := msgs.Load()
	if nmsgs == 0 {
		return fmt.Errorf("expected > 0, got %d", nmsgs)
	}
	return err
}

func aof_READONLY_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "POINT", "10", "10").OK(),
		Do("READONLY", "yes").OK(),
		Do("SET", "mykey", "myid", "POINT", "10", "10").Err("read only"),
		Do("READONLY", "no").OK(),
		Do("SET", "mykey", "myid", "POINT", "10", "10").OK(),
		Do("READONLY").Err("wrong number of arguments for 'readonly' command"),
		Do("READONLY", "maybe").Err("invalid argument 'maybe'"),
	)
}

//go:embed aof_legacy
var aofLegacy []byte

func aof_migrate_test(mc *mockServer) error {
	var aof []byte
	for i := 0; i < 10000; i++ {
		aof = append(aof, aofLegacy...)
	}
	var mc2 *mockServer
	var err error
	defer func() {
		mc2.Close()
	}()
	mc2, err = mockOpenServer(MockServerOptions{
		AOFFileName: "aof",
		AOFData:     aof,
		Silent:      true,
		Metrics:     true,
	})
	if err != nil {
		return err
	}
	err = mc2.DoBatch(
		Do("GET", "1", "2").Str(`{"type":"Point","coordinates":[20,10]}`),
	)
	if err != nil {
		return err
	}
	mc2.Close()

	mc2, err = mockOpenServer(MockServerOptions{
		AOFFileName: "aof",
		AOFData:     aofLegacy[:len(aofLegacy)-1],
		Silent:      true,
		Metrics:     true,
	})
	if err != io.ErrUnexpectedEOF {
		return fmt.Errorf("expected '%v', got '%v'", io.ErrUnexpectedEOF, err)
	}
	mc2.Close()

	mc2, err = mockOpenServer(MockServerOptions{
		AOFFileName: "aof",
		AOFData:     aofLegacy[1:],
		Silent:      true,
		Metrics:     true,
	})
	if err != io.ErrUnexpectedEOF {
		return fmt.Errorf("expected '%v', got '%v'", io.ErrUnexpectedEOF, err)
	}
	mc2.Close()

	return nil
}

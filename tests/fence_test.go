package tests

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestFence(g *testGroup) {

	// Standard
	g.regSubTest("basic", fence_basic_test)
	g.regSubTest("channel message order", fence_channel_message_order_test)
	g.regSubTest("detect inside,outside", fence_detect_inside_test)

	// Roaming
	g.regSubTest("roaming live", fence_roaming_live_test)
	g.regSubTest("roaming channel", fence_roaming_channel_test)
	g.regSubTest("roaming webhook", fence_roaming_webhook_test)

	// channel meta
	g.regSubTest("channel meta", fence_channel_meta_test)

	// live GET boundary (issue #813)
	g.regSubTest("get live", fence_get_live_test)
	g.regSubTest("get live channel", fence_get_live_channel_test)

	// various
	g.regSubTest("detect eecio", fence_eecio_test)
}

// polygons used by the "get live" tests. A and B are disjoint boxes.
const fenceGetLivePolyA = `{"type":"Polygon","coordinates":[[[-112.2,33.4],[-112.0,33.4],[-112.0,33.6],[-112.2,33.6],[-112.2,33.4]]]}`
const fenceGetLivePolyB = `{"type":"Polygon","coordinates":[[[-110.2,33.4],[-110.0,33.4],[-110.0,33.6],[-110.2,33.6],[-110.2,33.4]]]}`

// fence_get_live_test verifies that a live geofence using `GET key id LIVE`
// resolves the boundary object at match time, so the boundary follows the
// referenced object when it moves.
func fence_get_live_test(mc *mockServer) error {
	// Set up the boundary object BEFORE the fence references it via GET.
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer c.Close()

	if _, err := redis.String(c.Do("SET", "zones", "zone1", "OBJECT",
		fenceGetLivePolyA)); err != nil {
		return err
	}

	// Open a live geofence whose boundary is resolved at match time.
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn,
		"WITHIN fleet FENCE DETECT inside,outside GET zones zone1 LIVE\r\n"); err != nil {
		return err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	if res := string(buf[:n]); res != "+OK\r\n" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}

	// truck inside polygon A -> inside
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT",
		"33.5", "-112.1")); err != nil {
		return err
	}
	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "fleet",
		"id", "truck1",
		"object.coordinates", "[-112.1,33.5]"); err != nil {
		return err
	}

	// Move the boundary to polygon B (disjoint). This writes to "zones", not
	// the watched "fleet" key, so it must not itself produce a fence message.
	if _, err := redis.String(c.Do("SET", "zones", "zone1", "OBJECT",
		fenceGetLivePolyB)); err != nil {
		return err
	}

	// truck now inside polygon B -> inside, proving the boundary was resolved
	// at match time (a static GET snapshot of A would report "outside" here).
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT",
		"33.5", "-110.1")); err != nil {
		return err
	}
	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "fleet",
		"id", "truck1",
		"object.coordinates", "[-110.1,33.5]"); err != nil {
		return err
	}

	// truck back at the old location -> now outside (the boundary followed to B)
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT",
		"33.5", "-112.1")); err != nil {
		return err
	}
	if err := rd.receiveExpect("command", "set",
		"detect", "outside",
		"key", "fleet",
		"id", "truck1",
		"object.coordinates", "[-112.1,33.5]"); err != nil {
		return err
	}
	return nil
}

// fence_get_live_channel_test verifies the same behavior through a persisted
// channel (SETCHAN), which exercises the always-candidate hooksLive path.
func fence_get_live_channel_test(mc *mockServer) error {
	finalErr := make(chan error, 1)
	var ready atomic.Bool

	go func() {
		sc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
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
		want := []struct{ detect, coords string }{
			{"inside", "[-112.1,33.5]"},
			{"inside", "[-110.1,33.5]"},
			{"outside", "[-112.1,33.5]"},
		}
		var msgs []string
		for sc.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Message:
				if v.Channel == "status" && string(v.Data) == "ready" {
					ready.Store(true)
					continue
				}
				if v.Channel != "getlivechan" {
					continue
				}
				msgs = append(msgs, string(v.Data))
				if len(msgs) < len(want) {
					continue
				}
				for i, w := range want {
					if d := gjson.Get(msgs[i], "detect").String(); d != w.detect {
						finalErr <- fmt.Errorf("msg %d: expected detect '%s', got '%s'",
							i, w.detect, d)
						return
					}
					if c := gjson.Get(msgs[i], "object.coordinates").String(); c != w.coords {
						finalErr <- fmt.Errorf("msg %d: expected coords '%s', got '%s'",
							i, w.coords, c)
						return
					}
				}
				finalErr <- nil
				return
			case error:
				finalErr <- v
				return
			}
		}
	}()

	bc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer bc.Close()

	// The boundary object must exist before the channel references it via GET.
	if _, err := bc.Do("SET", "zones", "zone1", "OBJECT", fenceGetLivePolyA); err != nil {
		return err
	}
	if _, err := bc.Do("SETCHAN", "getlivechan", "WITHIN", "fleet", "FENCE",
		"DETECT", "inside,outside", "GET", "zones", "zone1", "LIVE"); err != nil {
		return err
	}

	// Wait for the subscriber to be ready.
	for !ready.Load() {
		if _, err := bc.Do("PUBLISH", "status", "ready"); err != nil {
			return err
		}
	}

	// truck inside A -> inside
	if _, err := bc.Do("SET", "fleet", "truck1", "POINT", "33.5", "-112.1"); err != nil {
		return err
	}
	// move boundary to B (watched key is "fleet", so no message from this write)
	if _, err := bc.Do("SET", "zones", "zone1", "OBJECT", fenceGetLivePolyB); err != nil {
		return err
	}
	// truck inside B -> inside (runtime resolution)
	if _, err := bc.Do("SET", "fleet", "truck1", "POINT", "33.5", "-110.1"); err != nil {
		return err
	}
	// truck back at old location -> outside (boundary followed to B)
	if _, err := bc.Do("SET", "fleet", "truck1", "POINT", "33.5", "-112.1"); err != nil {
		return err
	}

	return <-finalErr
}

type fenceReader struct {
	conn net.Conn
	rd   *bufio.Reader
}

func (fr *fenceReader) receive() (string, error) {
	if err := fr.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return "", err
	}
	line, err := fr.rd.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 4 || line[0] != '$' || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return "", errors.New("invalid message")
	}
	n, err := strconv.ParseUint(string(line[1:len(line)-2]), 10, 64)
	if err != nil {
		return "", err
	}
	buf := make([]byte, int(n)+2)
	_, err = io.ReadFull(fr.rd, buf)
	if err != nil {
		return "", err
	}
	if buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return "", errors.New("invalid message")
	}
	js := buf[:len(buf)-2]
	var m interface{}
	if err := json.Unmarshal(js, &m); err != nil {
		return "", err
	}
	return string(js), nil
}

func (fr *fenceReader) receiveExpect(valex ...string) error {
	s, err := fr.receive()
	if err != nil {
		return err
	}
	for i := 0; i < len(valex); i += 2 {
		if gjson.Get(s, valex[i]).String() != valex[i+1] {
			return fmt.Errorf("expected '%s'='%s', got '%s'", valex[i], valex[i+1], gjson.Get(s, valex[i]).String())
		}
	}
	return nil
}

func fence_basic_test(mc *mockServer) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "NEARBY mykey FENCE POINT 33 -115 5000\r\n")
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	res := string(buf[:n])
	if res != "+OK\r\n" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}

	// send a point
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer c.Close()

	res, err = redis.String(c.Do("SET", "mykey", "myid1", "POINT", 33, -115))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "enter",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,33]"); err != nil {
		return err
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,33]"); err != nil {
		return err
	}

	res, err = redis.String(c.Do("SET", "mykey", "myid1", "POINT", 34, -115))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "exit",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,34]"); err != nil {
		return err
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "outside",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,34]"); err != nil {
		return err
	}
	return nil
}

func fence_channel_message_order_test(mc *mockServer) error {
	// Create a channel to store the goroutines error
	finalErr := make(chan error)

	var ready atomic.Bool
	// Concurrently subscribe for notifications
	go func() {
		// Create the subscription connection to Tile38 to subscribe for updates
		sc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
		if err != nil {
			log.Println(err)
			return
		}
		defer sc.Close()

		// Subscribe the subscription client to the * pattern
		psc := redis.PubSubConn{Conn: sc}
		if err := psc.PSubscribe("*"); err != nil {
			log.Println(err)
			return
		}
		var msgs []string
		// While not a permanent error on the connection.
	loop:
		for sc.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Message:
				if v.Channel == "status" && string(v.Data) == "ready" {
					ready.Store(true)
				} else {
					msgs = append(msgs, string(v.Data))
					if len(msgs) == 8 {
						break loop
					}
				}
			case error:
				fmt.Printf("%s\n", err.Error())
			}
		}
		// Verify all messages
		correctOrder := []string{"exit:A", "exit:B", "outside:A", "outside:B", "enter:C", "enter:D", "inside:C", "inside:D"}
		for i := range msgs {
			if gjson.Get(msgs[i], "detect").String()+":"+
				gjson.Get(msgs[i], "hook").String() != correctOrder[i] {
				finalErr <- errors.New("INVALID MESSAGE ORDER")
			}
		}
		finalErr <- nil
	}()
	// Create the base connection for setting up points and geofences
	bc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer bc.Close()

	for !ready.Load() {
		if _, err := do(bc, "PUBLISH status ready"); err != nil {
			return err
		}
	}

	// Fire all setup commands on the base client
	for _, cmd := range []string{
		"SET points point POINT 33.412529053733444 -111.93368911743164",
		`SETCHAN A WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.95205688476562,33.400491820565236],[-111.92630767822266,33.400491820565236],[-111.92630767822266,33.422272258866045],[-111.95205688476562,33.422272258866045],[-111.95205688476562,33.400491820565236]]]}`,
		`SETCHAN B WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.93952560424803,33.403501285221594],[-111.92630767822266,33.403501285221594],[-111.92630767822266,33.41997983836345],[-111.93952560424803,33.41997983836345],[-111.93952560424803,33.403501285221594]]]}`,
		`SETCHAN C WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.9255781173706,33.40342963251261],[-111.91201686859131,33.40342963251261],[-111.91201686859131,33.41994401881284],[-111.9255781173706,33.41994401881284],[-111.9255781173706,33.40342963251261]]]}`,
		`SETCHAN D WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.92562103271484,33.40063513076968],[-111.90021514892578,33.40063513076968],[-111.90021514892578,33.42212898435788],[-111.92562103271484,33.42212898435788],[-111.92562103271484,33.40063513076968]]]}`,
		"SET points point POINT 33.412529053733444 -111.91909790039062",
	} {
		if _, err := do(bc, cmd); err != nil {
			return err
		}
	}
	return <-finalErr
}

func fence_detect_inside_test(mc *mockServer) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "WITHIN users FENCE DETECT inside,outside POINTS BOUNDS 33.618824 -84.457973 33.654359 -84.399859\r\n")
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	res := string(buf[:n])
	if res != "+OK\r\n" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}

	// send a point
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer c.Close()

	res, err = redis.String(c.Do("SET", "users", "200", "POINT", "33.642301", "-84.43118"))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "users",
		"id", "200",
		"point", `{"lat":33.642301,"lon":-84.43118}`); err != nil {
		return err
	}

	res, err = redis.String(c.Do("SET", "users", "200", "POINT", "34.642301", "-84.43118"))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "outside",
		"key", "users",
		"id", "200",
		"point", `{"lat":34.642301,"lon":-84.43118}`); err != nil {
		return err
	}
	return nil
}

// do performs the passed command on the passed redis client
func do(c redis.Conn, cmd string) (interface{}, error) {
	// Split out all parameters
	params := strings.Split(cmd, " ")

	// Produce a slice of interfaces for use in the arguments
	var args []interface{}
	for _, p := range params[1:] {
		args = append(args, p)
	}

	// Perform the request and return the response
	return c.Do(params[0], args...)
}

func fence_channel_meta_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SETCHAN", "carbon", "NEARBY", "x", "MATCH", "carbon*", "FENCE", "NODWELL", "points", "ROAM", "x", "*", "200000"}, {"1"},
		{"OUTPUT", "json"}, {`{"ok":true}`},
		// check for valid json on the chans command
		{"CHANS", "*"}, {
			func(v interface{}) (resp, expect interface{}) {
				// v is the value as strings or slices of strings
				// test will pass as long as `resp` and `expect` are the same.
				if !json.Valid([]byte(v.(string))) {
					return v, "Valid JSON"
				}
				return true, true
			},
		},
	})
}

func dialTile38(port int) (redis.Conn, error) {
	conn, err := redis.Dial("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	if _, err := conn.Do("OUTPUT", "json"); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func doTile38(c redis.Conn, cmd string, args ...interface{}) (string, error) {
	js, err := redis.String(c.Do(cmd, args...))
	if !gjson.Get(js, "ok").Bool() {
		return "", errors.New(gjson.Get(js, "err").String())
	}
	return js, err
}

func fence_eecio_test(mc *mockServer) error {
	// simulates issue #578
	var wg sync.WaitGroup
	wg.Add(3)
	ch := make(chan bool)
	var err1, err2, err3 error
	var msgs1, msgs2 []string
	// terminal 1
	go func() {
		defer wg.Done()
		err1 = func() error {
			conn, err := dialTile38(mc.port)
			if err != nil {
				return err
			}
			defer conn.Close()
			_, err = doTile38(conn,
				"SETCHAN", "test-eec", "NEARBY", "fleet",
				"FENCE", "DETECT", "enter,exit,cross",
				"POINT", "10.000", "10.000", "10000")
			if err != nil {
				return err
			}
			_, err = doTile38(conn, "SUBSCRIBE", "test-eec")
			if err != nil {
				return err
			}
			ch <- true
			for {
				js, err := redis.String(conn.Receive())
				if err != nil {
					return err
				}
				if js == `"DONE"` {
					break
				}
				msgs1 = append(msgs1, js)
			}
			return nil
		}()
	}()
	// terminal 2
	go func() {
		defer wg.Done()
		err2 = func() error {
			conn, err := dialTile38(mc.port)
			if err != nil {
				return err
			}
			defer conn.Close()
			_, err = doTile38(conn,
				"SETCHAN", "test-eecio", "NEARBY", "fleet",
				"FENCE", "DETECT", "enter,exit,cross,inside,outside",
				"POINT", "10.000", "10.000", "10000")
			if err != nil {
				return err
			}
			_, err = doTile38(conn, "SUBSCRIBE", "test-eecio")
			if err != nil {
				return err
			}
			ch <- true
			for {
				js, err := redis.String(conn.Receive())
				if err != nil {
					return err
				}
				if js == `"DONE"` {
					break
				}
				msgs2 = append(msgs2, js)
			}
			return nil
		}()
	}()
	// terminal 3
	var ok bool
	go func() {
		defer wg.Done()
		err3 = func() error {
			<-ch // terminal 1
			<-ch // terminal 2
			conn, err := dialTile38(mc.port)
			if err != nil {
				return err
			}
			defer conn.Close()
			if _, err = doTile38(conn,
				"SET", "fleet", "vehicle_1",
				"POINT", "10.0", "10.0"); err != nil {
				return err
			}
			if _, err = doTile38(conn,
				"SET", "fleet", "vehicle_1",
				"POINT", "0.0", "0.0"); err != nil {
				return err
			}
			if _, err = doTile38(conn,
				"SET", "fleet", "vehicle_1",
				"POINT", "20.0", "20.0"); err != nil {
				return err
			}
			if _, err = doTile38(conn, "PUBLISH", "test-eecio",
				"DONE"); err != nil {
				return err
			}
			if _, err = doTile38(conn, "PUBLISH", "test-eec",
				"DONE"); err != nil {
				return err
			}
			ok = true
			return nil
		}()
	}()
	var timeok atomic.Bool
	go func() {
		time.Sleep(time.Second * 30)
		if !timeok.Load() {
			panic("timeout")
		}
	}()
	wg.Wait()
	timeok.Store(true)
	if err3 != nil {
		return err3
	}
	if !ok {
		if err2 != nil {
			return err2
		}
		if err1 != nil {
			return err1
		}
	}
	var detects []string
	for i := 0; i < len(msgs1); i++ {
		detects = append(detects, gjson.Get(msgs1[i], "detect").String())
	}
	if strings.Join(detects, ",") != "enter,exit,cross" {
		errmsg := fmt.Sprintf("expected 'enter,exit,cross', got '%s'\n",
			strings.Join(detects, ","))
		return errors.New(errmsg)
	}
	detects = nil
	for i := 0; i < len(msgs2); i++ {
		detects = append(detects, gjson.Get(msgs2[i], "detect").String())
	}

	if strings.Join(detects, ",") != "enter,inside,exit,outside,cross,outside" {
		errmsg := fmt.Sprintf(
			"expected 'enter,inside,exit,outside,cross,outside', got '%s'\n",
			strings.Join(detects, ","))
		return errors.New(errmsg)
	}

	return nil
}

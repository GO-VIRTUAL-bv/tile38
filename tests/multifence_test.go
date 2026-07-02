package tests

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestMultiFence(g *testGroup) {
	// Grid fences (live connections)
	g.regSubTest("grid quadkey", multifence_grid_quadkey_test)
	g.regSubTest("grid tile", multifence_grid_tile_test)
	g.regSubTest("grid geohash", multifence_grid_geohash_test)
	g.regSubTest("grid detect inside", multifence_grid_inside_test)
	g.regSubTest("grid polygon coverage", multifence_grid_polygon_test)

	// Collection fences (channel + hooksMulti path)
	g.regSubTest("collection channel", multifence_coll_channel_test)

	// Validation
	g.regSubTest("validation", multifence_validation_test)
}

// openLiveFence opens a live fence on a fresh connection and returns a reader
// for the streamed messages, plus a separate client for issuing writes.
func openLiveFence(mc *mockServer, fenceCmd string) (*fenceReader, redis.Conn, net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return nil, nil, nil, err
	}
	if _, err = fmt.Fprintf(conn, "%s\r\n", fenceCmd); err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	if res := string(buf[:n]); res != "+OK\r\n" {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	return rd, c, conn, nil
}

func multifence_grid_quadkey_test(mc *mockServer) error {
	rd, c, conn, err := openLiveFence(mc,
		"WITHIN fleet FENCE DETECT enter,exit GRID QUADKEY 12")
	if err != nil {
		return err
	}
	defer conn.Close()
	defer c.Close()

	// enter cell A
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 33.5, -112.0)); err != nil {
		return err
	}
	if err := rd.receiveExpect("command", "set",
		"detect", "enter",
		"key", "fleet",
		"id", "truck1",
		"fence.type", "grid",
		"fence.system", "quadkey",
		"fence.id", "023102202123"); err != nil {
		return err
	}

	// move to cell B: exit A then enter B
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 40.0, -74.0)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "exit",
		"fence.system", "quadkey",
		"fence.id", "023102202123"); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "enter",
		"fence.system", "quadkey",
		"fence.id", "032010112330"); err != nil {
		return err
	}
	return nil
}

func multifence_grid_tile_test(mc *mockServer) error {
	rd, c, conn, err := openLiveFence(mc,
		"WITHIN fleet FENCE DETECT enter,exit GRID TILE 12")
	if err != nil {
		return err
	}
	defer conn.Close()
	defer c.Close()

	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 33.5, -112.0)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "enter",
		"fence.type", "grid",
		"fence.system", "tile",
		"fence.id", "12/773/1643"); err != nil {
		return err
	}
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 40.0, -74.0)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "exit", "fence.id", "12/773/1643"); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "enter", "fence.id", "12/1206/1550"); err != nil {
		return err
	}
	return nil
}

func multifence_grid_geohash_test(mc *mockServer) error {
	rd, c, conn, err := openLiveFence(mc,
		"WITHIN fleet FENCE DETECT enter,exit GRID GEOHASH 6")
	if err != nil {
		return err
	}
	defer conn.Close()
	defer c.Close()

	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 33.5, -112.0)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "enter",
		"fence.type", "grid",
		"fence.system", "geohash",
		"fence.id", "9tbqe6"); err != nil {
		return err
	}
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 40.0, -74.0)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "exit", "fence.id", "9tbqe6"); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "enter", "fence.id", "dr57s1"); err != nil {
		return err
	}
	return nil
}

func multifence_grid_inside_test(mc *mockServer) error {
	rd, c, conn, err := openLiveFence(mc,
		"WITHIN fleet FENCE DETECT inside GRID QUADKEY 12")
	if err != nil {
		return err
	}
	defer conn.Close()
	defer c.Close()

	// first set: enter falls back to inside
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 33.5, -112.0)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "inside",
		"fence.system", "quadkey",
		"fence.id", "023102202123"); err != nil {
		return err
	}
	// move within the same cell: continuous inside
	if _, err := redis.String(c.Do("SET", "fleet", "truck1", "POINT", 33.5003, -111.9997)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "inside",
		"fence.system", "quadkey",
		"fence.id", "023102202123"); err != nil {
		return err
	}
	return nil
}

// multifence_grid_polygon_test verifies that a polygon spanning multiple grid
// cells fires per covered cell (not just the centroid cell).
func multifence_grid_polygon_test(mc *mockServer) error {
	rd, c, conn, err := openLiveFence(mc,
		"INTERSECTS fleet FENCE DETECT enter,exit GRID QUADKEY 9")
	if err != nil {
		return err
	}
	defer conn.Close()
	defer c.Close()

	// a polygon straddling two quadkey-9 cells: 023102202 and 023102203
	poly := `{"type":"Polygon","coordinates":[[[-112.1,33.3],[-111.7,33.3],[-111.7,33.7],[-112.1,33.7],[-112.1,33.3]]]}`
	if _, err := redis.String(c.Do("SET", "fleet", "zone1", "OBJECT", poly)); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "enter",
		"fence.system", "quadkey",
		"fence.id", "023102202"); err != nil {
		return err
	}
	if err := rd.receiveExpect("detect", "enter",
		"fence.system", "quadkey",
		"fence.id", "023102203"); err != nil {
		return err
	}
	return nil
}

func multifence_coll_channel_test(mc *mockServer) error {
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
		if err := psc.Subscribe("mfchan"); err != nil {
			finalErr <- err
			return
		}
		var msgs []string
		for sc.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Subscription:
				ready.Store(true)
			case redis.Message:
				msgs = append(msgs, string(v.Data))
				if len(msgs) == 3 {
					finalErr <- verifyCollMsgs(msgs)
					return
				}
			case error:
				finalErr <- v
				return
			}
		}
		finalErr <- sc.Err()
	}()

	bc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer bc.Close()

	// two disjoint zones
	if _, err := do(bc, "SET zones zoneA BOUNDS 33.4 -112.1 33.6 -111.9"); err != nil {
		return err
	}
	if _, err := do(bc, "SET zones zoneB BOUNDS 39.9 -74.1 40.1 -73.9"); err != nil {
		return err
	}
	// multi-fence channel over the whole zones collection
	if _, err := do(bc, "SETCHAN mfchan INTERSECTS fleet FENCE DETECT enter,exit COLL zones"); err != nil {
		return err
	}

	// wait for the subscription to become active
	for i := 0; !ready.Load(); i++ {
		if i > 200 {
			return fmt.Errorf("timed out waiting for subscription")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := do(bc, "SET fleet truck POINT 33.5 -112.0"); err != nil {
		return err
	}
	if _, err := do(bc, "SET fleet truck POINT 40.0 -74.0"); err != nil {
		return err
	}
	return <-finalErr
}

// verifyCollMsgs checks the ordered collection-fence messages: enter zoneA,
// then (after moving) exit zoneA and enter zoneB.
func verifyCollMsgs(msgs []string) error {
	want := []struct{ detect, id string }{
		{"enter", "zoneA"},
		{"exit", "zoneA"},
		{"enter", "zoneB"},
	}
	for i, w := range want {
		if d := gjson.Get(msgs[i], "detect").String(); d != w.detect {
			return fmt.Errorf("msg %d: expected detect '%s', got '%s' (%s)",
				i, w.detect, d, msgs[i])
		}
		if typ := gjson.Get(msgs[i], "fence.type").String(); typ != "collection" {
			return fmt.Errorf("msg %d: expected fence.type 'collection', got '%s'", i, typ)
		}
		if key := gjson.Get(msgs[i], "fence.key").String(); key != "zones" {
			return fmt.Errorf("msg %d: expected fence.key 'zones', got '%s'", i, key)
		}
		if id := gjson.Get(msgs[i], "fence.id").String(); id != w.id {
			return fmt.Errorf("msg %d: expected fence.id '%s', got '%s'", i, w.id, id)
		}
	}
	return nil
}

func multifence_validation_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SETCHAN", "c1", "WITHIN", "fleet", "FENCE", "GRID", "QUADKEY", "0"}, {"ERR invalid argument '0'"},
		{"SETCHAN", "c2", "WITHIN", "fleet", "FENCE", "GRID", "GEOHASH", "13"}, {"ERR invalid argument '13'"},
		{"SETCHAN", "c3", "WITHIN", "fleet", "FENCE", "GRID", "TILE", "24"}, {"ERR invalid argument '24'"},
		{"SETCHAN", "c4", "WITHIN", "fleet", "FENCE", "GRID", "FOO", "5"}, {"ERR invalid argument 'foo'"},
		{"WITHIN", "fleet", "GRID", "QUADKEY", "12"}, {"ERR grid is only supported with fence"},
	})
}

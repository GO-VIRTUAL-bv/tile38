package server

import (
	"strconv"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/tile38/internal/bing"
	"github.com/tidwall/tile38/internal/object"
)

// multiSwitches configures a multi-fence: a fence whose area is a *set* of
// cells/zones resolved dynamically at match time rather than a single static
// geometry. A single hook using a multi-fence fires per cell/zone with an
// enter/exit detect and a cell identifier.
type multiSwitches struct {
	kind string     // "grid" | "collection"
	grid gridSystem // non-nil when kind=="grid"
	key  string     // collection key when kind=="collection"
	// tokens holds the canonical area tokens (e.g. ["GRID","QUADKEY","12"] or
	// ["COLL","regions"]) so HOOKS output and AOF round-trip stay faithful.
	tokens []string
}

// cell is one resolved fence element with a stable identifier. For grids the id
// is the cell address (quadkey string, "z/x/y" tile, or geohash); for
// collections it is the zone object's id.
type cell struct {
	id  string
	obj geojson.Object
}

// maxGridCells caps the number of candidate cells enumerated for a single
// object, guarding against a huge geometry over a fine grid.
const maxGridCells = 4096

// gridSystem is the pluggable deterministic-tiling interface. Additional cell
// systems (e.g. A5 pentagonal DGGS) plug in here by implementing this interface
// and adding a case to newGridSystem — no changes to the match engine.
type gridSystem interface {
	// name returns the canonical system token, e.g. "quadkey".
	name() string
	// level returns the configured resolution.
	level() int
	// cellsCovering returns every cell whose bounds overlap the given rectangle
	// (the object's bounding box). The caller applies the within/intersects
	// predicate against each returned cell's geometry, so a point yields a
	// single cell while a polygon yields every cell it spans.
	cellsCovering(rect geometry.Rect) []cell
}

// newGridSystem builds a gridSystem for the given system name and level,
// validating the level against each system's supported range.
func newGridSystem(system string, level int) (gridSystem, error) {
	switch system {
	case "quadkey":
		if level < 1 || level > 23 {
			return nil, errInvalidArgument(strconv.Itoa(level))
		}
		return &quadkeyGrid{lvl: level}, nil
	case "tile":
		if level < 0 || level > 23 {
			return nil, errInvalidArgument(strconv.Itoa(level))
		}
		return &tileGrid{z: level}, nil
	case "geohash":
		if level < 1 || level > 12 {
			return nil, errInvalidArgument(strconv.Itoa(level))
		}
		return &geohashGrid{p: level}, nil
	default:
		return nil, errInvalidArgument(system)
	}
}

func rectObj(minLat, minLon, maxLat, maxLon float64) geojson.Object {
	return geojson.NewRect(geometry.Rect{
		Min: geometry.Point{X: minLon, Y: minLat},
		Max: geometry.Point{X: maxLon, Y: maxLat},
	})
}

// tileRange returns the inclusive tile index range covering rect at the given
// level of detail.
func tileRange(rect geometry.Rect, lod uint64) (txMin, tyMin, txMax, tyMax int64) {
	// higher latitude -> smaller pixel/tile Y, so the top edge (maxLat) gives
	// the min tile Y and the bottom edge (minLat) gives the max tile Y.
	px0, py0 := bing.LatLongToPixelXY(rect.Max.Y, rect.Min.X, lod)
	txMin, tyMin = bing.PixelXYToTileXY(px0, py0)
	px1, py1 := bing.LatLongToPixelXY(rect.Min.Y, rect.Max.X, lod)
	txMax, tyMax = bing.PixelXYToTileXY(px1, py1)
	if txMax < txMin {
		txMin, txMax = txMax, txMin
	}
	if tyMax < tyMin {
		tyMin, tyMax = tyMax, tyMin
	}
	return
}

type quadkeyGrid struct{ lvl int }

func (g *quadkeyGrid) name() string { return "quadkey" }
func (g *quadkeyGrid) level() int   { return g.lvl }
func (g *quadkeyGrid) cellsCovering(rect geometry.Rect) []cell {
	txMin, tyMin, txMax, tyMax := tileRange(rect, uint64(g.lvl))
	var cells []cell
	for ty := tyMin; ty <= tyMax; ty++ {
		for tx := txMin; tx <= txMax; tx++ {
			qk := bing.TileXYToQuadKey(tx, ty, uint64(g.lvl))
			minLat, minLon, maxLat, maxLon, _ := bing.QuadKeyToBounds(qk)
			cells = append(cells, cell{id: qk, obj: rectObj(minLat, minLon, maxLat, maxLon)})
			if len(cells) >= maxGridCells {
				return cells
			}
		}
	}
	return cells
}

type tileGrid struct{ z int }

func (g *tileGrid) name() string { return "tile" }
func (g *tileGrid) level() int   { return g.z }
func (g *tileGrid) cellsCovering(rect geometry.Rect) []cell {
	txMin, tyMin, txMax, tyMax := tileRange(rect, uint64(g.z))
	var cells []cell
	for ty := tyMin; ty <= tyMax; ty++ {
		for tx := txMin; tx <= txMax; tx++ {
			minLat, minLon, maxLat, maxLon := bing.TileXYToBounds(tx, ty, uint64(g.z))
			id := strconv.Itoa(g.z) + "/" +
				strconv.FormatInt(tx, 10) + "/" + strconv.FormatInt(ty, 10)
			cells = append(cells, cell{id: id, obj: rectObj(minLat, minLon, maxLat, maxLon)})
			if len(cells) >= maxGridCells {
				return cells
			}
		}
	}
	return cells
}

type geohashGrid struct{ p int }

func (g *geohashGrid) name() string { return "geohash" }
func (g *geohashGrid) level() int   { return g.p }
func (g *geohashGrid) cellsCovering(rect geometry.Rect) []cell {
	// Geohash cells at a fixed precision form a regular lat/lon grid. Anchor on
	// the SW corner's cell and step by its dimensions, sampling cell centers.
	anchor := geohash.BoundingBox(
		geohash.EncodeWithPrecision(rect.Min.Y, rect.Min.X, uint(g.p)))
	cellH := anchor.MaxLat - anchor.MinLat
	cellW := anchor.MaxLng - anchor.MinLng
	if cellH <= 0 || cellW <= 0 {
		return nil
	}
	seen := make(map[string]bool)
	var cells []cell
	for lat := anchor.MinLat + cellH/2; lat <= rect.Max.Y+cellH/2; lat += cellH {
		for lon := anchor.MinLng + cellW/2; lon <= rect.Max.X+cellW/2; lon += cellW {
			hash := geohash.EncodeWithPrecision(lat, lon, uint(g.p))
			if seen[hash] {
				continue
			}
			seen[hash] = true
			box := geohash.BoundingBox(hash)
			cells = append(cells, cell{id: hash,
				obj: rectObj(box.MinLat, box.MinLng, box.MaxLat, box.MaxLng)})
			if len(cells) >= maxGridCells {
				return cells
			}
		}
	}
	return cells
}

// resolveCells returns the set of cells/zones that the object occupies. Grid
// cells come from the pluggable gridSystem; collection cells are the zone
// objects (looked up at match time) that the object matches under fence.cmd.
func resolveCells(s *Server, fence *liveFenceSwitches, o *object.Object) []cell {
	if o == nil {
		return nil
	}
	g := o.Geo()
	if g == nil {
		return nil
	}
	switch fence.multi.kind {
	case "grid":
		var cells []cell
		for _, c := range fence.multi.grid.cellsCovering(o.Rect()) {
			var hit bool
			if fence.cmd == "within" {
				hit = g.Within(c.obj)
			} else {
				hit = g.Intersects(c.obj)
			}
			if hit {
				cells = append(cells, c)
			}
		}
		return cells
	case "collection":
		col, _ := s.cols.Get(fence.multi.key)
		if col == nil {
			return nil
		}
		var cells []cell
		col.Intersects(geojson.NewRect(o.Rect()), 0, nil, nil,
			func(z *object.Object) bool {
				zg := z.Geo()
				var hit bool
				if fence.cmd == "within" {
					hit = g.Within(zg)
				} else {
					hit = g.Intersects(zg)
				}
				if hit {
					cells = append(cells, cell{id: z.ID(), obj: zg})
				}
				return true
			},
		)
		return cells
	}
	return nil
}

// multiFenceMatch computes per-cell enter/exit for a multi-fence hook. Because
// commandDetails carries both the old and new object positions, the cell
// membership diff is computed statelessly: cells present at the new position but
// not the old fire "enter", cells present at both fire "inside" (dwell), and
// cells present at the old position but not the new fire "exit".
func multiFenceMatch(hookName string, sw *scanWriter, fence *liveFenceSwitches,
	metas []FenceMeta, details *commandDetails,
) []string {
	// The WHERE clause / glob are applied per object version. A failing test at
	// a position drops all of that position's cells (mirrors single-fence
	// match1/match2 gating in fenceMatch).
	var newCells, oldCells []cell
	if ok, _, _ := sw.testObject(details.obj); ok {
		newCells = resolveCells(sw.s, fence, details.obj)
	}
	if details.old != nil {
		if ok, _, _ := sw.testObject(details.old); ok {
			oldCells = resolveCells(sw.s, fence, details.old)
		}
	}
	if len(newCells) == 0 && len(oldCells) == 0 {
		return nil
	}

	oldSet := make(map[string]bool, len(oldCells))
	for _, c := range oldCells {
		oldSet[c.id] = true
	}
	newSet := make(map[string]bool, len(newCells))
	for _, c := range newCells {
		newSet[c.id] = true
	}

	res, ok := renderFenceObject(sw, details)
	if !ok {
		return nil
	}

	var msgs []string
	// exits first (cells left behind), then enters/dwells — this matches the
	// exit→enter ordering that channels/webhooks impose via sortMsgs and reads
	// naturally on the unsorted live-fence path.
	for _, c := range oldCells {
		if newSet[c.id] {
			continue
		}
		msgs = append(msgs,
			cellMessages(sw, fence, hookName, metas, details, res, "exit", c)...)
	}
	for _, c := range newCells {
		detect := "enter"
		if oldSet[c.id] {
			detect = "inside"
		}
		if details.command == "fset" {
			detect = "inside"
		}
		msgs = append(msgs,
			cellMessages(sw, fence, hookName, metas, details, res, detect, c)...)
	}
	return msgs
}

// renderFenceObject renders the current object into the message tail, reusing
// the same scanWriter path as single-geometry fences. It returns the rendered
// object JSON (beginning with '{') and false when there is nothing to write.
func renderFenceObject(sw *scanWriter, details *commandDetails) (string, bool) {
	sw.fullFields = true
	sw.msg.OutputType = JSON
	sw.writeObject(ScanWriterParams{obj: details.obj, noTest: true})
	if sw.wr.Len() == 0 {
		return "", false
	}
	res := sw.wr.String()
	sw.wr.Reset()
	if len(res) > 0 && res[0] == ',' {
		res = res[1:]
	}
	if sw.output == outputIDs {
		res = `{"id":` + res + `}`
	}
	if len(res) == 0 || res[0] != '{' {
		return "", false
	}
	return res, true
}

// cellMessages builds the fence messages for a single cell, applying the same
// DETECT normalization and companion (enter→inside, exit→outside) behavior as
// single-geometry fences, and appends the cell identifier to each message.
func cellMessages(sw *scanWriter, fence *liveFenceSwitches, hookName string,
	metas []FenceMeta, details *commandDetails, res, detect string, c cell,
) []string {
	// normalize the primary detect against the requested DETECT set
	d := detect
	for {
		if fence.detect != nil && !fence.detect[d] {
			if d == "enter" {
				d = "inside"
				continue
			}
			if d == "exit" {
				d = "outside"
				continue
			}
			return nil
		}
		break
	}

	group := groupForCell(sw.s, hookName, details.key, details.obj.ID(), c.id, detect)
	tail := res[1:]

	var out []string
	if fence.detect == nil || fence.detect[d] {
		out = append(out, appendCellField(makemsg(details.command, group, d,
			hookName, metas, details.key, details.timestamp, tail), fence, c))
	}
	switch d {
	case "enter":
		if fence.detect == nil || fence.detect["inside"] {
			out = append(out, appendCellField(makemsg(details.command, group,
				"inside", hookName, metas, details.key, details.timestamp, tail),
				fence, c))
		}
	case "exit":
		if fence.detect == nil || fence.detect["outside"] {
			out = append(out, appendCellField(makemsg(details.command, group,
				"outside", hookName, metas, details.key, details.timestamp, tail),
				fence, c))
		}
	}
	return out
}

// groupForCell returns the correlation group id for an (object, cell) pair. The
// cell id is folded into the group object key so enter/inside/exit for the same
// cell share a group, while different cells stay independent.
func groupForCell(s *Server, hookName, key, objID, cellID, detect string) string {
	gid := objID + "\x00" + cellID
	if detect == "enter" {
		return s.groupConnect(hookName, key, gid)
	}
	group := s.groupGet(hookName, key, gid)
	if group == "" {
		group = s.groupConnect(hookName, key, gid)
	}
	return group
}

// appendCellField splices a "fence" object identifying the cell/zone into a
// message built by makemsg. The field is only ever present on multi-fence
// messages, keeping the format backward compatible for existing hooks.
func appendCellField(msg string, fence *liveFenceSwitches, c cell) string {
	b := []byte(msg[:len(msg)-1]) // hack off the trailing '}'
	b = append(b, `,"fence":{"type":`...)
	if fence.multi.kind == "grid" {
		b = appendJSONString(b, "grid")
		b = append(b, `,"system":`...)
		b = appendJSONString(b, fence.multi.grid.name())
		b = append(b, `,"id":`...)
		b = appendJSONString(b, c.id)
	} else {
		b = appendJSONString(b, "collection")
		b = append(b, `,"key":`...)
		b = appendJSONString(b, fence.multi.key)
		b = append(b, `,"id":`...)
		b = appendJSONString(b, c.id)
	}
	b = append(b, '}')
	b = append(b, '}') // re-add the trailing '}'
	return string(b)
}

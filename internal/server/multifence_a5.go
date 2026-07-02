package server

import (
	"strconv"

	a5 "github.com/a5geo/a5-go"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

// a5Grid is the A5 pentagonal-DGGS gridSystem. It reuses the a5-go bridge
// helpers in a5.go (a5EncodePoint / a5CellPolygon / a5ValidResolution) so the
// multi-fence engine treats A5 exactly like the quadkey/tile/geohash grids.
type a5Grid struct{ res int }

func (g *a5Grid) name() string { return "a5" }
func (g *a5Grid) level() int   { return g.res }

// cellsCovering returns every A5 cell at the configured resolution whose
// pentagon overlaps rect. A point (degenerate rect) is encoded directly; an
// area is resolved by descending the A5 hierarchy from the res-0 cells,
// pruning any branch that does not intersect the query rectangle.
func (g *a5Grid) cellsCovering(rect geometry.Rect) []cell {
	// Point: encode directly (matches the a5EncodePoint convention).
	if rect.Min == rect.Max {
		cellID, err := a5.LonLatToCell(a5.LonLat{rect.Min.X, rect.Min.Y}, g.res)
		if err != nil {
			return nil
		}
		poly, err := a5CellPolygon(cellID)
		if err != nil {
			return nil
		}
		return []cell{{id: strconv.FormatUint(cellID, 16), obj: poly}}
	}

	// Area: hierarchical descent with intersection pruning.
	res0, err := a5.GetRes0Cells()
	if err != nil {
		return nil
	}
	rectGeo := geojson.NewRect(rect)
	var cells []cell
	var descend func(cellID uint64)
	descend = func(cellID uint64) {
		if len(cells) >= maxGridCells {
			return
		}
		poly, err := a5CellPolygon(cellID)
		if err != nil {
			return
		}
		if !poly.Intersects(rectGeo) {
			return
		}
		res := a5.GetResolution(cellID)
		if res >= g.res {
			cells = append(cells, cell{id: strconv.FormatUint(cellID, 16), obj: poly})
			return
		}
		children, err := a5.CellToChildren(cellID, res+1)
		if err != nil {
			return
		}
		for _, ch := range children {
			descend(ch)
		}
	}
	for _, c := range res0 {
		descend(c)
	}
	return cells
}

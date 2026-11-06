package cluster

// hostLocation is a host's datacenter and rack as the plan saw them.
type hostLocation struct{ dc, rack string }

func (l hostLocation) String() string { return l.dc + "/" + l.rack }

// preferenceCovers reports whether loc is inside the reported preference. A
// report with no preference covers everything, since it constrains nothing.
func preferenceCovers(preference any, loc hostLocation) bool {
	switch p := preference.(type) {
	case InternalNodeLocationDCReport:
		return loc.dc == p.LocalDC
	case InternalNodeLocationRackReport:
		return loc.dc == p.LocalDC && loc.rack == p.LocalRack
	default:
		return true
	}
}

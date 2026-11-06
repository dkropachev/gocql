//go:build unit
// +build unit

package gocql

// typeOption writes a CQL [option] type description the way a server does.
type typeOption func(f *framer)

func optCustom(class string) typeOption {
	return func(f *framer) {
		f.WriteShort(uint16(TypeCustom))
		f.WriteString(class)
	}
}

func optTuple(elems ...typeOption) typeOption {
	return func(f *framer) {
		f.WriteShort(uint16(TypeTuple))
		f.WriteShort(uint16(len(elems)))
		for _, e := range elems {
			e(f)
		}
	}
}

type optField struct {
	name string
	typ  typeOption
}

func optUDT(keyspace, name string, fields ...optField) typeOption {
	return func(f *framer) {
		f.WriteShort(uint16(TypeUDT))
		f.WriteString(keyspace)
		f.WriteString(name)
		f.WriteShort(uint16(len(fields)))
		for _, fl := range fields {
			f.WriteString(fl.name)
			fl.typ(f)
		}
	}
}

func readTypeOption(opt typeOption) (TypeInfo, int) {
	f := &framer{Proto: protoVersion4}
	opt(f)
	info := f.ReadTypeInfo()
	return info, len(f.Buf)
}

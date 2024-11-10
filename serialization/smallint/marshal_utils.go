package smallint

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
)

var (
	maxBigInt = big.NewInt(math.MaxInt16)
	minBigInt = big.NewInt(math.MinInt16)
)

func EncInt8(v int8) ([]byte, error) {
	if v < 0 {
		return []byte{255, byte(v)}, nil
	}
	return []byte{0, byte(v)}, nil
}

func EncInt8R(v *int8) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncInt8(*v)
}

func EncInt16(v int16) ([]byte, error) {
	return encInt16(v), nil
}

func EncInt16R(v *int16) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncInt16(*v)
}

func EncInt32(v int32) ([]byte, error) {
	if v > math.MaxInt16 || v < math.MinInt16 {
		return nil, fmt.Errorf("failed to marshal smallint: value %#v out of range", v)
	}
	return []byte{byte(v >> 8), byte(v)}, nil
}

func EncInt32R(v *int32) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncInt32(*v)
}

func EncInt64(v int64) ([]byte, error) {
	if v > math.MaxInt16 || v < math.MinInt16 {
		return nil, fmt.Errorf("failed to marshal smallint: value %#v out of range", v)
	}
	return encInt64(v), nil
}

func EncInt64R(v *int64) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncInt64(*v)
}

func EncInt(v int) ([]byte, error) {
	if v > math.MaxInt16 || v < math.MinInt16 {
		return nil, fmt.Errorf("failed to marshal smallint: value %#v out of range", v)
	}
	return []byte{byte(v >> 8), byte(v)}, nil
}

func EncIntR(v *int) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncInt(*v)
}

func EncUint8(v uint8) ([]byte, error) {
	return []byte{0, v}, nil
}

func EncUint8R(v *uint8) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncUint8(*v)
}

func EncUint16(v uint16) ([]byte, error) {
	return []byte{byte(v >> 8), byte(v)}, nil
}

func EncUint16R(v *uint16) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncUint16(*v)
}

func EncUint32(v uint32) ([]byte, error) {
	if v > math.MaxUint16 {
		return nil, fmt.Errorf("failed to marshal smallint: value %#v out of range", v)
	}
	return []byte{byte(v >> 8), byte(v)}, nil
}

func EncUint32R(v *uint32) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncUint32(*v)
}

func EncUint64(v uint64) ([]byte, error) {
	if v > math.MaxUint16 {
		return nil, fmt.Errorf("failed to marshal smallint: value %#v out of range", v)
	}
	return encUint64(v), nil
}

func EncUint64R(v *uint64) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncUint64(*v)
}

func EncUint(v uint) ([]byte, error) {
	if v > math.MaxUint16 {
		return nil, fmt.Errorf("failed to marshal smallint: value %#v out of range", v)
	}
	return []byte{byte(v >> 8), byte(v)}, nil
}

func EncUintR(v *uint) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncUint(*v)
}

func EncBigInt(v big.Int) ([]byte, error) {
	if v.Cmp(maxBigInt) == 1 || v.Cmp(minBigInt) == -1 {
		return nil, fmt.Errorf("failed to marshal smallint: value (%T)(%s) out of range", v, v.String())
	}
	return encInt64(v.Int64()), nil
}

func EncBigIntR(v *big.Int) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	if v.Cmp(maxBigInt) == 1 || v.Cmp(minBigInt) == -1 {
		return nil, fmt.Errorf("failed to marshal smallint: value (%T)(%s) out of range", v, v.String())
	}
	return encInt64(v.Int64()), nil
}

func EncString(v string) ([]byte, error) {
	if v == "" {
		return nil, nil
	}

	n, err := strconv.ParseInt(v, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal smallint: can not marshal (%T)(%[1]v) %s", v, err)
	}
	return encInt64(n), nil
}

func EncStringR(v *string) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncString(*v)
}

func EncReflect(v reflect.Value) ([]byte, error) {
	switch v.Type().Kind() {
	case reflect.Int8:
		val := v.Int()
		if val < 0 {
			return []byte{255, byte(val)}, nil
		}
		return []byte{0, byte(val)}, nil
	case reflect.Int16:
		return encInt64(v.Int()), nil
	case reflect.Int, reflect.Int64, reflect.Int32:
		val := v.Int()
		if val > math.MaxInt16 || val < math.MinInt16 {
			return nil, fmt.Errorf("failed to marshal smallint: custom type value (%T)(%[1]v) out of range", v.Interface())
		}
		return encInt64(val), nil
	case reflect.Uint8:
		return []byte{0, byte(v.Uint())}, nil
	case reflect.Uint16:
		return encUint64(v.Uint()), nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32:
		val := v.Uint()
		if val > math.MaxUint16 {
			return nil, fmt.Errorf("failed to marshal smallint: custom type value (%T)(%[1]v) out of range", v.Interface())
		}
		return encUint64(val), nil
	case reflect.String:
		val := v.String()
		if val == "" {
			return nil, nil
		}

		n, err := strconv.ParseInt(val, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal smallint: can not marshal (%T)(%[1]v), %s", v.Interface(), err)
		}
		return encInt64(n), nil
	default:
		return nil, fmt.Errorf("failed to marshal smallint: unsupported value type (%T)(%[1]v)", v.Interface())
	}
}

func EncReflectR(v reflect.Value) ([]byte, error) {
	if v.IsNil() {
		return nil, nil
	}
	return EncReflect(v.Elem())
}

func encInt16(v int16) []byte {
	return []byte{byte(v >> 8), byte(v)}
}

func encInt64(v int64) []byte {
	return []byte{byte(v >> 8), byte(v)}
}

func encUint64(v uint64) []byte {
	return []byte{byte(v >> 8), byte(v)}
}
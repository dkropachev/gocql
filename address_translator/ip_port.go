package address_translator

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
)

type IPPort [18]byte

func FromIPPort(ip net.IP, port int) IPPort {
	var result IPPort
	copy(result[:16], ip.To16())
	binary.BigEndian.PutUint16(result[16:], uint16(port))
	return result
}

func ParseIPPort(s string) (IPPort, error) {
	var result IPPort
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return result, err
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return IPPort{}, fmt.Errorf("invalid IP format %s", host)
	}

	ip = ip.To16()

	copy(result[:16], ip)

	if portStr != "*" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return IPPort{}, fmt.Errorf("invalid port format %s", portStr)
		}
		binary.BigEndian.PutUint16(result[16:], uint16(port))
	}
	return result, nil
}

func (i IPPort) IP() net.IP {
	return i[:16]
}

func (i IPPort) Port() int {
	return int(binary.BigEndian.Uint16(i[16:]))
}

func ParseIPPortMust(s string) IPPort {
	result, err := ParseIPPort(s)
	if err != nil {
		panic(err)
	}
	return result
}

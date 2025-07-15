package address_translator

import (
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseTXTRecords(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expectErr  bool
		expectSize int
		validate   func(t *testing.T, mappings []Mapping)
	}{
		{
			name:       "ValidResolved",
			input:      "v=cqlpl 192.168.1.1:9042=10.0.0.1:9042",
			expectErr:  false,
			expectSize: 1,
			validate: func(t *testing.T, mappings []Mapping) {
				m := mappings[0]
				if !m.src.IP().Equal(net.ParseIP("192.168.1.1")) || m.src.Port() != 9042 {
					t.Errorf("unexpected src: %v", m.src)
				}
				dst := m.dst.Resolve(nil)
				if !dst.IP().Equal(net.ParseIP("10.0.0.1")) || dst.Port() != 9042 {
					t.Errorf("unexpected dst: %v", dst)
				}
			},
		},
		{
			name:       "ValidDNS",
			input:      "v=cqlpl 127.0.0.1:9042=localhost:9042",
			expectErr:  false,
			expectSize: 1,
			validate: func(t *testing.T, mappings []Mapping) {
				if _, ok := mappings[0].dst.(*DNSHostPort); !ok {
					t.Errorf("expected DNSHostPort, got %T", mappings[0].dst)
				}
			},
		},
		{
			name:       "MalformedRecord",
			input:      "v=cqlpl malformedrecord",
			expectErr:  true,
			expectSize: 0,
		},
		{
			name:       "BadSource",
			input:      "v=cqlpl badIP:9042=10.0.0.1:9042",
			expectErr:  true,
			expectSize: 0,
		},
		{
			name:       "BadDestination",
			input:      "v=cqlpl 192.168.1.1:9042=badDestPort",
			expectErr:  true,
			expectSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappings, err := parseTXTRecords(1*time.Minute, nil, tt.input)
			if tt.expectErr && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if len(mappings) != tt.expectSize {
				t.Errorf("expected %d mappings, got %d", tt.expectSize, len(mappings))
			}
			if tt.validate != nil && !tt.expectErr {
				tt.validate(t, mappings)
			}
		})
	}
}

// fakeDNSResolver implements DNSResolver for unit testing.
type fakeDNSResolver struct {
	lookupIPFunc  func(host string) ([]net.IP, error)
	lookupTXTFunc func(name string) ([]string, error)
}

func (f *fakeDNSResolver) LookupIP(host string) ([]net.IP, error) {
	return f.lookupIPFunc(host)
}

func (f *fakeDNSResolver) LookupTXT(name string) ([]string, error) {
	return f.lookupTXTFunc(name)
}

// dummy logger that does nothing
type dummyLogger struct{}

func (d dummyLogger) Print(v ...interface{})                 {}
func (d dummyLogger) Printf(format string, v ...interface{}) {}
func (d dummyLogger) Println(v ...interface{})               {}

func TestDNSBasedAddressTranslator(t *testing.T) {
	const ttl = time.Second * 5

	t.Run("TXT lookup error", func(t *testing.T) {
		d := NewDNSBasedAddressTranslator("example.com", ttl, dummyLogger{})
		d.dnsResolver = &fakeDNSResolver{
			lookupTXTFunc: func(name string) ([]string, error) {
				return nil, errors.New("DNS failure")
			},
		}
		err := d.UpdateRecords()
		if err == nil || !strings.Contains(err.Error(), "DNS failure") {
			t.Errorf("expected DNS failure, got %v", err)
		}
	})

	t.Run("Empty TXT records", func(t *testing.T) {
		d := NewDNSBasedAddressTranslator("example.com", ttl, dummyLogger{})
		d.dnsResolver = &fakeDNSResolver{
			lookupTXTFunc: func(name string) ([]string, error) {
				return []string{}, nil
			},
		}
		err := d.UpdateRecords()
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}
		if len(d.getCurrentMapping()) != 0 {
			t.Errorf("expected empty mapping")
		}
	})

	t.Run("Malformed TXT record", func(t *testing.T) {
		d := NewDNSBasedAddressTranslator("example.com", ttl, dummyLogger{})
		d.dnsResolver = &fakeDNSResolver{
			lookupTXTFunc: func(name string) ([]string, error) {
				return []string{"v=cqlpl not=a_valid_mapping"}, nil
			},
		}
		err := d.UpdateRecords()
		if err == nil || !strings.Contains(err.Error(), "failed to parse TXT records") {
			t.Errorf("expected parse error, got %v", err)
		}
	})

	t.Run("Valid static IP mapping", func(t *testing.T) {
		src := "1.2.3.4:9042"
		dst := "5.6.7.8:9042"

		d := NewDNSBasedAddressTranslator("example.com", ttl, dummyLogger{})
		d.dnsResolver = &fakeDNSResolver{
			lookupTXTFunc: func(name string) ([]string, error) {
				return []string{"v=cqlpl " + src + "=" + dst}, nil
			},
		}

		err := d.UpdateRecords()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ip := net.ParseIP("1.2.3.4")
		mappedIP, mappedPort := d.Translate(ip, 9042)
		expected := net.ParseIP("5.6.7.8")
		if !mappedIP.Equal(expected) || mappedPort != 9042 {
			t.Errorf("expected %s:9042, got %s:%d", expected, mappedIP, mappedPort)
		}
	})

	t.Run("Triggers update and respects TTL", func(t *testing.T) {
		src := "1.1.1.1:9042"
		dst := "2.2.2.2:9042"

		d := NewDNSBasedAddressTranslator("example.com", ttl, dummyLogger{})
		d.dnsResolver = &fakeDNSResolver{
			lookupTXTFunc: func(name string) ([]string, error) {
				return []string{"v=cqlpl " + src + "=" + dst}, nil
			},
		}

		ip, port := d.Translate(net.ParseIP("1.1.1.1"), 9042)
		if !ip.Equal(net.ParseIP("2.2.2.2")) || port != 9042 {
			t.Errorf("expected 2.2.2.2:9042, got %s:%d", ip, port)
		}

		dst = "3.3.3.3:9043"
		// Manually expire readTXTAfter to force update
		d.readTXTAfter = 0

		// This time it should provide old result, but start update process
		ip, port = d.Translate(net.ParseIP("1.1.1.1"), 9042)
		if !ip.Equal(net.ParseIP("2.2.2.2")) || port != 9042 {
			t.Errorf("expected 2.2.2.2:9042, got %s:%d", ip, port)
		}

		// Wait till update completes
		for atomic.LoadInt32(&d.pendingUpdate) != 0 {
			time.Sleep(time.Millisecond * 100)
		}

		// This time it should provide updated results
		ip, port = d.Translate(net.ParseIP("1.1.1.1"), 9042)
		if !ip.Equal(net.ParseIP("3.3.3.3")) || port != 9043 {
			t.Errorf("expected 3.3.3.3:9043, got %s:%d", ip, port)
		}

		dst = "4.4.4.4:9043"
		// Manually expire readTXTAfter to force update
		d.readTXTAfter = 0
		// Trigger update
		d.Translate(net.ParseIP("1.1.1.1"), 9042)

		// Wait till update completes
		for atomic.LoadInt32(&d.pendingUpdate) != 0 {
			time.Sleep(time.Millisecond * 100)
		}

		// Make sure that second update worked fine
		ip, port = d.Translate(net.ParseIP("1.1.1.1"), 9042)
		if !ip.Equal(net.ParseIP("4.4.4.4")) || port != 9043 {
			t.Errorf("expected 4.4.4.4:9043, got %s:%d", ip, port)
		}

	})
}

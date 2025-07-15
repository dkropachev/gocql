package address_translator

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type DNSResolver interface {
	LookupIP(host string) ([]net.IP, error)
	LookupTXT(name string) ([]string, error)
}

type dnsResolver struct {
	lookupIP  func(host string) ([]net.IP, error)
	lookupTXT func(name string) ([]string, error)
}

func (r *dnsResolver) LookupIP(host string) ([]net.IP, error) {
	return r.lookupIP(host)
}

func (r *dnsResolver) LookupTXT(name string) ([]string, error) {
	return r.lookupTXT(name)
}

func newDNSResolver(
	LookupIP func(host string) ([]net.IP, error),
	lookupTXT func(name string) ([]string, error),
) DNSResolver {
	return &dnsResolver{
		lookupIP:  LookupIP,
		lookupTXT: lookupTXT,
	}
}

type DNSBasedAddressTranslator struct {
	dnsEndpoint    string
	defaultDNSTTL  time.Duration
	currentMapping atomic.Value
	readTXTAfter   int64
	txtTTL         time.Duration
	logger         StdLogger
	pendingUpdate  int32
	dnsResolver    DNSResolver
}

func NewDNSBasedAddressTranslator(dnsEndpoint string, defaultTTL time.Duration, logger StdLogger) *DNSBasedAddressTranslator {
	return &DNSBasedAddressTranslator{
		dnsEndpoint:   dnsEndpoint,
		defaultDNSTTL: defaultTTL,
		logger:        logger,
		dnsResolver:   newDNSResolver(net.LookupIP, net.LookupTXT),
	}
}

func (t *DNSBasedAddressTranslator) scheduleUpdate() {
	if atomic.CompareAndSwapInt32(&t.pendingUpdate, 0, 1) {
		go func() {
			err := t.UpdateRecords()
			if err != nil {
				t.logger.Println("DNSBasedAddressTranslator: failed to update dns records: %s", err.Error())
			}
			atomic.StoreInt64(&t.readTXTAfter, time.Now().UnixMicro()+int64(t.txtTTL)*1000)
			atomic.StoreInt32(&t.pendingUpdate, 0)
		}()
	}
}

func (t *DNSBasedAddressTranslator) getCurrentMapping() []Mapping {
	value := t.currentMapping.Load()
	if value == nil {
		return nil
	}
	return value.([]Mapping)
}

func (t *DNSBasedAddressTranslator) setCurrentMapping(value []Mapping) {
	t.currentMapping.Store(value)
}

func (t *DNSBasedAddressTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	if time.Now().UnixMicro() > atomic.LoadInt64(&t.readTXTAfter) {
		if len(t.getCurrentMapping()) == 0 {
			if atomic.CompareAndSwapInt32(&t.pendingUpdate, 0, 1) {
				err := t.UpdateRecords()
				if err != nil {
					t.logger.Println("DNSBasedAddressTranslator: failed to update dns records: %s", err.Error())
				}
				atomic.StoreInt64(&t.readTXTAfter, time.Now().UnixMicro()+int64(t.txtTTL)*1000)
				atomic.StoreInt32(&t.pendingUpdate, 0)
			} else {
				for atomic.LoadInt32(&t.pendingUpdate) == 0 {
					time.Sleep(100 * time.Millisecond)
				}
			}
		} else {
			t.scheduleUpdate()
		}
	}

	var found *Mapping
	for _, mapping := range t.getCurrentMapping() {
		if mapping.src.IP().Equal(addr) {
			if mapping.src.Port() == port {
				resolved := mapping.dst.Resolve(t.dnsResolver)
				return resolved.IP(), resolved.Port()
			} else if mapping.src.Port() == 0 {

			}
			found = &mapping
		}
	}
	if found != nil {
		resolved := found.dst.Resolve(t.dnsResolver)
		return resolved.IP(), resolved.Port()
	}

	if t.logger != nil {
		t.logger.Printf("DNSBasedAddressTranslator: unknown endpoint %s:%d", addr, port)
	}

	return addr, port
}

func (t *DNSBasedAddressTranslator) getAndParseRecords() ([]Mapping, error) {
	// reads a record of the following format:
	// v=cqlpl <ip>:<port>=<ip/dns>:<port>
	records, err := t.dnsResolver.LookupTXT(t.dnsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup DNS records: %w", err)
	}
	result, err := parseTXTRecords(t.defaultDNSTTL, t.logger, records...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse records: %w", err)
	}
	return result, nil
}

func (t *DNSBasedAddressTranslator) UpdateRecords() error {
	// reads a record of the following format:
	// v=cqlpl <ip>:<port>=<ip/dns>:<port>

	records, err := t.getAndParseRecords()
	if err != nil {
		return err
	}
	if len(records) != 0 {
		t.setCurrentMapping(records)
	}
	return nil
}

func (t *DNSBasedAddressTranslator) GetAllEndpoints() ([]string, error) {
	err := t.UpdateRecords()
	if err != nil {
		return nil, err
	}

	var result []string

	for _, mapping := range t.getCurrentMapping() {
		if mapping.src.IP().IsUnspecified() {
			continue
		}
		result = append(result, fmt.Sprintf("%s:%d", mapping.src.IP(), mapping.src.Port()))
	}
	return result, nil
}

type DNSHostPort struct {
	host         string
	port         int
	resolved     IPPort
	resolveAfter time.Time
	ttl          time.Duration
	logger       StdLogger
	mu           sync.RWMutex
}

type ResolvedHostPort IPPort

func (r ResolvedHostPort) Resolve(_ DNSResolver) IPPort {
	return IPPort(r)
}

type HostPortInterface interface {
	Resolve(resolver DNSResolver) IPPort
}

func NewDNSHostPort(record string, ttl time.Duration, logger StdLogger) (*DNSHostPort, error) {
	host, portStr, err := net.SplitHostPort(record)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid port number", portStr)
	}
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("%s is not a valid port number", portStr)
	}

	return &DNSHostPort{
		host:   host,
		port:   port,
		ttl:    ttl,
		logger: logger,
	}, nil
}

func (hp *DNSHostPort) Resolve(resolver DNSResolver) IPPort {
	hp.mu.RLock()
	if !time.Now().After(hp.resolveAfter) {
		defer hp.mu.RUnlock()
		return hp.resolved
	}
	hp.mu.RUnlock()
	hp.mu.Lock()
	defer hp.mu.Unlock()
	if !time.Now().After(hp.resolveAfter) {
		return hp.resolved
	}
	ips, err := resolver.LookupIP(hp.host)
	if err != nil {
		if hp.logger != nil {
			hp.logger.Printf("DNSHostPort: failed to lookup IP address %s\n", hp.host)
		}
		return hp.resolved
	}
	if len(ips) == 0 {
		if hp.logger != nil {
			hp.logger.Printf("failed to lookup IP address %s: %s\n", hp.host, "dns returned no IP address")
		}
		return hp.resolved
	}
	currentIP := hp.resolved.IP()
	if currentIP.IsUnspecified() {
		hp.resolved = FromIPPort(ips[0], hp.port)
		hp.resolveAfter = time.Now().Add(hp.ttl)
		return hp.resolved
	}
	for _, ip := range ips {
		if currentIP.Equal(ip) {
			hp.resolveAfter = time.Now().Add(hp.ttl)
			return hp.resolved
		}
	}

	hp.resolved = FromIPPort(ips[0], hp.port)
	hp.resolveAfter = time.Now().Add(hp.ttl)
	return hp.resolved
}

type Mapping struct {
	src IPPort
	dst HostPortInterface
}

func parseTXTRecords(ttl time.Duration, logger StdLogger, recs ...string) ([]Mapping, error) {
	var result []Mapping
	var errors []error
	for _, record := range recs {
		if !strings.HasPrefix(record, "v=cqlpl ") {
			continue
		}
		record = strings.TrimPrefix(record, "v=cqlpl ")
		for _, mapping := range strings.Split(record, ";") {
			chunks := strings.SplitN(mapping, "=", 2)
			if len(chunks) != 2 {
				errors = append(errors, fmt.Errorf("malformed cqlpl TXT record %s", mapping))
				continue
			}
			src, err := ParseIPPort(chunks[0])
			if err != nil {
				errors = append(errors, fmt.Errorf("malformed source %q: %s", chunks[0], err.Error()))
				continue
			}

			dstResolved, err := ParseIPPort(chunks[1])
			if err == nil {
				result = append(result, Mapping{
					src: src,
					dst: ResolvedHostPort(dstResolved),
				})
				continue
			}

			dst, err := NewDNSHostPort(chunks[1], ttl, logger)
			if err != nil {
				errors = append(errors, fmt.Errorf("malformed destination %q: %s", chunks[1], err.Error()))
			}

			result = append(result, Mapping{
				src: src,
				dst: dst,
			})
		}
	}
	if len(errors) > 0 {
		return nil, fmt.Errorf("failed to parse TXT records: %v", errors)
	}
	return result, nil
}

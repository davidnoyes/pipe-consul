package resolver

type Resolver interface {
	FetchResults(qname, qtype, domainID string) ([]*Result, error)
	FetchAnyResult(qname, domainID string) []*Result
	FetchSOAResults(qname string) []*Result
	FetchNSResults(qname string) []*Result
	FetchAResults(qname, domainID string) []*Result
	FetchCNAMEResults(qname, domainID string) []*Result
}

type Result struct {
	Qname   string
	Qclass  string
	Qtype   string
	TTL     string
	ID      string
	Content string
}

type Question struct {
	Tag      string
	Qname    string
	Qclass   string // always "IN"
	Qtype    string // almost always "ANY"
	ID       string
	RemoteIp string
}

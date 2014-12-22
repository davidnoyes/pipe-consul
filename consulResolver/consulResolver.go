package consulResolver

import (
	"../resolver"
	"errors"
	"fmt"
	"hash/fnv"
)

type consulResolver struct {
	consulClient *Client
	TTL          string
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return fmt.Sprintf("%d", h.Sum32())
}

func New(environment, address, TTL string) (*consulResolver, error) {
	if address == "" {
		return nil, errors.New("Invalid parameters")
	}
	prefix := "services/dns/" + environment + "/"
	consulClient, err := NewConsulClient(address, prefix)
	if err != nil {
		return nil, err
	}
	return &consulResolver{consulClient, TTL}, nil
}

func (cr *consulResolver) FetchResults(qname, qtype, domainID string) ([]*resolver.Result, error) {
	if qname != "" && qtype != "" {
		switch qtype {
		case "ANY":
			return cr.FetchAnyResult(qname, domainID), nil
		case "SOA":
			return cr.FetchSOAResults(qname), nil
		case "CNAME":
			return cr.FetchCNAMEResults(qname, domainID), nil
		case "A":
			return cr.FetchAResults(qname, domainID), nil
		case "NS":
			return cr.FetchNSResults(qname), nil
		case "SRV":
		case "TXT":
		}
	}
	return nil, nil
}

func (cr *consulResolver) FetchAnyResult(qname, domainID string) (results []*resolver.Result) {
	results = append(results, cr.FetchSOAResults(qname)...)
	results = append(results, cr.FetchNSResults(qname)...)
	results = append(results, cr.FetchAResults(qname, domainID)...)
	results = append(results, cr.FetchCNAMEResults(qname, domainID)...)
	return
}

func (cr *consulResolver) FetchSOAResults(qname string) (results []*resolver.Result) {
	if cr.consulClient.KeyExists(qname) {
		content := fmt.Sprintf("%s hostmaster.%s 0 1800 600 3600 300", qname, qname)
		results = append(results, &resolver.Result{qname, "IN", "SOA", cr.TTL, hash(qname), content})
	}
	return
}

func (cr *consulResolver) FetchNSResults(qname string) (results []*resolver.Result) {
	keys := cr.consulClient.GetChildKeys(qname + "/NS/")
	for _, key := range keys {
		results = append(results, &resolver.Result{qname, "IN", "NS", cr.TTL, hash(qname), key})
	}
	return
}

func (cr *consulResolver) FetchAResults(qname, domainID string) (results []*resolver.Result) {
	domains := cr.consulClient.GetChildKeys("")
	for _, domain := range domains {
		if hash(domain) == domainID {
			value := cr.consulClient.GetValue(domain + "/A/" + qname)
			if value != "" {
				results = append(results, &resolver.Result{qname, "IN", "A", cr.TTL, hash(domain), value})
			}
			return
		}
	}

	return
}

func (cr *consulResolver) FetchCNAMEResults(qname, domainID string) (results []*resolver.Result) {
	domains := cr.consulClient.GetChildKeys("")
	for _, domain := range domains {
		if hash(domain) == domainID {
			value := cr.consulClient.GetValue(domain + "/CNAME/" + qname)
			if value != "" {
				results = append(results, &resolver.Result{qname, "IN", "CNAME", cr.TTL, hash(domain), value})
			}
			return
		}
	}

	return
}

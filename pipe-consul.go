package main

import (
	"./consul"
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	log "github.com/golang/glog"
	"hash/fnv"
	"io"
	"os"
)

const (
	END_REPLY        = "END\n"
	FAIL_REPLY       = "FAIL\n"
	GREETING_REPLY   = "OK\tpipe-consul\n"
	TAG_AXFR         = "AXFR" // ignored for now
	TAG_PING         = "PING"
	TAG_Q            = "Q"
	defaultScopebits = "0"
	defaultAuth      = "1"
	defaultTTL       = "500" //5 mins
	defaultId        = "2"
	defaultPriority  = 0
	defaultWeight    = 0
)

var (
	GREETING_ABI_V1 = []byte("HELO\t1")
	GREETING_ABI_V2 = []byte("HELO\t2")
	GREETING_ABI_V3 = []byte("HELO\t3")
	GREETING_ABI_V4 = []byte("HELO\t4")
	errLongLine     = errors.New("pdns line too long")
	errBadLine      = errors.New("pdns line unparseable")
)

type question struct {
	tag      string
	qname    string
	qclass   string // always "IN"
	qtype    string // almost always "ANY"
	id       string
	remoteIp string
}

type consulResolver struct {
	consulClient *consul.Client
}

type pdns struct {
	cr *consulResolver
	r  io.Reader
	w  io.Writer
}

type result struct {
	qname   string
	qclass  string
	qtype   string
	ttl     string
	id      string
	content string
}

func newConsulResolver(environment, address string) (*consulResolver, error) {
	if address == "" {
		return nil, errors.New("Invalid parameters")
	}
	prefix := "services/dns/" + environment + "/"
	consulClient, err := consul.NewConsulClient(address, prefix)
	if err != nil {
		return nil, err
	}
	return &consulResolver{consulClient}, nil
}

func (res *result) formatResult() string {
	return fmt.Sprintf("DATA\t%v\t%v\t%v\t%v\t%v\t%v\n", res.qname, res.qclass, res.qtype, res.ttl, res.id, res.content)
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return fmt.Sprintf("%d", h.Sum32())
}

func (cr *consulResolver) fetchResults(qname, qtype, domainID string) ([]*result, error) {
	if qname != "" && qtype != "" {
		switch qtype {
		case "ANY":
			return cr.fetchAllResults(qname, domainID), nil
		case "SOA":
			return cr.fetchSOAResults(qname), nil
		case "CNAME":
			return cr.fetchCNAMEResults(qname, domainID), nil
		case "A":
			return cr.fetchAResults(qname, domainID), nil
		case "NS":
			return cr.fetchNSResults(qname), nil
		case "SRV":
		case "TXT":
		}
	}
	return nil, nil
}

func (cr *consulResolver) fetchAllResults(qname, domainID string) (results []*result) {
	results = append(results, cr.fetchSOAResults(qname)...)
	results = append(results, cr.fetchNSResults(qname)...)
	results = append(results, cr.fetchAResults(qname, domainID)...)
	results = append(results, cr.fetchCNAMEResults(qname, domainID)...)
	return
}

func (cr *consulResolver) fetchSOAResults(qname string) (results []*result) {
	if cr.consulClient.KeyExists(qname) {
		content := fmt.Sprintf("%s hostmaster.%s 0 1800 600 3600 300", qname, qname)
		results = append(results, &result{qname, "IN", "SOA", defaultTTL, hash(qname), content})
	}
	return
}

func (cr *consulResolver) fetchNSResults(qname string) (results []*result) {
	keys := cr.consulClient.GetChildKeys(qname + "/NS/")
	for _, key := range keys {
		results = append(results, &result{qname, "IN", "NS", defaultTTL, hash(qname), key})
	}
	return
}

func (cr *consulResolver) fetchAResults(qname, domainID string) (results []*result) {
	domains := cr.consulClient.GetChildKeys("")
	for _, domain := range domains {
		if hash(domain) == domainID {
			value := cr.consulClient.GetValue(domain + "/A/" + qname)
			if value != "" {
				results = append(results, &result{qname, "IN", "A", defaultTTL, hash(domain), value})
			}
			return
		}
	}

	return
}

func (cr *consulResolver) fetchCNAMEResults(qname, domainID string) (results []*result) {
	domains := cr.consulClient.GetChildKeys("")
	for _, domain := range domains {
		if hash(domain) == domainID {
			value := cr.consulClient.GetValue(domain + "/CNAME/" + qname)
			if value != "" {
				results = append(results, &result{qname, "IN", "CNAME", defaultTTL, hash(domain), value})
			}
			return
		}
	}

	return
}
func (pd *pdns) answerQuestion(question *question) (answers []string, err error) {
	results, err := pd.cr.fetchResults(question.qname, question.qtype, question.id)
	if err != nil {
		pd.write(fmt.Sprintf("Query error %s %s: %s", question.qname, question.qtype, err))
		return nil, err
	}
	for _, result := range results {
		answers = append(answers, result.formatResult())
	}
	return answers, nil
}

func (pd *pdns) parseQuestion(line []byte) (*question, error) {
	fields := bytes.Split(line, []byte("\t"))
	tag := string(fields[0])
	switch tag {
	case TAG_Q:
		if len(fields) < 6 {
			return nil, errBadLine
		}
		return &question{tag: tag, qname: string(fields[1]), qclass: string(fields[2]), qtype: string(fields[3]), id: string(fields[4]), remoteIp: string(fields[5])}, nil

	case TAG_AXFR:
		return &question{tag: tag}, nil

	case TAG_PING:
		return &question{tag: tag}, nil

	default:
		return nil, errBadLine
	}
	panic("Unreachable parse")
}

func (pd *pdns) log(line string) {
	io.WriteString(pd.w, "LOG\t"+line+"\n")
}

func (pd *pdns) write(line string) {
	log.Info(line)
	io.WriteString(pd.w, line)
}

func (pd *pdns) Process() {
	log.Info("Starting Consul Resolver")
	buffer := bufio.NewReader(pd.r)
	needHandshake := true
	for {
		line, isPrefix, err := buffer.ReadLine()
		if err == nil && isPrefix {
			err = errLongLine
		} else if err == io.EOF {
			return
		} else if err != nil {
			log.Errorf("Failed reading question: %s", err)
		}

		log.Infof("INPUT: %s", line)
		if needHandshake {
			if !bytes.Equal(line, GREETING_ABI_V1) {
				log.Errorf("Handshake failed: %s != %s", line, GREETING_ABI_V1)
				pd.write(FAIL_REPLY)
			} else {
				needHandshake = false
				pd.write(GREETING_REPLY)
			}
			continue
		}

		question, err := pd.parseQuestion(line)
		if err != nil {
			log.Errorf("Failed to process question: %s", err)
			pd.write(FAIL_REPLY)
			continue
		}

		switch question.tag {
		case TAG_Q:
			responseLines, err := pd.answerQuestion(question)
			if err != nil {
				log.Errorf("Failed to answer question: %s %s", question.qname, err)
				pd.write(FAIL_REPLY)
				continue
			}
			for _, line := range responseLines {
				pd.write(line)
			}
		case TAG_AXFR:
			responseLines, err := pd.answerQuestion(question)
			if err != nil {
				log.Errorf("Failed to answer question: %s %s", question.qname, err)
				pd.write(FAIL_REPLY)
				continue
			}
			for _, line := range responseLines {
				pd.write(line)
			}
		case TAG_PING:
			//TODO: Implement if required
		}
		pd.write(END_REPLY)
	}
}

func main() {
	environment := flag.String("environment", "", "The name of the environment being served for")
	consulConn := flag.String("address", "", "The URL to the Consul service")
	flag.Parse()
	cRes, err := newConsulResolver(*environment, *consulConn)
	if err != nil {
		log.Errorf("FAIL %s", err)
		os.Exit(1)
	}
	pd := &pdns{cRes, os.Stdin, os.Stdout}
	pd.Process()
	os.Stdout.Close()
}

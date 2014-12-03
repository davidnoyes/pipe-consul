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
	defaultTTL       = "1"
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
	localIp  string
	subnet   string
}

type consulResolver struct {
	consulClient *consul.Client
}

type pdns struct {
	cr *consulResolver
}

type result struct {
	scopebits string
	auth      string
	qname     string
	qclass    string
	qtype     string
	ttl       string
	id        string
	content   string
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
	return fmt.Sprintf("DATA\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n", res.scopebits, res.auth, res.qname, res.qclass, res.qtype, res.ttl, res.id, res.content)
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return fmt.Sprintf("%d", h.Sum32())
}

func (cr *consulResolver) fetchResults(qname, qtype string) ([]*result, error) {
	if qname != "" && qtype != "" {
		switch qtype {
		case "ANY":
		case "SOA":

			//Do Consul test
			value, err := cr.consulClient.GetValue(qname + "/")
			log.Infof("value: %s", value)
			log.Errorf("error: %v", err)
			domain := "env.plus.net"
			if qname == fmt.Sprintf("%s", domain) {
				content := fmt.Sprintf("%s hostmaster%s 0 1800 600 3600 300", qname, qname)
				return []*result{&result{defaultScopebits, defaultAuth, qname, "IN", qtype, defaultTTL, hash(qname), content}}, nil
			}
			return nil, nil
		case "CNAME":
		case "A":
		case "SRV":
		case "TXT":
		}
		return nil, errors.New(fmt.Sprintf("Type: %s not supported", qtype))
	}
	return nil, nil
}

func (pd *pdns) answerQuestion(question *question) (answers []string, err error) {
	results, err := pd.cr.fetchResults(question.qname, question.qtype)
	if err != nil {
		log.Errorf("Query error %s %s: %s", question.qname, question.qtype, err)
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
		if len(fields) < 8 {
			return nil, errBadLine
		}
		return &question{tag: tag, qname: string(fields[1]), qclass: string(fields[2]), qtype: string(fields[3]), id: string(fields[4]), remoteIp: string(fields[5]), localIp: string(fields[6]), subnet: string(fields[7])}, nil

	case TAG_AXFR:
		return &question{tag: tag}, nil

	case TAG_PING:
		return &question{tag: tag}, nil

	default:
		return nil, errBadLine
	}
	panic("Unreachable parse")
}

func write(w io.Writer, line string) {
	log.Infof("OUTPUT: %s", line)
	_, err := io.WriteString(w, line)
	if err != nil {
		log.Errorf("Write failed: %s", err)
	}
}

func (pd *pdns) Process(r io.Reader, w io.Writer) {
	log.Info("Starting Consul Resolver")
	buffer := bufio.NewReader(r)
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
			if !bytes.Equal(line, GREETING_ABI_V4) {
				log.Errorf("Handshake failed: %s != %s", line, GREETING_ABI_V4)
				write(w, FAIL_REPLY)
			} else {
				needHandshake = false
				write(w, GREETING_REPLY)
			}
			continue
		}

		question, err := pd.parseQuestion(line)
		if err != nil {
			log.Errorf("Failed to process question: %s", err)
			write(w, FAIL_REPLY)
			continue
		}

		switch question.tag {
		case TAG_Q:
			responseLines, err := pd.answerQuestion(question)
			if err != nil {
				log.Errorf("Failed to answer question: %s %s", question.qname, err)
				write(w, FAIL_REPLY)
				continue
			}
			for _, line := range responseLines {
				write(w, line)
			}
		case TAG_AXFR:
			//TODO: Implement if required
		case TAG_PING:
			//TODO: Implement if required
		}
		write(w, END_REPLY)
	}
}

func main() {
	environment := flag.String("environment", "", "The name of the environment being served for")
	consulConn := flag.String("address", "", "The URL to the Consul service")
	flag.Parse()
	cRes, err := newConsulResolver(*environment, *consulConn)
	if err != nil {
		log.Errorf("%s", err)
		os.Exit(1)
	}
	pd := &pdns{cRes}
	pd.Process(os.Stdin, os.Stdout)
	os.Stdout.Close()
}

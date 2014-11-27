package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	log "github.com/golang/glog"
	"io"
	"os"
)

const (
	END_REPLY       = "END\n"
	FAIL_REPLY      = "FAIL\n"
	GREETING_REPLY  = "OK\tpipe-consul\n"
	TAG_AXFR        = "AXFR" // ignored for now
	TAG_PING        = "PING"
	TAG_Q           = "Q"
	defaultTTL      = "1"
	defaultId       = "1"
	defaultPriority = 0
	defaultWeight   = 0
)

var (
	GREETING_ABI_V1 = []byte("HELO\t1")
	GREETING_ABI_V2 = []byte("HELO\t2")
	GREETING_ABI_V3 = []byte("HELO\t3")
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
	authDomain string
	consulConn string
}

type pdns struct {
	cr *consulResolver
}

type result struct {
	qname   string
	qclass  string
	qtype   string
	ttl     string
	id      string
	content string
}

func newConsulResolver(authDomain, consulConn string) (*consulResolver, error) {
	if authDomain == "" || consulConn == "" {
		return nil, errors.New("Invalid parameters")
	}
	if authDomain[len(authDomain)-1] == '.' {
		authDomain = authDomain[:len(authDomain)-1]
	}
	if authDomain[0] != '.' {
		authDomain = "." + authDomain
	}
	return &consulResolver{authDomain, consulConn}, nil
}

func (res *result) formatResult() string {
	return fmt.Sprintf("DATA\t%v\t%v\t%v\t%v\t%v\t%v\n", res.qname, res.qclass, res.qtype, res.ttl, res.id, res.content)
}

func (cr *consulResolver) fetchResults(qname, qtype string) ([]*result, error) {
	switch qtype {
	case "ANY":
		return nil, nil
	case "SOA":
		content := fmt.Sprintf("%s hostmaster%s 0 1800 600 3600 300", cr.authDomain, cr.authDomain)
		return []*result{&result{qname, "IN", qtype, defaultTTL, defaultId, content}}, nil
	case "CNAME":
	case "A":
	case "SRV":
	case "TXT":
	}
	return nil, nil
}

func (pd *pdns) answerQuestion(question *question) (answers []string, err error) {
	if question.tag == "ANY" {
		return nil, nil
	} else {
		log.Info(question)
		results, err := pd.cr.fetchResults(question.qname, question.qtype)
		if err != nil {
			log.Errorf("Query error %s %s: %s", question.qname, question.qtype, err)
			return nil, nil
		}
		for _, result := range results {
			answers = append(answers, result.formatResult())
		}
		return answers, nil
	}
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

		if needHandshake {
			if !bytes.Equal(line, GREETING_ABI_V1) {
				log.Errorf("Handshake failed: %s != %s", line, GREETING_ABI_V1)
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
	authDomain := flag.String("auth-domain", "", "The name of the authorititive domain being served")
	consulConn := flag.String("consul-conn", "", "The URL to the Consul service")
	flag.Parse()
	cRes, err := newConsulResolver(*authDomain, *consulConn)
	if err != nil {
		log.Errorf("%s", err)
		os.Exit(1)
	}
	pd := &pdns{cRes}
	pd.Process(os.Stdin, os.Stdout)
	os.Stdout.Close()
}

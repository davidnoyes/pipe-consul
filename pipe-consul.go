package main

import (
	"bufio"
	"bytes"
	"consul"
	"errors"
	"flag"
	"fmt"
	log "github.com/golang/glog"
	"hash/fnv"
	"io"
	"os"
	"pdns"
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

func (res *result) formatResult() string {
	return fmt.Sprintf("DATA\t%v\t%v\t%v\t%v\t%v\t%v\n", res.qname, res.qclass, res.qtype, res.ttl, res.id, res.content)
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

package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	log "github.com/golang/glog"
	"io"
	"os"
)

const (
	END_REPLY      = "END\n"
	FAIL_REPLY     = "FAIL\n"
	GREETING_REPLY = "OK\tpipe-consul\n"
	TAG_AXFR       = "AXFR" // ignored for now
	TAG_PING       = "PING"
	TAG_Q          = "Q"
)

var (
	GREETING_ABI_V2 = []byte("HELO\t2")
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
}

func answerQuestion(question *question) (answers []string, err error) {
	if question.tag == "ANY" {
		return nil, nil
	} else {

	}
}

func parseQuestion(line []byte) (*question, error) {
	fields := bytes.Split(line, []byte("\t"))
	tag := string(fields[0])
	switch tag {
	case TAG_Q:
		if len(fields) < 7 {
			return nil, errBadLine
		}
		return &question{tag: tag, qname: string(fields[1]), qclass: string(fields[2]), qtype: string(fields[3]), id: string(fields[4]), remoteIp: string(fields[5]), localIp: string(fields[6])}, nil

	case TAG_AXFR:
		if len(fields) < 3 {
			return nil, errBadLine
		}
		return &question{tag: tag, id: string(fields[1])}, nil

	case TAG_PING:
		return &question{tag: tag}, nil

	default:
		return nil, errBadLine
	}
	panic("Unreachable parse")
}

func write(w io.Writer, line string) {
	_, err := io.WriteString(w, line)
	if err != nil {
		log.Errorf("Write failed: %v", err)
	}
}

func Process(r io.Reader, w io.Writer) {
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
			log.Errorf("Failed reading question: %v", err)
		}

		if needHandshake {
			if !bytes.Equal(line, GREETING_ABI_V2) {
				log.Errorf("Handshake failed: %v != %v", line, GREETING_ABI_V2)
				write(w, FAIL_REPLY)
			} else {
				needHandshake = false
				write(w, GREETING_REPLY)
			}
			continue
		}

		question, err := parseQuestion(line)
		if err != nil {
			log.Errorf("Failed to process question: %v", err)
			write(w, FAIL_REPLY)
			continue
		}

		log.Info(question)
		switch question.tag {
		case TAG_Q:
			responseLines, err := answerQuestion(question)
			if err != nil {
				log.Errorf("Failed to answer question: %v %v", question.qname, err)
				write(w, FAIL_REPLY)
				continue
			}
		case TAG_AXFR:
		case TAG_PING:
		}
		write(w, END_REPLY)
	}
}

func main() {
	flag.Parse()
	Process(os.Stdin, os.Stdout)
	os.Stdout.Close()
}

package pdns

import (
	"../resolver"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	log "github.com/golang/glog"
	"io"
)

const (
	TAG_Q          = "Q"
	TAG_AXFR       = "AXFR" // ignored for now
	TAG_PING       = "PING"
	END_REPLY      = "END\n"
	FAIL_REPLY     = "FAIL\n"
	GREETING_REPLY = "OK\tpipe-consul\n"
)

var (
	errBadLine      = errors.New("pdns line unparsable")
	errLongLine     = errors.New("pdns line too long")
	GREETING_ABI_V1 = []byte("HELO\t1")
)

type PDNS struct {
	Resolver resolver.Resolver
	R        io.Reader
	W        io.Writer
}

func formatResult(result *resolver.Result) string {
	return fmt.Sprintf("DATA\t%v\t%v\t%v\t%v\t%v\t%v\n", result.Qname, result.Qclass, result.Qtype, result.TTL, result.ID, result.Content)
}

func (pd *PDNS) answerQuestion(question *resolver.Question) (answers []string, err error) {
	results, err := pd.Resolver.FetchResults(question.Qname, question.Qtype, question.ID)
	if err != nil {
		pd.write(fmt.Sprintf("Query error %s %s: %s", question.Qname, question.Qtype, err))
		return nil, err
	}
	for _, result := range results {
		answers = append(answers, formatResult(result))
	}
	return answers, nil
}

func (pd *PDNS) parseQuestion(line []byte) (*resolver.Question, error) {
	fields := bytes.Split(line, []byte("\t"))
	tag := string(fields[0])
	switch tag {
	case TAG_Q:
		if len(fields) < 6 {
			return nil, errBadLine
		}
		return &resolver.Question{Tag: tag, Qname: string(fields[1]), Qclass: string(fields[2]), Qtype: string(fields[3]), ID: string(fields[4]), RemoteIp: string(fields[5])}, nil

	case TAG_AXFR:
		return &resolver.Question{Tag: tag}, nil

	case TAG_PING:
		return &resolver.Question{Tag: tag}, nil

	default:
		return nil, errBadLine
	}
	panic("Unreachable parse")
}

func (pd *PDNS) log(line string) {
	io.WriteString(pd.W, "LOG\t"+line+"\n")
}

func (pd *PDNS) write(line string) {
	io.WriteString(pd.W, line)
}

func (pd *PDNS) Process() {
	buffer := bufio.NewReader(pd.R)
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
		switch question.Tag {
		case TAG_Q:
			responseLines, err := pd.answerQuestion(question)
			if err != nil {
				log.Errorf("Failed to answer question: %s %s", question.Qname, err)
				pd.write(FAIL_REPLY)
				continue
			}
			for _, line := range responseLines {
				pd.write(line)
			}
		case TAG_AXFR:
			responseLines, err := pd.answerQuestion(question)
			if err != nil {
				log.Errorf("Failed to answer question: %s %s", question.Qname, err)
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

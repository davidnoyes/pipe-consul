package main

import (
	"flag"
	"os"

	log "github.com/golang/glog"
	consulResolver "github.com/raspberrypython/consulResolver"
	pdns "github.com/raspberrypython/pdns"
)

const (
	defaultTTL = "500" //5 mins
)

func main() {
	environment := flag.String("environment", "", "The name of the environment being served for")
	consulConn := flag.String("address", "", "The URL to the Consul service")
	flag.Parse()
	cRes, err := consulResolver.New(*environment, *consulConn, defaultTTL)
	if err != nil {
		log.Errorf("FAIL %s", err)
		os.Exit(1)
	}
	pd := &pdns.PDNS{cRes, os.Stdin, os.Stdout}
	pd.Process()
	os.Stdout.Close()
}

package consul

import (
	"errors"
	"github.com/armon/consul-api"
	log "github.com/golang/glog"
)

type Client struct {
	kv     *consulapi.KV
	prefix string
}

func NewConsulClient(address, prefix string) (*Client, error) {
	config := consulapi.DefaultConfig()
	config.Address = address + ":8500"
	client, err := consulapi.NewClient(config)
	if err != nil {
		return nil, err
	}
	kv := client.KV()
	return &Client{
		kv:     kv,
		prefix: prefix,
	}, nil
}

func (client *Client) GetValue(key string) ([]byte, error) {
	var value []byte
	log.Infof("search: %s", client.prefix+key)
	kvPair, _, err := client.kv.Get(client.prefix+key, nil)
	log.Infof("%s:%s", kvPair.Key, kvPair.Value)
	if err != nil {
		return value, err
	}
	if kvPair == nil {
		return value, errors.New("No kvPair found")
	}
	value = kvPair.Value
	return value, nil
}

func (client *Client) List(key string) {
	kvPairs, _, err := client.kv.List(client.prefix+key, nil)
	if err != nil {
		log.Error(err)
	}
	log.Info(kvPairs)
	for _, kv := range kvPairs {
		log.Infof("%s: %s", kv.Key, kv.Value)
	}
}

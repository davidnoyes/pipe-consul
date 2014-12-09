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
	kvPair, _, err := client.kv.Get(client.prefix+key, nil)
	if err != nil {
		return value, err
	}
	if kvPair == nil {
		return value, errors.New("No kvPair found")
	}
	value = kvPair.Value
	return value, nil
}

func (client *Client) GetKeys(domain string) {
	keys, _, _ := client.kv.Keys(client.prefix+domain+"/NS", "NS/", nil)
	log.Info(keys)
	for _, key := range keys {
		log.Infof("%#v", key)
	}
}

func (client *Client) KeyExists(key string) bool {
	keys, _, _ := client.kv.Keys(client.prefix+key, "/", nil)
	return len(keys) > 0
}

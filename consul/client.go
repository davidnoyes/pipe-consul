package consul

import (
	"errors"
	"github.com/armon/consul-api"
	"strings"
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

func (client *Client) GetKeys(path string) []string {
	keys, _, _ := client.kv.Keys(client.prefix+path, "/", nil)
	return keys
}

func (client *Client) GetChildKeys(path string) (childKeys []string) {
	keys := client.GetKeys(path)
	//var childKeys []string
	for i, _ := range keys {
		keys[i] = strings.TrimPrefix(keys[i], client.prefix+path)
		keys[i] = strings.TrimSuffix(keys[i], "/")
		if keys[i] != "" {
			childKeys = append(childKeys, keys[i])
		}
	}
	return
}

func (client *Client) KeyExists(key string) bool {
	keys, _, _ := client.kv.Keys(client.prefix+key, "/", nil)
	return len(keys) > 0
}

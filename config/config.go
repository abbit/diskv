package config

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

type configfile struct {
	Shards []struct {
		Name     string `yaml:"name"`
		Host     string `yaml:"host"`
		HttpPort int    `yaml:"http_port"`
	} `yaml:"shards"`
}

type Config struct {
	name   string
	shards map[string]Shard
}

type Shard struct {
	Index    int
	Name     string
	host     string
	httpPort int
}

func New(filepath string, name string) (*Config, error) {
	config, err := parseFile(filepath)
	if err != nil {
		return nil, err
	}

	shards := make(map[string]Shard)
	for i, server := range config.Shards {
		shards[server.Name] = Shard{
			Index:    i,
			Name:     server.Name,
			host:     server.Host,
			httpPort: server.HttpPort,
		}
	}

	if _, ok := shards[name]; !ok {
		return nil, fmt.Errorf("shard with name \"%s\" not found in config", name)
	}

	return &Config{
		name:   name,
		shards: shards,
	}, nil
}

func (c *Config) ThisShard() Shard {
	return c.shards[c.name]
}

func (c *Config) GetShardForKey(key []byte) Shard {
	keyhash := hash(key)
	shardIndex := keyhash % len(c.shards)
	return c.getShardByIndex(shardIndex)
}

func (c *Config) getShardByIndex(index int) Shard {
	for _, shard := range c.shards {
		if shard.Index == index {
			return shard
		}
	}

	return Shard{}
}

func (s Shard) HttpAddress() string {
	return fmt.Sprintf("%s:%d", s.host, s.httpPort)
}

func parseFile(filepath string) (*configfile, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	var config configfile
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func hash(value []byte) int {
	h := fnv.New32a()
	h.Write(value)
	return int(h.Sum32())
}

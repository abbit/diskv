package config

import (
	"fmt"
	"hash/fnv"
	"os"

	"gopkg.in/yaml.v3"
)

type configfile struct {
	Shards []struct {
		Name     string `yaml:"name"`
		Host     string `yaml:"host"`
		Index    int    `yaml:"index"`
		HttpPort int    `yaml:"http_port"`
		Replicas []struct {
			Name     string `yaml:"name"`
			Host     string `yaml:"host"`
			HttpPort int    `yaml:"http_port"`
		} `yaml:"replicas"`
	} `yaml:"shards"`
}

type Config struct {
	shards    [][]*Shard
	thisShard *Shard
}

type Shard struct {
	Index    int
	Replica  bool
	Name     string
	host     string
	httpPort int
}

func New(filepath string, name string) (*Config, error) {
	cfgfile, err := parseFile(filepath)
	if err != nil {
		return nil, err
	}

	shards := make([][]*Shard, len(cfgfile.Shards))
	var thisShard *Shard
	for _, shard := range cfgfile.Shards {
		shards[shard.Index] = make([]*Shard, len(shard.Replicas)+1)
		s := &Shard{
			Index:    shard.Index,
			Replica:  false,
			Name:     shard.Name,
			host:     shard.Host,
			httpPort: shard.HttpPort,
		}
		shards[shard.Index][0] = s
		if shard.Name == name {
			thisShard = s
		}
		for i, replica := range shard.Replicas {
			s := &Shard{
				Index:    shard.Index,
				Replica:  true,
				Name:     replica.Name,
				host:     replica.Host,
				httpPort: replica.HttpPort,
			}
			shards[shard.Index][i+1] = s
			if replica.Name == name {
				thisShard = s
			}
		}
	}

	if thisShard == nil {
		return nil, fmt.Errorf("shard with name \"%s\" not found in config", name)
	}

	return &Config{
		shards:    shards,
		thisShard: thisShard,
	}, nil
}

func (c *Config) ThisShard() *Shard {
	return c.thisShard
}

func (c *Config) GetShardsForKey(key string) []*Shard {
	keyhash := hash([]byte(key))
	shardIndex := keyhash % len(c.shards)
	return c.shards[shardIndex]
}

func (c *Config) GetMasterShardForKey(key string) *Shard {
	shards := c.GetShardsForKey(key)
	for _, shard := range shards {
		if !shard.Replica {
			return shard
		}
	}
	return nil
}

func (s *Shard) HttpAddress() string {
	return fmt.Sprintf("%s:%d", s.host, s.httpPort)
}

func parseFile(filepath string) (*configfile, error) {
	data, err := os.ReadFile(filepath)
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

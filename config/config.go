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
	nodes    [][]*Node // [shard][replica]
	thisNode *Node
}

type Node struct {
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

	nodes := make([][]*Node, len(cfgfile.Shards))
	var thisNode *Node
	for _, shard := range cfgfile.Shards {
		nodes[shard.Index] = make([]*Node, len(shard.Replicas)+1)
		s := &Node{
			Index:    shard.Index,
			Replica:  false,
			Name:     shard.Name,
			host:     shard.Host,
			httpPort: shard.HttpPort,
		}
		nodes[shard.Index][0] = s
		if shard.Name == name {
			thisNode = s
		}
		for i, replica := range shard.Replicas {
			s := &Node{
				Index:    shard.Index,
				Replica:  true,
				Name:     replica.Name,
				host:     replica.Host,
				httpPort: replica.HttpPort,
			}
			nodes[shard.Index][i+1] = s
			if replica.Name == name {
				thisNode = s
			}
		}
	}

	if thisNode == nil {
		return nil, fmt.Errorf("shard with name \"%s\" not found in config", name)
	}

	return &Config{
		nodes:    nodes,
		thisNode: thisNode,
	}, nil
}

func (c *Config) ThisNode() *Node {
	return c.thisNode
}

func (c *Config) GetNodesForKey(key string) []*Node {
	keyhash := hash([]byte(key))
	shardIndex := keyhash % len(c.nodes)
	return c.nodes[shardIndex]
}

func (c *Config) GetMasterNodeForKey(key string) *Node {
	return c.GetNodesForKey(key)[0]
}

func (c *Config) GetShardNodes(shardIndex int) []*Node {
	return c.nodes[shardIndex]
}

func (c *Config) GetShardMasterNode(shardIndex int) *Node {
	return c.GetShardNodes(shardIndex)[0]
}

func (s *Node) HttpAddress() string {
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

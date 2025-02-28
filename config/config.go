package config

import (
	"flag"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env             string        `yaml:"env"`
	BrokerPort      string        `yaml:"broker_port"`
	QueueNames      []string      `yaml:"queue_names"`
	QueueLength     int           `yaml:"queue_length"`
	MaxSubscribers  int           `yaml:"max_subscribers"`
	CallbackTimeout time.Duration `yaml:"callback_timeout"`
}

func ReadConfig() *Config {
	var configPath string

	flag.StringVar(&configPath, "config_path", "", "path to config")
	flag.Parse()

	if configPath == "" {
		configPath = os.Getenv("CONFIG_PATH")
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic(err)
	}

	return &cfg
}

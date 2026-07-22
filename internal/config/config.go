package config

import (
	"flag"
	"log"
	"os"
	"time"

	yaml "github.com/goccy/go-yaml"
	"github.com/joho/godotenv"
)

type Config struct {
	Env            string         `yaml:"env"`
	Port           int            `yaml:"-"` // только из PORT env
	HttpServer     HttpServer     `yaml:"http_server"`
	ProcessingMode ProcessingMode `yaml:"processing_mode"`
	Database       DatabaseConfig `yaml:"-"`
	Kafka          KafkaConfig    `yaml:"-"`
}

type ProcessingMode struct {
	Mode string `yaml:"mode"`
}

const (
	OrderModeSync  = "sync"
	OrderModeAsync = "async"
)

func (c ProcessingMode) IsAsync() bool {
	return c.Mode == OrderModeAsync
}

type HttpServer struct {
	ReadTimeout             time.Duration `yaml:"read_timeout"`
	WriteTimeout            time.Duration `yaml:"write_timeout"`
	IdleTimeout             time.Duration `yaml:"idle_timeout"`
	RequestTimeout          time.Duration `yaml:"request_timeout"`
	GracefulShutdownTimeout time.Duration `yaml:"graceful_shutdown_timeout"`
}

func MustLoad() *Config {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatal("error loading .env file: ", err)
	}

	path := fetchConfigPath()
	if path == "" {
		panic("config path is empty")
	}

	cfg := MustLoadByPath(path)
	loadEnv(cfg)
	if err := cfg.validateEnv(); err != nil {
		log.Fatal(err)
	}

	return cfg
}

// fetchConfigPath fetches the config path from the command line arguments.
// Priority: flag > env > default
// Default value is empty string
func fetchConfigPath() string {
	var res string

	// Переменная res передается по указателю чтобы внутри ее можно было переписать
	// "config" - имя флага, "" - значение по умолчанию, "path to config file" - описание флага
	flag.StringVar(&res, "config", "", "path to config file")
	flag.Parse()

	if res == "" {
		res = os.Getenv("CONFIG_PATH")
	}

	return res
}

func MustLoadByPath(configPath string) *Config {

	configFile, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			panic("config file does not exist")
		}
		panic("failed to read config: " + err.Error())
	}
	var cfg Config

	if err := yaml.Unmarshal(configFile, &cfg); err != nil {
		panic("failed to read config: " + err.Error())
	}

	// Почему возвращаем указатель на структуру?
	// чтобы не копировать всю структуру при возврате (актуально для больших структур);
	// чтобы можно было вернуть nil при ошибке ((*Config, error) — очень частый паттерн);
	// чтобы дальше работать с одним и тем же объектом (изменения видны по этому указателю);
	return &cfg
}

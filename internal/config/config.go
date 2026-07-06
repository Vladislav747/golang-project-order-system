package config

import (
	"flag"
	"os"
	"time"

	yaml "github.com/goccy/go-yaml"
)

type Config struct {
	Env        string     `yaml:"env"`
	Port       int        `yaml:"port"`
	HttpServer HttpServer `yaml:"http_server"`
}

type HttpServer struct {
	ReadTimeout             time.Duration `yaml:"read_timeout"`
	WriteTimeout            time.Duration `yaml:"write_timeout"`
	IdleTimeout             time.Duration `yaml:"idle_timeout"`
	RequestTimeout          time.Duration `yaml:"request_timeout"`
	GracefulShutdownTimeout time.Duration `yaml:"graceful_shutdown_timeout"`
}

func MustLoad() *Config {

	path := fetchConfigPath()
	if path == "" {
		panic("config path is empty")
	}

	return MustLoadByPath(path)
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

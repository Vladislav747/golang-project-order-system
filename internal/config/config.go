package config

import (
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env            string     `yaml:"env" env-default:"local"`
	Port           int        `yaml:"port" env-default:"8082"`
	HttpServer     HttpServer `yaml:"http_server"`
}

type HttpServer struct {
	ReadTimeout int `yaml:"read_timeout" env-default:"10"`
	WriteTimeout int `yaml:"write_timeout" env-default:"10"`
	IdleTimeout int `yaml:"idle_timeout" env-default:"60"`
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

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic("config file does not exist")
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("failed to read config: " + err.Error())
	}

	// Почему возвращаем указатель на структуру?
	// чтобы не копировать всю структуру при возврате (актуально для больших структур);
	// чтобы можно было вернуть nil при ошибке ((*Config, error) — очень частый паттерн);
	// чтобы дальше работать с одним и тем же объектом (изменения видны по этому указателю);
	return &cfg
}
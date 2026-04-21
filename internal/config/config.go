package config

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	ServerPort    string `mapstructure:"SERVER_PORT"`
	EthRpcHttpUrl string `mapstructure:"ETH_RPC_HTTP_URL"`
	EthRpcWsUrl   string `mapstructure:"ETH_RPC_WS_URL"`
}

func LoadConfig() *Config {
	viper.AddConfigPath(".")
	viper.SetConfigFile(".env")
	viper.SetConfigType("env")

	viper.AutomaticEnv() //환경 변수 자동 매핑

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("can't read .env file: %v", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatalf("Unable to decode into config struct : %v", err)
	}

	log.Println("Config load successfully")
	return &config
}

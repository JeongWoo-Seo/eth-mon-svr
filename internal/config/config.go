package config

import (
	"context"
	"os"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/spf13/viper"
)

type Config struct {
	Service       string `mapstructure:"SERVICE"`
	Env           string `mapstructure:"ENV"`
	ServerPort    string `mapstructure:"SERVER_PORT"`
	EthRpcHttpUrl string `mapstructure:"ETH_RPC_HTTP_URL"`
	EthRpcWsUrl   string `mapstructure:"ETH_RPC_WS_URL"`
}

func LoadConfig() *Config {
	ctx := context.Background()
	viper.AddConfigPath(".")
	viper.SetConfigFile(".env")
	viper.SetConfigType("env")

	viper.AutomaticEnv() //환경 변수 자동 매핑

	if err := viper.ReadInConfig(); err != nil {
		logger.Warn(ctx, "config file not found, using environment variables", "error", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		logger.Error(ctx, "failed to unmarshal configuration", err)
		os.Exit(1)
	}

	logger.Info(ctx, "configuration loaded successfully")
	return &config
}

package config

import (
	"context"
	"os"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/spf13/viper"
)

type Config struct {
	Service           string `mapstructure:"SERVICE"`
	Env               string `mapstructure:"ENV"`
	ServerPort        string `mapstructure:"SERVER_PORT"`
	GrpcServerAddr    string `mapstructure:"GRPC_SERVER_ADDR"`
	EthSepoliaChainId string `mapstructure:"ETH_SEPOLIA_CHAIN_ID"`
	EthAlcRpcHttpUrl  string `mapstructure:"ETH_ALC_RPC_HTTP_URL"`
	EthAlcRpcWsUrl    string `mapstructure:"ETH_ALC_RPC_WS_URL"`
	EthInfRpcHttpUrl  string `mapstructure:"ETH_INF_RPC_HTTP_URL"`
	EthInfRpcWsUrl    string `mapstructure:"ETH_INF_RPC_WS_URL"`
	EthChaRpcHttpUrl  string `mapstructure:"ETH_CHA_RPC_HTTP_URL"`
	EthChaRpcWsUrl    string `mapstructure:"ETH_CHA_RPC_WS_URL"`
	WorkerCount       int    `mapstructure:"WORKER_COUNT"`
	TxStoreBlockTTL   uint64 `mapstructure:"TX_STORE_BLOCK_TTL"`
	MaxBlockCount     int    `mapstructure:"MAX_BLOCK_COUNT"`
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

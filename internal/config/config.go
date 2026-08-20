package config

import (
	"context"
	"os"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/ingestion"
	rpcmanager "github.com/JeongWoo-Seo/eth-mon-svr/internal/network/rpcManager"
	"github.com/spf13/viper"
)

type Config struct {
	Service           string
	Env               string
	ServerPort        string
	GrpcServerAddr    string
	EthSepoliaChainId string

	TxStoreBlockTTL uint64
	MaxBlockCount   int

	RPCs map[string]string
	WSs  []ingestion.Provider
}

type rawConfig struct {
	Service           string `mapstructure:"SERVICE"`
	Env               string `mapstructure:"ENV"`
	ServerPort        string `mapstructure:"SERVER_PORT"`
	GrpcServerAddr    string `mapstructure:"GRPC_SERVER_ADDR"`
	EthSepoliaChainId string `mapstructure:"ETH_SEPOLIA_CHAIN_ID"`

	AlcHTTP string `mapstructure:"ETH_ALC_RPC_HTTP_URL"`
	AlcWS   string `mapstructure:"ETH_ALC_RPC_WS_URL"`

	ChaHTTP string `mapstructure:"ETH_CHA_RPC_HTTP_URL"`
	ChaWS   string `mapstructure:"ETH_CHA_RPC_WS_URL"`

	InfHTTP string `mapstructure:"ETH_INF_RPC_HTTP_URL"`
	InfWS   string `mapstructure:"ETH_INF_RPC_WS_URL"`

	TxStoreBlockTTL uint64 `mapstructure:"TX_STORE_BLOCK_TTL"`
	MaxBlockCount   int    `mapstructure:"MAX_BLOCK_COUNT"`
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

	var raw rawConfig
	if err := viper.Unmarshal(&raw); err != nil {
		logger.Error(ctx, "failed to unmarshal configuration", err)
		os.Exit(1)
	}

	rpcs := map[string]string{
		rpcmanager.ProviderAlchemy:    raw.AlcHTTP,
		rpcmanager.ProviderChainstack: raw.ChaHTTP,
	}

	wss := []ingestion.Provider{
		{
			Name: ingestion.ProviderAlchemy,
			Url:  raw.AlcWS,
		},
		{
			Name: ingestion.ProviderChainstack,
			Url:  raw.ChaWS,
		},
	}

	return &Config{
		Service:           raw.Service,
		Env:               raw.Env,
		ServerPort:        raw.ServerPort,
		GrpcServerAddr:    raw.GrpcServerAddr,
		EthSepoliaChainId: raw.EthSepoliaChainId,
		TxStoreBlockTTL:   raw.TxStoreBlockTTL,
		MaxBlockCount:     raw.MaxBlockCount,
		RPCs:              rpcs,
		WSs:               wss,
	}
}

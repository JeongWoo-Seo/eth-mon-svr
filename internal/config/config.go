package config

import (
	"os"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/ingestion"
	rpcmanager "github.com/JeongWoo-Seo/eth-mon-svr/internal/network/rpcManager"
	"github.com/ethereum/go-ethereum/log"
	"github.com/spf13/viper"
)

type Config struct {
	Service          string
	Env              string
	ServerPort       string
	AuthClientSecret string

	GrpcServerAddr    string
	EthSepoliaChainId string

	RPCs map[string]string
	WSs  []ingestion.Provider
}

// 외부 설정 파일 / 환경변수와 매핑되는 구조체
type rawConfig struct {
	Service          string `mapstructure:"SERVICE"`
	Env              string `mapstructure:"ENV"`
	ServerPort       string `mapstructure:"SERVER_PORT"`
	AuthClientSecret string `mapstructure:"AUTH_CLIENT_SECRET"`

	GrpcServerAddr    string `mapstructure:"GRPC_SERVER_ADDR"`
	EthSepoliaChainId string `mapstructure:"ETH_SEPOLIA_CHAIN_ID"`

	AlcHTTP string `mapstructure:"ETH_ALC_RPC_HTTP_URL"`
	AlcWS   string `mapstructure:"ETH_ALC_RPC_WS_URL"`

	ChaHTTP string `mapstructure:"ETH_CHA_RPC_HTTP_URL"`
	ChaWS   string `mapstructure:"ETH_CHA_RPC_WS_URL"`

	InfHTTP string `mapstructure:"ETH_INF_RPC_HTTP_URL"`
	InfWS   string `mapstructure:"ETH_INF_RPC_WS_URL"`
}

func LoadConfig() *Config {
	// --------------------------------------------------
	// 1. 로컬 환경에서는 .env 파일 사용
	// --------------------------------------------------
	viper.SetConfigFile(".env")
	viper.SetConfigType("env")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Warn(
				"failed to read config file",
				"error", err,
			)
		}
	}

	// --------------------------------------------------
	// 2. 환경변수 연결
	//
	// Docker Compose에서는 container 환경변수가
	// .env 파일보다 높은 우선순위로 적용됨
	// --------------------------------------------------
	envKeys := []string{
		"SERVICE",
		"ENV",
		"SERVER_PORT",
		"AUTH_CLIENT_SECRET",

		"GRPC_SERVER_ADDR",
		"ETH_SEPOLIA_CHAIN_ID",

		"ETH_ALC_RPC_HTTP_URL",
		"ETH_ALC_RPC_WS_URL",

		"ETH_CHA_RPC_HTTP_URL",
		"ETH_CHA_RPC_WS_URL",

		"ETH_INF_RPC_HTTP_URL",
		"ETH_INF_RPC_WS_URL",
	}

	for _, key := range envKeys {
		if err := viper.BindEnv(key); err != nil {
			log.Error(
				"failed to bind environment variable",
				"key", key,
				"error", err,
			)
			os.Exit(1)
		}
	}

	// --------------------------------------------------
	// 3. 외부 설정 → rawConfig
	// --------------------------------------------------
	var raw rawConfig

	if err := viper.Unmarshal(&raw); err != nil {
		log.Error(
			"failed to unmarshal configuration",
			"error", err,
		)
		os.Exit(1)
	}

	// --------------------------------------------------
	// 5. RPC 설정 생성
	// --------------------------------------------------
	rpcs := map[string]string{
		rpcmanager.ProviderAlchemy:    raw.AlcHTTP,
		rpcmanager.ProviderChainstack: raw.ChaHTTP,
	}

	// --------------------------------------------------
	// 6. WebSocket Provider 설정 생성
	// --------------------------------------------------
	wss := []ingestion.Provider{
		{
			Name: rpcmanager.ProviderAlchemy,
			Url:  raw.AlcWS,
		},
		{
			Name: rpcmanager.ProviderChainstack,
			Url:  raw.ChaWS,
		},
	}

	// --------------------------------------------------
	// 7. 애플리케이션용 Config 생성
	// --------------------------------------------------
	return &Config{
		Service:           raw.Service,
		Env:               raw.Env,
		ServerPort:        raw.ServerPort,
		AuthClientSecret:  raw.AuthClientSecret,
		GrpcServerAddr:    raw.GrpcServerAddr,
		EthSepoliaChainId: raw.EthSepoliaChainId,

		RPCs: rpcs,
		WSs:  wss,
	}
}

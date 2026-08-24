#변수 설정
MAIN_PATH=./cmd/server/main.go

#개발 실행
server:
	go run ${MAIN_PATH}

#    - go_out: 데이터 구조체(struct) 생성 및 상대 경로 지정
#    - go-grpc_out: gRPC 서비스(service) 인터페이스 생성 및 상대 경로 지정
proto:
	rm ./internal/pb/*.go || true
	protoc --proto_path=./api/proto \
	   --go_out=paths=source_relative:./internal/pb \
	   --go-grpc_out=paths=source_relative:./internal/pb \
	   ./api/proto/*.proto

.PHONY: server proto
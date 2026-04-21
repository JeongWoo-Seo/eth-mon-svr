#변수 설정
MAIN_PATH=./cmd/server/main.go

#개발 실행
server:
	go run ${MAIN_PATH}

.PHONY: server
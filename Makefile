generate:
	protoc --go_out=internal/generated --go_opt=paths=source_relative \
	--go-grpc_out=internal/generated --go-grpc_opt=paths=source_relative \
	--queque-ymq_out=internal/generated --queque-ymq_opt=paths=source_relative,msg=Msg \
	proto/*/*.proto


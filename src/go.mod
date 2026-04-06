module github.com/Mekruba/gorums-benchmark

go 1.26.0

replace github.com/relab/gorums => ../gorums

require (
	github.com/joho/godotenv v1.5.1
	github.com/relab/gorums v0.11.0
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/kr/text v0.2.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260319201613-d00831a3d3e7 // indirect
)

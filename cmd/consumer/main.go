package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/dr3dnought/queque_ymq_protoc_plugin/client"
	"github.com/dr3dnought/queque_ymq_protoc_plugin/internal/generated/proto/popa"
	types "github.com/dr3dnought/quequetypes"
)

var wasNack = false

func main() {
		AccessKey:       "",
		SecretAccessKey: "",
	cfg := &types.Config{
		QueueName:       "oleg",
		Region:          "ru-central1",
		BaseUrl:         "https://message-queue.api.cloud.yandex.net",
		MaxRetryCount:   4,
		RetryTimestep:   1,
	}

	tr := &http.Transport{
		IdleConnTimeout:     10 * time.Minute,
		TLSHandshakeTimeout: 10 * time.Minute,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Minute,
			KeepAlive: 10 * time.Minute,
		}).DialContext,
	}

	httpClient := http.Client{
		Transport: tr,
	}
	cl := client.New(cfg, &httpClient)

	ctx := context.Background()

	for {
		err := cl.Consume(ctx, func(ctx context.Context, msg *popa.Popa, m2 *types.Meta) types.Result {
			fmt.Println(msg.Name)
			if msg.Name == "1" {
				return types.ACK
			}
			if msg.Name == "2" {
				if !wasNack {
					wasNack = true
					return types.NACK
				}
				return types.ACK
			}
			return types.DEFER
		})

		if err != nil {
			panic(err)
		}

	}

}

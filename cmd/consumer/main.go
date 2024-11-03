package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/dr3dnought/queque_ymq_protoc_plugin/client"
	"github.com/dr3dnought/queque_ymq_protoc_plugin/config"
	"github.com/dr3dnought/queque_ymq_protoc_plugin/types"
)

var wasNack = false

func main() {
	cfg := &config.Config{
		AccessKey:       "",
		SecretAccessKey: "",
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
		err := cl.Consume(ctx, func(ctx context.Context, msg client.Message, m2 *types.Meta) types.Result {
			fmt.Println(msg.Info)
			if msg.Info == "1" {
				return types.ACK
			}
			if msg.Info == "2" {
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

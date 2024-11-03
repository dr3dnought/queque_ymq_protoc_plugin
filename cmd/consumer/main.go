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
		err := cl.Consume(ctx, func(ctx context.Context, m1 client.Message, m2 *types.Meta) (types.Result, error) {
			fmt.Println(m1.Info)
			return types.DEFER, nil
		})

		if err != nil {
			panic(err)
		}

	}

}

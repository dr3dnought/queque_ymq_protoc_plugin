package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/dr3dnought/queque_ymq_protoc_plugin/client"
	"github.com/dr3dnought/queque_ymq_protoc_plugin/internal/generated/proto/popa"
	types "github.com/dr3dnought/quequetypes"
)

var (
	msg = flag.String("msg", "1", "")
)

func main() {
	flag.Parse()
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

	for range 10 {
		msgs := make([]*popa.Popa, 0, 10)
		for range 10 {
			info := rand.Intn(2)
			info += 1
			strinfo := strconv.Itoa(info)
			msgs = append(msgs, &popa.Popa{
				Name: strinfo,
			})
		}

		err := cl.Produce(ctx, msgs...)
		if err != nil {
			log.Fatal(err)
			return
		}
	}

	log.Println("zaebis")

}

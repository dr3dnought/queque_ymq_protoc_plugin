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
	"github.com/dr3dnought/queque_ymq_protoc_plugin/config"
)

var (
	msg = flag.String("msg", "1", "")
)

func main() {
	flag.Parse()
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

	for range 10 {
		msgs := make([]client.Message, 0, 100)
		for range 10 {
			info := rand.Intn(2)
			info += 1
			strinfo := strconv.Itoa(info)
			msgs = append(msgs, client.Message{
				Info: strinfo,
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

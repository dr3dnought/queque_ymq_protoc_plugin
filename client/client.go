package client

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/dr3dnought/gospadi"
	"github.com/dr3dnought/queque_ymq_protoc_plugin/config"
	"github.com/dr3dnought/queque_ymq_protoc_plugin/types"
	"github.com/google/uuid"
)

const (
	attemptNumberAttributeKey = "ApproximateReceiveCount"
)

type Message struct {
	Info string
}

type Client struct {
	cfg      *config.Config
	queueUrl string
	mu       sync.RWMutex
	sqs      *sqs.Client
}

func New(cfg *config.Config, httpClient *http.Client) *Client {

	sqsConfig := aws.NewConfig()
	sqsConfig.Region = cfg.Region
	sqsConfig.Credentials = credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretAccessKey, "")
	sqsConfig.HTTPClient = httpClient
	sqsConfig.BaseEndpoint = &cfg.BaseUrl

	client := sqs.NewFromConfig(*sqsConfig)
	return &Client{
		cfg: cfg,
		sqs: client,
	}
}

func (c *Client) createQueue(ctx context.Context) error {
	c.mu.Lock()

	if c.queueUrl != "" {
		c.mu.Unlock()
		return nil
	}

	queue, err := c.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: &c.cfg.QueueName,
	})
	if err != nil {
		c.mu.Unlock()
		return err
	}

	c.queueUrl = *queue.QueueUrl
	c.mu.Unlock()
	return nil
}

func (c *Client) ensureConnection(ctx context.Context) error {
	c.mu.RLock()

	if c.queueUrl == "" {
		c.mu.RUnlock()
		return c.createQueue(ctx)
	}

	c.mu.RUnlock()
	return nil
}

func (c *Client) Produce(ctx context.Context, msgs ...Message) error {

	err := c.ensureConnection(ctx)
	if err != nil {
		return err
	}

	entries, err := gospadi.MapErr(msgs, func(m Message) (awstypes.SendMessageBatchRequestEntry, error) {
		bytes, err := json.Marshal(m)
		if err != nil {
			return awstypes.SendMessageBatchRequestEntry{}, err
		}
		return awstypes.SendMessageBatchRequestEntry{
			DelaySeconds: 0,
			Id:           aws.String(uuid.New().String()),
			MessageBody:  aws.String(string(bytes)),
		}, nil
	})

	if err != nil {
		return err
	}

	_, err = c.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &c.queueUrl,
		Entries:  entries,
	})

	return err
}

func (c *Client) handleMessage(ctx context.Context, msg awstypes.Message, handler types.ConsumerFunc[Message]) (types.Result, int, error) {

	dest := new(Message)
	err := json.Unmarshal([]byte(*msg.Body), dest)
	if err != nil {
		return -1, -1, err
	}

	attemptCount := 0
	v, ok := msg.Attributes[attemptNumberAttributeKey]
	if ok {
		attemptCount, err = strconv.Atoi(v)
		if err != nil {
			return -1, -1, err
		}
	}

	result, err := handler(ctx, *dest, &types.Meta{
		AttemptCount: attemptCount,
	})
	if err != nil {
		return -1, -1, err
	}

	return result, attemptCount, nil
}

func (c *Client) Consume(ctx context.Context, handler types.ConsumerFunc[Message]) error {
	err := c.ensureConnection(ctx)
	if err != nil {
		return err
	}

	received, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:        &c.queueUrl,
		WaitTimeSeconds: 10,
	})

	for _, msg := range received.Messages {
		result, attemptCount, err := c.handleMessage(ctx, msg, handler)
		if err != nil {
			return err
		}

		switch result {
		case types.ACK:
			_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &c.queueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				return err
			}
		case types.NACK:
			_, err := c.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &c.queueUrl,
				ReceiptHandle:     msg.ReceiptHandle,
				VisibilityTimeout: 0,
			})
			if err != nil {
				return err
			}
		case types.DEFER:
			if attemptCount > c.cfg.MaxRetryCount {
				_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      &c.queueUrl,
					ReceiptHandle: msg.ReceiptHandle,
				})

				return err
			}

			_, err := c.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &c.queueUrl,
				ReceiptHandle:     msg.ReceiptHandle,
				VisibilityTimeout: int32(c.cfg.RetryTimestep) * int32(attemptCount),
			})
			if err != nil {
				return err
			}
		}
	}

	return nil

}

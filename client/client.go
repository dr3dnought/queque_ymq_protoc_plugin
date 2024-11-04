package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/dr3dnought/gospadi"
	types "github.com/dr3dnought/quequetypes"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const (
	attemptNumberAttributeKey = "ApproximateReceiveCount"
)

type Message struct {
	Info string
}

type Client struct {
	cfg      *types.Config
	queueUrl string
	mu       sync.RWMutex
	sqs      *sqs.Client
}

func New(cfg *types.Config, httpClient *http.Client) *Client {
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

func (c *Client) Consume(ctx context.Context, handler types.ConsumerFunc[Message]) error {
	err := c.ensureConnection(ctx)
	if err != nil {
		return err
	}

	received, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &c.queueUrl,
		WaitTimeSeconds:     10,
		MaxNumberOfMessages: 10,
	})
	if err != nil {
		return err
	}

	dirtyErrors := make([]error, len(received.Messages))

	type handlerOutput struct {
		msg        awstypes.Message
		retryCount int
	}

	acks := make([]*handlerOutput, len(received.Messages))
	nacks := make([]*handlerOutput, len(received.Messages))

	var wg sync.WaitGroup
	wg.Add(len(received.Messages))

	for i, msg := range received.Messages {
		go func(i int, msg awstypes.Message) {
			result, attemptCount, err := c.handleMessage(ctx, msg, handler)
			if err != nil {
				dirtyErrors[i] = err
				wg.Done()
				return
			}

			switch result {
			case types.ACK:
				acks[i] = &handlerOutput{
					msg: msg,
				}
			case types.NACK:
				nacks[i] = &handlerOutput{
					msg:        msg,
					retryCount: 0,
				}
			case types.DEFER:
				if attemptCount > c.cfg.MaxRetryCount {
					acks[i] = &handlerOutput{
						msg: msg,
					}
				} else {
					nacks[i] = &handlerOutput{
						msg:        msg,
						retryCount: attemptCount,
					}
				}
			}
			wg.Done()
		}(i, msg)
	}

	wg.Wait()

	var commonErrors error
	if clearErrors := slices.DeleteFunc(dirtyErrors, func(err error) bool {
		return err == nil
	}); len(clearErrors) > 0 {
		commonErrors = errors.Join(clearErrors...)
	}

	if clearAcks := slices.DeleteFunc(acks, func(ouput *handlerOutput) bool {
		return ouput == nil
	}); len(clearAcks) > 0 {
		_, err := c.sqs.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: &c.queueUrl,
			Entries: gospadi.Map(clearAcks, func(output *handlerOutput) awstypes.DeleteMessageBatchRequestEntry {
				return awstypes.DeleteMessageBatchRequestEntry{
					Id:            output.msg.MessageId,
					ReceiptHandle: output.msg.ReceiptHandle,
				}
			}),
		})

		if err != nil {
			return err
		}
	}

	if clearNacks := slices.DeleteFunc(nacks, func(ouput *handlerOutput) bool {
		return ouput == nil
	}); len(clearNacks) > 0 {
		_, err := c.sqs.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: &c.queueUrl,
			Entries: gospadi.Map(clearNacks, func(output *handlerOutput) awstypes.ChangeMessageVisibilityBatchRequestEntry {
				return awstypes.ChangeMessageVisibilityBatchRequestEntry{
					Id:                output.msg.MessageId,
					ReceiptHandle:     output.msg.ReceiptHandle,
					VisibilityTimeout: int32(output.retryCount) * int32(c.cfg.RetryTimestep),
				}
			}),
		})

		if err != nil {
			return err
		}
	}

	return commonErrors

}

func (c *Client) handleMessage(ctx context.Context, msg awstypes.Message, handler types.ConsumerFunc[Message]) (types.Result, int, error) {

	dest := new(Message)
	err := proto.Unmarshal([]byte(*msg.Body), dest)
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

	result := handler(ctx, *dest, &types.Meta{
		AttemptCount: attemptCount,
	})
	if err != nil {
		return -1, -1, err
	}

	return result, attemptCount, nil
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

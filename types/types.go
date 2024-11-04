package types

import "context"

type Result int

const (
	ACK Result = iota
	NACK
	DEFER
)

type Meta struct {
	AttemptCount int
}

type ConsumerFunc[T any] func(context.Context, T, *Meta) Result

type Consumer[T any] interface {
	Consume(ctx context.Context, handler ConsumerFunc[T]) error
}

type Producer[T any] interface {
	Produce(ctx context.Context, msg T) error
}

type QueuesClient[T any] interface {
	Consumer[T]
	Producer[T]
}

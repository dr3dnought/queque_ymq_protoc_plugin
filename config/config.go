package config

type Config struct {
	AccessKey       string
	SecretAccessKey string
	QueueName       string
	BatchSize       int
	MaxRetryCount   int
	RetryTimestep   int
	Region          string
	BaseUrl         string
}

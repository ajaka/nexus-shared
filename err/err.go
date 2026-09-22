package err

import "errors"

var (
	ErrInvalidPollDuration error = errors.New("Pollduration should be greater than zero")
	ErrInvalidBatchSize    error = errors.New("Batch size must be greater than zero")
	ErrKafkaTopicEmpty     error = errors.New("Kafka Topic must be non empty")
)

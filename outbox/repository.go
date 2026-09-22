package outbox

import "context"

// Record represents a single row in the outbox table.
type Record struct {
	ID        int64
	EventName string
	Payload   []byte
}

// Repository defines what the outbox worker needs from the database.
type Repository interface {
	FetchPending(ctx context.Context, limit int) ([]Record, error)
	MarkAsSending(ctx context.Context, ids []int64) error
	MarkAsProcessed(ctx context.Context, id int64) error
	MarkAsFailed(ctx context.Context, id int64, reason string) error
}

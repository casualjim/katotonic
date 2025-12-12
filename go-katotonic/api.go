package katotonic

import "github.com/google/uuid"

type IDGenerator interface {
	NextId() (uuid.UUID, error)
}

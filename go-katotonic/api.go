package katotonic

import "github.com/oklog/ulid/v2"

type IDGenerator interface {
	NextId() (ulid.ULID, error)
}

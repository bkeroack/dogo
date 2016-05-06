package cmd

// BadQuery is implemented by any error returned as a result of malformed
// query (syntax error, etc).
//
// Use by doing a checked type assertion (below assumes err != nil):
//
// bq, ok := err.(BadQuery)
// if ok && bq.BadQuery() {
//		// error was caused by a bad query
// } else {
//    // not a bad query error
// }
//
// see: http://dave.cheney.net/2014/12/24/inspecting-errors
type BadQuery interface {
	BadQuery() bool
}

type badQueryError struct {
	err error
}

func (bqe badQueryError) BadQuery() bool {
	return true
}

func (bqe badQueryError) Error() string {
	return bqe.err.Error()
}

// InternalError is implemented by errors returned for internal/unspecified
// errors (see BadQuery for usage)
type InternalError interface {
	InternalError() bool
}

type internalErrorError struct {
	err error
}

func (iee internalErrorError) InternalError() bool {
	return true
}

func (iee internalErrorError) Error() string {
	return iee.err.Error()
}

// KeyExists is implemented by errors returned because a key insert that was
// attempted failed due to UNIQUE constraint violation
// (see BadQuery for usage)
type KeyExists interface {
	KeyExists() bool
}

type keyExistsError struct {
	err error
}

func (kee keyExistsError) KeyExist() bool {
	return true
}

func (kee keyExistsError) Error() string {
	return kee.err.Error()
}

func newKeyExistsError(err error) error {
	return keyExistsError{err: err}
}

func newBadQueryError(err error) error {
	return badQueryError{err: err}
}

func newInternalErrorError(err error) error {
	return internalErrorError{err: err}
}

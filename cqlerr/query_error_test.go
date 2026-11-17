package cqlerr

import (
	"errors"
	"testing"
	"time"
)

func TestQueryError(t *testing.T) {
	t.Parallel()

	t.Run("Error", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name                string
			err                 error
			potentiallyExecuted bool
			timeout             time.Duration
			inFlight            int
			expected            string
		}{
			{
				name:                "with potentially executed true",
				err:                 errors.New("connection error"),
				potentiallyExecuted: true,
				expected:            "connection error (maybe executed)",
			},
			{
				name:                "with potentially executed false",
				err:                 errors.New("syntax error"),
				potentiallyExecuted: false,
				expected:            "syntax error",
			},
			{
				name:                "with timeout",
				err:                 ErrTimeoutNoResponse,
				potentiallyExecuted: true,
				timeout:             11 * time.Second,
				inFlight:            42,
				expected:            "gocql: no response received from cassandra within timeout period (timeout: 11s, in-flight: 42) (maybe executed)",
			},
			{
				name:                "with zero timeout omits timeout",
				err:                 errors.New("some error"),
				potentiallyExecuted: false,
				timeout:             0,
				expected:            "some error",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				qErr := newQueryError(queryErrorState{
					InternalErr:                 tt.err,
					InternalPotentiallyExecuted: tt.potentiallyExecuted,
					InternalTimeout:             tt.timeout,
					InternalInFlight:            tt.inFlight,
				})

				got := qErr.Error()
				if got != tt.expected {
					t.Errorf("QueryError.Error() = %v, expected %v", got, tt.expected)
				}
			})
		}
	})
}

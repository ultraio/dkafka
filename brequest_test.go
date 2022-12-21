package dkafka

import (
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/streamingfast/bstream/forkable"
)

const opaqueCursor1 = "6QXTyGxzkcfhhxXa-8kWF6WzLpc-DFJmUQLgLBFEj4vz9XfM1Z6jBWNxaRuDxqDwjka-TA79jN2bHXspoJZRvYTrlbg25SM-RC8lmt_oqeXncKH3MV8Ydbw3C-KJY9nRUzXTaw_9c7AK4NDiP_rRbxA7Zc90LGLg2z9Y84dUJqIQ6ndnw22rJc_S0P-SoIdE_LF2RO2jliimBzF8eBtTOs-BZ_Kbuzp2MA=="

var cursor1 = cursorFromOpaque(opaqueCursor1)

const opaqueCursor2 = "hsDNMretWsuoLJ-5SF69saWzLpc-DFJmUQLgLBFFj4vz9XfM1Z6iBmRxbEmBwvqm2kS6Swup2dvLEHl8o8IEtNPpx-xkvyE9RHopxtru-7Xre6b7PFgbd-lhXu2JZNnRUzXTaw_9c7AK4NDiP_rRbxA7Zc90LGLg2z9Y84dUJqIQ6ndnw22rJc_S0P-SoIdE_LF2RO2jliimBzF8eBtTOs-BZ_Kbuzp2MA=="

var cursor2 = cursorFromOpaque(opaqueCursor2)

func cursorFromOpaque(in string) (cursor *forkable.Cursor) {
	cursor, _ = forkable.CursorFromOpaque(in)
	return
}

func Test_findPosition(t *testing.T) {
	type args struct {
		headers []kafka.Header
	}
	tests := []struct {
		name         string
		args         args
		wantPosition position
	}{
		{
			name:         "empty",
			args:         args{[]kafka.Header{}},
			wantPosition: position{},
		},
		{
			name: "only cursor",
			args: args{[]kafka.Header{{
				Key:   CursorHeaderKey,
				Value: []byte(opaqueCursor1),
			},
			},
			},
			wantPosition: position{
				opaqueCursor: opaqueCursor1,
			},
		},
		{
			name: "full cursors",
			args: args{[]kafka.Header{{
				Key:   CursorHeaderKey,
				Value: []byte(opaqueCursor1),
			},
				{
					Key:   PreviousCursorHeaderKey,
					Value: []byte(opaqueCursor2),
				},
				{
					Key:   CursorHeaderKey,
					Value: []byte(opaqueCursor1),
				},
			},
			},
			wantPosition: position{
				opaqueCursor:         opaqueCursor1,
				previousOpaqueCursor: opaqueCursor2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotPosition := findPosition(tt.args.headers); !reflect.DeepEqual(gotPosition, tt.wantPosition) {
				t.Errorf("findPosition() = %v, want %v", gotPosition, tt.wantPosition)
			}
		})
	}
}

func Test_position_gt(t *testing.T) {
	tests := []struct {
		name string
		this position
		that position
		want bool
	}{
		{
			name: "no previous this greater than that",
			this: position{
				cursor: cursor1,
			},
			that: position{
				cursor: cursor2,
			},
			want: true,
		},
		{
			name: "no previous this lower than that",
			this: position{
				cursor: cursor2,
			},
			that: position{
				cursor: cursor1,
			},
			want: false,
		},
		{
			name: "no previous this equals to that",
			this: position{
				cursor: cursor1,
			},
			that: position{
				cursor: cursor1,
			},
			want: false,
		},
		{
			name: "this with previous and not that",
			this: position{
				cursor:         cursor1,
				previousCursor: cursor2,
			},
			that: position{
				cursor: cursor2,
			},
			want: true,
		},
		{
			name: "this without previous but that",
			this: position{
				cursor: cursor1,
			},
			that: position{
				cursor:         cursor2,
				previousCursor: cursor2,
			},
			want: false,
		},
		{
			name: "this and that with previous with this gt that",
			this: position{
				previousCursor: cursor1,
			},
			that: position{
				previousCursor: cursor2,
			},
			want: true,
		},
		{
			name: "this and that with previous with that gt this",
			this: position{
				previousCursor: cursor2,
			},
			that: position{
				previousCursor: cursor1,
			},
			want: false,
		},
		{
			name: "this and that cursor empty",
			this: position{},
			that: position{},
			want: false,
		},
		{
			name: "this cursor empty and not that",
			this: position{},
			that: position{
				cursor: cursor1,
			},
			want: false,
		},
		{
			name: "this cursor not empty but that",
			this: position{
				cursor: cursor1,
			},
			that: position{},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.this.gt(tt.that); got != tt.want {
				t.Errorf("position.gt() = %v, want %v", got, tt.want)
			}
		})
	}
}

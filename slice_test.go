package dkafka

import (
	"reflect"
	"testing"
)

func TestReverse(t *testing.T) {
	tests := []struct {
		name string
		args []int
		want []int
	}{
		{
			name: "int slice reverse order",
			args: []int{1, 2, 3, 4, 5},
			want: []int{5, 4, 3, 2, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Reverse(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reverse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReverseInPlace(t *testing.T) {
	tests := []struct {
		name string
		args []int
		want []int
	}{
		{
			name: "int slice reverse order",
			args: []int{1, 2, 3, 4, 5},
			want: []int{5, 4, 3, 2, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReverseInPlace(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reverse() = %v, want %v", got, tt.want)
			}
		})
	}
}

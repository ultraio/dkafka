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
		{
			name: "int empty slice reverse order",
			args: []int{},
			want: []int{},
		},
		{
			name: "int singleton slice reverse order",
			args: []int{1},
			want: []int{1},
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
		{
			name: "int empty slice reverse order",
			args: []int{},
			want: []int{},
		},
		{
			name: "int singleton slice reverse order",
			args: []int{1},
			want: []int{1},
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

func TestNewIndexedEntrySlice(t *testing.T) {
	tests := []struct {
		name string
		args []int
		want []*IndexedEntry[int]
	}{
		{
			name: "index slice many",
			args: []int{1, 2, 3, 4, 5},
			want: []*IndexedEntry[int]{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}},
		},
		{
			name: "index singleton slice",
			args: []int{1},
			want: []*IndexedEntry[int]{{0, 1}},
		},
		{
			name: "index empty slice",
			args: []int{},
			want: []*IndexedEntry[int]{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewIndexedEntrySlice(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewIndexedEntrySlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

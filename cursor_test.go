package dkafka

import (
	"testing"

	"github.com/streamingfast/opaque"
)

func TestOpaque(t *testing.T) {
	// 2Yc5z3OwLqqIBgaffIdxcve7LJY_DFA9UgnhKkQS0N6ipXc=  => "1:807::b7240eceede5"
	cursor := "2Yc5z3OwLqqIBgaffIdxcve7LJY_DFA9UgnhKkQS0N6ipXc="
	out, err := opaque.FromOpaque(cursor)
	if err != nil {
		t.Errorf("opaque.FromOpaque: %v", err)
	}
	dout, _ := opaque.DecodeToString(cursor)
	if out != dout {
		t.Errorf("out: %s != dout: %s", out, dout)
	}
}

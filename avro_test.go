package dkafka

import (
	"testing"
)

func Test_checkName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"valid",
			args{
				"ValidName",
			},
			"ValidName",
			false,
		},
		{
			"invalid",
			args{
				"$ValidName",
			},
			"$ValidName",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkName(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("checkName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkNamespace(t *testing.T) {
	type args struct {
		np string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"empty",
			args{
				"",
			},
			"",
			false,
		},
		{
			"valid",
			args{
				"io.dkafka",
			},
			"io.dkafka",
			false,
		},
		{
			"invalid",
			args{
				"io::dkafka",
			},
			"io::dkafka",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkNamespace(tt.args.np)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("checkNamespace() = %v, want %v", got, tt.want)
			}
		})
	}
}

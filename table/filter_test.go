package table

import "testing"

func TestFilter(t *testing.T) {
	type args struct {
		account       string
		inlineSources []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "eosio.token-empty-inline",
			args: args{
				account:       "eosio.token",
				inlineSources: []string{},
			},
			want: `account=="eosio.token"`,
		},
		{
			name: "eosio.eba-nil-inline",
			args: args{
				account: "eosio.eba",
			},
			want: `account=="eosio.eba"`,
		},
		{
			name: "eosio.eba-1-empty-inline",
			args: args{
				account:       "eosio.eba",
				inlineSources: []string{""},
			},
			want: `account=="eosio.eba"`,
		},
		{
			name: "eosio.eba-1-inline",
			args: args{
				account:       "eosio.eba",
				inlineSources: []string{"eosio.token"},
			},
			want: `account=="eosio.eba" || account=="eosio.token"`,
		},
		{
			name: "eosio.eba-2-inline",
			args: args{
				account:       "eosio.eba",
				inlineSources: []string{"eosio.token", "eosio.nft.ft"},
			},
			want: `account=="eosio.eba" || account=="eosio.token" || account=="eosio.nft.ft"`,
		},
		{
			name: "eosio.token-duplicate-inline",
			args: args{
				account:       "eosio.token",
				inlineSources: []string{"eosio.token"},
			},
			want: `account=="eosio.token"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Filter(tt.args.account, tt.args.inlineSources); got != tt.want {
				t.Errorf("Filter() = '%v', want '%v'", got, tt.want)
			}
		})
	}
}

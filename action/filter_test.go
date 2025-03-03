package action

import "testing"

func TestFilter(t *testing.T) {
	type args struct {
		account string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "eosio.token",
			args: args{
				account: "eosio.token",
			},
			want: `(account=="eosio.token" && receiver=="eosio.token")`,
		},
		{
			name: "eosio.eba",
			args: args{
				account: "eosio.eba",
			},
			want: `(account=="eosio.eba" && receiver=="eosio.eba")`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Filter(tt.args.account); got != tt.want {
				t.Errorf("Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

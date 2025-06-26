package table

import "fmt"

func Filter(account string) string {
	return fmt.Sprintf("account==\"%s\" || account==\"eosio.token\"", account)
}

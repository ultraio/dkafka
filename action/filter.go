package action

import "fmt"

func Filter(account string) string {
	return fmt.Sprintf("(account==\"%s\" && receiver==\"%s\")", account, account)
}

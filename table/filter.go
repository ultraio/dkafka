package table

import "fmt"

func Filter(account string, inlineSources []string) string {
	filter := fmt.Sprintf("account==\"%s\"", account)
	for _, source := range inlineSources {
		if source != "" && source != account {
			filter += fmt.Sprintf(" || account==\"%s\"", source)
		}
	}
	return filter
}

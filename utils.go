package dbgo

import "strings"

func DBNameFromOrg(org string) string {
	return strings.ToLower(strings.ReplaceAll(org, "o-", "db"))
}

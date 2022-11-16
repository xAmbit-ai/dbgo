package dbgo

import (
	"os"
	"strings"
)

func DBNameFromOrg(org string) string {
	return strings.ToLower(strings.ReplaceAll(org, "o-", "db"))
}

func getZoneCert() string {
	if os.Getenv("X_ENV") == "prod" {
		return "roach.crt"
	} else {
		return "dev-roach.crt"
	}
}

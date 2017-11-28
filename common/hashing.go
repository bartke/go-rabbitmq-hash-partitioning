package common

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
)

var RouteKeys string = "abcdefgh" //ijklmnopqrstuvwxyz"

func Hash(input string) string {
	i, _ := strconv.Atoi(input)
	return string(byte(i%len(RouteKeys) + 97))
}

func secureRandom(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

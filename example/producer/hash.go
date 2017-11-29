package main

import "strconv"

// all possible types
var routeKeys []string = []string{"a", "b", "c", "d", "e", "f", "g", "h"}

// hash function to map payload to type
func hash(input string) string {
	i, _ := strconv.Atoi(input)
	return string(byte(i%len(routeKeys) + 97))
}

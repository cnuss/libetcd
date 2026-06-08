// Command basic is the smallest golib example: build a value through the
// generic builder and print the result.
package main

import (
	"fmt"

	"github.com/cnuss/golib"
)

func main() {
	res := golib.New[string]().
		WithName("greeting").
		WithValue("hello world").
		Build()

	fmt.Printf("%s: %s\n", res.Name, res.Value)
}

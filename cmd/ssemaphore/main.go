package main

import (
	"os"

	"github.com/omar07ibrahim/ssemaphore/internal/app"
)

func main() {
	os.Exit(app.Main(os.Args[1:], os.Stdout, os.Stderr))
}

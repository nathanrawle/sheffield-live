package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func main() {
	body, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read passphrase: %v\n", err)
		os.Exit(1)
	}
	passphrase := strings.TrimRight(string(body), "\r\n")
	if passphrase == "" {
		fmt.Fprintln(os.Stderr, "passphrase is required on stdin")
		os.Exit(1)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(passphrase), 12)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hash passphrase: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(hash))
}

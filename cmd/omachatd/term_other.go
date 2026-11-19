//go:build !linux

package main

import "errors"

func readSecret(string) ([]byte, error) {
	return nil, errors.New("reading a PIN without echo is only supported on Linux")
}

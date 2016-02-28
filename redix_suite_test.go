package redix_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestRedix(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redix Suite")
}

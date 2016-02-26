package resp_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestResp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Resp Suite")
}

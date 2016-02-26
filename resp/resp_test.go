package resp_test

import (
	"github.com/kevin-cantwell/haredis/resp"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HumanReadable", func() {
	It("Should parse simple strings.", func() {
		args, _ := resp.HumanReadable([]byte("+This is a SIMPLE STRING\r\n+OK"))
		Expect(args).To(Equal([][]byte{[]byte("This is a SIMPLE STRING"), []byte("OK")}))
	})
	// It("Should parse bulk strings.", func() {
	//   args := resp.HumanReadable("$This is a BULK STRING\r\n+OK")
	//   Expect(args).To(Equal([]string{"This is a BULK STRING", "OK"}))
	// })
})

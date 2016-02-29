package redix_test

import (
	"bytes"

	"github.com/kevin-cantwell/redix"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ParseResp", func() {
	Context("Valid RESP", func() {
		It("Should parse integers.", func() {
			// Max signed 64 bit
			resp, err := redix.NewReader(bytes.NewReader([]byte(":9223372036854775807\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("9223372036854775807"))

			// Minus one, which is the null value for bulk strings
			resp, err = redix.NewReader(bytes.NewReader([]byte(":-1\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("-1"))

			// Allow negation symbol with zero (not defined in spec, we just allow it)
			resp, err = redix.NewReader(bytes.NewReader([]byte(":-0\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("0"))

			// Zero
			resp, err = redix.NewReader(bytes.NewReader([]byte(":0\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("0"))

			// Min singed 64 bit
			resp, err = redix.NewReader(bytes.NewReader([]byte(":-9223372036854775808\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("-9223372036854775808"))
		})
		It("Should parse simple strings.", func() {
			resp, err := redix.NewReader(bytes.NewReader([]byte("+OK\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("OK"))

			// Empty strings are valid
			resp, err = redix.NewReader(bytes.NewReader([]byte("+\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal(""))
		})
		It("Should parse errors.", func() {
			resp, err := redix.NewReader(bytes.NewReader([]byte("-ERR something\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("ERR something"))

			// Empty strings are valid
			resp, err = redix.NewReader(bytes.NewReader([]byte("-\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal(""))
		})
		It("Should parse bulk strings.", func() {
			resp, err := redix.NewReader(bytes.NewReader([]byte("$10\r\n1234\r\n7890\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("1234\r\n7890"))

			// Empty strings are valid
			resp, err = redix.NewReader(bytes.NewReader([]byte("$0\r\n\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal(""))

			// Null Bulk String
			resp, err = redix.NewReader(bytes.NewReader([]byte("$-1\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(resp[0]).To(BeNil())
		})
		It("Should parse arrays.", func() {
			// Empty array
			resp, err := redix.NewReader(bytes.NewReader([]byte("*0\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(resp).To(Equal([][]byte{}))

			// Null Array
			resp, err = redix.NewReader(bytes.NewReader([]byte("*-1\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(resp).To(BeNil())

			// Array of three integers
			resp, err = redix.NewReader(bytes.NewReader([]byte("*3\r\n:1\r\n:2\r\n:3\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(resp).To(HaveLen(3))
			Expect(string(resp[0])).To(Equal("1"))
			Expect(string(resp[1])).To(Equal("2"))
			Expect(string(resp[2])).To(Equal("3"))

			// Array of mixed types
			resp, err = redix.NewReader(bytes.NewReader([]byte("*4\r\n:1\r\n+OK\r\n-ERR\r\n$6\r\nhey\nho\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(resp).To(HaveLen(4))
			Expect(string(resp[0])).To(Equal("1"))
			Expect(string(resp[1])).To(Equal("OK"))
			Expect(string(resp[2])).To(Equal("ERR"))
			Expect(string(resp[3])).To(Equal("hey\nho"))

			// Make sure :+-$ chars may appear in bulk strings
			resp, err = redix.NewReader(bytes.NewReader([]byte("*1\r\n$4\r\n:+-$\r\n"))).ParseObject()
			Expect(err).To(BeNil())
			Expect(resp).To(HaveLen(1))
			Expect(string(resp[0])).To(Equal(":+-$"))

			// _, err = redix.NewReader(bytes.NewReader([]byte("*4\r\n$6\r\nLRANGE\r\n$28\r\nsampler:screenshots:20160229\r\n$1\r\n0\r\n$2\r\n-1\r\n"))).ParseObject()
			// Expect(err).To(BeNil())
		})
	})
	Context("Invalid RESP", func() {
		It("Should validate integers.", func() {
			// Check for a value
			_, err := redix.NewReader(bytes.NewReader([]byte(":\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))

			// Check for CRLF
			_, err = redix.NewReader(bytes.NewReader([]byte(":123\r"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`EOF`))

			// Check positive 64-bit boundary +1
			_, err = redix.NewReader(bytes.NewReader([]byte(":9223372036854775808\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))

			// Check negative 64-bit boundary -1
			_, err = redix.NewReader(bytes.NewReader([]byte(":-9223372036854775809\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))
		})
		It("Should validate simple strings.", func() {
			// Check for CR
			_, err := redix.NewReader(bytes.NewReader([]byte("+No\rCR\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))

			// Check for LF
			_, err = redix.NewReader(bytes.NewReader([]byte("+No\nLF\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))

			// Check for terminating CRLF
			_, err = redix.NewReader(bytes.NewReader([]byte("+NOTOK\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))
		})
		It("Should validate errors.", func() {
			// Check for CR
			_, err := redix.NewReader(bytes.NewReader([]byte("-No\rCR\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))

			// Check for LF
			_, err = redix.NewReader(bytes.NewReader([]byte("-NOTOK\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))
		})
		It("Should validate bulk strings.", func() {
			_, err := redix.NewReader(bytes.NewReader([]byte("$123"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`EOF`))

			r := redix.NewReader(bytes.NewReader([]byte("$3\r\n123")))
			_, err = r.ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`EOF`))

			_, err = redix.NewReader(bytes.NewReader([]byte("%3\r\nfoo\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))

			_, err = redix.NewReader(bytes.NewReader([]byte("$three\r\nfoo\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))

			_, err = redix.NewReader(bytes.NewReader([]byte("$9\r\n1234\r\n7890\r\n"))).ParseObject()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: invalid syntax`))
		})
	})
})

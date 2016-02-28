package redix_test

import (
	"github.com/kevin-cantwell/redix"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ParseResp", func() {
	Context("Valid RESP", func() {
		It("Should parse integers.", func() {
			// Max signed 64 bit
			resp, err := redix.ParseRESP([]byte(":9223372036854775807\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("9223372036854775807"))

			// Minus one, which is the null value for bulk strings
			resp, err = redix.ParseRESP([]byte(":-1\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("-1"))

			// Allow negation symbol with zero (not defined in spec, we just allow it)
			resp, err = redix.ParseRESP([]byte(":-0\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("0"))

			// Zero
			resp, err = redix.ParseRESP([]byte(":0\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("0"))

			// Min singed 64 bit
			resp, err = redix.ParseRESP([]byte(":-9223372036854775808\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("-9223372036854775808"))
		})
		It("Should parse simple strings.", func() {
			resp, err := redix.ParseRESP([]byte("+OK\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("OK"))

			// Empty strings are valid
			resp, err = redix.ParseRESP([]byte("+\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal(""))
		})
		It("Should parse errors.", func() {
			resp, err := redix.ParseRESP([]byte("-ERR something\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("ERR something"))

			// Empty strings are valid
			resp, err = redix.ParseRESP([]byte("-\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal(""))
		})
		It("Should parse bulk strings.", func() {
			resp, err := redix.ParseRESP([]byte("$10\r\n1234\r\n7890\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal("1234\r\n7890"))

			// Empty strings are valid
			resp, err = redix.ParseRESP([]byte("$0\r\n\r\n"))
			Expect(err).To(BeNil())
			Expect(string(resp[0])).To(Equal(""))

			// Null Bulk String
			resp, err = redix.ParseRESP([]byte("$-1\r\n"))
			Expect(err).To(BeNil())
			Expect(resp[0]).To(BeNil())
		})
		FIt("Should parse arrays.", func() {
			// Empty array
			resp, err := redix.ParseRESP([]byte("*0\r\n"))
			Expect(err).To(BeNil())
			Expect(resp).To(Equal([][]byte{}))

			// Null Array
			resp, err = redix.ParseRESP([]byte("*-1\r\n"))
			Expect(err).To(BeNil())
			Expect(resp).To(BeNil())

			// Array of three integers
			resp, err = redix.ParseRESP([]byte("*3\r\n:1\r\n:2\r\n:3\r\n"))
			Expect(err).To(BeNil())
			Expect(resp).To(HaveLen(3))
			Expect(string(resp[0])).To(Equal("1"))
			Expect(string(resp[1])).To(Equal("2"))
			Expect(string(resp[2])).To(Equal("3"))

			// Array of mixed types
			resp, err = redix.ParseRESP([]byte("*4\r\n:1\r\n+OK\r\n-ERR\r\n$6\r\nhey\nho\r\n"))
			Expect(err).To(BeNil())
			Expect(resp).To(HaveLen(4))
			Expect(string(resp[0])).To(Equal("1"))
			Expect(string(resp[1])).To(Equal("OK"))
			Expect(string(resp[2])).To(Equal("ERR"))
			Expect(string(resp[3])).To(Equal("hey\nho"))
		})
	})
	Context("Invalid RESP", func() {
		It("Should validate integers.", func() {
			// Check for a value
			_, err := redix.ParseRESP([]byte(":\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: ":\r\n" must contain a value and end with CRLF`))

			// Check for CRLF
			_, err = redix.ParseRESP([]byte(":123\r"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: ":123\r" must be terminated with CRLF`))

			// Check positive 64-bit boundary +1
			_, err = redix.ParseRESP([]byte(":9223372036854775808\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: ":9223372036854775808\r\n" is not a 64 bit integer`))

			// Check negative 64-bit boundary -1
			_, err = redix.ParseRESP([]byte(":-9223372036854775809\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: ":-9223372036854775809\r\n" is not a 64 bit integer`))
		})
		It("Should validate simple strings.", func() {
			// Check for CR
			_, err := redix.ParseRESP([]byte("+No\rCR\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "+No\rCR\r\n" may not contain CR or LF`))

			// Check for LF
			_, err = redix.ParseRESP([]byte("+No\nLF\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "+No\nLF\r\n" may not contain CR or LF`))

			// Check for terminating CRLF
			_, err = redix.ParseRESP([]byte("+NOTOK\r"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "+NOTOK\r" must be terminated with CRLF`))
		})
		It("Should validate errors.", func() {
			// Check for CR
			_, err := redix.ParseRESP([]byte("-No\rCR\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "-No\rCR\r\n" may not contain CR or LF`))

			// Check for LF
			_, err = redix.ParseRESP([]byte("-NOTOK\r"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "-NOTOK\r" must be terminated with CRLF`))
		})
		It("Should validate bulk strings.", func() {
			_, err := redix.ParseRESP([]byte("$123"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "$123" must indicate both a length and a value`))

			_, err = redix.ParseRESP([]byte("$3\r\n123"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "$3\r\n123" must be terminated with CRLF`))

			_, err = redix.ParseRESP([]byte("%3\r\nfoo\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "%3\r\nfoo\r\n" contains invalid prefix`))

			_, err = redix.ParseRESP([]byte("$three\r\nfoo\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "$three\r\nfoo\r\n" must specify an integer length`))

			_, err = redix.ParseRESP([]byte("$9\r\n1234\r\n7890\r\n"))
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(Equal(`resp: "$9\r\n1234\r\n7890\r\n" incorrect length`))
		})
	})
})

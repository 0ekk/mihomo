package xhttp

import (
	"crypto/rand"
	"math"
	"net/url"
	"strings"

	"github.com/metacubex/http"
	"golang.org/x/net/http2/hpack"
)

const (
	PlacementQueryInHeader = "queryInHeader"
	PlacementCookie        = "cookie"
	PlacementHeader        = "header"
	PlacementQuery         = "query"
)

type PaddingMethod string

const (
	PaddingMethodRepeatX  PaddingMethod = "repeat-x"
	PaddingMethodTokenish PaddingMethod = "tokenish"
)

const charsetBase62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

const avgHuffmanBytesPerCharBase62 = 0.8

const paddingValidationTolerance = 2

type XPaddingPlacement struct {
	Placement string
	Key       string
	Header    string
	RawURL    string
}

type XPaddingConfig struct {
	Length    int
	Placement XPaddingPlacement
	Method    PaddingMethod
}

func randStringFromCharset(n int, charset string) (string, bool) {
	if n <= 0 || len(charset) == 0 {
		return "", false
	}

	m := len(charset)
	limit := byte(256 - (256 % m))

	result := make([]byte, n)
	i := 0

	buf := make([]byte, 256)
	for i < n {
		if _, err := rand.Read(buf); err != nil {
			return "", false
		}
		for _, rb := range buf {
			if rb >= limit {
				continue
			}
			result[i] = charset[int(rb)%m]
			i++
			if i == n {
				break
			}
		}
	}

	return string(result), true
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func generateTokenishPaddingBase62(targetHuffmanBytes int) string {
	n := int(math.Ceil(float64(targetHuffmanBytes) / avgHuffmanBytesPerCharBase62))
	if n < 1 {
		n = 1
	}

	randBase62Str, ok := randStringFromCharset(n, charsetBase62)
	if !ok {
		return ""
	}

	const maxIter = 150
	adjustChar := byte('X')

	for iter := 0; iter < maxIter; iter++ {
		currentLength := int(hpack.HuffmanEncodeLength(randBase62Str))
		diff := currentLength - targetHuffmanBytes

		if absInt(diff) <= paddingValidationTolerance {
			return randBase62Str
		}

		if diff < 0 {
			randBase62Str += string(adjustChar)
			if adjustChar == 'X' {
				adjustChar = 'Z'
			} else {
				adjustChar = 'X'
			}
		} else {
			if len(randBase62Str) <= 1 {
				return randBase62Str
			}
			randBase62Str = randBase62Str[:len(randBase62Str)-1]
		}
	}

	return randBase62Str
}

func GeneratePadding(method PaddingMethod, length int) string {
	if length <= 0 {
		return ""
	}

	switch method {
	case PaddingMethodRepeatX:
		return strings.Repeat("X", length)
	case PaddingMethodTokenish:
		paddingValue := generateTokenishPaddingBase62(length)
		if paddingValue == "" {
			return strings.Repeat("X", length)
		}
		return paddingValue
	default:
		return strings.Repeat("X", length)
	}
}

func applyPaddingToCookie(req *http.Request, name, value string) {
	if req == nil || name == "" || value == "" {
		return
	}
	req.AddCookie(&http.Cookie{
		Name:  name,
		Value: value,
		Path:  "/",
	})
}

func applyPaddingToQuery(u *url.URL, key, value string) {
	if u == nil || key == "" || value == "" {
		return
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
}

func (c *Config) buildRequestXPaddingConfig(rawURL string) XPaddingConfig {
	length := int(c.XPaddingBytes.WithDefault(100, 1000).Random())
	config := XPaddingConfig{Length: length}

	if c.XPaddingObfsMode {
		config.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
			RawURL:    rawURL,
		}
		config.Method = PaddingMethod(c.XPaddingMethod)
		return config
	}

	config.Placement = XPaddingPlacement{
		Placement: PlacementQueryInHeader,
		Key:       "x_padding",
		Header:    "Referer",
		RawURL:    rawURL,
	}
	return config
}

func (c *Config) buildResponseXPaddingConfig() XPaddingConfig {
	length := int(c.XPaddingBytes.WithDefault(100, 1000).Random())
	config := XPaddingConfig{Length: length}

	if c.XPaddingObfsMode {
		config.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
		}
		config.Method = PaddingMethod(c.XPaddingMethod)
		return config
	}

	config.Placement = XPaddingPlacement{
		Placement: PlacementHeader,
		Header:    "X-Padding",
	}
	return config
}

func (c *Config) ApplyXPaddingToHeader(h http.Header, config XPaddingConfig) {
	if h == nil {
		return
	}

	paddingValue := GeneratePadding(config.Method, config.Length)

	switch p := config.Placement; p.Placement {
	case PlacementHeader:
		h.Set(p.Header, paddingValue)
	case PlacementQueryInHeader:
		u, err := url.Parse(p.RawURL)
		if err != nil || u == nil {
			return
		}
		u.RawQuery = p.Key + "=" + paddingValue
		h.Set(p.Header, u.String())
	}
}

func (c *Config) ApplyXPaddingToRequest(req *http.Request, config XPaddingConfig) {
	if req == nil {
		return
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}

	placement := config.Placement.Placement
	if placement == PlacementHeader || placement == PlacementQueryInHeader {
		c.ApplyXPaddingToHeader(req.Header, config)
		return
	}

	paddingValue := GeneratePadding(config.Method, config.Length)

	switch placement {
	case PlacementCookie:
		applyPaddingToCookie(req, config.Placement.Key, paddingValue)
	case PlacementQuery:
		applyPaddingToQuery(req.URL, config.Placement.Key, paddingValue)
	}
}

func (c *Config) ExtractXPaddingFromRequest(req *http.Request, obfsMode bool) (string, string) {
	if req == nil {
		return "", ""
	}

	if !obfsMode {
		referrer := req.Header.Get("Referer")
		if referrer != "" {
			if referrerURL, err := url.Parse(referrer); err == nil {
				paddingValue := referrerURL.Query().Get("x_padding")
				paddingPlacement := PlacementQueryInHeader + "=Referer, key=x_padding"
				return paddingValue, paddingPlacement
			}
		} else {
			paddingValue := req.URL.Query().Get("x_padding")
			return paddingValue, PlacementQuery + ", key=x_padding"
		}
	}

	key := c.XPaddingKey
	header := c.XPaddingHeader

	if cookie, err := req.Cookie(key); err == nil {
		if cookie != nil && cookie.Value != "" {
			paddingValue := cookie.Value
			paddingPlacement := PlacementCookie + ", key=" + key
			return paddingValue, paddingPlacement
		}
	}

	headerValue := req.Header.Get(header)
	if headerValue != "" {
		if c.XPaddingPlacement == PlacementHeader {
			paddingPlacement := PlacementHeader + "=" + header
			return headerValue, paddingPlacement
		}

		if parsedURL, err := url.Parse(headerValue); err == nil {
			paddingPlacement := PlacementQueryInHeader + "=" + header + ", key=" + key
			return parsedURL.Query().Get(key), paddingPlacement
		}
	}

	queryValue := req.URL.Query().Get(key)
	if queryValue != "" {
		paddingPlacement := PlacementQuery + ", key=" + key
		return queryValue, paddingPlacement
	}

	return "", ""
}

func (c *Config) IsPaddingValid(paddingValue string, from, to int32, method PaddingMethod) bool {
	if paddingValue == "" {
		return false
	}
	if to <= 0 {
		r := c.XPaddingBytes.WithDefault(100, 1000)
		from, to = r.From, r.To
	}

	switch method {
	case PaddingMethodRepeatX:
		n := int32(len(paddingValue))
		return n >= from && n <= to
	case PaddingMethodTokenish:
		const tolerance = int32(paddingValidationTolerance)

		n := int32(hpack.HuffmanEncodeLength(paddingValue))
		f := from - tolerance
		t := to + tolerance
		if f < 0 {
			f = 0
		}
		return n >= f && n <= t
	default:
		n := int32(len(paddingValue))
		return n >= from && n <= to
	}
}

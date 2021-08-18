package influxdb2

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	metrics "github.com/rcrowley/go-metrics"
)

func TestRun(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:8086")
	r := metrics.NewRegistry()
	ct := metrics.NewCounter()
	ct.Inc(1)
	r.Register("count", ct)
	r.Register("r", metrics.NewGauge())
	rep := &reporter2{
		reg:      r,
		interval: time.Second,
		url:      *u,
		org:      "zsh",
		bucket:   "m",
		token:    "QSCQLBVf81wO0GJ7pJ6J5sU4rtHuyL19n9WDYmbpqKYQdEefI6goRu_xsHHiJckKXp2w-Vr47u8CdyW0TDd2ig==",
		tags:     map[string]string{},
	}
	rep.makeClient()
	err := rep.send()
	assert.Nil(t, err)
	// test error

	rep = &reporter2{
		reg:      r,
		interval: time.Second,
		url:      *u,
		org:      "zsh111", // invalid org
		bucket:   "m",
		token:    "QSCQLBVf81wO0GJ7pJ6J5sU4rtHuyL19n9WDYmbpqKYQdEefI6goRu_xsHHiJckKXp2w-Vr47u8CdyW0TDd2ig==",
		tags:     map[string]string{},
	}
	rep.makeClient()
	err = rep.send()
	assert.NotNil(t, err)
}

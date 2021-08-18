package influxdb2

import (
	"fmt"
	"log"
	"math"
	uurl "net/url"
	"time"

	client "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/rcrowley/go-metrics"
)

type reporter2 struct {
	reg      metrics.Registry
	interval time.Duration

	url    uurl.URL
	bucket string
	org    string
	token  string
	tags   map[string]string

	client client.Client
}

// InfluxDB starts a InfluxDB reporter which will post the metrics from the given registry at each d interval.
func InfluxDB2(r metrics.Registry, d time.Duration, url, bucket, token, org string) {
	InfluxDB2WithTags(r, d, url, bucket, token, org, nil)
}

// InfluxDBWithTags starts a InfluxDB reporter which will post the metrics from the given registry at each d interval with the specified tags
func InfluxDB2WithTags(r metrics.Registry, d time.Duration, url, bucket, token, org string, tags map[string]string) {
	u, err := uurl.Parse(url)
	if err != nil {
		log.Printf("unable to parse InfluxDB url %s. err=%v", url, err)
		return
	}

	rep := &reporter2{
		reg:      r,
		interval: d,
		url:      *u,
		bucket:   bucket,
		token:    token,
		org:      org,
		tags:     tags,
	}
	if err := rep.makeClient(); err != nil {
		log.Printf("unable to make InfluxDB client. err=%v", err)
		return
	}

	rep.run()
}

func (r *reporter2) makeClient() (err error) {
	r.client = client.NewClient(r.url.String(), r.token)
	return
}

func (r *reporter2) run() {
	intervalTicker := time.NewTicker(r.interval)
	defer intervalTicker.Stop()

	for {
		select {
		case <-intervalTicker.C:
			if err := r.send(); err != nil {
				log.Printf("unable to send metrics to InfluxDB. err=%v", err)
			}
		}
	}
}

func (r *reporter2) send() error {
	var err error
	var pts []*write.Point
	r.reg.Each(func(name string, i interface{}) {
		now := time.Now()
		switch metric := i.(type) {
		case metrics.Counter:
			ms := metric.Snapshot()
			pts = append(pts, client.NewPoint(
				fmt.Sprintf("%s.count", name),
				r.tags,
				map[string]interface{}{
					"value": ms.Count(),
				},
				now,
			))
		case metrics.Gauge:
			ms := metric.Snapshot()
			pts = append(pts, client.NewPoint(
				fmt.Sprintf("%s.gauge", name),
				r.tags,
				map[string]interface{}{
					"value": ms.Value(),
				},
				now,
			))
		case metrics.GaugeFloat64:
			ms := metric.Snapshot()
			pts = append(pts, client.NewPoint(
				fmt.Sprintf("%s.gauge", name),
				r.tags,
				map[string]interface{}{
					"value": ms.Value(),
				},
				now,
			))
		case metrics.Histogram:
			ms := metric.Snapshot()
			isInf := math.IsInf(ms.Mean(), 0)
			var mean float64
			if isInf {
				mean = 0
			}
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			pts = append(pts, client.NewPoint(
				fmt.Sprintf("%s.histogram", name),
				r.tags,
				map[string]interface{}{
					"count":    ms.Count(),
					"max":      ms.Max(),
					"mean":     mean,
					"min":      ms.Min(),
					"stddev":   ms.StdDev(),
					"variance": ms.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
				},
				now,
			))
		case metrics.Meter:
			ms := metric.Snapshot()
			var m1, m5, m15, mean float64
			isInf := math.IsInf(ms.Rate1(), 0)
			if isInf {
				m1 = 0
			}
			isInf = math.IsInf(ms.Rate5(), 0)
			if isInf {
				m5 = 0
			}
			isInf = math.IsInf(ms.Rate15(), 0)
			if isInf {
				m15 = 0
			}
			isInf = math.IsInf(ms.RateMean(), 0)
			if isInf {
				mean = 0
			}

			pts = append(pts, client.NewPoint(
				fmt.Sprintf("%s.meter", name),
				r.tags,
				map[string]interface{}{
					"count": ms.Count(),
					"m1":    m1,
					"m5":    m5,
					"m15":   m15,
					"mean":  mean,
				},
				now,
			))
		case metrics.Timer:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			pts = append(pts, client.NewPoint(
				fmt.Sprintf("%s.timer", name),
				r.tags,
				map[string]interface{}{
					"count":    ms.Count(),
					"max":      ms.Max(),
					"mean":     ms.Mean(),
					"min":      ms.Min(),
					"stddev":   ms.StdDev(),
					"variance": ms.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
					"m1":       ms.Rate1(),
					"m5":       ms.Rate5(),
					"m15":      ms.Rate15(),
					"meanrate": float32(ms.RateMean()),
				},
				now,
			))
		}
	})
	writeAPI := r.client.WriteAPI(r.org, r.bucket)
	errCh := writeAPI.Errors()
	go func() {
		err = <-errCh
		if err != nil {
			log.Printf("write error err=%v", err)
		}
	}()
	for _, pt := range pts {
		writeAPI.WritePoint(pt)
	}
	writeAPI.Flush()
	// writeAPI.(*api.WriteAPIImpl).Close()
	return err
}

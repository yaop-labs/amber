package obsbench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"gopkg.in/yaml.v3"
)

// TargetConfig describes one system under test. Protocol selects the payload
// builder: "otlp" is the shared protobuf encoding every system accepts (the
// same-client fairness baseline); "native" is the system's own bulk API
// (documented per system, METHODOLOGY.md §5).
type TargetConfig struct {
	// Kind: amber | loki | victorialogs | otlp
	Kind     string `yaml:"kind"`
	BaseURL  string `yaml:"base_url"`
	APIKey   string `yaml:"api_key,omitempty"`
	Protocol string `yaml:"protocol"` // native | otlp
	// OTLPPath overrides the OTLP logs path; systems mount it differently
	// (amber /v1/logs, Loki /otlp/v1/logs, VictoriaLogs /insert/opentelemetry/v1/logs).
	OTLPPath string `yaml:"otlp_path,omitempty"`
}

type Config struct {
	Targets map[string]TargetConfig `yaml:"targets"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

// payloadBuilder renders one batch into an HTTP body. now is the send-time
// timestamp applied to every record in the batch (the dataset carries no
// event time by design — see Record).
type payloadBuilder interface {
	// Build returns (urlPath, contentType, body).
	Build(batch []Record, now time.Time) (string, string, []byte, error)
}

func builderFor(t TargetConfig) (payloadBuilder, error) {
	if t.Protocol == "otlp" {
		path := t.OTLPPath
		if path == "" {
			switch t.Kind {
			case "loki":
				path = "/otlp/v1/logs"
			case "victorialogs":
				path = "/insert/opentelemetry/v1/logs"
			default:
				path = "/v1/logs"
			}
		}
		return &otlpBuilder{path: path}, nil
	}
	switch t.Kind {
	case "amber":
		return &amberBuilder{}, nil
	case "loki":
		return &lokiBuilder{}, nil
	case "victorialogs":
		return &victoriaLogsBuilder{}, nil
	default:
		return nil, fmt.Errorf("target: no native builder for kind %q (use protocol: otlp)", t.Kind)
	}
}

// amberBuilder posts amber's native bulk JSON array. The seq rides in attrs
// for loss accounting.
type amberBuilder struct{}

func (amberBuilder) Build(batch []Record, _ time.Time) (string, string, []byte, error) {
	type req struct {
		Level   string            `json:"level"`
		Service string            `json:"service"`
		Host    string            `json:"host"`
		Body    string            `json:"body"`
		Attrs   map[string]string `json:"attrs,omitempty"`
	}
	out := make([]req, len(batch))
	for i, r := range batch {
		out[i] = req{
			Level: r.Level, Service: r.Service, Host: r.Host, Body: r.Body,
			Attrs: withSeq(r.Attrs, r.Seq),
		}
	}
	body, err := json.Marshal(out)
	return "/api/v1/logs", "application/json", body, err
}

// lokiBuilder posts /loki/api/v1/push with one stream per (service, level)
// pair — the label-cardinality model Loki expects.
type lokiBuilder struct{}

func (lokiBuilder) Build(batch []Record, now time.Time) (string, string, []byte, error) {
	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	ns := strconv.FormatInt(now.UnixNano(), 10)
	streams := make(map[string]*stream)
	for _, r := range batch {
		key := r.Service + "\x00" + r.Level + "\x00" + r.Host
		s := streams[key]
		if s == nil {
			s = &stream{Stream: map[string]string{
				"service": r.Service, "level": r.Level, "host": r.Host,
			}}
			streams[key] = s
		}
		line := fmt.Sprintf("seq=%d %s", r.Seq, r.Body)
		s.Values = append(s.Values, [2]string{ns, line})
	}
	payload := struct {
		Streams []*stream `json:"streams"`
	}{Streams: make([]*stream, 0, len(streams))}
	for _, s := range streams {
		payload.Streams = append(payload.Streams, s)
	}
	body, err := json.Marshal(payload)
	return "/loki/api/v1/push", "application/json", body, err
}

// victoriaLogsBuilder posts newline-delimited JSON to /insert/jsonline.
// Post-ingest count verification is mandatory for this target: the v0 round
// excluded VictoriaLogs after this very endpoint silently dropped records.
type victoriaLogsBuilder struct{}

func (victoriaLogsBuilder) Build(batch []Record, now time.Time) (string, string, []byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	ts := now.Format(time.RFC3339Nano)
	for _, r := range batch {
		line := map[string]string{
			"_time":   ts,
			"_msg":    r.Body,
			"service": r.Service,
			"level":   r.Level,
			"host":    r.Host,
			"seq":     strconv.FormatUint(r.Seq, 10),
		}
		if err := enc.Encode(line); err != nil {
			return "", "", nil, err
		}
	}
	return "/insert/jsonline?_stream_fields=service,level,host", "application/x-ndjson", buf.Bytes(), nil
}

// otlpBuilder renders the shared OTLP protobuf payload, grouped into one
// resource per service the way collectors batch real traffic.
type otlpBuilder struct {
	path string
}

func (b *otlpBuilder) Build(batch []Record, now time.Time) (string, string, []byte, error) {
	nowNano := uint64(now.UnixNano())
	byService := make(map[string][]*logspb.LogRecord)
	for _, r := range batch {
		lr := &logspb.LogRecord{
			TimeUnixNano:   nowNano,
			SeverityText:   r.Level,
			SeverityNumber: severityNumber(r.Level),
			Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: r.Body}},
			Attributes: []*commonpb.KeyValue{
				{Key: "host", Value: strVal(r.Host)},
				{Key: "seq", Value: strVal(strconv.FormatUint(r.Seq, 10))},
			},
		}
		byService[r.Service] = append(byService[r.Service], lr)
	}
	req := &collectorlogs.ExportLogsServiceRequest{}
	for service, records := range byService {
		req.ResourceLogs = append(req.ResourceLogs, &logspb.ResourceLogs{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: strVal(service)},
			}},
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
		})
	}
	body, err := proto.Marshal(req)
	return b.path, "application/x-protobuf", body, err
}

func strVal(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func severityNumber(level string) logspb.SeverityNumber {
	switch level {
	case "TRACE":
		return logspb.SeverityNumber_SEVERITY_NUMBER_TRACE
	case "DEBUG":
		return logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG
	case "INFO":
		return logspb.SeverityNumber_SEVERITY_NUMBER_INFO
	case "WARN":
		return logspb.SeverityNumber_SEVERITY_NUMBER_WARN
	case "ERROR":
		return logspb.SeverityNumber_SEVERITY_NUMBER_ERROR
	case "FATAL":
		return logspb.SeverityNumber_SEVERITY_NUMBER_FATAL
	default:
		return logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED
	}
}

func withSeq(attrs map[string]string, seq uint64) map[string]string {
	out := make(map[string]string, len(attrs)+1)
	for k, v := range attrs {
		out[k] = v
	}
	out["seq"] = strconv.FormatUint(seq, 10)
	return out
}

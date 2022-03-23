package otlp

import (
	"fmt"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	v1 "go.opentelemetry.io/collector/model/internal/data/protogen/trace/v1"
	"go.opentelemetry.io/collector/model/pdata"
)

var tracesOTLPFull = func() pdata.Traces {
	traceID := pdata.NewTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10})
	spanID := pdata.NewSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	td := pdata.NewTraces()
	// Add ResourceSpans.
	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("schemaURL")
	// Add resource.
	rs.Resource().Attributes().UpsertString("host.name", "testHost")
	rs.Resource().Attributes().UpsertString("service.name", "testService")
	//rs.Resource().SetDroppedAttributesCount(1)
	// Add InstrumentationLibrarySpans.
	il := rs.InstrumentationLibrarySpans().AppendEmpty()
	il.InstrumentationLibrary().SetName("instrumentation name")
	il.InstrumentationLibrary().SetVersion("instrumentation version")
	il.SetSchemaUrl("schemaURL")
	// Add spans.
	sp := il.Spans().AppendEmpty()
	sp.SetName("testSpan")
	sp.SetKind(pdata.SpanKindClient)
	sp.SetDroppedAttributesCount(1)
	sp.SetStartTimestamp(pdata.NewTimestampFromTime(time.Now()))
	sp.SetTraceID(traceID)
	sp.SetSpanID(spanID)
	sp.SetDroppedEventsCount(1)
	sp.SetDroppedLinksCount(1)
	sp.SetEndTimestamp(pdata.NewTimestampFromTime(time.Now()))
	sp.SetParentSpanID(spanID)
	sp.SetTraceState("state")
	sp.Status().SetCode(pdata.StatusCodeOk)
	sp.Status().SetMessage("message")
	// Add attributes.
	sp.Attributes().UpsertString("string", "value")
	sp.Attributes().UpsertBool("bool", true)
	sp.Attributes().UpsertInt("int", 1)
	sp.Attributes().UpsertDouble("double", 1.1)
	sp.Attributes().UpsertBytes("bytes", []byte("foo"))
	arr := pdata.NewAttributeValueArray()
	arr.SliceVal().AppendEmpty().SetIntVal(1)
	sp.Attributes().Upsert("array", arr)
	// Add events.
	event := sp.Events().AppendEmpty()
	event.SetName("eventName")
	event.SetTimestamp(pdata.NewTimestampFromTime(time.Now()))
	event.SetDroppedAttributesCount(1)
	event.Attributes().UpsertString("string", "value")
	event.Attributes().UpsertBool("bool", true)
	event.Attributes().UpsertInt("int", 1)
	event.Attributes().UpsertDouble("double", 1.1)
	event.Attributes().UpsertBytes("bytes", []byte("foo"))
	// Add links.
	link := sp.Links().AppendEmpty()
	link.SetTraceState("state")
	link.SetTraceID(traceID)
	link.SetSpanID(spanID)
	link.SetDroppedAttributesCount(1)
	link.Attributes().UpsertString("string", "value")
	link.Attributes().UpsertBool("bool", true)
	link.Attributes().UpsertInt("int", 1)
	link.Attributes().UpsertDouble("double", 1.1)
	link.Attributes().UpsertBytes("bytes", []byte("foo"))
	// Add another span.
	sp2 := il.Spans().AppendEmpty()
	sp2.SetName("testSpan2")
	return td
}()

func TestLogsJSONIter(t *testing.T) {
	encoder := NewJSONLogsMarshaler()
	jsonBuf, err := encoder.MarshalLogs(logsOTLP)
	assert.NoError(t, err)

	decoder := NewJSONIterLogsUnmarshaler()
	_, err = decoder.UnmarshalLogs(jsonBuf)
	assert.Error(t, err)
}

func TestMetricsJSONIter(t *testing.T) {
	encoder := NewJSONMetricsMarshaler()
	jsonBuf, err := encoder.MarshalMetrics(metricsOTLP)
	assert.NoError(t, err)

	decoder := NewJSONIterMetricsUnmarshaler()
	_, err = decoder.UnmarshalMetrics(jsonBuf)
	assert.Error(t, err)
}

func TestTracesJSONIter(t *testing.T) {
	encoder := NewJSONTracesMarshaler()
	jsonBuf, err := encoder.MarshalTraces(tracesOTLPFull)
	assert.NoError(t, err)

	decoder := NewJSONIterTracesUnmarshaler()
	got, err := decoder.UnmarshalTraces(jsonBuf)
	assert.NoError(t, err)
	assert.EqualValues(t, tracesOTLPFull, got)
}

func BenchmarkTracesJSONUnmarshal(b *testing.B) {
	b.ReportAllocs()

	encoder := NewJSONTracesMarshaler()
	jsonBuf, err := encoder.MarshalTraces(tracesOTLPFull)
	assert.NoError(b, err)
	decoder := newJSONUnmarshaler()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := decoder.UnmarshalTraces(jsonBuf)
			assert.NoError(b, err)
		}
	})
}

func BenchmarkTracesJSONiterUnmarshal(b *testing.B) {
	b.ReportAllocs()

	encoder := NewJSONTracesMarshaler()
	jsonBuf, err := encoder.MarshalTraces(tracesOTLPFull)
	assert.NoError(b, err)
	decoder := newJSONIterUnmarshaler()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := decoder.UnmarshalTraces(jsonBuf)
			assert.NoError(b, err)
		}
	})
}

func TestReadInt64(t *testing.T) {
	var data = `{"intAsNumber":1,"intAsString":"1"}`
	iter := jsoniter.ConfigFastest.BorrowIterator([]byte(data))
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "intAsNumber":
			v := readInt64(iter)
			assert.Equal(t, int64(1), v)
		case "intAsString":
			v := readInt64(iter)
			assert.Equal(t, int64(1), v)
		}
		return true
	})
	assert.NoError(t, iter.Error)
}

func Test_readTraceData(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readTraceData(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
}

func Test_readResourceSpans(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readResourceSpans(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
	t.Run("unknown resource field", func(t *testing.T) {
		jsonStr := `{"resource":{"extra":""}}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readResourceSpans(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
}

func Test_readInstrumentationLibrarySpans(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readInstrumentationLibrarySpans(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
	t.Run("unknown instrumentationLibrary field", func(t *testing.T) {
		jsonStr := `{"instrumentationLibrary":{"extra":""}}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readInstrumentationLibrarySpans(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
}

func Test_readSpan(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpan(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
	t.Run("unknown status field", func(t *testing.T) {
		jsonStr := `{"status":{"extra":""}}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpan(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
	t.Run("invalid trace_id field", func(t *testing.T) {
		jsonStr := `{"trace_id":"--"}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpan(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "parse trace_id")
		}
	})
	t.Run("invalid span_id field", func(t *testing.T) {
		jsonStr := `{"span_id":"--"}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpan(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "parse span_id")
		}
	})
	t.Run("invalid parent_span_id field", func(t *testing.T) {
		jsonStr := `{"parent_span_id":"--"}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpan(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "parse parent_span_id")
		}
	})
	t.Run("process status.deprecatedCode", func(t *testing.T) {
		jsonStr := `{"status":{"deprecatedCode":1}}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		sp:=readSpan(iter)
		assert.Equal(t, int(sp.Status.DeprecatedCode), 1)
	})
}

func Test_readSpanLink(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpanLink(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
	t.Run("invalid trace_id field", func(t *testing.T) {
		jsonStr := `{"trace_id":"--"}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpanLink(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "parse trace_id")
		}
	})
	t.Run("invalid span_id field", func(t *testing.T) {
		jsonStr := `{"span_id":"--"}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpanLink(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "parse span_id")
		}
	})
}

func Test_readSpanEvent(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readSpanEvent(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
}

func Test_readAttribute(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readAttribute(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
	t.Run("missing key", func(t *testing.T) {
		jsonStr := `{"key":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readAttribute(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "missing key")
		}
	})
}

func Test_readAnyValue(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readAnyValue(iter,"")
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
	t.Run("invalid bytesValue", func(t *testing.T) {
		jsonStr := `"--"`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readAnyValue(iter, "bytesValue")
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "base64")
		}
	})
}

func Test_readArray(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		jsonStr := `{"extra":""}`
		iter := jsoniter.ConfigFastest.BorrowIterator([]byte(jsonStr))
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		readArray(iter)
		if assert.Error(t, iter.Error) {
			assert.Contains(t, iter.Error.Error(), "unknown field")
		}
	})
}

func Test_readSpanKind(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		want    v1.Span_SpanKind
	}{
		{
			name:    "string",
			jsonStr: fmt.Sprintf(`"%s"`, v1.Span_SPAN_KIND_INTERNAL.String()),
			want:    v1.Span_SPAN_KIND_INTERNAL,
		},
		{
			name:    "int",
			jsonStr: fmt.Sprintf("%d", v1.Span_SPAN_KIND_INTERNAL),
			want:    v1.Span_SPAN_KIND_INTERNAL,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := jsoniter.ConfigFastest.BorrowIterator([]byte(tt.jsonStr))
			defer jsoniter.ConfigFastest.ReturnIterator(iter)
			if got := readSpanKind(iter); got != tt.want {
				t.Errorf("readSpanKind() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readStatusCode(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		want    v1.Status_StatusCode
	}{
		{
			name:    "string",
			jsonStr: fmt.Sprintf(`"%s"`, v1.Status_STATUS_CODE_ERROR.String()),
			want:    v1.Status_STATUS_CODE_ERROR,
		},
		{
			name:    "int",
			jsonStr: fmt.Sprintf("%d", v1.Status_STATUS_CODE_ERROR),
			want:    v1.Status_STATUS_CODE_ERROR,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := jsoniter.ConfigFastest.BorrowIterator([]byte(tt.jsonStr))
			defer jsoniter.ConfigFastest.ReturnIterator(iter)
			if got := readStatusCode(iter); got != tt.want {
				t.Errorf("readStatusCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readStatusDeprecatedStatusCode(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		want    v1.Status_DeprecatedStatusCode
	}{
		{
			name:    "string",
			jsonStr: fmt.Sprintf(`"%s"`, v1.Status_DEPRECATED_STATUS_CODE_NOT_FOUND.String()),
			want:    v1.Status_DEPRECATED_STATUS_CODE_NOT_FOUND,
		},
		{
			name:    "int",
			jsonStr: fmt.Sprintf("%d", v1.Status_DEPRECATED_STATUS_CODE_NOT_FOUND),
			want:    v1.Status_DEPRECATED_STATUS_CODE_NOT_FOUND,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := jsoniter.ConfigFastest.BorrowIterator([]byte(tt.jsonStr))
			defer jsoniter.ConfigFastest.ReturnIterator(iter)
			if got := readStatusDeprecatedCode(iter); got != tt.want {
				t.Errorf("readStatusCode() = %v, want %v", got, tt.want)
			}
		})
	}
}
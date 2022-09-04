// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp // import "go.opentelemetry.io/collector/pdata/pmetric"

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"go.opentelemetry.io/collector/model/internal"
	otlplogs "go.opentelemetry.io/collector/model/internal/data/protogen/logs/v1"
	"go.opentelemetry.io/collector/model/internal/json"
	"go.opentelemetry.io/collector/model/pdata"
)

func (d *jsonIterUnmarshaler) UnmarshalLogs(buf []byte) (pdata.Logs, error) {
	iter := jsoniter.ConfigFastest.BorrowIterator(buf)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	ld := d.readLogsData(iter)
	if iter.Error != nil {
		return pdata.Logs{}, iter.Error
	}
	return pdata.LogsFromInternalRep(internal.LogsFromOtlp(ld)), nil
}

func (d *jsonIterUnmarshaler) readLogsData(iter *jsoniter.Iterator) *otlplogs.LogsData {
	ld := &otlplogs.LogsData{}
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "resource_logs", "resourceLogs":
			iter.ReadArrayCB(func(iterator *jsoniter.Iterator) bool {
				ld.ResourceLogs = append(ld.ResourceLogs, d.readResourceLogs(iter))
				return true
			})
		default:
			iter.Skip()
		}
		return true
	})
	return ld
}

func (d *jsonIterUnmarshaler) readResourceLogs(iter *jsoniter.Iterator) *otlplogs.ResourceLogs {
	rs := &otlplogs.ResourceLogs{}
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "resource":
			iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
				switch f {
				case "attributes":
					iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
						rs.Resource.Attributes = append(rs.Resource.Attributes, json.ReadAttribute(iter))
						return true
					})
				case "droppedAttributesCount", "dropped_attributes_count":
					rs.Resource.DroppedAttributesCount = json.ReadUint32(iter)
				default:
					iter.Skip()
				}
				return true
			})
		case "scope_logs", "scopeLogs", "instrumentation_library_logs", "instrumentationLibraryLogs":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				rs.InstrumentationLibraryLogs = append(rs.InstrumentationLibraryLogs,
					d.readScopeLogs(iter))
				return true
			})
		case "schemaUrl", "schema_url":
			rs.SchemaUrl = iter.ReadString()
		default:
			iter.Skip()
		}
		return true
	})
	return rs
}

func (d *jsonIterUnmarshaler) readScopeLogs(iter *jsoniter.Iterator) *otlplogs.InstrumentationLibraryLogs {
	ils := &otlplogs.InstrumentationLibraryLogs{}
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "scope", "instrumentation_library", "instrumentationLibrary":
			json.ReadScope(iter, &ils.InstrumentationLibrary)
		case "log_records", "logRecords", "logs":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				ils.Logs = append(ils.Logs, d.readLog(iter))
				return true
			})
		case "schemaUrl", "schema_url":
			ils.SchemaUrl = iter.ReadString()
		default:
			iter.Skip()
		}
		return true
	})
	return ils
}

func (d *jsonIterUnmarshaler) readLog(iter *jsoniter.Iterator) *otlplogs.LogRecord {
	lr := &otlplogs.LogRecord{}
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
		switch f {
		case "timeUnixNano", "time_unix_nano":
			lr.TimeUnixNano = json.ReadUint64(iter)
		case "severity_number", "severityNumber":
			lr.SeverityNumber = d.readSeverityNumber(iter)
		case "severity_text", "severityText":
			lr.SeverityText = iter.ReadString()
		case "name":
			lr.Name = iter.ReadString()
		case "body":
			iter.ReadObjectCB(func(iter *jsoniter.Iterator, f string) bool {
				lr.Body = json.ReadAnyValue(iter, f)
				return true
			})
		case "attributes":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				lr.Attributes = append(lr.Attributes, json.ReadAttribute(iter))
				return true
			})
		case "droppedAttributesCount", "dropped_attributes_count":
			lr.DroppedAttributesCount = json.ReadUint32(iter)
		case "flags":
			lr.Flags = json.ReadUint32(iter)
		case "traceId", "trace_id":
			if err := lr.TraceId.UnmarshalJSON([]byte(iter.ReadString())); err != nil {
				iter.ReportError("readLog.traceId", fmt.Sprintf("parse trace_id:%v", err))
			}
		case "spanId", "span_id":
			if err := lr.SpanId.UnmarshalJSON([]byte(iter.ReadString())); err != nil {
				iter.ReportError("readLog.spanId", fmt.Sprintf("parse span_id:%v", err))
			}
		default:
			iter.Skip()
		}
		return true
	})
	return lr
}

func (d *jsonIterUnmarshaler) readSeverityNumber(iter *jsoniter.Iterator) otlplogs.SeverityNumber {
	return otlplogs.SeverityNumber(json.ReadEnumValue(iter, otlplogs.SeverityNumber_value))
}

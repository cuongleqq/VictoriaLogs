package logstorage

import "testing"

func TestStatsFieldMaxTime(t *testing.T) {
	f := func(pipeStr string, rows, rowsExpected [][]Field) {
		t.Helper()
		expectPipeResults(t, pipeStr, rows, rowsExpected)
	}

	f("stats field_max(_time, a) as x", [][]Field{
		{
			{"_time", "2026-04-07T11:00:00Z"},
			{"a", "first"},
		},
		{
			{"_time", "2026-04-07T12:00:00Z"},
			{"a", "second"},
		},
		{
			{"_time", "2026-04-07T13:00:00Z"},
			{"a", "third"},
		},
	}, [][]Field{
		{
			{"x", "third"},
		},
	})

	f("stats field_max(ip, a) as x", [][]Field{
		{
			{"ip", "127.0.0.1"},
			{"a", "first"},
		},
		{
			{"ip", "127.0.0.3"},
			{"a", "third"},
		},
		{
			{"ip", "127.0.0.2"},
			{"a", "second"},
		},
	}, [][]Field{
		{
			{"x", "third"},
		},
	})
}

package query

const BenchmarkTestResultVersion = "0.1"

type LoaderTestResult struct {
	ResultFormatVersion string `json:"ResultFormatVersion"`

	RunnerConfig BenchmarkRunnerConfig `json:"RunnerConfig"`

	StartTime      int64 `json:"StartTime`
	EndTime        int64 `json:"EndTime"`
	DurationMillis int64 `json:"DurationMillis"`

	Totals map[string]interface{} `json:"Totals"`
}

package load

const LoaderTestResultVersion = "0.1"

// LoaderTestResult aggregates the results of an insert or load benchmark in a common format across targets
type LoaderTestResult struct {
	ResultFormatVersion string `json:"ResultFormatVersion"`

	RunnerConfig BenchmarkRunnerConfig `json:"RunnerConfig"`

	StartTime      int64 `json:"StartTime`
	EndTime        int64 `json:"EndTime"`
	DurationMillis int64 `json:"DurationMillis"`

	Totals map[string]interface{} `json:"Totals"`
}

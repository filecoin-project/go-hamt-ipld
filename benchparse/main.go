package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/perf/benchstat"
)

func main() {
	if len(os.Args) < 3 {
		panic("need to specify input and output file")
	}
	fIn, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	c := benchstat.Collection{}
	if err := c.AddFile("config1", fIn); err != nil {
		panic(err)
	}
	tables := c.Tables()

	reports, err := genReports(tables)
	if err != nil {
		panic(err)
	}
	if err := processReports(reports); err != nil {
		panic(err)
	}

	fOut, err := os.Create(os.Args[2])
	if err != nil {
		panic(err)
	}
	defer fOut.Close()
	if err := writeReports(reports, fOut); err != nil {
		panic(err)
	}
}

// All metrics for all benchmarks for one HAMT configuration
type HAMTReport struct {
	ID         string
	Benchmarks map[string]Benchmark
}

func (hr *HAMTReport) Write(datawriter *bufio.Writer) error {
	// Write title
	if _, err := datawriter.WriteString(hr.ID + "\n"); err != nil {
		return err
	}

	// Write benchmarks
	for _, benchmark := range hr.Benchmarks {
		if err := benchmark.Write(datawriter); err != nil {
			return err
		}
	}
	return nil
}

type Benchmark struct {
	Name    string
	Metrics map[string]struct{}
	Sweep   map[int]map[string]float64 // map[Bitwidth]MetricsMap
}

func (b *Benchmark) Write(datawriter *bufio.Writer) error {
	// Get sorted keys
	metrics := make([]string, 0)
	for metric := range b.Metrics {
		metrics = append(metrics, metric)
	}
	sort.Strings(metrics)

	bws := make([]int, 0)
	for bw := range b.Sweep {
		bws = append(bws, bw)
	}
	sort.Ints(bws)

	// Write headers
	headers := []string{"Benchmark", "Bitwidth"}
	headers = append(headers, metrics...)
	if _, err := datawriter.WriteString(strings.Join(headers, ",")); err != nil {
		return err
	}
	if _, err := datawriter.WriteString("\n"); err != nil {
		return err
	}

	// Write sweeps
	for _, bw := range bws {
		if _, err := datawriter.WriteString(b.Name); err != nil {
			return err
		}
		if _, err := datawriter.WriteString("," + strconv.Itoa(bw)); err != nil {
			return err
		}
		for _, metric := range metrics {
			m, found := b.Sweep[bw][metric]
			if !found {
				// empty column
				if _, err := datawriter.WriteString(","); err != nil {
					return err
				}
				continue
			}
			if _, err := datawriter.WriteString(fmt.Sprintf(",%.3f", m)); err != nil {
				return err
			}
		}
		if _, err := datawriter.WriteString("\n"); err != nil {
			return err
		}
	}

	return nil
}

func writeReports(reports map[string]*HAMTReport, w io.Writer) error {
	datawriter := bufio.NewWriter(w)
	for _, report := range reports {
		if err := report.Write(datawriter); err != nil {
			return err
		}
		if _, err := datawriter.WriteString("\n\n"); err != nil {
			return err
		}
	}
	return datawriter.Flush()
}

func genReports(tables []*benchstat.Table) (map[string]*HAMTReport, error) {
	reports := make(map[string]*HAMTReport)
	for _, table := range tables {
		for _, row := range table.Rows {
			if len(row.Metrics) == 0 {
				continue
			}
			if len(row.Metrics) >= 2 {
				panic("don't know how to handle lots of metrics")
			}
			good, bname, hamtID, bw := parseBench(row.Benchmark)
			if !good {
				continue
			}
			fmt.Printf("bname: %s, hamtID: %s, bw: %d\n", bname, hamtID, bw)

			if _, found := reports[hamtID]; !found {
				reports[hamtID] = &HAMTReport{
					ID:         hamtID,
					Benchmarks: make(map[string]Benchmark),
				}
			}
			if _, found := reports[hamtID].Benchmarks[bname]; !found {
				reports[hamtID].Benchmarks[bname] = Benchmark{
					Name:    bname,
					Metrics: make(map[string]struct{}),
					Sweep:   make(map[int]map[string]float64),
				}
			}
			if _, found := reports[hamtID].Benchmarks[bname].Sweep[bw]; !found {
				reports[hamtID].Benchmarks[bname].Sweep[bw] = make(map[string]float64)
			}
			reports[hamtID].Benchmarks[bname].Metrics[table.Metric] = struct{}{}
			reports[hamtID].Benchmarks[bname].Sweep[bw][table.Metric] = row.Metrics[0].Mean
		}
	}
	return reports, nil
}

const GasCostGet = 114617
const GasCostPut = 353640
const GasCostPerBytePut = 1300

const EffectiveTimeGet = 11462
const EffectiveTimePut = 35364

const getsKey = "getEvts"
const putsKey = "putEvts"
const bytesPutKey = "bytesPut"
const timeKey = "time/op"

// Add GasCost, Time/1000, and EffectiveRuntime
func processReports(reports map[string]*HAMTReport) error {
	for _, report := range reports {
		for _, bench := range report.Benchmarks {
			bench.Metrics["gasCost"] = struct{}{}
			bench.Metrics["time/1000"] = struct{}{}
			bench.Metrics["effectiveRuntime"] = struct{}{}

			for _, row := range bench.Sweep {
				row["gasCost"] = row[getsKey]*GasCostGet + row[putsKey]*GasCostPut + row[bytesPutKey]*GasCostPerBytePut
				row["time/1000"] = row[timeKey] / float64(1000)
				row["effectiveRuntime"] = row["time/1000"] + row[getsKey]*EffectiveTimeGet + row[putsKey]*EffectiveTimePut
			}
		}
	}
	return nil
}

// Unedited hamt benchmark strings are ugly but consistent
// BenchmarkName/hamtID_--_bw=Bitwidth-8
// This can be simplified once we clean up generation
func parseBench(bstatBench string) (bool, string, string, int) {
	slashSeps := strings.Split(bstatBench, "/")
	if len(slashSeps) != 2 {
		return false, "", "", 0
	}
	bname := slashSeps[0]

	uScoreSeps := strings.Split(slashSeps[1], "_")
	if len(uScoreSeps) != 3 {
		return false, "", "", 0
	}
	hamtID := uScoreSeps[0]

	bitwidthStr := strings.TrimPrefix(uScoreSeps[2], "bw=")
	bitwidthStr = strings.TrimSuffix(bitwidthStr, "-8")
	bw, err := strconv.Atoi(bitwidthStr)
	if err != nil {
		panic(err)
	}
	return true, bname, hamtID, bw
}

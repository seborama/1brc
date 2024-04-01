package main

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/seborama/1brc/model"
	"github.com/seborama/1brc/v2a"
)

func main() {
	debug.SetGCPercent(-1) // disable GC

	startT := time.Now()
	defer func() {
		// when pprof is enabled, "defer" is called a little later than without pprof
		fmt.Println("elapsed (defer):", time.Since(startT))
	}()

	if len(os.Args) == 2 && os.Args[1] == "pprof" {
		f, err := os.Create("/tmp/cpuprofile.go.pprof")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer func() { _ = f.Close() }()

		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()

		traceF, err := os.Create("/tmp/trace.out")
		if err != nil {
			panic(err)
		}
		defer func() {
			_ = traceF.Close()
		}()
		if err := trace.Start(traceF); err != nil {
			panic(err)
		}
		defer trace.Stop()
	}

	file, err := os.Open("/tmp/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	res, err := v2a.Run(file)
	if err != nil {
		panic(err)
	}
	printResults(res)

	fmt.Println("elapsed:", time.Since(startT))
}

func printResults(res []*model.StationStats) {
	fmt.Print("{")

	for _, ss := range res {
		fmt.Printf(
			"%s=%.1f/%.1f/%.1f, ",
			ss.Name,
			float64(ss.Min)/10,
			(float64(ss.Sum)/10.)/float64(ss.Count),
			float64(ss.Max)/10,
		)
	}

	fmt.Println("}")
}

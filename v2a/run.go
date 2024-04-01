package v2a

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/alphadose/haxmap"
	"github.com/seborama/1brc/model"
	"github.com/seborama/chronos"
)

func Run(src io.Reader) ([]*model.StationStats, error) {
	readerWG.Add(numReaders)
	for n := range numReaders {
		go read(n, src)
	}

	readerWG.Wait()

	c1 := chronos.Builder{}.Build()
	c1.Start()
	res := aggregateAndSortResults()
	c1.Stop()
	c1.Println("aggregateAndSortResults")

	return res, nil
}

const (
	bufLen               = 4 * 1024 * 1024
	numReaders           = 75
	measurementsFileSize = 13795211963
	numClippings         = measurementsFileSize/bufLen + 1
	numResults           = numReaders + 1 // +1 to store the clippings after unwrapping
)

var (
	readerWG  sync.WaitGroup
	lock      sync.Mutex
	clipIdx   int = -1 // it is pre-incremented
	results   [numResults]*haxmap.Map[uint64, *model.StationStats]
	clippings [numClippings]*clipping // note arrays are semi-thread-safe: concurrent R/W access of *separate* elements is safe
)

type clipping struct {
	head string
	tail string
}

func read(resultsIdx int, file io.Reader) error {
	defer readerWG.Done()

	buffer := make([]byte, bufLen)
	results[resultsIdx] = haxmap.New[uint64, *model.StationStats](2048)
	threadClipIdx := -1

	for {
		lock.Lock()
		n, err := file.Read(buffer)
		if err != nil {
			lock.Unlock()
			if err == io.EOF {
				err = nil
				break
			}
			return err
		}
		clipIdx++
		threadClipIdx = clipIdx
		lock.Unlock()

		// locate the '\n' that terminates the head.
		headTermination := newlineIndexForward(buffer, 0, n)
		startIdx := headTermination + 1 // skip forward the '\n'

		// locate the '\n' that starts the tail.
		// in the most condensed scenario, the buffer only contains 2 lines: the head and the tail.
		tailStart := newlineIndexBackward(buffer, headTermination, n)
		endIdx := tailStart // retain the '\n'

		clippings[threadClipIdx] = &clipping{
			head: string(buffer[:headTermination+1]), // keep '\n' at the end
			tail: string(buffer[tailStart+1 : n]),    // drop the '\n' at the start, one will be provided by the head when reconnecting the clippings.
		}

		// process main body of data
		for i := startIdx; i <= endIdx; i++ {
			semiColonPos, newlinePos, hash, temp := tokeniseNextLine(buffer[i:])
			upsertStats(results[resultsIdx], hash, buffer[i:i+semiColonPos], temp)
			i += newlinePos
		}
	}

	return nil
}

// returns the position of the ';' separator, the position of the ending '\n',
// the station name hash and the temperature.
func tokeniseNextLine(buffer []byte) (int, int, uint64, int) {
	// compute the station name hash
	var hash uint64 = 5381
	k := 0
	for buffer[k] != ';' {
		hash = (hash << 5) + hash + uint64(buffer[k])
		k++
	}

	semiColonPos := k

	// extract temperature
	temp := 0
	neg := false
	if buffer[k+1] == '-' {
		k++
		neg = true
	}
	for k = k + 1; buffer[k] != '\n'; k++ {
		if buffer[k] == '.' {
			continue
		}
		temp *= 10
		temp += int(buffer[k])
		temp -= '0'
	}
	if neg {
		temp = -temp
	}

	return semiColonPos, k, hash, temp
}

func upsertStats(results *haxmap.Map[uint64, *model.StationStats], hash uint64, name []byte, temp int) {
	stats, ok := results.Get(hash)
	if ok {
		if temp < stats.Min {
			stats.Min = temp
		} else if temp > stats.Max {
			stats.Max = temp
		}
		stats.Sum += temp
		stats.Count++
	} else {
		results.Set(hash, &model.StationStats{
			Name:  string(name),
			Min:   temp,
			Max:   temp,
			Sum:   temp,
			Count: 1,
		})
	}
}

func newlineIndexForward(buffer []byte, left, right int) int {
	for i, c := range buffer[left:right] {
		if c == '\n' {
			return i
		}
	}
	// either the head is not '\n'-terminated or it's empty
	fmt.Println("head fallback!")
	return right - 1
}

func newlineIndexBackward(buffer []byte, left, right int) int {
	for i := right - 1; i >= left; i-- {
		if buffer[i] == '\n' {
			return i
		}
	}
	// either the tail is not '\n'-starting or it's empty
	fmt.Println("tail fallback!")
	return left
}

// note: almost certainly, there will be significantly less elements in results[resultsIdx] than there will
// be in clippings. That's because name (hash) drives the key in results[resultsIdx] and it will be repeated
// in the clippings.
func processClippings() {
	resultsIdx := numReaders // the last slot is for clippings
	results[resultsIdx] = haxmap.New[uint64, *model.StationStats](2048)

	fullLine := []byte(clippings[0].head)
	semiColonPos, _, hash, temp := tokeniseNextLine(fullLine)
	upsertStats(results[resultsIdx], hash, fullLine[:semiColonPos], temp)

	leftHandSide := clippings[0].tail
	for i := range clippings[1:] {
		if clippings[i] == nil {
			// this may happen once, with the very last element in clippings because we round up to the next capacity when we declare clippings
			fmt.Printf("DEBUG: nil clipping[%d] - numClippings=%dclippings=%v\n", i, numClippings, clippings)
		}

		fullLine := []byte(leftHandSide + clippings[i].head)
		semiColonPos, _, hash, temp := tokeniseNextLine(fullLine)
		upsertStats(results[resultsIdx], hash, fullLine[:semiColonPos], temp)
		leftHandSide = clippings[i].tail
	}
}

func aggregateAndSortResults() []*model.StationStats {
	processClippings()
	data := aggregatetResults()
	return sortResults(data)
}

func aggregatetResults() *haxmap.Map[uint64, *model.StationStats] {
	data := haxmap.New[uint64, *model.StationStats](2048)

	for _, m := range results {
		m.ForEach(func(station uint64, stationStats *model.StationStats) bool {
			v, ok := data.Get(station)
			if !ok {
				data.Set(station, stationStats)
			} else {
				if stationStats.Min < v.Min {
					v.Min = stationStats.Min
				}
				if stationStats.Max > v.Max {
					v.Max = stationStats.Max
				}
				v.Sum += stationStats.Sum
				v.Count += stationStats.Count
			}

			return true
		})
	}

	return data
}

func sortResults(data *haxmap.Map[uint64, *model.StationStats]) []*model.StationStats {
	orderedResults := make([]*model.StationStats, 0, data.Len())

	data.ForEach(func(_ uint64, v *model.StationStats) (stop bool) {
		orderedResults = append(orderedResults, v)
		return true
	})
	slices.SortFunc(orderedResults, func(a, b *model.StationStats) int {
		return strings.Compare(a.Name, b.Name)
	})

	return orderedResults
}

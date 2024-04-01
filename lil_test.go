package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/sigurn/crc16"
)

type data struct {
	i int
}

func TestArray(t *testing.T) {
	var a [1_000_000_000]*data

	startT := time.Now()

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range len(a) / 2 {
			a[i] = &data{i: i}
		}
	}()
	go func() {
		defer wg.Done()
		for i := len(a)/2 + 1; i < len(a); i++ {
			a[i] = &data{i: i}
		}
	}()
	wg.Wait()

	fmt.Println("elapsed:", time.Since(startT))

	wg.Add(2)
	k := 0
	go func() {
		defer wg.Done()
		for i := range len(a) / 2 {
			k = a[i].i
		}
	}()
	j := 0
	go func() {
		defer wg.Done()
		for i := len(a)/2 + 1; i < len(a); i++ {
			j = a[i].i
		}
	}()
	wg.Wait()
	fmt.Println("k=", k, "j=", j)

	fmt.Println("elapsed:", time.Since(startT))

	fmt.Println("last element:", a[len(a)-1])
}

func TestMap(t *testing.T) {
	s := 10_000_000
	var m = make(map[int]*data, s)

	startT := time.Now()

	for i := range s {
		m[i] = &data{i: i}
	}

	fmt.Println("elapsed:", time.Since(startT))

	fmt.Println("last element:", m[s-1])
}

func TestSlice(t *testing.T) {
	var a = make([]data, 1_000_000_000)

	startT := time.Now()

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range len(a) / 2 {
			a[i] = data{i: i}
		}
	}()
	go func() {
		defer wg.Done()
		for i := len(a)/2 + 1; i < len(a); i++ {
			a[i] = data{i: i}
		}
	}()
	wg.Wait()

	fmt.Println("elapsed:", time.Since(startT))

	fmt.Println("last element:", a[len(a)-1])
}

func TestHaxMap(t *testing.T) {
	s := 10_000_000

	var m = haxmap.New[int, *data]()

	startT := time.Now()

	for i := range s {
		m.Set(i, &data{i: i})
	}

	fmt.Println("elapsed:", time.Since(startT))

	el, _ := m.Get(s - 1)
	fmt.Println("last element:", el)
}

func TestStringVsBytes(t *testing.T) {
	startT := time.Now()

	b := make([]byte, 10)
	copy(b, []byte("some bytes"))
	for i := 1; i <= 100_000_000; i++ {
		a := string(b)
		_ = a
	}

	fmt.Println("elapsed:", time.Since(startT))

	for i := 1; i <= 100_000_000; i++ {
		b := make([]byte, 10)
		copy(b, []byte("some bytes"))
	}

	fmt.Println("elapsed:", time.Since(startT))
}

func TestHashSpeed(t *testing.T) {
	table := crc16.MakeTable(crc16.CRC16_MAXIM)

	startT := time.Now()

	for i := 1; i <= 1_000_000_000; i++ {
		_ = crc16.Checksum([]byte("Hello world!"), table)
	}

	fmt.Println("elapsed:", time.Since(startT))
}

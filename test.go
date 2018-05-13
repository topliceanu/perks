package main

import (
	"fmt"
	"github.com/beorn7/perks/quantile"
)

func main() {
	t := quantile.NewTargeted(map[float64]float64{
		0.5: 0.05, 0.9: 0.01, 0.99: 0.001,
	})
	input := []float64{16, 3, 5, 18, 4, 14, 11, 15, 17, 12, 6, 8, 10, 13, 2, 24, 25, 26, 23, 27, 28, 20, 19, 7, 22, 21, 29, 1, 30, 9}
	fmt.Printf("Data stream: %v\n", input)
	inflex := 10
	for i := 0; i < inflex; i ++ {
		t.Insert(input[i])
	}
	fmt.Printf("Action    |                         Samples                         | 0.5 | 0.9 | 0.99 | Removed | Added\n")
	fmt.Printf("----------+---------------------------------------------------------+-----+-----+------+---------+------\n")
	fmt.Printf("First 10  | %-55v | %3.0f | %3.0f | %4.0f |       0 |    0 \n",
		prints(t.Samples()), t.Query(0.5), t.Query(0.9), t.Query(0.99))
	var newSamples, oldSamples = quantile.Samples{}, quantile.Samples{}
	for i := inflex; i < len(input); i ++ {
		t.Insert(input[i])
		newSamples = clone(t.Samples())
		removed, added := diff(oldSamples, newSamples)
		fmt.Printf("Insert %2.0f | %-55v | %3.0f | %3.0f | %4.0f | %7.0f | %4.0f\n",
			input[i], prints(newSamples), t.Query(0.5), t.Query(0.9), t.Query(0.99), removed, added)
		oldSamples = clone(newSamples)
	}
}

func prints(samples quantile.Samples) string {
	out := make([]float64, len(samples))
	for i, s := range samples {
		out[i] = s.Value
	}
	return fmt.Sprintf("%v", out)
}

func clone(samples quantile.Samples) quantile.Samples {
	c := make(quantile.Samples, len(samples), len(samples))
	copy(c, samples)
	return c
}

func diff(old, new quantile.Samples) (removed, added float64) {
	oldIndex := make(map[float64]struct{}, len(old))
	for _, s := range old {
		oldIndex[s.Value] = struct{}{}
	}
	newIndex := make(map[float64]struct{}, len(new))
	for _, s := range new {
		newIndex[s.Value] = struct{}{}
	}
	for _, s := range old {
		if _, ok := newIndex[s.Value]; !ok {
			removed = s.Value
			break
		}
	}
	for _, s := range new {
		if _, ok := oldIndex[s.Value]; !ok {
			added = s.Value
			break
		}
	}
	return removed, added
}


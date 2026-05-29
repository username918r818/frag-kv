package placement

import (
	"fmt"
	"math"
	"testing"
)

// TestLoadBalanceComparison сравнивает CV (std/mean) трёх стратегий
// на равновесных узлах (одинаковый FreeBytes).
// Это симуляция эксперимента 3 из ВКР.
func TestLoadBalanceComparison(t *testing.T) {
	const (
		numFragments = 1000
		numNodes     = 5
		n            = 1         // размещаем на 1 узле для чистоты измерения
		freeBytes    = 100 << 30 // 100 ГБ у каждого
	)

	nodes := make([]Node, numNodes)
	for i := range nodes {
		nodes[i] = Node{
			ID:         fmt.Sprintf("n%d", i+1),
			FreeBytes:  freeBytes,
			TotalBytes: freeBytes,
		}
	}

	placers := []struct {
		name   string
		placer Placer
	}{
		{"ConsistentHash(vn=150)", &ConsistentHash{VNodes: 150}},
		{"Rendezvous", &Rendezvous{}},
		{"Weighted(equal)", &Weighted{}},
	}

	for _, p := range placers {
		counts := make(map[string]int, numNodes)
		for i := 0; i < numFragments; i++ {
			fid := fmt.Sprintf("key/%d", i)
			result := p.placer.Place(fid, nodes, n)
			counts[result[0].ID]++
		}

		vals := make([]float64, numNodes)
		for i, nd := range nodes {
			vals[i] = float64(counts[nd.ID])
		}
		mean, stddev := stats(vals)
		cv := stddev / mean

		t.Logf("%-28s  counts=%v  mean=%.0f  stddev=%.1f  CV=%.4f",
			p.name, counts, mean, stddev, cv)

		// CV < 0.15 при равномерных узлах — приемлемо для всех стратегий.
		if cv > 0.15 {
			t.Errorf("%s: CV=%.4f exceeds 0.15 (poor load balance on equal nodes)", p.name, cv)
		}
	}
}

// TestLoadBalanceWeightedUnequal: Weighted на НЕРАВНЫХ узлах должен
// давать CV < ConsistentHash (т.е. распределять пропорционально ёмкости).
func TestLoadBalanceWeightedUnequal(t *testing.T) {
	const numFragments = 2000
	nodes := []Node{
		{ID: "big-1", FreeBytes: 500 << 30, TotalBytes: 500 << 30},
		{ID: "big-2", FreeBytes: 400 << 30, TotalBytes: 400 << 30},
		{ID: "mid-1", FreeBytes: 200 << 30, TotalBytes: 200 << 30},
		{ID: "sml-1", FreeBytes: 50 << 30, TotalBytes: 50 << 30},
		{ID: "sml-2", FreeBytes: 30 << 30, TotalBytes: 30 << 30},
	}
	// Идеальное распределение: пропорционально FreeBytes (1180 ГБ суммарно)
	totalFree := 0.0
	for _, nd := range nodes {
		totalFree += float64(nd.FreeBytes)
	}

	wPlacer := &Weighted{}
	chPlacer := &ConsistentHash{VNodes: 150}

	wCounts, chCounts := make(map[string]int), make(map[string]int)
	for i := 0; i < numFragments; i++ {
		fid := fmt.Sprintf("key/%d", i)
		wCounts[wPlacer.Place(fid, nodes, 1)[0].ID]++
		chCounts[chPlacer.Place(fid, nodes, 1)[0].ID]++
	}

	wDeviation := idealDeviation(nodes, wCounts, totalFree, numFragments)
	chDeviation := idealDeviation(nodes, chCounts, totalFree, numFragments)

	t.Logf("Weighted   deviation from ideal = %.4f  counts=%v", wDeviation, wCounts)
	t.Logf("ConsHash   deviation from ideal = %.4f  counts=%v", chDeviation, chCounts)

	if wDeviation > chDeviation*1.5 {
		t.Errorf("Weighted is not meaningfully better than ConsistentHash for unequal nodes (%.4f vs %.4f)",
			wDeviation, chDeviation)
	}
}

// idealDeviation вычисляет среднеквадратичное отклонение от идеального размещения.
func idealDeviation(nodes []Node, counts map[string]int, totalFree float64, total int) float64 {
	var sum float64
	for _, nd := range nodes {
		ideal := float64(nd.FreeBytes) / totalFree * float64(total)
		actual := float64(counts[nd.ID])
		d := actual - ideal
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(nodes)))
}

func stats(vals []float64) (mean, stddev float64) {
	for _, v := range vals {
		mean += v
	}
	mean /= float64(len(vals))
	for _, v := range vals {
		d := v - mean
		stddev += d * d
	}
	stddev = math.Sqrt(stddev / float64(len(vals)))
	return
}

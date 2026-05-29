package placement

import (
	"fmt"
	"math"
	"testing"
)

var testNodes = []Node{
	{ID: "n1", FreeBytes: 100 * 1024 * 1024 * 1024, TotalBytes: 200 * 1024 * 1024 * 1024},
	{ID: "n2", FreeBytes: 80 * 1024 * 1024 * 1024, TotalBytes: 200 * 1024 * 1024 * 1024},
	{ID: "n3", FreeBytes: 60 * 1024 * 1024 * 1024, TotalBytes: 200 * 1024 * 1024 * 1024},
	{ID: "n4", FreeBytes: 40 * 1024 * 1024 * 1024, TotalBytes: 200 * 1024 * 1024 * 1024},
	{ID: "n5", FreeBytes: 20 * 1024 * 1024 * 1024, TotalBytes: 200 * 1024 * 1024 * 1024},
}

func placers() []struct {
	name   string
	placer Placer
} {
	return []struct {
		name   string
		placer Placer
	}{
		{"ConsistentHash", &ConsistentHash{VNodes: 150}},
		{"Rendezvous", &Rendezvous{}},
		{"Weighted", &Weighted{}},
	}
}

// TestPlace_ReturnsN проверяет что каждый placer возвращает ровно N уникальных узлов.
func TestPlace_ReturnsN(t *testing.T) {
	for _, p := range placers() {
		for _, n := range []int{1, 2, 3, 5} {
			result := p.placer.Place("key/0", testNodes, n)
			if len(result) != n {
				t.Errorf("%s: Place returned %d nodes, want %d", p.name, len(result), n)
			}
			seen := map[string]bool{}
			for _, nd := range result {
				if seen[nd.ID] {
					t.Errorf("%s: duplicate node %s in result", p.name, nd.ID)
				}
				seen[nd.ID] = true
			}
		}
	}
}

// TestPlace_Deterministic проверяет детерминированность при одинаковых входных данных.
func TestPlace_Deterministic(t *testing.T) {
	for _, p := range placers() {
		r1 := p.placer.Place("key/42", testNodes, 3)
		r2 := p.placer.Place("key/42", testNodes, 3)
		for i := range r1 {
			if r1[i].ID != r2[i].ID {
				t.Errorf("%s: not deterministic: run1[%d]=%s run2[%d]=%s", p.name, i, r1[i].ID, i, r2[i].ID)
			}
		}
	}
}

// TestPlace_FewerNodesThanN проверяет поведение когда узлов меньше чем N.
func TestPlace_FewerNodesThanN(t *testing.T) {
	twoNodes := testNodes[:2]
	for _, p := range placers() {
		result := p.placer.Place("key/0", twoNodes, 5)
		if len(result) != 2 {
			t.Errorf("%s: expected 2 (capped), got %d", p.name, len(result))
		}
	}
}

// TestRendezvous_MinimalMigration проверяет что при добавлении узла перемещается ≈1/N фрагментов.
func TestRendezvous_MinimalMigration(t *testing.T) {
	r := &Rendezvous{}
	N := 5
	newNode := Node{ID: "n6", FreeBytes: 80 * 1024 * 1024 * 1024}

	total := 1000
	moved := 0
	for i := 0; i < total; i++ {
		fid := fmt.Sprintf("key/%d", i)
		before := r.Place(fid, testNodes, 1)[0].ID
		after := r.Place(fid, append(testNodes, newNode), 1)[0].ID
		if before != after {
			moved++
		}
	}

	// ожидаем ~1/6 ≈ 16.7% перемещений (±5%)
	expected := float64(total) / float64(N+1)
	pct := float64(moved) / float64(total) * 100
	t.Logf("Rendezvous: moved %d/%d fragments (%.1f%%), expected ~%.1f%%", moved, total, pct, expected/float64(total)*100)

	if math.Abs(float64(moved)-expected) > float64(total)*0.05 {
		t.Errorf("migration rate %.1f%% too far from expected %.1f%%", pct, expected/float64(total)*100)
	}
}

// TestWeighted_PrefersHighFreeSpace проверяет что Weighted чаще выбирает узлы с большим free space.
func TestWeighted_PrefersHighFreeSpace(t *testing.T) {
	w := &Weighted{}
	counts := map[string]int{}
	total := 2000
	for i := 0; i < total; i++ {
		fid := fmt.Sprintf("key/%d", i)
		result := w.Place(fid, testNodes, 1)
		counts[result[0].ID]++
	}
	// n1 (100GB free) должен получать больше фрагментов чем n5 (20GB free)
	if counts["n1"] <= counts["n5"] {
		t.Errorf("Weighted: n1 (100GB free) got %d fragments, n5 (20GB free) got %d — expected n1 > n5",
			counts["n1"], counts["n5"])
	}
	t.Logf("Weighted distribution: %v", counts)
}

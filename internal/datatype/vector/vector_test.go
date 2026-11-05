package vector

import "testing"

func TestSearch(t *testing.T) {
	x := New()
	x.Add("east", []float32{1, 0}, `{"kind":"dir"}`)
	x.Add("north", []float32{0, 1}, `{"kind":"dir"}`)
	x.Add("northeast", []float32{1, 1}, `{"kind":"mix"}`)
	if _, err := x.Add("bad", []float32{1, 2, 3}, ""); err == nil {
		t.Fatal("dimension mismatch accepted")
	}
	res, err := x.Search([]float32{2, 0.1}, 2, nil)
	if err != nil || len(res) != 2 || res[0].ID != "east" || res[1].ID != "northeast" {
		t.Fatalf("%v %v", res, err)
	}
	res, _ = x.Search([]float32{1, 0}, 5, func(id, attr string) bool { return attr == `{"kind":"mix"}` })
	if len(res) != 1 || res[0].ID != "northeast" {
		t.Fatal(res)
	}
	x.Remove("east")
	if _, _, ok := x.Get("east"); ok || x.Len() != 2 {
		t.Fatal("remove")
	}
	if v, _, ok := x.Get("north"); !ok || v[1] != 1 {
		t.Fatal("get after swap-remove")
	}
}

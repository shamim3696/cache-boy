package jsondoc

import "testing"

func TestPaths(t *testing.T) {
	d, err := Parse(`{"user":{"name":"ana","tags":["a","b"],"age":30}}`)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := d.Get("$.user.name"); len(v) != 1 || v[0] != "ana" {
		t.Fatal(v)
	}
	if v, _ := d.Get(`$["user"].tags[-1]`); v[0] != "b" {
		t.Fatal(v)
	}
	if v, _ := d.Get("user.tags[*]"); len(v) != 2 {
		t.Fatal(v)
	}
	if n, _ := d.Set("$.user.email", `"a@x.io"`); n != 1 {
		t.Fatal("create member")
	}
	if n, _ := d.Set("$.user.missing.deep", `1`); n != 0 {
		t.Fatal("must not create nested parents")
	}
	if out, _ := d.NumIncrBy("$.user.age", 1.5); out[0] != 31.5 {
		t.Fatal(out)
	}
	if lens, _ := d.ArrAppend("$.user.tags", `"c"`); lens[0] != 3 {
		t.Fatal(lens)
	}
	if n, _ := d.Del("$.user.tags[0]"); n != 1 {
		t.Fatal("del index")
	}
	want := `{"user":{"age":31.5,"email":"a@x.io","name":"ana","tags":["b","c"]}}`
	if d.String() != want {
		t.Fatalf("got %s", d.String())
	}
	if _, err := d.Get("$.a[x"); err == nil {
		t.Fatal("bad path accepted")
	}
	if _, err := Parse(`{"a":1} trailing`); err == nil {
		t.Fatal("trailing data accepted")
	}
}

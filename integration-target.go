package main

import (
	"fmt"
	"net/http"
	"strings"
)

func main() {
	http.HandleFunc("/metrics/baseline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "baseline_metric{index=\"%d\"} %d\n", i, i)
		}
	})

	http.HandleFunc("/metrics/medium", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		// ~20 KB of metrics
		var b strings.Builder
		for i := 0; i < 500; i++ {
			b.WriteString(fmt.Sprintf("medium_metric{index=\"%d\", label=\"some_meaningless_padding_to_make_this_larger_and_larger_and_larger\"} %d\n", i, i))
		}
		w.Write([]byte(b.String()))
	})

	http.HandleFunc("/metrics/massive", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		// ~5 MB of metrics
		var b strings.Builder
		for i := 0; i < 50000; i++ {
			b.WriteString(fmt.Sprintf("massive_metric{index=\"%d\", label=\"some_extremely_meaningless_padding_to_make_this_larger_and_larger_and_larger_and_larger_and_larger\"} %d\n", i, i))
		}
		w.Write([]byte(b.String()))
	})

	fmt.Println("Listening on :18080")
	if err := http.ListenAndServe(":18080", nil); err != nil {
		panic(err)
	}
}

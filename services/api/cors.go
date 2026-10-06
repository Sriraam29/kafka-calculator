package main

import "net/http"

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		if origin == "http://localhost:5500" ||
			origin == "http://127.0.0.1:5500" {

			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set(
				"Access-Control-Allow-Headers",
				"Content-Type, Authorization",
			)
			w.Header().Set(
				"Access-Control-Allow-Methods",
				"GET, POST, OPTIONS",
			)
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

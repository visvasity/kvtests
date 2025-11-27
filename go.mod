module github.com/visvasity/kvtests

go 1.23.2

toolchain go1.24.10

require (
	github.com/visvasity/kv v0.0.0-20251127181103-190fe23c8632
	github.com/visvasity/kvmemdb v0.0.0-20250824163232-8b180c2d78ef
	github.com/visvasity/kvpostgres v0.0.0-00010101000000-000000000000
)

require (
	github.com/lib/pq v1.10.9 // indirect
	github.com/visvasity/syncmap v0.0.0-20241218025521-5599e6c230a7 // indirect
)

replace github.com/visvasity/kvpostgres => ../kvpostgres

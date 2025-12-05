main_package_path = ./

.PHONY: help
help:
	@echo "Usage:"
	@echo -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e
	's/^/ /'

run:
	go run ${main_package_pah}
	@echo "Application started."

fmt:
	gofmt -s -w ${main_package_pah}

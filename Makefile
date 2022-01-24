

tests: styletests unittests inttests

styletests :
	./scripts/style-checks.sh

unittests :
	./scripts/unit-test.sh

inttests :
	./scripts/integration-test.sh

setpoetrypython :
	export PYSPARK_PYTHON="$(poetry env info -p)/bin/python"
	export PYSPARK_DRIVER_PYTHON="$(poetry env info -p)/bin/python"
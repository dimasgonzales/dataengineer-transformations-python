#!/bin/bash

set -e

if [ $(whoami) == hadoop ]; then
    unset SPARK_HOME
fi

poetry run pytest tests/integration
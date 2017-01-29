#!/bin/bash

spark-submit --master local[4] --conf spark.eventLog.enabled=true --class pl.edu.pw.elka.mbi.cli.VariantCaller target/variant-caller-1.0-SNAPSHOT.jar $@

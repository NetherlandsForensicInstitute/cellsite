#!/bin/sh

set -e

DATABASE=data
CELLSCANNER_DATA=$DATABASE/cellscanner

LIMIT=1000
MAX_DELAY=60


do_run() {
	DATADIR=$DATABASE/$1
	MEASUREMENTS_FILE=$DATADIR/measurements.csv

	CELLSCANNER_PAIRS=$CELLSCANNER_DATA/pairs_delay${MAX_DELAY}_limit${LIMIT}.db

	OUTDIR=$DATADIR/out
	PAIRS_FILE=$OUTDIR/pairs.db
	FEATURES_FILE=$OUTDIR/features.csv

	mkdir -p $OUTDIR

	CELLSCANNER_ARGS="--cellscanner-config cellsite.yaml --celldb-config cellsite.yaml --on-duplicate-cell take_first"

	if [ -e $CELLSCANNER_PAIRS ]; then
		echo "using existing cellscanner pairs from $CELLSCANNER_PAIRS"
	else
		echo "generating cellscanner pairs into $CELLSCANNER_PAIRS"
		time python -m cellscanner $CELLSCANNER_ARGS \
			 generate-cellscanner-pairs \
			--max-delay $MAX_DELAY \
			--limit-colocated $LIMIT \
			--limit-dislocated $LIMIT \
			--write-pairs $CELLSCANNER_PAIRS

		echo "evaluating vanilla cellscanner classifier"
		time python -m colocation evaluate_with_background --training-pairs $CELLSCANNER_PAIRS
	fi

	if [ -e $PAIRS_FILE ]; then
		echo "using existing pairs from $PAIRS_FILE"
	else
		echo "generating pairs from $MEASUREMENTS_FILE into $PAIRS_FILE"
		time python -m colocation pair-measurements --measurements-file $MEASUREMENTS_FILE --write-pairs $PAIRS_FILE
	fi

	EVALUATE_ARGS=
	EVALUATE_ARGS="$EVALUATE_ARGS --background-measurements $MEASUREMENTS_FILE"
	time python -m colocation evaluate-with-background \
		--training-pairs $CELLSCANNER_PAIRS \
		--test-pairs $PAIRS_FILE \
		--max-delay $MAX_DELAY \
		--write-results $FEATURES_FILE \
		--plot-pav $OUTDIR/pav.png \
		--plot-lr-histogram $OUTDIR/hist.png \
		$EVALUATE_ARGS
}

do_run $1

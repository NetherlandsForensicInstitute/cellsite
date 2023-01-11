colocation
==========

Package for estimating probabilities and probability ratios of registering
two cells from the same location.

Dependencies
------------

* Python3
* Optional: access to a cell database (see package: `celldb`)
* Optional: access to a postgres instance with pairs of cell measurements (see package:
  `cellscanner`)

Setup
-----

Make sure the Python active environment satisfies the requirements listed in
`requirements.in`.

Usage
-----

See command listing:
```commandline
python -m colocation --help
```

## Pairing measurements

The command `pair-measurements` takes cell measurements as input and combines them into pairs.
This generates both colocated and dislocated pairs. A pair consists of a `left` measurement and a
`right` measurement. See documentation of `MeasurementCombinations` on the formation of pairs.

## Pairing cellscanner measurements

The command `generate-cellscanner-pairs` takes cellscanner measurements as input and combines them
into pairs. See documentation of `CellscannerMeasurementPairGenerator` on the formation of pairs.


## Evaluate performance

Use `evaluate` to calculate LRs.

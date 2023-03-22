# Experiment directory overview

The result directory of this measurement should contain the following files:

   1. Files that should be present even if the measurement aborted:

      * `MANIFEST.md`           This manifest file.
      * `scorep.cfg`            Listing of used environment variables.

   2. Files that will be created by subsystems of the measurement core:

      * Tracing:

        * `traces.otf2`         OTF2 anchor file.
        * `traces.def`          OTF2 global definitions file.
        * `traces/`             Sub-directory containing per location trace
                                data.

# List of Score-P variables that were explicitly set for this measurement

The complete list of Score-P variables used, incl. current default values,
can be found in `scorep.cfg`.

    SCOREP_ENABLE_PROFILING
    SCOREP_ENABLE_TRACING
    SCOREP_METRIC_PAPI

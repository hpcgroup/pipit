from pipit import Trace
import pandas as pd
import pytest

def test_events(data_dir, nbody_nvtx):
    events_df = Trace.from_nsight(str(nbody_nvtx+'/trace.csv')).events
    
    # 262 events in nbody trace
    assert len(events_df) == 262
    
    # timestamp is increasing
    assert pd.Index(events_df['Timestamp (ns)']).is_monotonic_increasing == True
    
    # event types for trace 
    assert set(events_df['Event Type']) == set(['Enter', 'Leave'])
    
    # event names in trace
    assert set(events_df['Name']) == set(
        [
            'main',
            'cudaMallocManaged',
            'cudaMemPrefechAsync',
            'read_values_from_file',
            'computeNBody11',
            'bodyForce',
            'cudaGetLastError',
            'cudaDeviceSynchronize',
            'integratePositions',
            'computeNBody15',
            'write_values_to_file'
        ]
    )
    
    # single process, single thread
    assert len(events_df.loc[events_df['Name'] == 'main']) == 2
    
    assert events_df['PID'].equals(events_df['TID']) == True
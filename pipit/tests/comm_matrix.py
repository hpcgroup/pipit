from pipit import Trace


def test_comm_matrix(otf2_dir):
    bytes_comm_matrix = Trace.from_otf2(str(otf2_dir)).comm_matrix()
    counts_comm_matrix = Trace.from_otf2(str(otf2_dir)).comm_matrix("counts")

    assert(bytes_comm_matrix.shape == counts_comm_matrix.shape == (2,2))

    assert(bytes_comm_matrix[0][0] == bytes_comm_matrix[1][1] == 
           counts_comm_matrix[0][0] ==  counts_comm_matrix[1][1] == 0)

    assert(bytes_comm_matrix[0][1] == bytes_comm_matrix[1][0] == 4177920)
    assert(counts_comm_matrix[0][1] == counts_comm_matrix[1][0] == 8)
    
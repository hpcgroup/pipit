#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

int main(int argc, char *argv[])
{
	/* -------------------------------------------------------------------------------------------
		MPI Initialization 
	--------------------------------------------------------------------------------------------*/
	MPI_Init(&argc, &argv);

	int size;
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	MPI_Status stat;

	if(size != 2){
		if(rank == 0){
			printf("This program requires exactly 2 MPI ranks, but you are attempting to use %d! Exiting...\n", size);
		}
		MPI_Finalize();
		exit(0);
	}

	/* -------------------------------------------------------------------------------------------
		Loop from 8 B to 1 GB
	--------------------------------------------------------------------------------------------*/

	for(int i=11; i<=18; i++){

		long int N = 1 << i;
	
   	 	// Allocate memory for A on CPU
		double *A = (double*)malloc(N*sizeof(double));

		// Initialize all elements of A to 0.0
		for(int i=0; i<N; i++){
			A[i] = 0.0;
		}
	
		int tag1 = 10;
		int tag2 = 20;
	
		int loop_count = 50;

		// Time ping-pong for loop_count iterations of data transfer size 8*N bytes
		double start_time, stop_time, elapsed_time;
		start_time = MPI_Wtime();
	
		for(int i=1; i<=loop_count; i++){
			if(rank == 0){
				MPI_Send(A, N, MPI_DOUBLE, 1, tag1, MPI_COMM_WORLD);
				MPI_Recv(A, N, MPI_DOUBLE, 1, tag2, MPI_COMM_WORLD, &stat);
			}
			else if(rank == 1){
				MPI_Recv(A, N, MPI_DOUBLE, 0, tag1, MPI_COMM_WORLD, &stat);
				MPI_Send(A, N, MPI_DOUBLE, 0, tag2, MPI_COMM_WORLD);
			}
		}

		stop_time = MPI_Wtime();
		elapsed_time = stop_time - start_time;

		long int num_B = 8*N;
		long int B_in_GB = 1 << 30;
		double num_GB = (double)num_B / (double)B_in_GB;
		double avg_time_per_transfer = elapsed_time / (2.0*(double)loop_count);

		if(rank == 0) printf("Transfer size (B): %10li, Transfer Time (s): %15.9f, Bandwidth (GB/s): %15.9f\n", num_B, avg_time_per_transfer, num_GB/avg_time_per_transfer );

		free(A);
	}

	MPI_Finalize();

	return 0;
}

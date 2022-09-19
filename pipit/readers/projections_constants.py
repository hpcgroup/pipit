# Message Creation po
CREATION                 = 1;

BEGIN_PROCESSING         = 2;
END_PROCESSING           = 3;
ENQUEUE                  = 4;
DEQUEUE                  = 5;
BEGIN_COMPUTATION        = 6;
END_COMPUTATION          = 7;

BEGIN_INTERRUPT          = 8;
END_INTERRUPT            = 9;
MESSAGE_RECV             = 10;
BEGIN_TRACE              = 11;
END_TRACE                = 12;
USER_EVENT               = 13;
BEGIN_IDLE               = 14;
END_IDLE                 = 15;
BEGIN_PACK               = 16;
END_PACK                 = 17;
BEGIN_UNPACK             = 18;
END_UNPACK               = 19;
CREATION_BCAST           = 20;

CREATION_MULTICAST       = 21;

# A record for a user supplied integer value, likely a timestep 
USER_SUPPLIED            = 26;

# A record for the memory usage 
MEMORY_USAGE            = 27;

# A record for a user supplied string 
USER_SUPPLIED_NOTE            = 28;
USER_SUPPLIED_BRACKETED_NOTE            = 29;


BEGIN_USER_EVENT_PAIR    = 98;
END_USER_EVENT_PAIR      = 99;
USER_EVENT_PAIR          = 100;
USER_STAT 		 = 32;
# *** USER category *** 
NEW_CHARE_MSG            = 0;
#NEW_CHARE_NO_BALANCE_MSG = 1;
FOR_CHARE_MSG            = 2;
BOC_INIT_MSG             = 3;
#BOC_MSG                  = 4;
#TERMINATE_TO_ZERO        = 5;  # never used ??
#TERMINATE_SYS            = 6;  # never used ??
#INIT_COUNT_MSG           = 7;
#READ_VAR_MSG             = 8;
#READ_MSG_MSG             = 9;
#BROADCAST_BOC_MSG        = 10;
#DYNAMIC_BOC_INIT_MSG     = 11;

# *** IMMEDIATE category *** 
LDB_MSG                  = 12;
#VID_SEND_OVER_MSG        = 13;
QD_BOC_MSG               = 14;
QD_BROADCAST_BOC_MSG     = 15;
#IMM_BOC_MSG              = 16;
#IMM_BROADCAST_BOC_MSG    = 17;
#INIT_BARRIER_PHASE_1     = 18;
#INIT_BARRIER_PHASE_2     = 19;